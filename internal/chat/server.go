package chat

import (
	"io"
	"net/http"
	"time"

	"github.com/ashanniwantha/chat-odyssey/internal/chatpb"
	"github.com/coder/websocket"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

type Server struct {
	hub  *Hub
	logf func(string, ...any)
}

func NewServer(hub *Hub, logf func(string, ...any)) *Server {
	return &Server{
		hub:  hub,
		logf: logf,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"echo"},
	})
	if err != nil {
		s.logf("failed to intialize websocket: %v", err)
		return
	}
	defer c.CloseNow()

	if c.Subprotocol() != "echo" {
		c.Close(websocket.StatusPolicyViolation, "client must speak echo subprotocol")
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		c.Close(websocket.StatusPolicyViolation, "name of the user must be included")
		return
	}

	cl := NewClient(c, name, uuid.NewString())
	s.hub.Register(cl)
	defer s.hub.Unregister(cl)

	// write goroutine: pulls from cl.send, writes to the websocket
	go cl.WriteLoop(r.Context())

	// read loop: read from the websocket and pushes to h.broadcast
	l := rate.NewLimiter(rate.Every(time.Millisecond*100), 10)
	for {
		if err := l.Wait(r.Context()); err != nil {
			return
		}

		_, rd, err := c.Reader(r.Context())
		if err != nil {
			s.logf("read failed: %v", err)
			return
		}

		msg, err := io.ReadAll(rd)
		if err != nil {
			s.logf("read body failed: %v", err)
			return
		}

		// Validation: Unmarshal the incoming structure to ensure it's valid JSON
		var incoming chatpb.MessageUpload
		if err := proto.Unmarshal(msg, &incoming); err != nil {
			s.logf("invalid Protobuf message received: %v", err)
			continue
		}

		bMsg := BroadcastMessage{
			Sender:  cl,
			Content: []byte(incoming.Content),
		}

		s.hub.Broadcast(bMsg)
	}
}
