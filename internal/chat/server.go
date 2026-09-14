package chat

import (
	"io"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"
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
	}
	defer c.CloseNow()

	if c.Subprotocol() != "echo" {
		c.Close(websocket.StatusPolicyViolation, "client must speak echo subprotocol")
	}

	cl := NewClient(c)
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

		bMsg := BroadcastMessage{
			Sender:  cl,
			Content: msg,
		}

		s.hub.Broadcast(bMsg)
	}
}
