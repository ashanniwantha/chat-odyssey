package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"
)

type echoServer struct {
	logf    func(f string, v ...any)
	mu      sync.Mutex
	clients map[*websocket.Conn]struct{}
}

func (s echoServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"echo"},
	})
	if err != nil {
		s.logf("failed to intialize websocket: %v", err)
		return
	}
	defer c.CloseNow()

	if c.Subprotocol() != "echo" {
		c.Close(websocket.StatusPolicyViolation, "client must speak the echo subprotocol")
		return
	}

	s.mu.Lock()
	s.clients[c] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, c)
		s.mu.Unlock()
	}()

	l := rate.NewLimiter(rate.Every(time.Millisecond*100), 10)
	for {
		err := echo(c, l)
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return
		}
		if err != nil {
			s.logf("failed to echo with %v: %v", r.RemoteAddr, err)
			return
		}
	}

}

func echo(c *websocket.Conn, l *rate.Limiter) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	err := l.Wait(ctx)
	if err != nil {
		return err
	}

	typ, r, err := c.Reader(ctx)
	if err != nil {
		return err
	}

	w, err := c.Writer(ctx, typ)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, r)
	if err != nil {
		return fmt.Errorf("failed to io.Copy: %w", err)
	}

	err = w.Close()
	return err
}

func (s echoServer) broadcastLoop(ctx context.Context, c *websocket.Conn, l *rate.Limiter) error {
	for {
		if err := l.Wait(ctx); err != nil {
			return err
		}

		typ, r, err := c.Reader(ctx)
		if err != nil {
			return err
		}

		msg, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		s.broadcast(ctx, typ, msg)
	}
}

func (s echoServer) broadcast(ctx context.Context, typ websocket.MessageType, msg []byte) {
	s.mu.Lock()
	clients := make([]*websocket.Conn, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.Unlock()

	for _, client := range clients {
		if err := client.Write(ctx, typ, msg); err != nil {
			s.logf("broadcast write failed: %v", err)
			s.mu.Lock()
			delete(s.clients, client)
			s.mu.Unlock()
		}
	}
}
