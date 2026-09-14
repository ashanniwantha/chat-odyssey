package chat

import (
	"github.com/coder/websocket"
)

type Hub struct {
	clients    map[*Client]struct{}
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte),
	}
}

// only goroutine that touch h.clients
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = struct{}{}
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
		case msg := <-h.broadcast:
			for c := range h.clients {
				select {
				case c.send <- msg:
				default:
					// send buffer full -> client is slow
					close(c.send)
					delete(h.clients, c)
					c.conn.Close(websocket.StatusPolicyViolation, "too slow")
				}
			}
		}
	}
}

func (h *Hub) Register(c *Client)   { h.register <- c }
func (h *Hub) Unregister(c *Client) { h.unregister <- c }
func (h *Hub) Broadcast(msg []byte) { h.broadcast <- msg }
