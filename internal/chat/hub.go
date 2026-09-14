package chat

import (
	"encoding/json"
	"fmt"

	"github.com/coder/websocket"
)

type BroadcastMessage struct {
	Sender  *Client
	Content []byte
}

type Hub struct {
	clients    map[*Client]struct{}
	register   chan *Client
	unregister chan *Client
	broadcast  chan BroadcastMessage
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan BroadcastMessage),
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
			outboundPayload := WSMessage{
				SenderID: fmt.Sprintf("%p", msg.Sender),
				Content:  string(msg.Content),
			}

			formattedMsg, err := json.Marshal(outboundPayload)
			if err != nil {
				continue // Skip bad payload
			}

			for c := range h.clients {
				// prevent the message being sent to the sender
				if c == msg.Sender {
					continue
				}

				select {
				case c.send <- formattedMsg:
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

func (h *Hub) Register(c *Client)             { h.register <- c }
func (h *Hub) Unregister(c *Client)           { h.unregister <- c }
func (h *Hub) Broadcast(msg BroadcastMessage) { h.broadcast <- msg }
