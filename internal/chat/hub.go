package chat

import (
	"context"

	"github.com/ashanniwantha/chat-odyssey/internal/chatpb"
	"github.com/ashanniwantha/chat-odyssey/internal/storage"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
)

type Hub struct {
	clients    map[*Client]struct{}
	register   chan *Client
	unregister chan *Client
	broadcast  chan BroadcastMessage
	history    *storage.RecentChatHistory
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan BroadcastMessage),
		history:    storage.NewRecentChatHistory(20),
	}
}

// only goroutine that touch h.clients
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for c := range h.clients {
				close(c.send)
				delete(h.clients, c)
				c.conn.Close(websocket.StatusGoingAway, "server shutting down")
			}
			return

		case c := <-h.register:
			h.clients[c] = struct{}{}

			// Show the message history
			pastMessages := h.history.GetAll()
			// Loop through the messages in chronological order
			for _, oldMsg := range pastMessages {
				c.send <- oldMsg
			}

		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}

		case msg := <-h.broadcast:
			outboundPayload := &chatpb.MessageBroadcast{
				SenderId:   msg.Sender.id,
				SenderName: msg.Sender.name,
				Content:    string(msg.Content),
			}

			formattedMsg, err := proto.Marshal(outboundPayload)
			if err != nil {
				continue // Skip bad payload
			}

			// Save the binary payload in history ring buffer
			h.history.Add(formattedMsg)

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
