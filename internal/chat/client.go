package chat

import (
	"context"

	"github.com/coder/websocket"
)

type Client struct {
	conn *websocket.Conn
	id   string
	name string
	send chan []byte
}

func NewClient(c *websocket.Conn, id string, name string) *Client {
	return &Client{
		conn: c,
		id:   id,
		name: name,
		send: make(chan []byte, 32),
	}
}

func (c *Client) Send(msg []byte) bool {
	select {
	case c.send <- msg:
		return true
	default:
		return false // buffer full, client slow
	}
}

func (c *Client) WriteLoop(ctx context.Context) {
	for msg := range c.send {
		if err := c.conn.Write(ctx, websocket.MessageText, msg); err != nil {
			c.conn.Close(websocket.StatusInternalError, "write failed")
		}
	}
	c.conn.Close(websocket.StatusNormalClosure, "")
}
