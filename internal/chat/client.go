package chat

import (
	"context"

	"github.com/coder/websocket"
)

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

func NewClient(c *websocket.Conn) *Client {
	return &Client{
		conn: c,
		send: make(chan []byte),
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
