package chat

import "github.com/coder/websocket"

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
