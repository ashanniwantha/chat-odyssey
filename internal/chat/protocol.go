package chat

type WSMessage struct {
	SenderID string `json:"sender_id"`
	Content  string `json:"content"`
}

type BroadcastMessage struct {
	Sender  *Client
	Content []byte
}
