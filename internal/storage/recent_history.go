package storage

import (
	"sync"

	"github.com/ashanniwantha/chat-odyssey/internal/chat"
)

type RecentChatHistory struct {
	mu      sync.Mutex
	message []chat.BroadcastMessage
	size    int
	head    int
	tail    int
	isFull  bool
}

func NewRecentChatHistory(capacity int) *RecentChatHistory {
	return &RecentChatHistory{
		message: make([]chat.BroadcastMessage, capacity),
		size:    capacity,
	}
}

// Add inserts a new message, override the oldest if the buffer is full
func (h *RecentChatHistory) Add(msg chat.BroadcastMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.message[h.tail] = msg
	h.tail = (h.tail + 1) % h.size

	if h.isFull {
		h.head = (h.head + 1) % h.size
	} else if h.head == h.tail {
		h.isFull = true
	}
}

// GetAll returns all messages in chronological order
func (h *RecentChatHistory) GetAll() []chat.BroadcastMessage {
	h.mu.Lock()
	defer h.mu.Unlock()

	var history []chat.BroadcastMessage
	if !h.isFull {
		history = append(history, h.message[:h.tail]...)
	} else {
		history = append(history, h.message[h.head:]...)
		history = append(history, h.message[:h.tail]...)
	}

	return history
}
