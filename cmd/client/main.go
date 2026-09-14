package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/coder/websocket"
)

type WSMessage struct {
	SenderID string `json:"sender_id,omitempty"`
	Content  string `json:"content"`
}

func main() {
	log.SetFlags(0)

	if len(os.Args) < 3 {
		log.Fatal("usage: client <ws-url>")
	}

	url := os.Args[1]
	username := os.Args[2]

	fmt.Printf("Client connected: %s\n", username)

	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"echo"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.CloseNow()

	done := make(chan struct{})
	// One goroutite reads from the socket and prints
	go func() {
		defer close(done)
		for {
			_, msg, err := c.Read(ctx)
			if err != nil {
				log.Printf("read closed: %v", err)
				return
			}

			var inbound WSMessage
			if err := json.Unmarshal(msg, &inbound); err != nil {
				log.Printf("[%s] Raw output received: %s", username, string(msg))
				continue
			}

			fmt.Printf("\n[%s] %s: %s\n>", username, inbound.SenderID, inbound.Content)
		}
	}()

	// Main goroutine reads from stdin and sends.
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				fmt.Print("> ")
				continue
			}

			// Package the content to our payload structure
			payload := WSMessage{
				Content: line,
			}

			// Marshall the payload into binary JSON data
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				log.Printf("failed to marshal JSON payload: %v", err)
				continue
			}

			if err := c.Write(ctx, websocket.MessageText, jsonBytes); err != nil {
				log.Fatal(err)
				return
			}
			fmt.Print("> ")
		}

		// Check for scanning errors after terminating the loop
		if err := scanner.Err(); err != nil {
			log.Fatalf("error reading standard input: %v", err)
			return
		}
	}()

	<-done
	c.Close(websocket.StatusNormalClosure, "")
}
