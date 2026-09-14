package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/coder/websocket"
)

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		log.Fatal("usage: client <ws-url>")
	}

	fmt.Printf("Client connected: %s\n", os.Args[2])

	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, os.Args[1], &websocket.DialOptions{
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
			fmt.Printf("[%s] received: %s\n", msg, os.Args[2])
		}
	}()

	// Main goroutine reads from stdin and sends.
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			if err := c.Write(ctx, websocket.MessageText, []byte(line)); err != nil {
				log.Fatal(err)
				return
			}
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
