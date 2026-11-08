package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/coder/websocket"
	relayws "github.com/netbirdio/netbird/shared/relay/client/dialer/ws"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err := websocket.Dial(ctx, "wss://relay.invalid/relay", nil)
	if err == nil {
		os.Exit(1)
	}
	_, err = (relayws.Dialer{}).Dial(ctx, "rels://203.0.113.1:443", "relay.example")
	if err == nil {
		os.Exit(1)
	}
	fmt.Println("websocket error returned")
}
