//go:build js && wasm

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall/js"
	"testing"
	"time"
)

func testNode(t *testing.T) *node {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{ctx: ctx, cancel: cancel, connections: make(map[net.Conn]struct{}), httpClient: &http.Client{}}
	t.Cleanup(func() { n.stop() })
	return n
}

func TestMoreThanFourConnectionsAndShutdown(t *testing.T) {
	t.Parallel()
	n := testNode(t)
	var remotes []net.Conn
	for i := 0; i < 8; i++ {
		conn, remote := net.Pipe()
		t.Cleanup(func() { conn.Close(); remote.Close() })
		if err := n.track(conn); err != nil {
			t.Fatalf("connection %d was rejected: %v", i+1, err)
		}
		remotes = append(remotes, remote)
	}
	if err := n.stop(); err != nil {
		t.Fatal(err)
	}
	for i, remote := range remotes {
		if _, err := remote.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			t.Errorf("connection %d remained open after stop: %v", i+1, err)
		}
	}
	conn, remote := net.Pipe()
	defer remote.Close()
	if err := n.track(conn); err == nil {
		t.Fatal("a stopped node accepted a connection")
	}
}

func TestRawSocketReadAndWriteSurviveFifteenSeconds(t *testing.T) {
	t.Parallel()
	n := testNode(t)
	ctx, cancel := context.WithTimeout(n.ctx, 25*time.Second)
	defer cancel()
	readConn, readRemote := net.Pipe()
	writeConn, writeRemote := net.Pipe()
	defer readRemote.Close()
	defer writeRemote.Close()
	readSocket, writeSocket := n.exportConn(readConn), n.exportConn(writeConn)
	defer readSocket.Call("close")
	defer writeSocket.Call("close")
	payload := []byte{0, 127, 128, 255}
	read := readSocket.Call("read")
	write := writeSocket.Call("write", jsBytes(payload))
	// Delay both ends beyond the old per-operation deadline.
	writeReceived := make(chan []byte, 1)
	timer := time.AfterFunc(16*time.Second, func() {
		go func() { readRemote.Write(payload) }()
		b := make([]byte, len(payload))
		if _, err := io.ReadFull(writeRemote, b); err != nil {
			writeReceived <- nil
			return
		}
		writeReceived <- b
	})
	defer timer.Stop()
	value, err := await(ctx, read)
	if err != nil {
		t.Fatalf("idle read failed: %v", err)
	}
	if !bytes.Equal(goBytes(value), payload) {
		t.Fatal("idle read corrupted binary data")
	}
	if _, err := await(ctx, write); err != nil {
		t.Fatalf("blocked write failed: %v", err)
	}
	select {
	case b := <-writeReceived:
		if !bytes.Equal(b, payload) {
			t.Fatal("blocked write corrupted binary data")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

type emptyFirstRead struct {
	net.Conn
	empty bool
}

func (c *emptyFirstRead) Read(b []byte) (int, error) {
	if !c.empty {
		c.empty = true
		return 0, nil
	}
	return c.Conn.Read(b)
}

func TestEmptyReadIsNotEOF(t *testing.T) {
	n := testNode(t)
	conn, remote := net.Pipe()
	defer remote.Close()
	socket := n.exportConn(&emptyFirstRead{Conn: conn})
	defer socket.Call("close")
	ctx, cancel := context.WithTimeout(n.ctx, time.Second)
	defer cancel()
	value, err := await(ctx, socket.Call("read"))
	if err != nil || value.IsNull() || value.Get("byteLength").Int() != 0 {
		t.Fatalf("an empty read was mistaken for EOF: %v, %v", value, err)
	}
	go func() { remote.Write([]byte("hello")); remote.Close() }()
	value, err = await(ctx, socket.Call("read"))
	if err != nil || string(goBytes(value)) != "hello" {
		t.Fatalf("read after empty read failed: %v", err)
	}
	value, err = await(ctx, socket.Call("read"))
	if err != nil || !value.IsNull() {
		t.Fatalf("actual EOF was lost: %v", err)
	}
}

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestHTTPBridgeBuffersLargeRequestAndResponseBodies(t *testing.T) {
	t.Parallel()
	n := testNode(t)
	payload := make([]byte, 3*1024*1024+5)
	for i := range payload {
		payload[i] = byte(i)
	}
	n.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		b, err := io.ReadAll(req.Body)
		if err != nil || !bytes.Equal(b, payload) {
			return nil, errors.New("large HTTP request was truncated")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	input := js.Global().Get("Object").New()
	input.Set("url", "http://peer.test/echo")
	input.Set("method", "POST")
	input.Set("body", jsBytes(payload))
	result, err := n.fetch(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(goBytes(result.(js.Value).Get("body")), payload) {
		t.Fatal("large HTTP response was truncated")
	}
	callback := js.FuncOf(func(_ js.Value, args []js.Value) any {
		body := args[0].Get("body")
		return promise(func() (any, error) {
			// A handler may also take longer than the old five-second deadline.
			time.Sleep(6 * time.Second)
			response := js.Global().Get("Object").New()
			response.Set("status", http.StatusOK)
			response.Set("body", body)
			return response, nil
		})
	})
	defer callback.Release()
	req := httptest.NewRequest("POST", "http://peer.test/echo", bytes.NewReader(payload))
	// No enrolled peer is needed for this local bridge test.
	req.RemoteAddr = "local-test"
	recorder := httptest.NewRecorder()
	n.httpServer(callback.Value).Handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK || !bytes.Equal(recorder.Body.Bytes(), payload) {
		t.Fatalf("large served HTTP body did not round-trip: status=%d bytes=%d", recorder.Code, recorder.Body.Len())
	}
}
