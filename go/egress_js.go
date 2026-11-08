//go:build js && wasm

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"syscall/js"
	"time"
)

func (n *node) dialHost(ctx context.Context, network, address string) (net.Conn, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return nil, err
	}
	if network == "udp" {
		if port != 53 {
			return nil, errors.New("Workers exit mode supports UDP DNS only")
		}
		ctx, cancel := context.WithCancel(ctx)
		conn := &dohConn{ctx: ctx, cancel: cancel, host: n.host, responses: make(chan []byte, 4), remote: stringAddr(address)}
		conn.onClose = func() { n.mu.Lock(); delete(n.connections, conn); n.mu.Unlock() }
		if err := n.track(conn); err != nil {
			return nil, err
		}
		return conn, nil
	}
	if network != "tcp" {
		return nil, errors.New("unsupported exit transport")
	}
	sock, err := await(ctx, n.host.Call("dialTCP", host, port))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	conn := &hostConn{ctx: ctx, cancel: cancel, sock: sock, remote: stringAddr(address)}
	conn.onClose = func() { n.mu.Lock(); delete(n.connections, conn); n.mu.Unlock() }
	if err := n.track(conn); err != nil {
		return nil, err
	}
	return conn, nil
}

type hostConn struct {
	ctx             context.Context
	cancel          context.CancelFunc
	sock            js.Value
	remote          net.Addr
	readMu, writeMu sync.Mutex
	pending         []byte
	once            sync.Once
	onClose         func()
}

func (c *hostConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for len(c.pending) == 0 {
		value, err := await(c.ctx, c.sock.Call("read"))
		if err != nil {
			return 0, err
		}
		if value.IsNull() || value.IsUndefined() {
			return 0, io.EOF
		}
		c.pending = goBytes(value)
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}
func (c *hostConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := await(c.ctx, c.sock.Call("write", jsBytes(p)))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
func (c *hostConn) Close() error {
	c.once.Do(func() {
		c.cancel()
		c.sock.Call("close")
		if c.onClose != nil {
			c.onClose()
		}
	})
	return nil
}
func (c *hostConn) CloseWrite() error                { _, err := await(c.ctx, c.sock.Call("closeWrite")); return err }
func (c *hostConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *hostConn) RemoteAddr() net.Addr             { return c.remote }
func (c *hostConn) SetDeadline(time.Time) error      { return nil }
func (c *hostConn) SetReadDeadline(time.Time) error  { return nil }
func (c *hostConn) SetWriteDeadline(time.Time) error { return nil }

// A connected UDP DNS flow carries datagrams through the host's DoH endpoint.
type dohConn struct {
	ctx       context.Context
	cancel    context.CancelFunc
	host      js.Value
	responses chan []byte
	remote    net.Addr
	once      sync.Once
	onClose   func()
}

func (c *dohConn) Read(p []byte) (int, error) {
	select {
	case b := <-c.responses:
		if len(b) > len(p) {
			return 0, io.ErrShortBuffer
		}
		return copy(p, b), nil
	case <-c.ctx.Done():
		return 0, net.ErrClosed
	}
}
func (c *dohConn) Write(p []byte) (int, error) {
	if len(p) > 4096 {
		return 0, errors.New("DNS query too large")
	}
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	value, err := await(ctx, c.host.Call("dnsQuery", jsBytes(p)))
	if err != nil {
		return 0, err
	}
	response := goBytes(value)
	if len(response) > 4096 {
		return 0, errors.New("DNS response too large")
	}
	select {
	case c.responses <- response:
		return len(p), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
func (c *dohConn) Close() error {
	c.once.Do(func() {
		c.cancel()
		if c.onClose != nil {
			c.onClose()
		}
	})
	return nil
}
func (c *dohConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *dohConn) RemoteAddr() net.Addr             { return c.remote }
func (c *dohConn) SetDeadline(time.Time) error      { return nil }
func (c *dohConn) SetReadDeadline(time.Time) error  { return nil }
func (c *dohConn) SetWriteDeadline(time.Time) error { return nil }
