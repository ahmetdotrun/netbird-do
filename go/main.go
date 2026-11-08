//go:build js && wasm

// The engine embeds NetBird with the sandbox adapters in patches/netbird.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall/js"
	"time"

	netbird "github.com/netbirdio/netbird/client/embed"
	"github.com/netbirdio/netbird/client/firewall/uspfilter/forwarder"
	nbssh "github.com/netbirdio/netbird/client/ssh"
	"github.com/netbirdio/netbird/client/system"
	management "github.com/netbirdio/netbird/shared/management/client"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func main() {
	api := js.Global().Get("Object").New()
	api.Set("generateKey", js.FuncOf(func(js.Value, []js.Value) any {
		key, err := wgtypes.GeneratePrivateKey()
		if err != nil {
			return js.Null()
		}
		return key.String()
	}))
	api.Set("newNode", js.FuncOf(func(_ js.Value, args []js.Value) any {
		cfg := args[0]
		return promise(func() (any, error) { return newNode(cfg) })
	}))
	api.Set("terminate", js.FuncOf(func(js.Value, []js.Value) any { os.Exit(0); return nil }))
	js.Global().Set("__netbirdDO", api)
	select {}
}

func promise(work func() (any, error)) js.Value {
	var executor js.Func
	executor = js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve, reject := args[0], args[1]
		go func() {
			value, err := work()
			if err != nil {
				reject.Invoke(js.Global().Get("Error").New(err.Error()))
				return
			}
			resolve.Invoke(value)
		}()
		executor.Release()
		return nil
	})
	return js.Global().Get("Promise").New(executor)
}

// await keeps callbacks alive until settlement, including after cancellation.
func await(ctx context.Context, value js.Value) (js.Value, error) {
	type outcome struct {
		value js.Value
		err   error
	}
	result := make(chan outcome, 1)
	var success, failure js.Func
	var once sync.Once
	finish := func(r outcome) { once.Do(func() { result <- r; success.Release(); failure.Release() }) }
	success = js.FuncOf(func(_ js.Value, args []js.Value) any { finish(outcome{value: args[0]}); return nil })
	failure = js.FuncOf(func(_ js.Value, _ []js.Value) any {
		finish(outcome{err: errors.New("JavaScript handler failed")})
		return nil
	})
	js.Global().Get("Promise").Call("resolve", value).Call("then", success, failure)
	select {
	case r := <-result:
		return r.value, r.err
	case <-ctx.Done():
		return js.Undefined(), ctx.Err()
	}
}

func jsBytes(b []byte) js.Value {
	value := js.Global().Get("Uint8Array").New(len(b))
	js.CopyBytesToJS(value, b)
	return value
}

func goBytes(value js.Value) []byte {
	if value.IsNull() || value.IsUndefined() {
		return nil
	}
	bytes := make([]byte, value.Get("byteLength").Int())
	js.CopyBytesToGo(bytes, value)
	return bytes
}

func jsonValue(value any) js.Value {
	b, _ := json.Marshal(value)
	return js.Global().Get("JSON").Call("parse", string(b))
}

func stringOption(cfg js.Value, name, fallback string) string {
	if value := cfg.Get(name); value.Type() == js.TypeString && value.String() != "" {
		return value.String()
	}
	return fallback
}

type node struct {
	client                            *netbird.Client
	ctx                               context.Context
	cancel                            context.CancelFunc
	started                           atomic.Bool
	mu                                sync.Mutex
	closed                            bool
	listeners                         map[uint16]net.Listener
	connections                       map[net.Conn]struct{}
	httpClient                        *http.Client
	privateKey                        wgtypes.Key
	hostname, managementURL, setupKey string
	host                              js.Value
	exitNode                          bool
}

func newNode(cfg js.Value) (any, error) {
	key, err := wgtypes.ParseKey(stringOption(cfg, "privateKey", ""))
	if err != nil {
		return nil, errors.New("invalid persisted device identity")
	}
	managementURL := stringOption(cfg, "managementURL", "https://api.netbird.io:443")
	u, err := url.Parse(managementURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("managementURL must be an HTTPS origin")
	}
	hostname := stringOption(cfg, "hostname", "netbird-do")
	buffers, batch := uint32(32), uint32(1)
	client, err := netbird.New(netbird.Options{
		PrivateKey: key.String(), ManagementURL: managementURL, DeviceName: hostname,
		LogOutput: io.Discard, LogLevel: "error",
		Performance:        netbird.Performance{PreallocatedBuffersPerPool: &buffers, MaxBatchSize: &batch},
		EnableServerRoutes: cfg.Get("exitNode").Truthy(),
	})
	if err != nil {
		return nil, fmt.Errorf("create NetBird client: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{client: client, ctx: ctx, cancel: cancel, privateKey: key,
		hostname: hostname, managementURL: managementURL, setupKey: stringOption(cfg, "setupKey", ""),
		listeners: make(map[uint16]net.Listener), connections: make(map[net.Conn]struct{}),
		host: cfg.Get("host"), exitNode: cfg.Get("exitNode").Truthy()}
	if n.exitNode {
		if n.host.IsNull() || n.host.IsUndefined() {
			cancel()
			return nil, errors.New("exit mode requires host socket adapters")
		}
		forwarder.SetHostDialContext(n.dialHost)
	}
	n.httpClient = client.NewHTTPClient()
	n.httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return n.export(), nil
}

// enroll persists no credentials remotely beyond the usual NetBird registration.
func (n *node) enroll(ctx context.Context) error {
	u, _ := url.Parse(n.managementURL)
	mgm, err := management.NewClient(ctx, u.Host, n.privateKey, true)
	if err != nil {
		return errors.New("management connection failed")
	}
	defer mgm.Close()
	cfg, err := n.client.GetConfig()
	if err != nil {
		return err
	}
	sshKey, err := nbssh.GeneratePublicKey([]byte(cfg.SSHKey))
	if err != nil {
		return errors.New("prepare peer metadata")
	}
	info := system.GetInfo(ctx)
	info.Hostname = n.hostname
	info.SetFlags(cfg.RosenpassEnabled, cfg.RosenpassPermissive, cfg.ServerSSHAllowed,
		cfg.DisableClientRoutes, cfg.DisableServerRoutes, cfg.DisableDNS, cfg.DisableFirewall,
		cfg.BlockLANAccess, cfg.BlockInbound, cfg.DisableIPv6, cfg.SyncMessageVersion,
		cfg.EnableSSHRoot, cfg.EnableSSHSFTP, cfg.EnableSSHLocalPortForwarding,
		cfg.EnableSSHRemotePortForwarding, cfg.DisableSSHAuth, cfg.RemoteJobsAllowed)
	if _, err := mgm.Login(info, sshKey, cfg.DNSLabels); err == nil {
		return nil
	} else if grpcstatus.Code(err) != codes.PermissionDenied {
		return errors.New("management login failed")
	}
	if n.setupKey == "" {
		return errors.New("device is not registered; a NetBird setup key is required")
	}
	if _, err := mgm.Register(n.setupKey, "", info, sshKey, cfg.DNSLabels); err != nil {
		message := grpcstatus.Convert(err).Message()
		for _, secret := range []string{n.setupKey, strings.ToLower(n.setupKey), n.privateKey.String()} {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
		if len(message) > 300 {
			message = message[:300]
		}
		return fmt.Errorf("NetBird registration rejected (%s): %s", grpcstatus.Code(err), message)
	}
	return nil
}

func (n *node) export() js.Value {
	obj := js.Global().Get("Object").New()
	obj.Set("start", js.FuncOf(func(js.Value, []js.Value) any {
		return promise(func() (any, error) {
			ctx, cancel := context.WithTimeout(n.ctx, 25*time.Second)
			defer cancel()
			if err := n.enroll(ctx); err != nil {
				return nil, err
			}
			if err := n.client.Start(ctx); err != nil {
				return nil, errors.New("NetBird startup failed")
			}
			n.started.Store(true)
			n.setupKey = ""
			return n.status(), nil
		})
	}))
	obj.Set("status", js.FuncOf(func(js.Value, []js.Value) any {
		return promise(func() (any, error) { return n.status(), nil })
	}))
	obj.Set("stop", js.FuncOf(func(js.Value, []js.Value) any {
		return promise(func() (any, error) { return nil, n.stop() })
	}))
	obj.Set("dial", js.FuncOf(func(_ js.Value, args []js.Value) any {
		address := args[0].String()
		return promise(func() (any, error) {
			ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
			defer cancel()
			conn, err := n.client.Dial(ctx, "tcp", address)
			if err != nil {
				return nil, errors.New("NetBird TCP connection failed")
			}
			if err := n.track(conn); err != nil {
				return nil, err
			}
			return n.exportConn(conn), nil
		})
	}))
	obj.Set("fetch", js.FuncOf(func(_ js.Value, args []js.Value) any {
		req := args[0]
		return promise(func() (any, error) { return n.fetch(req) })
	}))
	obj.Set("listen", js.FuncOf(func(_ js.Value, args []js.Value) any {
		port, cb := args[0].Int(), args[1]
		return promise(func() (any, error) {
			listener, err := n.listen(port)
			if err != nil {
				return nil, err
			}
			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					if err := n.track(conn); err != nil {
						continue
					}
					cb.Invoke(n.exportConn(conn), n.whois(conn.RemoteAddr()))
				}
			}()
			return nil, nil
		})
	}))
	obj.Set("serveHTTP", js.FuncOf(func(_ js.Value, args []js.Value) any {
		port, cb := args[0].Int(), args[1]
		return promise(func() (any, error) { return nil, n.serveHTTP(port, cb) })
	}))
	obj.Set("unlisten", js.FuncOf(func(_ js.Value, args []js.Value) any {
		port := uint16(args[0].Int())
		return promise(func() (any, error) {
			n.mu.Lock()
			listener := n.listeners[port]
			delete(n.listeners, port)
			n.mu.Unlock()
			if listener != nil {
				return nil, listener.Close()
			}
			return nil, nil
		})
	}))
	obj.Set("whois", js.FuncOf(func(_ js.Value, args []js.Value) any {
		return n.whois(stringAddr(args[0].String()))
	}))
	return obj
}

func (n *node) track(conn net.Conn) error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		conn.Close()
		return errors.New("node stopped")
	}
	n.connections[conn] = struct{}{}
	n.mu.Unlock()
	return nil
}

func (n *node) closeConn(conn net.Conn) {
	conn.Close()
	n.mu.Lock()
	delete(n.connections, conn)
	n.mu.Unlock()
}

func (n *node) exportConn(conn net.Conn) js.Value {
	obj := js.Global().Get("Object").New()
	var readMu sync.Mutex
	var closeOnce sync.Once
	var funcs []js.Func
	add := func(name string, cb func(js.Value, []js.Value) any) {
		fn := js.FuncOf(cb)
		funcs = append(funcs, fn)
		obj.Set(name, fn)
	}
	add("read", func(js.Value, []js.Value) any {
		return promise(func() (any, error) {
			readMu.Lock()
			defer readMu.Unlock()
			buf := make([]byte, 32<<10)
			count, err := conn.Read(buf)
			if count > 0 {
				return jsBytes(buf[:count]), nil
			}
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return js.Null(), nil
			}
			if err == nil {
				return jsBytes(nil), nil
			}
			return nil, err
		})
	})
	add("write", func(_ js.Value, args []js.Value) any {
		b := goBytes(args[0])
		return promise(func() (any, error) {
			_, err := conn.Write(b)
			return nil, err
		})
	})
	add("closeWrite", func(js.Value, []js.Value) any {
		return promise(func() (any, error) {
			if half, ok := conn.(interface{ CloseWrite() error }); ok {
				return nil, half.CloseWrite()
			}
			return nil, nil
		})
	})
	add("close", func(js.Value, []js.Value) any {
		closeOnce.Do(func() {
			n.closeConn(conn)
			for _, fn := range funcs {
				fn.Release()
			}
		})
		return nil
	})
	obj.Set("localAddress", conn.LocalAddr().String())
	obj.Set("remoteAddress", conn.RemoteAddr().String())
	return obj
}

func (n *node) listen(port int) (net.Listener, error) {
	if port < 1 || port > 65535 {
		return nil, errors.New("invalid TCP port")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, errors.New("node stopped")
	}
	if _, exists := n.listeners[uint16(port)]; exists {
		return nil, errors.New("port already listening")
	}
	listener, err := n.client.ListenTCP(net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		return nil, errors.New("NetBird listen failed")
	}
	n.listeners[uint16(port)] = listener
	return listener, nil
}

func headersToJS(header http.Header) js.Value {
	values := js.Global().Get("Array").New()
	for key, entries := range header {
		for _, value := range entries {
			values.Call("push", js.ValueOf([]any{key, value}))
		}
	}
	return values
}

func headersFromJS(header http.Header, values js.Value) {
	if values.IsNull() || values.IsUndefined() {
		return
	}
	for i := 0; i < values.Length(); i++ {
		pair := values.Index(i)
		header.Add(pair.Index(0).String(), pair.Index(1).String())
	}
}

func (n *node) fetch(input js.Value) (any, error) {
	body := goBytes(input.Get("body"))
	ctx, cancel := context.WithTimeout(n.ctx, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, input.Get("method").String(), input.Get("url").String(), bytes.NewReader(body))
	if err != nil || (req.URL.Scheme != "http" && req.URL.Scheme != "https") {
		return nil, errors.New("invalid HTTP URL")
	}
	headersFromJS(req.Header, input.Get("headers"))
	response, err := n.httpClient.Do(req)
	if err != nil {
		return nil, errors.New("NetBird HTTP request failed")
	}
	defer response.Body.Close()
	b, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	result := js.Global().Get("Object").New()
	result.Set("status", response.StatusCode)
	result.Set("headers", headersToJS(response.Header))
	result.Set("body", jsBytes(b))
	return result, nil
}

func (n *node) serveHTTP(port int, cb js.Value) error {
	listener, err := n.listen(port)
	if err != nil {
		return err
	}
	server := n.httpServer(cb)
	go func() { _ = server.Serve(listener) }()
	return nil
}

func (n *node) httpServer(cb js.Value) *http.Server {
	return &http.Server{
		BaseContext: func(net.Listener) context.Context { return n.ctx },
		ConnState: func(conn net.Conn, state http.ConnState) {
			if state == http.StateNew {
				if err := n.track(conn); err != nil {
					conn.Close()
				}
			}
			if state == http.StateClosed || state == http.StateHijacked {
				n.closeConn(conn)
			}
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				http.Error(w, "request body could not be read", http.StatusBadRequest)
				return
			}
			input := js.Global().Get("Object").New()
			input.Set("method", req.Method)
			input.Set("url", "http://"+req.Host+req.URL.RequestURI())
			input.Set("headers", headersToJS(req.Header))
			input.Set("body", jsBytes(body))
			response, err := await(req.Context(), cb.Invoke(input, n.whois(stringAddr(req.RemoteAddr))))
			if err != nil {
				http.Error(w, "handler failed", http.StatusBadGateway)
				return
			}
			b := goBytes(response.Get("body"))
			headersFromJS(w.Header(), response.Get("headers"))
			w.WriteHeader(response.Get("status").Int())
			w.Write(b)
		}),
	}
}

type stringAddr string

func (stringAddr) Network() string  { return "tcp" }
func (a stringAddr) String() string { return string(a) }

func (n *node) whois(addr net.Addr) js.Value {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return js.Null()
	}
	key, name, ok := n.client.IdentityForIP(ip.Unmap())
	if !ok {
		return js.Null()
	}
	return jsonValue(map[string]any{"publicKey": key, "fqdn": name, "address": addr.String()})
}

func (n *node) status() js.Value {
	state := "stopped"
	if n.started.Load() {
		state = "running"
	}
	result := map[string]any{"state": state, "publicKey": n.privateKey.PublicKey().String(), "exitNodeEnabled": n.exitNode}
	n.mu.Lock()
	result["activeConnections"] = len(n.connections)
	n.mu.Unlock()
	info := system.GetInfo(n.ctx)
	result["platform"] = map[string]any{"os": info.OS, "goOS": info.GoOS, "kernelVersion": info.KernelVersion}
	if n.started.Load() {
		if status, err := n.client.Status(); err == nil {
			result["address"] = status.LocalPeerState.IP
			result["fqdn"] = strings.TrimSuffix(status.LocalPeerState.FQDN, ".")
			result["managementConnected"] = status.ManagementState.Connected
			result["signalConnected"] = status.SignalState.Connected
			peers := make([]map[string]any, 0, len(status.Peers))
			for _, peer := range status.Peers {
				peers = append(peers, map[string]any{"address": peer.IP, "fqdn": peer.FQDN, "connected": peer.ConnStatus == netbird.PeerStatusConnected, "relayed": peer.Relayed})
			}
			result["peers"] = peers
			routes := make([]string, 0, len(status.LocalPeerState.Routes))
			for route := range status.LocalPeerState.Routes {
				routes = append(routes, route)
			}
			result["routes"] = routes
		}
	}
	return jsonValue(result)
}

func (n *node) stop() error {
	n.cancel()
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	listeners, conns := n.listeners, n.connections
	n.listeners = make(map[uint16]net.Listener)
	n.connections = make(map[net.Conn]struct{})
	n.mu.Unlock()
	for _, listener := range listeners {
		listener.Close()
	}
	for conn := range conns {
		conn.Close()
	}
	n.httpClient.CloseIdleConnections()
	if n.started.Swap(false) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return n.client.Stop(ctx)
	}
	return nil
}
