package main

import (
	"io"
	"net"
	"sync"
	"time"
)

// tcpProxy is owned by the experiment process and affects only its child
// services. Disabling it actively closes existing sessions and rejects new
// connections until restored; it does not stop the shared Redis/PostgreSQL.
type tcpProxy struct {
	listener    net.Listener
	target      string
	mu          sync.Mutex
	available   bool
	closed      bool
	connections map[net.Conn]struct{}
}

func newTCPProxy(target string) (*tcpProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &tcpProxy{listener: ln, target: target, available: true, connections: map[net.Conn]struct{}{}}
	go p.acceptLoop()
	return p, nil
}

func (p *tcpProxy) Addr() string { return p.listener.Addr().String() }

func (p *tcpProxy) SetAvailable(available bool) {
	p.mu.Lock()
	p.available = available
	if !available {
		for c := range p.connections {
			_ = c.Close()
		}
		p.connections = map[net.Conn]struct{}{}
	}
	p.mu.Unlock()
}

func (p *tcpProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.available = false
	for c := range p.connections {
		_ = c.Close()
	}
	p.connections = map[net.Conn]struct{}{}
	p.mu.Unlock()
	_ = p.listener.Close()
}

func (p *tcpProxy) acceptLoop() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		available := p.available && !p.closed
		p.mu.Unlock()
		if !available {
			_ = client.Close()
			continue
		}
		backend, err := net.DialTimeout("tcp", p.target, 2*time.Second)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		if !p.available || p.closed {
			p.mu.Unlock()
			_ = client.Close()
			_ = backend.Close()
			continue
		}
		p.connections[client] = struct{}{}
		p.connections[backend] = struct{}{}
		p.mu.Unlock()
		go p.bridge(client, backend)
	}
}

func (p *tcpProxy) bridge(client, backend net.Conn) {
	var once sync.Once
	closePair := func() {
		once.Do(func() {
			_ = client.Close()
			_ = backend.Close()
			p.mu.Lock()
			delete(p.connections, client)
			delete(p.connections, backend)
			p.mu.Unlock()
		})
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(backend, client); closePair() }()
	go func() { defer wg.Done(); _, _ = io.Copy(client, backend); closePair() }()
	wg.Wait()
}
