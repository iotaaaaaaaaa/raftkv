package raft

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// TCPTransport sends Raft RPCs over TCP using Go's net/rpc. It keeps one
// connection per peer and reconnects after any failure.
type TCPTransport struct {
	mu      sync.Mutex
	addrs   map[string]string // peer id -> host:port
	clients map[string]*rpc.Client
}

func NewTCPTransport(addrs map[string]string) *TCPTransport {
	return &TCPTransport{addrs: addrs, clients: map[string]*rpc.Client{}}
}

func (t *TCPTransport) client(peer string) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.clients[peer]; c != nil {
		return c, nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, errors.New("raft: unknown peer " + peer)
	}
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return nil, err
	}
	c := rpc.NewClient(conn)
	t.clients[peer] = c
	return c, nil
}

func (t *TCPTransport) drop(peer string, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[peer] == c {
		delete(t.clients, peer)
	}
	t.mu.Unlock()
	c.Close()
}

func (t *TCPTransport) call(ctx context.Context, peer, method string, args, reply any) error {
	c, err := t.client(peer)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			t.drop(peer, c)
		}
		return call.Error
	case <-ctx.Done():
		t.drop(peer, c) // the connection may be stuck; start fresh next time
		return ctx.Err()
	}
}

func (t *TCPTransport) RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := new(RequestVoteReply)
	if err := t.call(ctx, peer, "Raft.RequestVote", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (t *TCPTransport) AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := new(AppendEntriesReply)
	if err := t.call(ctx, peer, "Raft.AppendEntries", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// Close drops all cached connections.
func (t *TCPTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.clients {
		c.Close()
		delete(t.clients, id)
	}
}

var errStopped = errors.New("raft: node stopped")

// rpcService adapts a Raft node to the method shape net/rpc requires.
type rpcService struct{ r *Raft }

func (s *rpcService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	out := s.r.HandleRequestVote(args)
	if out == nil {
		return errStopped
	}
	*reply = *out
	return nil
}

func (s *rpcService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	out := s.r.HandleAppendEntries(args)
	if out == nil {
		return errStopped
	}
	*reply = *out
	return nil
}

// ServeTCP answers Raft RPCs for r on ln until ln is closed.
func ServeTCP(ln net.Listener, r *Raft) {
	srv := rpc.NewServer()
	srv.RegisterName("Raft", &rpcService{r})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()
}
