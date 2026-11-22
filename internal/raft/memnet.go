package raft

import (
	"context"
	"errors"
	"sync"
)

// MemNetwork connects nodes inside one process. It exists so tests can cut a
// node off from the others and reconnect it on demand, which is how network
// partitions and crashes are simulated.
type MemNetwork struct {
	mu       sync.Mutex
	nodes    map[string]*Raft
	isolated map[string]bool
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{nodes: map[string]*Raft{}, isolated: map[string]bool{}}
}

var errUnreachable = errors.New("raft: peer unreachable")

// Register makes a node reachable under id (replacing any earlier node, as
// after a restart).
func (n *MemNetwork) Register(id string, r *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = r
}

// Isolate cuts a node off from everyone, in both directions.
func (n *MemNetwork) Isolate(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isolated[id] = true
}

// Heal reconnects a node.
func (n *MemNetwork) Heal(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.isolated, id)
}

func (n *MemNetwork) route(from, to string) (*Raft, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.isolated[from] || n.isolated[to] || n.nodes[to] == nil {
		return nil, errUnreachable
	}
	return n.nodes[to], nil
}

// Transport returns the network as seen from one node.
func (n *MemNetwork) Transport(self string) Transport { return &memTransport{n, self} }

type memTransport struct {
	net  *MemNetwork
	self string
}

func (t *memTransport) RequestVote(_ context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	target, err := t.net.route(t.self, peer)
	if err != nil {
		return nil, err
	}
	reply := target.HandleRequestVote(args)
	// The reply also has to make it back.
	if _, err := t.net.route(peer, t.self); err != nil || reply == nil {
		return nil, errUnreachable
	}
	return reply, nil
}

func (t *memTransport) AppendEntries(_ context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	target, err := t.net.route(t.self, peer)
	if err != nil {
		return nil, err
	}
	reply := target.HandleAppendEntries(args)
	if _, err := t.net.route(peer, t.self); err != nil || reply == nil {
		return nil, errUnreachable
	}
	return reply, nil
}
