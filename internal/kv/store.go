// Package kv is a key-value store replicated with Raft.
//
// The pattern is called a replicated state machine. The map itself is plain
// and single-threaded. What makes it fault tolerant is that no node changes
// its map directly: every operation is first agreed on through the Raft log,
// and each node applies the committed log in order. Same log, same order,
// same map on every node.
package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/iotaaaaaaaaa/raftkv/internal/raft"
)

// Op is one command in the replicated log.
type Op struct {
	Kind  string `json:"kind"` // "put", "get" or "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type Result struct {
	Value string
	Found bool
}

// NotLeaderError tells the client which node to retry on, when known.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "not the leader; no leader known yet"
	}
	return "not the leader; try " + e.Leader
}

// ErrLeadershipLost means this node stopped being leader before the command
// committed. The command may or may not take effect; the client should retry.
var ErrLeadershipLost = errors.New("leadership lost before the command committed")

type outcome struct {
	res Result
	err error
}

type waiter struct {
	term uint64
	ch   chan outcome
}

type Store struct {
	rf *raft.Raft

	mu      sync.Mutex
	data    map[string]string
	waiters map[uint64]waiter // log index -> client waiting for that entry
}

// New starts applying rf's committed entries to a fresh map.
func New(rf *raft.Raft) *Store {
	s := &Store{rf: rf, data: map[string]string{}, waiters: map[uint64]waiter{}}
	go s.applyLoop()
	return s
}

// Do runs op through the Raft log and returns once it has been committed and
// applied on this node.
//
// Reads go through the log too. That costs a round of replication per read,
// but it means a read can never return stale data from a node that has been
// deposed and does not know it yet (linearizable reads).
func (s *Store) Do(ctx context.Context, op Op) (Result, error) {
	cmd, err := json.Marshal(op)
	if err != nil {
		return Result{}, err
	}

	// The lock is held from Propose until the waiter is registered, so the
	// apply loop cannot process this entry before we are listening for it.
	s.mu.Lock()
	index, term, isLeader := s.rf.Propose(cmd)
	if !isLeader {
		s.mu.Unlock()
		return Result{}, &NotLeaderError{Leader: s.rf.Status().LeaderID}
	}
	if old, ok := s.waiters[index]; ok {
		// Someone was waiting on this index from an earlier term in which we
		// were leader; their entry has been overwritten.
		old.ch <- outcome{err: ErrLeadershipLost}
	}
	ch := make(chan outcome, 1)
	s.waiters[index] = waiter{term: term, ch: ch}
	s.mu.Unlock()

	select {
	case out := <-ch:
		return out.res, out.err
	case <-ctx.Done():
		s.mu.Lock()
		if s.waiters[index].ch == ch {
			delete(s.waiters, index)
		}
		s.mu.Unlock()
		return Result{}, fmt.Errorf("timed out waiting for commit: %w", ctx.Err())
	}
}

func (s *Store) applyLoop() {
	for msg := range s.rf.ApplyCh() {
		s.mu.Lock()
		var res Result
		if len(msg.Cmd) > 0 { // empty = a leader's no-op entry
			var op Op
			if err := json.Unmarshal(msg.Cmd, &op); err == nil {
				res = s.apply(op)
			}
		}
		if w, ok := s.waiters[msg.Index]; ok {
			delete(s.waiters, msg.Index)
			if w.term == msg.Term {
				w.ch <- outcome{res: res}
			} else {
				// A different leader's entry landed at the index we proposed.
				w.ch <- outcome{err: ErrLeadershipLost}
			}
		}
		s.mu.Unlock()
	}
}

// apply must be deterministic: it is the only code that touches the map, and
// every node runs it on the same sequence of ops.
func (s *Store) apply(op Op) Result {
	switch op.Kind {
	case "put":
		s.data[op.Key] = op.Value
		return Result{Value: op.Value, Found: true}
	case "delete":
		_, found := s.data[op.Key]
		delete(s.data, op.Key)
		return Result{Found: found}
	case "get":
		v, found := s.data[op.Key]
		return Result{Value: v, Found: found}
	}
	return Result{}
}

// Snapshot copies this node's local map without going through Raft. It may
// be behind the leader. It is meant for tests and debugging.
func (s *Store) Snapshot() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
