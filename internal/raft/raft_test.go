package raft

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// cluster is a set of in-process nodes on a MemNetwork, each with a recorder
// that collects what the node applies.
type cluster struct {
	t       *testing.T
	net     *MemNetwork
	ids     []string
	nodes   map[string]*Raft
	stores  map[string]*MemoryStorage
	mu      sync.Mutex
	applied map[string][]string // node -> commands applied, in order (no-ops skipped)
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{
		t: t, net: NewMemNetwork(),
		nodes: map[string]*Raft{}, stores: map[string]*MemoryStorage{}, applied: map[string][]string{},
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.stores[id] = NewMemoryStorage()
		c.start(id)
	}
	t.Cleanup(func() {
		for _, r := range c.nodes {
			r.Stop()
		}
	})
	return c
}

func (c *cluster) start(id string) {
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	r, err := New(Config{
		ID: id, Peers: peers,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  40 * time.Millisecond,
	}, c.net.Transport(id), c.stores[id])
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[id] = r
	c.applied[id] = nil // a restarted node replays its log from the start
	c.mu.Unlock()
	c.net.Register(id, r)

	go func() {
		for msg := range r.ApplyCh() {
			if len(msg.Cmd) == 0 {
				continue
			}
			c.mu.Lock()
			if c.nodes[id] == r { // ignore a node that has since been replaced
				c.applied[id] = append(c.applied[id], string(msg.Cmd))
			}
			c.mu.Unlock()
		}
	}()
}

// crash stops a node, keeping its storage.
func (c *cluster) crash(id string) {
	c.net.Isolate(id)
	c.nodes[id].Stop()
}

func (c *cluster) restart(id string) {
	c.start(id)
	c.net.Heal(id)
}

// leader waits until the given nodes agree on exactly one leader.
func (c *cluster) leader(among ...string) string {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []string
		var maxTerm uint64
		for _, id := range among {
			st := c.nodes[id].Status()
			if st.Term > maxTerm {
				maxTerm = st.Term
				leaders = nil
			}
			if st.State == Leader && st.Term == maxTerm {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			// Stable only if everyone else has caught up to that term.
			agreed := true
			for _, id := range among {
				st := c.nodes[id].Status()
				if st.Term != maxTerm || st.LeaderID != leaders[0] {
					agreed = false
				}
			}
			if agreed {
				return leaders[0]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single leader among %v", among)
	return ""
}

func (c *cluster) others(id string) []string {
	var out []string
	for _, p := range c.ids {
		if p != id {
			out = append(out, p)
		}
	}
	return out
}

func (c *cluster) appliedBy(id string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.applied[id]...)
}

// waitApplied waits until each listed node has applied exactly want.
func (c *cluster) waitApplied(want []string, ids ...string) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok := true
		for _, id := range ids {
			if !reflect.DeepEqual(c.appliedBy(id), want) {
				ok = false
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for _, id := range ids {
				c.t.Logf("%s applied %v", id, c.appliedBy(id))
			}
			c.t.Fatalf("nodes %v did not converge on %v", ids, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *cluster) propose(id, cmd string) {
	c.t.Helper()
	if _, _, ok := c.nodes[id].Propose([]byte(cmd)); !ok {
		c.t.Fatalf("%s rejected proposal %q: not leader", id, cmd)
	}
}

// ---------------------------------------------------------------------------

func TestElectsOneLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	term := c.nodes[leader].Status().Term

	// With a healthy leader sending heartbeats, nobody should start a new election.
	time.Sleep(600 * time.Millisecond)
	if got := c.leader(); got != leader || c.nodes[got].Status().Term != term {
		t.Fatalf("leadership changed without a failure: %s term %d -> %s term %d",
			leader, term, got, c.nodes[got].Status().Term)
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1)
	leader := c.leader()
	c.propose(leader, "a")
	c.propose(leader, "b")
	c.waitApplied([]string{"a", "b"}, leader)
}

func TestReplicatesToAllNodes(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	var want []string
	for i := 0; i < 30; i++ {
		cmd := fmt.Sprintf("cmd-%d", i)
		c.propose(leader, cmd)
		want = append(want, cmd)
	}
	c.waitApplied(want, c.ids...)
}

func TestFollowerRejectsProposals(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	for _, id := range c.others(leader) {
		if _, _, ok := c.nodes[id].Propose([]byte("x")); ok {
			t.Fatalf("follower %s accepted a proposal", id)
		}
	}
}

func TestNewLeaderAfterLeaderFails(t *testing.T) {
	c := newCluster(t, 3)
	old := c.leader()
	oldTerm := c.nodes[old].Status().Term
	c.propose(old, "before")
	c.waitApplied([]string{"before"}, c.ids...)

	c.net.Isolate(old)
	rest := c.others(old)
	next := c.leader(rest...)
	if c.nodes[next].Status().Term <= oldTerm {
		t.Fatal("new leader must have a higher term")
	}
	c.propose(next, "after")
	c.waitApplied([]string{"before", "after"}, rest...)

	// The old leader comes back, learns the new term and catches up.
	c.net.Heal(old)
	if got := c.leader(); got != next {
		t.Fatalf("leader changed to %s when the old leader rejoined; want %s", got, next)
	}
	c.waitApplied([]string{"before", "after"}, c.ids...)
}

// A leader cut off from the majority must not commit anything, and what it
// accepted while cut off must be discarded when it rejoins.
func TestIsolatedLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 3)
	old := c.leader()
	c.propose(old, "committed")
	c.waitApplied([]string{"committed"}, c.ids...)

	c.net.Isolate(old)
	c.propose(old, "lost-1") // still believes it is the leader
	c.propose(old, "lost-2")

	rest := c.others(old)
	next := c.leader(rest...)
	c.propose(next, "kept")
	c.waitApplied([]string{"committed", "kept"}, rest...)

	if got := c.appliedBy(old); !reflect.DeepEqual(got, []string{"committed"}) {
		t.Fatalf("isolated leader applied %v", got)
	}
	c.net.Heal(old)
	c.waitApplied([]string{"committed", "kept"}, c.ids...)
}

func TestNoProgressWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	for _, id := range c.others(leader) {
		c.net.Isolate(id)
	}
	c.propose(leader, "stuck")
	time.Sleep(500 * time.Millisecond)
	if got := c.appliedBy(leader); len(got) != 0 {
		t.Fatalf("applied %v with no majority", got)
	}
	for _, id := range c.ids {
		c.net.Heal(id)
	}
	// Once the partition heals, some leader emerges and all nodes agree. The
	// stuck entry may or may not survive, but every node must hold the same log.
	final := c.leader()
	c.propose(final, "done")
	deadline := time.Now().Add(5 * time.Second)
	for {
		a := c.appliedBy(c.ids[0])
		same := len(a) > 0 && a[len(a)-1] == "done"
		for _, id := range c.ids[1:] {
			same = same && reflect.DeepEqual(a, c.appliedBy(id))
		}
		if same {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("nodes did not converge after healing")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLaggingFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	slow := c.others(leader)[0]
	c.net.Isolate(slow)

	var want []string
	for i := 0; i < 1200; i++ { // more than two AppendEntries batches
		cmd := fmt.Sprintf("c%d", i)
		c.propose(leader, cmd)
		want = append(want, cmd)
	}
	c.waitApplied(want, leader)
	c.net.Heal(slow)
	c.waitApplied(want, c.ids...)
}

func TestStateSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader()
	want := []string{"a", "b", "c"}
	for _, cmd := range want {
		c.propose(leader, cmd)
	}
	c.waitApplied(want, c.ids...)

	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	leader = c.leader()
	c.propose(leader, "d")
	c.waitApplied(append(want, "d"), c.ids...)
}

// Random crashes and partitions while writing. At the end every node must
// hold the same log, and every command that any node applied must be in it.
func TestAgreementUnderChurn(t *testing.T) {
	c := newCluster(t, 5)
	rng := rand.New(rand.NewSource(7))
	seen := map[string]bool{} // commands observed as applied somewhere, i.e. committed
	n := 0

	for round := 0; round < 12; round++ {
		victim := c.ids[rng.Intn(len(c.ids))]
		crash := rng.Intn(2) == 0
		if crash {
			c.crash(victim)
		} else {
			c.net.Isolate(victim)
		}
		up := c.others(victim)
		leader := c.leader(up...)
		for i := 0; i < 5; i++ {
			n++
			// A proposal can fail if leadership moves; that is allowed.
			c.nodes[leader].Propose([]byte(fmt.Sprintf("w%d", n)))
		}
		time.Sleep(time.Duration(50+rng.Intn(150)) * time.Millisecond)
		for _, id := range up {
			for _, cmd := range c.appliedBy(id) {
				seen[cmd] = true
			}
		}
		if crash {
			c.restart(victim)
		} else {
			c.net.Heal(victim)
		}
	}

	leader := c.leader()
	c.propose(leader, "final")
	deadline := time.Now().Add(10 * time.Second)
	for {
		ref := c.appliedBy(c.ids[0])
		done := len(ref) > 0 && ref[len(ref)-1] == "final"
		for _, id := range c.ids[1:] {
			done = done && reflect.DeepEqual(ref, c.appliedBy(id))
		}
		if done {
			have := map[string]bool{}
			for _, cmd := range ref {
				if have[cmd] {
					t.Fatalf("command %s applied twice", cmd)
				}
				have[cmd] = true
			}
			for cmd := range seen {
				if !have[cmd] {
					t.Fatalf("committed command %s was lost", cmd)
				}
			}
			t.Logf("%d commands committed out of %d proposed", len(ref)-1, n)
			return
		}
		if time.Now().After(deadline) {
			for _, id := range c.ids {
				t.Logf("%s: %d applied, status %+v", id, len(c.appliedBy(id)), c.nodes[id].Status())
			}
			t.Fatal("nodes did not converge")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------

func TestFileStorage(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if term, vote, entries, err := s.Load(); err != nil || term != 0 || vote != "" || len(entries) != 0 {
		t.Fatalf("fresh storage: %d %q %v %v", term, vote, entries, err)
	}
	s.SaveState(3, "n1")
	s.Append([]Entry{{1, 1, []byte("a")}, {2, 1, nil}, {3, 2, []byte("ccc")}})
	s.TruncateFrom(3)
	s.Append([]Entry{{3, 3, []byte("new")}, {4, 3, []byte("d")}})
	s.Close()

	want := []Entry{{1, 1, []byte("a")}, {2, 1, nil}, {3, 3, []byte("new")}, {4, 3, []byte("d")}}
	s, _ = OpenFileStorage(dir)
	term, vote, entries, err := s.Load()
	if err != nil || term != 3 || vote != "n1" || !reflect.DeepEqual(entries, want) {
		t.Fatalf("reload: %d %q %v %v", term, vote, entries, err)
	}
	s.Close()

	// A crash mid-append leaves a partial record. It must be dropped, and
	// appends after it must still work.
	path := filepath.Join(dir, "log")
	info, _ := os.Stat(path)
	os.Truncate(path, info.Size()-2)
	s, _ = OpenFileStorage(dir)
	_, _, entries, _ = s.Load()
	if !reflect.DeepEqual(entries, want[:3]) {
		t.Fatalf("after torn tail: %v", entries)
	}
	s.Append([]Entry{{4, 4, []byte("again")}})
	s.Close()
	s, _ = OpenFileStorage(dir)
	_, _, entries, _ = s.Load()
	if len(entries) != 4 || string(entries[3].Cmd) != "again" {
		t.Fatalf("append after torn tail: %v", entries)
	}
	s.Close()
}

// Three nodes over real TCP sockets with file storage.
func TestClusterOverTCP(t *testing.T) {
	ids := []string{"a", "b", "c"}
	addrs := map[string]string{}
	listeners := map[string]net.Listener{}
	for _, id := range ids {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = ln
		addrs[id] = ln.Addr().String()
	}

	nodes := map[string]*Raft{}
	var mu sync.Mutex
	applied := map[string][]string{}
	for _, id := range ids {
		var peers []string
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		store, err := OpenFileStorage(filepath.Join(t.TempDir(), id))
		if err != nil {
			t.Fatal(err)
		}
		tr := NewTCPTransport(addrs)
		r, err := New(Config{
			ID: id, Peers: peers,
			ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval: 40 * time.Millisecond,
		}, tr, store)
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = r
		ServeTCP(listeners[id], r)
		go func(id string) {
			for m := range r.ApplyCh() {
				if len(m.Cmd) > 0 {
					mu.Lock()
					applied[id] = append(applied[id], string(m.Cmd))
					mu.Unlock()
				}
			}
		}(id)
		t.Cleanup(func() {
			listeners[id].Close()
			r.Stop()
			tr.Close()
			store.Close()
		})
	}

	var leader *Raft
	deadline := time.Now().Add(5 * time.Second)
	for leader == nil && time.Now().Before(deadline) {
		for _, r := range nodes {
			if r.Status().State == Leader {
				leader = r
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader over TCP")
	}
	want := []string{"x", "y", "z"}
	for _, cmd := range want {
		if _, _, ok := leader.Propose([]byte(cmd)); !ok {
			t.Fatal("leader rejected proposal")
		}
	}
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := true
		for _, id := range ids {
			ok = ok && reflect.DeepEqual(applied[id], want)
		}
		mu.Unlock()
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("not replicated over TCP: %v", applied)
}
