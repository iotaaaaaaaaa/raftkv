package kv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iotaaaaaaaaa/raftkv/internal/raft"
)

type testCluster struct {
	t      *testing.T
	net    *raft.MemNetwork
	ids    []string
	rafts  map[string]*raft.Raft
	stores map[string]*Store
}

func newTestCluster(t *testing.T, n int) *testCluster {
	c := &testCluster{t: t, net: raft.NewMemNetwork(), rafts: map[string]*raft.Raft{}, stores: map[string]*Store{}}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		var peers []string
		for _, p := range c.ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		rf, err := raft.New(raft.Config{
			ID: id, Peers: peers,
			ElectionTimeoutMin: 150 * time.Millisecond, ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval: 40 * time.Millisecond,
		}, c.net.Transport(id), raft.NewMemoryStorage())
		if err != nil {
			t.Fatal(err)
		}
		c.net.Register(id, rf)
		c.rafts[id] = rf
		c.stores[id] = New(rf)
	}
	t.Cleanup(func() {
		for _, rf := range c.rafts {
			rf.Stop()
		}
	})
	return c
}

// do sends op to whichever of the given nodes is leader, retrying through
// elections, the way a real client would.
func (c *testCluster) do(op Op, among ...string) Result {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range among {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			res, err := c.stores[id].Do(ctx, op)
			cancel()
			if err == nil {
				return res
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("op %+v never succeeded", op)
	return Result{}
}

func (c *testCluster) leader(among ...string) string {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range among {
			if c.rafts[id].Status().State == raft.Leader {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return ""
}

func (c *testCluster) waitSameState(ids ...string) map[string]string {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ref := c.stores[ids[0]].Snapshot()
		same := true
		for _, id := range ids[1:] {
			same = same && reflect.DeepEqual(ref, c.stores[id].Snapshot())
		}
		if same {
			return ref
		}
		if time.Now().After(deadline) {
			c.t.Fatal("stores did not converge")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPutGetDelete(t *testing.T) {
	c := newTestCluster(t, 3)
	if res := c.do(Op{Kind: "get", Key: "a"}); res.Found {
		t.Fatal("empty store returned a value")
	}
	c.do(Op{Kind: "put", Key: "a", Value: "1"})
	c.do(Op{Kind: "put", Key: "a", Value: "2"})
	c.do(Op{Kind: "put", Key: "b", Value: "x"})
	if res := c.do(Op{Kind: "get", Key: "a"}); !res.Found || res.Value != "2" {
		t.Fatalf("get a = %+v", res)
	}
	if res := c.do(Op{Kind: "delete", Key: "b"}); !res.Found {
		t.Fatal("delete b reported missing")
	}
	if got := c.waitSameState(c.ids...); !reflect.DeepEqual(got, map[string]string{"a": "2"}) {
		t.Fatalf("final state %v", got)
	}
}

func TestFollowerPointsToLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	c.do(Op{Kind: "put", Key: "k", Value: "v"}) // also ensures followers know the leader
	leader := c.leader()
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		_, err := c.stores[id].Do(context.Background(), Op{Kind: "get", Key: "k"})
		var nl *NotLeaderError
		if !errors.As(err, &nl) || nl.Leader != leader {
			t.Fatalf("follower %s returned %v, want NotLeaderError pointing to %s", id, err, leader)
		}
	}
}

// Every write acknowledged to a client must still be there after the leader
// that acknowledged it disappears.
func TestAcknowledgedWritesSurviveLeaderFailure(t *testing.T) {
	c := newTestCluster(t, 3)
	want := map[string]string{}
	for i := 0; i < 20; i++ {
		k, v := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		c.do(Op{Kind: "put", Key: k, Value: v})
		want[k] = v
	}

	old := c.leader()
	c.net.Isolate(old)
	var rest []string
	for _, id := range c.ids {
		if id != old {
			rest = append(rest, id)
		}
	}
	for k, v := range want {
		if res := c.do(Op{Kind: "get", Key: k}, rest...); res.Value != v {
			t.Fatalf("after failover %s = %q, want %q", k, res.Value, v)
		}
	}
	c.do(Op{Kind: "put", Key: "after", Value: "failover"}, rest...)
	want["after"] = "failover"

	c.net.Heal(old)
	if got := c.waitSameState(c.ids...); !reflect.DeepEqual(got, want) {
		t.Fatalf("state after heal: %d keys, want %d", len(got), len(want))
	}
}

// A deposed leader must not answer reads from its stale copy.
func TestIsolatedLeaderDoesNotServeReads(t *testing.T) {
	c := newTestCluster(t, 3)
	c.do(Op{Kind: "put", Key: "k", Value: "old"})
	old := c.leader()
	c.net.Isolate(old)
	var rest []string
	for _, id := range c.ids {
		if id != old {
			rest = append(rest, id)
		}
	}
	c.do(Op{Kind: "put", Key: "k", Value: "new"}, rest...)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if res, err := c.stores[old].Do(ctx, Op{Kind: "get", Key: "k"}); err == nil {
		t.Fatalf("isolated leader answered a read with %q", res.Value)
	}
}

func TestConcurrentClients(t *testing.T) {
	c := newTestCluster(t, 3)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				c.do(Op{Kind: "put", Key: fmt.Sprintf("g%d-k%d", g, i), Value: "v"})
			}
		}(g)
	}
	wg.Wait()
	if got := c.waitSameState(c.ids...); len(got) != 200 {
		t.Fatalf("%d keys, want 200", len(got))
	}
}

func TestHTTPAPI(t *testing.T) {
	c := newTestCluster(t, 3)
	servers := map[string]*httptest.Server{}
	httpAddrs := map[string]string{}
	for _, id := range c.ids {
		servers[id] = httptest.NewUnstartedServer(nil)
		httpAddrs[id] = servers[id].Listener.Addr().String()
	}
	for _, id := range c.ids {
		servers[id].Config.Handler = Handler(c.stores[id], c.rafts[id], httpAddrs, 2*time.Second)
		servers[id].Start()
		defer servers[id].Close()
	}
	c.do(Op{Kind: "put", Key: "warmup", Value: "1"})
	leader := c.leader()
	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
		}
	}

	request := func(client *http.Client, method, url, body string) (int, string, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(method, url, strings.NewReader(body))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), resp.Header
	}
	base := servers[leader].URL

	if code, _, _ := request(http.DefaultClient, "PUT", base+"/kv/city", "Chennai"); code != 204 {
		t.Fatalf("PUT = %d", code)
	}
	if code, body, _ := request(http.DefaultClient, "GET", base+"/kv/city", ""); code != 200 || body != "Chennai" {
		t.Fatalf("GET = %d %q", code, body)
	}
	if code, _, _ := request(http.DefaultClient, "GET", base+"/kv/nope", ""); code != 404 {
		t.Fatalf("GET missing = %d", code)
	}

	// A follower redirects to the leader...
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	code, _, hdr := request(noFollow, "GET", servers[follower].URL+"/kv/city", "")
	if code != 307 || hdr.Get("X-Raft-Leader") != leader {
		t.Fatalf("follower GET = %d, leader header %q", code, hdr.Get("X-Raft-Leader"))
	}
	// ...and a client that follows redirects gets the answer.
	if code, body, _ := request(http.DefaultClient, "GET", servers[follower].URL+"/kv/city", ""); code != 200 || body != "Chennai" {
		t.Fatalf("GET via follower = %d %q", code, body)
	}

	if code, _, _ := request(http.DefaultClient, "DELETE", base+"/kv/city", ""); code != 204 {
		t.Fatalf("DELETE = %d", code)
	}
	if code, body, _ := request(http.DefaultClient, "GET", base+"/status", ""); code != 200 || !strings.Contains(body, `"state":"leader"`) {
		t.Fatalf("status = %d %s", code, body)
	}
}
