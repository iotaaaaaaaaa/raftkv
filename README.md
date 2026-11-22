# raftkv

A fault-tolerant key-value store in Go. Three or five nodes keep identical copies of the data using the Raft consensus algorithm, implemented here from scratch with only the standard library.

Any minority of nodes can crash or be cut off and the cluster keeps serving requests without losing a single acknowledged write.

## See it work

```sh
./scripts/demo.sh
```

```
leader elected: n3
wrote greeting; read from every node (followers redirect to the leader):
  :8001 -> hello raft
  :8002 -> hello raft
  :8003 -> hello raft
killing leader n3 with SIGKILL
new leader elected: n2
  :8001 -> hello raft
  :8002 -> hello raft
restarting n3; it rejoins and catches up from the new leader
  :8003 {"commit_index":8,"id":"n3","last_index":8,"leader":"n2","state":"follower","term":2}
```

## Run a cluster by hand

```sh
make build
CLUSTER="n1=127.0.0.1:7001/127.0.0.1:8001,n2=127.0.0.1:7002/127.0.0.1:8002,n3=127.0.0.1:7003/127.0.0.1:8003"
./bin/raftkv -id n1 -cluster "$CLUSTER" -data data/n1 &
./bin/raftkv -id n2 -cluster "$CLUSTER" -data data/n2 &
./bin/raftkv -id n3 -cluster "$CLUSTER" -data data/n3 &

curl -L -X PUT --data "Chennai" localhost:8001/kv/city
curl -L localhost:8002/kv/city        # any node works; followers redirect
curl localhost:8003/status
```

Each node entry is `id=raftAddress/httpAddress`.

| Request | Result |
|---|---|
| `PUT /kv/{key}` with the value as body | 204 |
| `GET /kv/{key}` | 200 and the value, or 404 |
| `DELETE /kv/{key}` | 204 |
| `GET /status` | Role, term, leader, commit index |

## Layout

| Path | Contents |
|---|---|
| `internal/raft/raft.go` | The algorithm: elections, replication, commit, apply |
| `internal/raft/storage.go` | Durable term, vote and log (and an in-memory version for tests) |
| `internal/raft/tcp.go` | RPC between nodes over TCP |
| `internal/raft/memnet.go` | In-process network that tests can partition |
| `internal/kv/store.go` | The key-value state machine on top of the Raft log |
| `internal/kv/http.go` | HTTP API with leader redirect |

## How it works

**Replicated state machine.** No node changes its map directly. Every operation is appended to a log, the log is replicated, and each node applies committed entries in order. Same log, same order, same data everywhere.

**Leader election.** Time is split into terms. A follower that hears no heartbeat for a random 300 to 600 ms becomes a candidate, bumps the term and requests votes. A node votes once per term, and only for a candidate whose log is at least as up to date as its own. Whoever gets a majority leads.

**Replication.** The leader sends new entries to each follower along with the index and term of the entry before them. A follower accepts only if it has that previous entry, which guarantees the logs match up to that point. An entry stored on a majority is committed.

**Why committed entries are never lost.** A committed entry is on a majority. A new leader needs votes from a majority. Any two majorities share at least one node, and that node refuses to vote for anyone missing the entry. So every future leader has it.

**Persistence.** Term, vote and log are fsynced before a node answers any RPC. The vote file is replaced by atomic rename; log records carry checksums so a half-written record from a crash is detected and dropped.

**Linearizable reads.** Reads go through the log like writes. A leader that has been cut off cannot commit, so it cannot answer a read with stale data. `TestIsolatedLeaderDoesNotServeReads` checks this.

## Details worth reading in the code

- **The no-op on election** (`becomeLeader`): a leader may only count replicas for entries of its own term, so it appends an empty entry immediately to get earlier entries committed.
- **The commit rule** (`advanceCommit`): the comment explains the scenario from Figure 8 of the Raft paper that makes the current-term restriction necessary.
- **Fast log backtracking** (`ConflictIndex`, `ConflictTerm`): a follower that is far behind tells the leader where its log diverges, so catching up skips a whole term per round trip.
- **One replicator goroutine per follower**: woken by new entries or by a heartbeat tick, so the number of goroutines stays fixed under load.

## Tests

```sh
make test     # go test -race ./...
```

The Raft tests run real nodes on an in-memory network that can isolate or crash any of them:

| Test | What it proves |
|---|---|
| `TestElectsOneLeader` | Exactly one leader, and no elections while it is healthy |
| `TestNewLeaderAfterLeaderFails` | Failover, then the old leader rejoins as a follower |
| `TestIsolatedLeaderCannotCommit` | Writes accepted by a cut-off leader are discarded |
| `TestNoProgressWithoutMajority` | Nothing commits with only a minority |
| `TestLaggingFollowerCatchesUp` | A node 1,200 entries behind catches up |
| `TestStateSurvivesRestart` | The whole cluster restarts from disk state |
| `TestAgreementUnderChurn` | 12 rounds of random crashes and partitions on 5 nodes: all logs end identical, nothing committed is lost or duplicated |
| `TestClusterOverTCP` | The same over real sockets and files |

## Limits

- **No snapshots.** The log grows forever and a restarted node replays all of it.
- **No membership changes.** The set of nodes is fixed at startup.
- **No client request IDs.** A client that retries after a timeout may apply its write twice. Harmless for put and delete, which give the same result when repeated; it would matter for something like increment.
- **Reads cost a log entry.** Leader leases or ReadIndex would make them cheaper.

## Questions to check your understanding

1. Why must election timeouts be random?
2. A node votes, crashes before saving the vote, restarts and gets another vote request in the same term. What can go wrong?
3. Why can a leader not commit an entry from an earlier term just because it is on a majority?
4. With 5 nodes split 3 and 2, which side keeps working, and what happens to writes sent to the other side?
5. Why does `kv.Store.Do` hold its lock from `Propose` until the waiter is registered?
6. A client's `PUT` times out. Was the write applied? What should the client do?
7. Why is serving a `GET` straight from the leader's map, without the log, unsafe?

## Reference

Diego Ongaro and John Ousterhout, "In Search of an Understandable Consensus Algorithm", 2014. Figure 2 of the paper is the specification this code follows.
