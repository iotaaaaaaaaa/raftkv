// Package raft implements the Raft consensus algorithm: leader election, log
// replication and persistence, as described in "In Search of an
// Understandable Consensus Algorithm" (Ongaro and Ousterhout, 2014).
//
// The problem Raft solves: several servers must agree on the same sequence of
// commands, even when some of them crash or the network drops messages. If
// every server applies the same commands in the same order, they all end up
// in the same state.
//
// The rules, in short:
//
//   - Time is divided into terms. Each term has at most one leader.
//   - A follower that hears nothing from a leader for a while becomes a
//     candidate, increments the term and asks everyone for a vote.
//   - A server votes at most once per term, and only for a candidate whose
//     log is at least as up to date as its own.
//   - The leader appends client commands to its log and sends them to the
//     followers. Once a majority has stored an entry, it is committed and
//     can never be lost.
//
// Not implemented: log compaction (snapshots) and cluster membership changes.
package raft

import (
	"context"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"follower", "candidate", "leader"}[s]
}

// Entry is one slot in the replicated log.
type Entry struct {
	Index uint64
	Term  uint64
	Cmd   []byte // empty for the no-op a new leader appends
}

// ApplyMsg is delivered on the apply channel once an entry is committed.
// Entries arrive in index order, exactly once per process lifetime.
type ApplyMsg struct {
	Index uint64
	Term  uint64
	Cmd   []byte
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64 // index of the entry just before Entries
	PrevLogTerm  uint64
	Entries      []Entry // empty for a heartbeat
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// On failure, where the follower's log diverges, so the leader can skip
	// back a whole term at a time instead of one entry per round trip.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Transport sends RPCs to other nodes. An error means "no answer"; Raft
// simply tries again later.
type Transport interface {
	RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Storage holds the state that must survive a crash. Every method must have
// reached stable storage before it returns.
type Storage interface {
	Load() (term uint64, votedFor string, entries []Entry, err error)
	SaveState(term uint64, votedFor string) error
	Append(entries []Entry) error
	// TruncateFrom removes every entry with Index >= index.
	TruncateFrom(index uint64) error
}

type Config struct {
	ID    string
	Peers []string // IDs of the other nodes

	// A follower starts an election after a random time in [Min, Max) without
	// hearing from a leader. The randomness is what stops candidates from
	// splitting the vote forever.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration // must be well below ElectionTimeoutMin

	Logger *slog.Logger
}

const maxBatch = 512 // entries per AppendEntries RPC

type Raft struct {
	cfg       Config
	transport Transport
	storage   Storage
	logger    *slog.Logger

	mu sync.Mutex

	// Persistent state: saved to Storage before answering any RPC.
	currentTerm uint64
	votedFor    string
	log         []Entry // log[0] is a placeholder so log[i].Index == i

	// Volatile state.
	state            State
	leaderID         string
	commitIndex      uint64 // highest entry known to be on a majority
	lastApplied      uint64 // highest entry handed to the apply channel
	electionDeadline time.Time

	// Leader only: for each follower, the next entry to send and the highest
	// entry known to be stored there.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	wake       map[string]chan struct{} // nudges a peer's replicator

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// New starts a node. It restores persisted state, then runs as a follower.
func New(cfg Config, transport Transport, storage Storage) (*Raft, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	term, votedFor, entries, err := storage.Load()
	if err != nil {
		return nil, err
	}
	r := &Raft{
		cfg:         cfg,
		transport:   transport,
		storage:     storage,
		logger:      cfg.Logger.With("node", cfg.ID),
		currentTerm: term,
		votedFor:    votedFor,
		log:         append([]Entry{{}}, entries...),
		nextIndex:   map[string]uint64{},
		matchIndex:  map[string]uint64{},
		wake:        map[string]chan struct{}{},
		applyCh:     make(chan ApplyMsg, 256),
		stopCh:      make(chan struct{}),
	}
	for _, p := range cfg.Peers {
		r.wake[p] = make(chan struct{}, 1)
	}
	r.applyCond = sync.NewCond(&r.mu)
	r.resetElectionTimer()

	r.wg.Add(2)
	go r.electionLoop()
	go r.applyLoop()
	return r, nil
}

// ApplyCh delivers committed entries. The consumer must keep reading it.
func (r *Raft) ApplyCh() <-chan ApplyMsg { return r.applyCh }

// Stop shuts the node down and waits for its goroutines.
func (r *Raft) Stop() {
	r.mu.Lock()
	select {
	case <-r.stopCh:
		r.mu.Unlock()
		return
	default:
	}
	close(r.stopCh)
	r.applyCond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *Raft) stopped() bool {
	select {
	case <-r.stopCh:
		return true
	default:
		return false
	}
}

// Status is a snapshot of the node for monitoring.
type Status struct {
	ID          string
	State       State
	Term        uint64
	LeaderID    string
	CommitIndex uint64
	LastIndex   uint64
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{r.cfg.ID, r.state, r.currentTerm, r.leaderID, r.commitIndex, r.lastIndex()}
}

// Propose asks the cluster to agree on cmd. Only the leader accepts. A true
// result means the entry was appended locally; it is committed only when it
// later shows up on the apply channel with the same index and term. If a
// different term shows up at that index, this proposal was lost to a leader
// change.
func (r *Raft) Propose(cmd []byte) (index, term uint64, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader || r.stopped() {
		return 0, 0, false
	}
	e := Entry{Index: r.lastIndex() + 1, Term: r.currentTerm, Cmd: cmd}
	r.appendLocked([]Entry{e})
	r.advanceCommit() // commits at once in a single-node cluster
	r.wakeReplicators()
	return e.Index, e.Term, true
}

// --- helpers; all assume r.mu is held --------------------------------------

func (r *Raft) lastIndex() uint64 { return uint64(len(r.log) - 1) }
func (r *Raft) lastTerm() uint64  { return r.log[len(r.log)-1].Term }
func (r *Raft) majority() int     { return (len(r.cfg.Peers)+1)/2 + 1 }

func (r *Raft) resetElectionTimer() {
	spread := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin
	r.electionDeadline = time.Now().Add(r.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(spread)+1)))
}

// A storage failure means we can no longer keep Raft's promises, so the only
// safe reaction is to stop.
func (r *Raft) must(err error) {
	if err != nil {
		panic("raft: storage failure: " + err.Error())
	}
}

func (r *Raft) saveState() { r.must(r.storage.SaveState(r.currentTerm, r.votedFor)) }

func (r *Raft) appendLocked(entries []Entry) {
	r.must(r.storage.Append(entries))
	r.log = append(r.log, entries...)
}

// becomeFollower is called whenever we see a term at least as new as ours
// from a legitimate leader or candidate.
func (r *Raft) becomeFollower(term uint64) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = "" // a new term means a fresh vote
		r.saveState()
	}
	if r.state != Follower {
		r.logger.Info("stepping down", "term", r.currentTerm)
	}
	r.state = Follower
}

func (r *Raft) wakeReplicators() {
	for _, ch := range r.wake {
		select {
		case ch <- struct{}{}:
		default: // already has a pending wake-up
		}
	}
}

// --- elections -------------------------------------------------------------

func (r *Raft) electionLoop() {
	defer r.wg.Done()
	tick := time.NewTicker(r.cfg.HeartbeatInterval / 5)
	defer tick.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-tick.C:
		}
		r.mu.Lock()
		if r.state != Leader && time.Now().After(r.electionDeadline) {
			r.startElection()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) startElection() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.cfg.ID
	r.leaderID = ""
	r.saveState()
	r.resetElectionTimer()
	r.logger.Info("starting election", "term", r.currentTerm)

	term := r.currentTerm
	args := &RequestVoteArgs{Term: term, CandidateID: r.cfg.ID, LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm()}
	votes := 1 // our own
	if votes >= r.majority() {
		r.becomeLeader()
		return
	}

	for _, peer := range r.cfg.Peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), r.cfg.ElectionTimeoutMin)
			defer cancel()
			reply, err := r.transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.stopped() {
				return
			}
			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
				return
			}
			// Ignore replies that arrive after this election is over.
			if r.state != Candidate || r.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes >= r.majority() {
				r.becomeLeader()
			}
		}(peer)
	}
}

func (r *Raft) becomeLeader() {
	if r.stopped() {
		return
	}
	r.state = Leader
	r.leaderID = r.cfg.ID
	r.logger.Info("became leader", "term", r.currentTerm)

	for _, p := range r.cfg.Peers {
		r.nextIndex[p] = r.lastIndex() + 1
		r.matchIndex[p] = 0
	}
	// A leader may only count replicas for entries of its own term (see
	// advanceCommit). Appending a no-op right away gives it such an entry, so
	// anything left over from earlier terms gets committed without waiting
	// for a client to write.
	r.appendLocked([]Entry{{Index: r.lastIndex() + 1, Term: r.currentTerm}})
	r.advanceCommit()

	for _, p := range r.cfg.Peers {
		r.wg.Add(1)
		go r.replicator(p, r.currentTerm)
	}
}

// HandleRequestVote answers a candidate. It returns nil if the node has been
// stopped; transports must report that as an error.
func (r *Raft) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped() {
		return nil
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}
	reply := &RequestVoteReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply
	}

	// The election restriction: only vote for a log that is at least as up to
	// date as ours. Any committed entry is on a majority, so every winning
	// candidate must have it. This is what makes committed entries permanent.
	upToDate := args.LastLogTerm > r.lastTerm() ||
		(args.LastLogTerm == r.lastTerm() && args.LastLogIndex >= r.lastIndex())

	if (r.votedFor == "" || r.votedFor == args.CandidateID) && upToDate {
		r.votedFor = args.CandidateID
		r.saveState() // must be durable before we answer, or we could vote twice after a crash
		r.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply
}

// --- replication -----------------------------------------------------------

// replicator keeps one follower in sync for as long as we lead in `term`.
// It sends whenever there is something new, and at least once per heartbeat
// interval so the follower does not start an election.
func (r *Raft) replicator(peer string, term uint64) {
	defer r.wg.Done()
	tick := time.NewTicker(r.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		stillLeader, more := r.replicateOnce(peer, term)
		if !stillLeader {
			return
		}
		if more {
			continue
		}
		select {
		case <-r.stopCh:
			return
		case <-r.wake[peer]:
		case <-tick.C:
		}
	}
}

func (r *Raft) replicateOnce(peer string, term uint64) (stillLeader, more bool) {
	r.mu.Lock()
	if r.state != Leader || r.currentTerm != term || r.stopped() {
		r.mu.Unlock()
		return false, false
	}
	next := r.nextIndex[peer]
	end := min(r.lastIndex()+1, next+maxBatch)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     r.cfg.ID,
		PrevLogIndex: next - 1,
		PrevLogTerm:  r.log[next-1].Term,
		Entries:      append([]Entry(nil), r.log[next:end]...), // copy: the RPC runs without the lock
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*r.cfg.HeartbeatInterval)
	reply, err := r.transport.AppendEntries(ctx, peer, args)
	cancel()
	if err != nil {
		return true, false // unreachable: try again on the next heartbeat
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped() {
		return false, false
	}
	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		r.resetElectionTimer()
		return false, false
	}
	if r.state != Leader || r.currentTerm != term {
		return false, false
	}

	if reply.Success {
		if match := args.PrevLogIndex + uint64(len(args.Entries)); match > r.matchIndex[peer] {
			r.matchIndex[peer] = match
		}
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.advanceCommit()
		return true, r.nextIndex[peer] <= r.lastIndex()
	}

	// The follower's log does not contain our PrevLog entry. Move nextIndex
	// back using the hint it sent.
	next = reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		// If we also have entries from the conflicting term, resume just
		// after our last one; otherwise skip that whole term.
		for i := r.lastIndex(); i >= 1; i-- {
			if r.log[i].Term == reply.ConflictTerm {
				next = i + 1
				break
			}
		}
	}
	next = max(next, r.matchIndex[peer]+1, 1)
	r.nextIndex[peer] = min(next, r.lastIndex()+1)
	return true, true
}

// advanceCommit moves commitIndex to the highest entry stored on a majority.
//
// It only counts entries from the current term. An entry from an older term
// can sit on a majority and still be overwritten by a future leader (Figure 8
// in the paper), so it becomes committed only indirectly, when a current-term
// entry after it commits.
func (r *Raft) advanceCommit() {
	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			break
		}
		count := 1 // the leader itself
		for _, p := range r.cfg.Peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if count >= r.majority() {
			r.commitIndex = n
			r.applyCond.Broadcast()
			r.wakeReplicators() // tell followers the new commit index promptly
			break
		}
	}
}

// HandleAppendEntries answers the leader: both heartbeats and new entries.
// Like HandleRequestVote, it returns nil once the node is stopped.
func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped() {
		return nil
	}

	if args.Term < r.currentTerm {
		return &AppendEntriesReply{Term: r.currentTerm} // a stale leader; tell it the new term
	}
	r.becomeFollower(args.Term)
	r.leaderID = args.LeaderID
	r.resetElectionTimer()
	reply := &AppendEntriesReply{Term: r.currentTerm}

	// Consistency check: we must already hold the entry that precedes the new
	// ones. By induction, if that entry matches, the whole log before it
	// matches too.
	if args.PrevLogIndex > r.lastIndex() {
		reply.ConflictIndex = r.lastIndex() + 1
		return reply
	}
	if t := r.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && r.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i // first entry of the conflicting term
		return reply
	}

	// Skip entries we already have. At the first mismatch, drop our entry and
	// everything after it, then append the leader's version.
	for i, e := range args.Entries {
		if e.Index <= r.lastIndex() {
			if r.log[e.Index].Term == e.Term {
				continue
			}
			r.must(r.storage.TruncateFrom(e.Index))
			r.log = r.log[:e.Index]
		}
		r.appendLocked(args.Entries[i:])
		break
	}

	// Entries up to the leader's commit index are safe to apply, but only as
	// far as we have verified our log matches the leader's.
	lastVerified := args.PrevLogIndex + uint64(len(args.Entries))
	if c := min(args.LeaderCommit, lastVerified); c > r.commitIndex {
		r.commitIndex = c
		r.applyCond.Broadcast()
	}
	reply.Success = true
	return reply
}

// --- applying --------------------------------------------------------------

// applyLoop hands committed entries to the state machine, in order.
func (r *Raft) applyLoop() {
	defer r.wg.Done()
	defer close(r.applyCh)
	for {
		r.mu.Lock()
		for r.lastApplied >= r.commitIndex && !r.stopped() {
			r.applyCond.Wait()
		}
		if r.stopped() {
			r.mu.Unlock()
			return
		}
		batch := append([]Entry(nil), r.log[r.lastApplied+1:r.commitIndex+1]...)
		r.lastApplied = r.commitIndex
		r.mu.Unlock()

		for _, e := range batch {
			select {
			case r.applyCh <- ApplyMsg{Index: e.Index, Term: e.Term, Cmd: e.Cmd}:
			case <-r.stopCh:
				return
			}
		}
	}
}
