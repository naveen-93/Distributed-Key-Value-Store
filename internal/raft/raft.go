package raft

import (
	pb "Distributed-Key-Value-Store/raft/proto"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/exp/rand"
	"google.golang.org/grpc"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Constants for timeouts
const (
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// Config contains configuration for a Raft node
type Config struct {
	ID               string
	Peers            map[string]string // map[ID]Address
	DataDir          string
	RPCPort          string
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// Raft represents a single node in the Raft cluster
type Raft struct {
	pb.UnimplementedRaftServer
	mu     sync.RWMutex
	config Config

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         []pb.LogEntry

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	state       State

	// Leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Channels
	applyCh chan ApplyMsg
	stopCh  chan struct{}

	// RPC clients
	peerClients map[string]pb.RaftClient
	rpcServer   *grpc.Server

	// Storage
	storage *FileStorage

	// Additional fields
	stopped bool
}

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64
}

// FileStorage handles persistent storage
type FileStorage struct {
	stateFile string
	logFile   string
}

// NewFileStorage creates a new file-based storage
func NewFileStorage(dataDir string) (*FileStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	fs := &FileStorage{
		stateFile: filepath.Join(dataDir, "state.json"),
		logFile:   filepath.Join(dataDir, "log.json"),
	}

	// Initialize empty state files if they don't exist
	if _, err := os.Stat(fs.stateFile); os.IsNotExist(err) {
		initialState := struct {
			Term     uint64 `json:"term"`
			VotedFor string `json:"votedFor"`
		}{0, ""}
		data, _ := json.Marshal(initialState)
		if err := os.WriteFile(fs.stateFile, data, 0644); err != nil {
			return nil, err
		}
	}

	if _, err := os.Stat(fs.logFile); os.IsNotExist(err) {
		if err := os.WriteFile(fs.logFile, []byte("[]"), 0644); err != nil {
			return nil, err
		}
	}

	return fs, nil
}

// SaveState persists Raft state
func (fs *FileStorage) SaveState(term uint64, votedFor string) error {
	state := struct {
		Term     uint64
		VotedFor string
	}{term, votedFor}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return os.WriteFile(fs.stateFile, data, 0644)
}

// LoadState loads persisted state
func (fs *FileStorage) LoadState() (uint64, string, error) {
	data, err := os.ReadFile(fs.stateFile)
	if os.IsNotExist(err) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}

	var state struct {
		Term     uint64
		VotedFor string
	}

	if err := json.Unmarshal(data, &state); err != nil {
		return 0, "", err
	}

	return state.Term, state.VotedFor, nil
}

// SaveLog persists the Raft log
func (fs *FileStorage) SaveLog(entries []pb.LogEntry) error {
    // Save as direct array without wrapper
    data, err := json.Marshal(entries)
    if err != nil {
        return err
    }
    return atomicWrite(fs.logFile, data)
}

func atomicWrite(filename string, data []byte) error {
	tmpFile := filename + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpFile, filename)
}

// LoadLog loads the persisted Raft log
func (fs *FileStorage) LoadLog() ([]pb.LogEntry, error) {
    data, err := os.ReadFile(fs.logFile)
    if os.IsNotExist(err) {
        return []pb.LogEntry{{Term: 0, Command: ""}}, nil
    }
    if err != nil {
        return nil, err
    }

    var entries []pb.LogEntry
    if err := json.Unmarshal(data, &entries); err != nil {
        return nil, fmt.Errorf("log unmarshal error: %v (data: %s)", err, string(data))
    }
    
    // Ensure dummy entry exists
    if len(entries) == 0 || entries[0].Term != 0 {
        entries = append([]pb.LogEntry{{Term: 0, Command: ""}}, entries...)
    }
    return entries, nil
}
// loadPersistedState loads the persisted state
func (r *Raft) loadPersistedState() error {
	// Load log first
	log, err := r.storage.LoadLog()
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Initialize with dummy entry if log is empty or missing
	if len(log) == 0 {
		r.log = []pb.LogEntry{{Term: 0, Command: ""}}
	} else {
		r.log = log
		// Ensure first entry is dummy
		if r.log[0].Term != 0 {
			r.log = append([]pb.LogEntry{{Term: 0, Command: ""}}, r.log...)
		}
	}

	// Load metadata
	term, votedFor, err := r.storage.LoadState()
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	r.currentTerm = term
	r.votedFor = votedFor
	r.commitIndex = 0
	r.lastApplied = 0

	// Find last committed entry from previous session
	for i := len(r.log) - 1; i > 0; i-- {
		if r.log[i].Term <= r.currentTerm {
			r.commitIndex = uint64(i)
			r.lastApplied = uint64(i)
			break
		}
	}

	return nil
}

// updateCommitIndex updates the commit index based on matchIndex values
func (r *Raft) updateCommitIndex() {
	// Find the highest index that has been replicated to a majority
	for n := r.commitIndex + 1; n < uint64(len(r.log)); n++ {
		if r.log[n].Term != r.currentTerm {
			continue
		}

		count := 1 // Count self
		for _, matchIndex := range r.matchIndex {
			if matchIndex >= n {
				count++
			}
		}

		if count > len(r.peerClients)/2 {
			r.commitIndex = n
			go r.applyCommitted()
		}
	}
}

// applyCommitted applies committed log entries to the state machine
func (r *Raft) applyCommitted() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		r.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      []byte(r.log[r.lastApplied].Command),
			CommandIndex: r.lastApplied,
			CommandTerm:  r.log[r.lastApplied].Term,
		}
	}
}

// NewRaft creates a new Raft node
func NewRaft(config Config) (*Raft, error) {
	storage, err := NewFileStorage(config.DataDir)
	if err != nil {
		return nil, err
	}

	r := &Raft{
		config:      config,
		storage:     storage,
		applyCh:     make(chan ApplyMsg),
		stopCh:      make(chan struct{}),
		peerClients: make(map[string]pb.RaftClient),
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
	}

	if err := r.loadPersistedState(); err != nil {
		return nil, err
	}

	// Ensure the log always starts with a dummy entry.
	if len(r.log) == 0 || r.log[0].Term != 0 {
        r.log = []pb.LogEntry{{Term: 0, Command: ""}}
        if err := r.storage.SaveLog(r.log); err != nil {
            return nil, err
        }
    }
    
    // Initialize commit index safely
    r.commitIndex = min(r.commitIndex, uint64(len(r.log)-1))

	// Start RPC server
	if err := r.startRPCServer(); err != nil {
		return nil, err
	}

	// Connect to peers
	if err := r.connectToPeers(); err != nil {
		return nil, err
	}

	// Start background routines
	go r.run()

	return r, nil
}

// run is the main event loop for the Raft node
func (r *Raft) run() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// startRPCServer starts the gRPC server
func (r *Raft) startRPCServer() error {
	lis, err := net.Listen("tcp", ":"+r.config.RPCPort)
	if err != nil {
		return err
	}

	r.rpcServer = grpc.NewServer()
	pb.RegisterRaftServer(r.rpcServer, r)

	go r.rpcServer.Serve(lis)
	return nil
}

// connectToPeers establishes connections to all peers
func (r *Raft) connectToPeers() error {
	for id, addr := range r.config.Peers {
		if id == r.config.ID {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}

		r.peerClients[id] = pb.NewRaftClient(conn)
	}
	return nil
}

// RequestVote RPC handler
func (r *Raft) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.currentTerm {
		return &pb.VoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
	}

	if req.Term > r.currentTerm {
		r.stepDown(req.Term)
	}

	// Use our helper so that the dummy entry is not counted.
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()

	// If we haven't voted for anyone in this term or we've already voted for this candidate
	if (r.votedFor == "" || r.votedFor == req.CandidateId) && req.Term >= r.currentTerm {
		// Vote for candidate if their log is at least as up-to-date as ours
		if req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			r.currentTerm = req.Term
			r.votedFor = req.CandidateId
			r.persist()
			return &pb.VoteResponse{Term: r.currentTerm, VoteGranted: true}, nil
		}
	}

	return &pb.VoteResponse{Term: r.currentTerm, VoteGranted: false}, nil
}

// AppendEntries RPC handler
func (r *Raft) AppendEntries(ctx context.Context, req *pb.EntriesRequest) (*pb.EntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Reject if term is outdated.
	if req.Term < r.currentTerm {
		return &pb.EntriesResponse{Term: r.currentTerm, Success: false}, nil
	}

	// Update term/state if needed.
	if req.Term > r.currentTerm {
		r.stepDown(req.Term)
	} else {
		r.state = Follower
	}

	var index uint64
	// If PrevLogIndex is positive, check that the follower's log contains an entry at that index.
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex >= uint64(len(r.log)) {
			return &pb.EntriesResponse{Term: r.currentTerm, Success: false}, nil
		}
		// The follower's log is 1-indexed (dummy at index 0), so check entry at PrevLogIndex.
		if r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			// Conflict: delete the conflicting entry and everything after it.
			r.log = r.log[:req.PrevLogIndex]
			return &pb.EntriesResponse{Term: r.currentTerm, Success: false}, nil
		}
		index = req.PrevLogIndex + 1
	} else {
		// If PrevLogIndex is 0 ensure the dummy entry is present.
		if len(r.log) == 0 {
			r.log = []pb.LogEntry{{Term: 0, Command: ""}}
		}
		index = 1
	}

	// Append new entries, replacing any conflicting entries.
	for i, newEntry := range req.Entries {
		pos := index + uint64(i)
		if pos < uint64(len(r.log)) {
			// If there is a conflict, delete this entry and all that follow.
			if r.log[pos].Term != newEntry.Term {
				r.log = r.log[:pos]
				r.log = append(r.log, *newEntry)
				for j := i + 1; j < len(req.Entries); j++ {
					r.log = append(r.log, *req.Entries[j])
				}
				break
			}
			// Otherwise the entry matches; no action needed.
		} else {
			// Append new entries.
			r.log = append(r.log, *newEntry)
		}
	}

	// Update commit index without exceeding the index of the last log entry.
	if req.LeaderCommit > r.commitIndex && len(r.log) > 0 {
		r.commitIndex = min(req.LeaderCommit, uint64(len(r.log)-1))
		go r.applyCommitted()
	}

	return &pb.EntriesResponse{Term: r.currentTerm, Success: true}, nil
}

// Submit adds a new command to the log
// Add robust submission with leader tracking
func (r *Raft) Submit(command string) (uint64, error) {
    const maxAttempts = 5
    var lastLeader string
    
    for attempt := 0; attempt < maxAttempts; attempt++ {
        r.mu.RLock()
        if r.state == Leader {
            index := uint64(len(r.log))
            entry := pb.LogEntry{
                Term:    r.currentTerm,
                Command: command,
            }
            r.log = append(r.log, entry)
            r.persist()
            r.mu.RUnlock()
            return index, nil
        }
        
        // Track last known leader
        if lastLeader == "" {
            lastLeader = r.votedFor
        }
        r.mu.RUnlock()

        // Check last known leader
        if lastLeader != "" {
            if client, ok := r.peerClients[lastLeader]; ok {
                ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
                defer cancel()
                if _, err := client.AppendEntries(ctx, &pb.EntriesRequest{}); err == nil {
                    continue
                }
            }
        }
        
        // Find new leader
        time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
    }
    return 0, errors.New("max submission attempts exceeded")
}
func (r *Raft) findCurrentLeader() string {
    r.mu.RLock()
    defer r.mu.RUnlock()
    for id, client := range r.peerClients {
        ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
        defer cancel()
        if _, err := client.AppendEntries(ctx, &pb.EntriesRequest{Term: r.currentTerm}); err == nil {
            return id
        }
    }
    return ""
}
// replicateLog replicates log entries to all followers
func (r *Raft) replicateLog() {
	r.mu.RLock()
	if r.state != Leader {
		r.mu.RUnlock()
		return
	}

	// Send AppendEntries to each follower
	for id := range r.peerClients {
		if id != r.config.ID {
			go func(peerID string) {
				for {
					select {
					case <-r.stopCh:
						return
					default:
						r.syncLog(peerID)
						time.Sleep(r.config.HeartbeatTimeout / 2)
					}
				}
			}(id)
		}
	}
	r.mu.RUnlock()
}

// syncLog synchronizes log with a single follower
func (r *Raft) syncLog(peerID string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	// Initialize nextIndex if needed
	if _, ok := r.nextIndex[peerID]; !ok {
		r.nextIndex[peerID] = uint64(len(r.log))
		r.matchIndex[peerID] = 0
	}

	prevLogIndex := uint64(0)
	if r.nextIndex[peerID] > 0 {
		prevLogIndex = r.nextIndex[peerID] - 1
	}

	prevLogTerm := uint64(0)
	if prevLogIndex < uint64(len(r.log)) && prevLogIndex >= 0 {
		prevLogTerm = r.log[prevLogIndex].Term
	}

	entries := make([]*pb.LogEntry, 0)
	if r.nextIndex[peerID] < uint64(len(r.log)) {
		entries = convertToPointerSlice(r.log[r.nextIndex[peerID]:])
	}

	req := &pb.EntriesRequest{
		Term:         r.currentTerm,
		LeaderId:     r.config.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := r.peerClients[peerID].AppendEntries(ctx, req)
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.stepDown(resp.Term)
		return
	}

	if resp.Success {
		r.nextIndex[peerID] = uint64(len(r.log))
		r.matchIndex[peerID] = r.nextIndex[peerID] - 1
		r.updateCommitIndex()
	} else {
		if r.nextIndex[peerID] > 0 {
			r.nextIndex[peerID]--
		}
	}
}

// Stop gracefully stops the Raft node
func (r *Raft) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.state = Follower // Reset state before stopping
	r.mu.Unlock()

	close(r.stopCh)
	if r.rpcServer != nil {
		r.rpcServer.GracefulStop()
	}
	// Close all peer connections
	for _, client := range r.peerClients {
		if conn, ok := client.(interface{ Close() error }); ok {
			conn.Close()
		}
	}
}

// getState returns the current state of the Raft node
func (r *Raft) getState() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}
const preVoteEnabled = true
// runCandidate implements the candidate state
func (r *Raft) runCandidate() {
	if preVoteEnabled {
        // Run pre-vote phase first
        if !r.collectPreVotes() {
            return
        }
    }
	r.mu.Lock()
	r.currentTerm++
	r.votedFor = r.config.ID
	currentTerm := r.currentTerm
	lastLogIndex := r.getLastLogIndex()
	lastLogTerm := r.getLastLogTerm()
	r.persist()
	r.mu.Unlock()

	votes := 1
	voteCh := make(chan bool, len(r.config.Peers))

	// Request votes from all peers
	for peerID := range r.peerClients {
		if peerID == r.config.ID {
			continue
		}
		go func(peer string) {
			resp, err := r.peerClients[peer].RequestVote(context.Background(), &pb.VoteRequest{
				Term:         currentTerm,
				CandidateId:  r.config.ID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err == nil && resp.VoteGranted {
				voteCh <- true
			}
		}(peerID)
	}

	// Wait for votes or timeout
	timer := time.NewTimer(r.config.ElectionTimeout)
	defer timer.Stop()

	for {
		select {
		case <-voteCh:
			votes++
			if votes > len(r.config.Peers)/2 {
				r.mu.Lock()
				if r.state == Candidate && r.currentTerm == currentTerm {
					r.state = Leader
					// Initialize leader state
					for peer := range r.config.Peers {
						r.nextIndex[peer] = r.getLastLogIndex() + 1
						r.matchIndex[peer] = 0
					}
				}
				r.mu.Unlock()
				return
			}
		case <-timer.C:
			return
		case <-r.stopCh:
			return
		}
	}
}
func (r *Raft) collectPreVotes() bool {
    r.mu.Lock()
    currentTerm := r.currentTerm
    lastLogIndex := r.getLastLogIndex()
    lastLogTerm := r.getLastLogTerm()
    r.mu.Unlock()

    votes := 1
    var wg sync.WaitGroup
    var mu sync.Mutex

    for peerID := range r.peerClients {
        wg.Add(1)
        go func(id string) {
            defer wg.Done()
            resp, err := r.peerClients[id].RequestVote(context.Background(), &pb.VoteRequest{
                Term:         currentTerm + 1,
                CandidateId:  r.config.ID,
                LastLogIndex: lastLogIndex,
                LastLogTerm:  lastLogTerm,
            })
            
            if err == nil && resp.VoteGranted {
                mu.Lock()
                votes++
                mu.Unlock()
            }
        }(peerID)
    }

    wg.Wait()
    return votes > len(r.peerClients)/2
}
// runLeader implements the leader state
func (r *Raft) runLeader() {
	heartbeatTicker := time.NewTicker(r.config.HeartbeatTimeout)
    defer heartbeatTicker.Stop()

    
	r.mu.Lock()
	for peer := range r.peerClients {
		r.nextIndex[peer] = uint64(len(r.log))
		r.matchIndex[peer] = 0
	}
	r.mu.Unlock()

	// Start heartbeat ticker
	ticker := time.NewTicker(r.config.HeartbeatTimeout / 2)
	defer ticker.Stop()
	minInterval := r.config.HeartbeatTimeout / 2
	maxInterval := r.config.HeartbeatTimeout
	currentInterval := minInterval

	// Send initial heartbeats
	r.broadcastAppendEntries()

	for {
		select {
		case <-heartbeatTicker.C:
            if !r.broadcastAppendEntries() {
                return
            }
        case <-r.stopCh:
            return
        
		case <-time.After(currentInterval):
			if r.broadcastAppendEntries() {
				currentInterval = minInterval // Reset to fast mode
			} else {
				currentInterval = time.Duration(float64(currentInterval) * 1.5)
				if currentInterval > maxInterval {
					currentInterval = maxInterval
				}
			}

		case <-r.stopCh:
			return
		case <-ticker.C:
			if !r.broadcastAppendEntries() {
				return
			}
		}
	}
}

// broadcastAppendEntries sends AppendEntries RPCs to all peers
func (r *Raft) broadcastAppendEntries() bool {
	log.Printf("Leader %s broadcasting heartbeats", r.config.ID)
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return false
	}
	term := r.currentTerm
	r.mu.Unlock()

	responses := make(chan bool, len(r.peerClients))
	for peerID, client := range r.peerClients {
		if peerID == r.config.ID {
			continue
		}
		go func(peerID string, client pb.RaftClient) {
			success := r.sendAppendEntries(peerID, client)
			responses <- success
		}(peerID, client)
	}

	// Wait for majority responses
	successCount := 1 // Count self
	failCount := 0
	majority := (len(r.peerClients) + 1) / 2
	for i := 0; i < len(r.peerClients)-1; i++ {
		if <-responses {
			successCount++
			if successCount > majority {
				return true
			}
		} else {
			failCount++
			if failCount >= majority {
				r.mu.Lock()
				if r.state == Leader && r.currentTerm == term {
					r.stepDown(r.currentTerm)
				}
				r.mu.Unlock()
				return false
			}
		}
	}
	return true
}

// sendAppendEntries sends a single AppendEntries RPC to a peer
func (r *Raft) sendAppendEntries(peerID string, client pb.RaftClient) bool {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return false
	}

	prevLogIndex := r.nextIndex[peerID] - 1
	var prevLogTerm uint64
	if prevLogIndex < uint64(len(r.log)) {
		prevLogTerm = r.log[prevLogIndex].Term
	}

	entries := r.log[r.nextIndex[peerID]:]
	pbEntries := make([]*pb.LogEntry, len(entries))
	for i, entry := range entries {
		pbEntries[i] = &pb.LogEntry{
			Term:    entry.Term,
			Command: entry.Command,
		}
	}

	req := &pb.EntriesRequest{
		Term:         r.currentTerm,
		LeaderId:     r.config.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: r.commitIndex,
	}
	term := r.currentTerm
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.config.HeartbeatTimeout)
	defer cancel()

	resp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.stepDown(resp.Term)
		return false
	}

	if r.state != Leader || r.currentTerm != term {
		return false
	}

	if resp.Success {
		r.nextIndex[peerID] = prevLogIndex + uint64(len(pbEntries)) + 1
		r.matchIndex[peerID] = r.nextIndex[peerID] - 1
		r.updateCommitIndex()
		return true
	}

	// If AppendEntries fails, decrement nextIndex and retry
	if r.nextIndex[peerID] > 1 {
		r.nextIndex[peerID]--
	}
	return false
}

// runFollower implements the follower state
func (r *Raft) runFollower() {
	electionTimeout := time.Duration(rand.Int63n(int64(r.config.ElectionTimeout))) + r.config.ElectionTimeout
	timer := time.NewTimer(electionTimeout)
	defer timer.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-timer.C:
			r.mu.Lock()
			if r.state == Follower {
				r.state = Candidate
				r.currentTerm++
				r.votedFor = r.config.ID // vote for self
				r.persist()
			}
			r.mu.Unlock()
			return
		}
	}
}

// stepDown updates term and converts to follower state
func (r *Raft) stepDown(term uint64) {
	log.Printf("Node %s stepping down from term %d to %d", r.config.ID, r.currentTerm, term)
	if term > r.currentTerm {
		r.currentTerm = term
		r.state = Follower
		r.votedFor = ""
		r.persist()
	}
}

// persist saves the current state
func (r *Raft) persist() {
	if err := r.storage.SaveState(r.currentTerm, r.votedFor); err != nil {
		log.Printf("Error saving state: %v", err)
	}
	if err := r.storage.SaveLog(r.log); err != nil {
		log.Printf("Error saving log: %v", err)
	}
}

// Helper function to convert []LogEntry to []*LogEntry
func convertToPointerSlice(entries []pb.LogEntry) []*pb.LogEntry {
	result := make([]*pb.LogEntry, len(entries))
	for i := range entries {
		result[i] = &entries[i]
	}
	return result
}

// getLastLogIndex returns the index of the last real entry (ignoring the dummy).
func (r *Raft) getLastLogIndex() uint64 {
	if len(r.log) == 0 {
		return 0
	}
	// We assume r.log[0] is always the dummy.
	return uint64(len(r.log) - 1)
}

// getLastLogTerm returns the term of the last real log entry.
func (r *Raft) getLastLogTerm() uint64 {
	if len(r.log) == 0 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
