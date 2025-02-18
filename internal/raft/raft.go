// // Package raft implements the Raft consensus protocol
// package raft

// import (
//     "context"
//     "sync"
//     "time"
//     "encoding/json"
//     "errors"
//     "log"
// )

// // State represents the role of a Raft node
// type State int

// const (
//     Follower State = iota
//     Candidate
//     Leader
// )

// // Config contains configuration for a Raft node
// type Config struct {
//     ID string
//     Peers []string
//     HeartbeatTimeout time.Duration
//     ElectionTimeout  time.Duration
//     Storage         Storage
// }

// // LogEntry represents a single entry in the Raft log
// type LogEntry struct {
//     Term    uint64
//     Index   uint64
//     Command []byte
// }

// // Raft represents a single node in the Raft cluster

// // ApplyMsg represents a message to be applied to the state machine


// // Storage interface defines persistence operations
// type Storage interface {
//     SaveState(term uint64, votedFor string) error
//     LoadState() (term uint64, votedFor string, err error)
//     SaveLog(entries []LogEntry) error
//     LoadLog() ([]LogEntry, error)
// }

// // NewRaft creates a new Raft node
// func NewRaft(config Config) (*Raft, error) {
//     if err := validateConfig(config); err != nil {
//         return nil, err
//     }

//     r := &Raft{
//         config:      config,
//         state:       Follower,
//         nextIndex:   make(map[string]uint64),
//         matchIndex:  make(map[string]uint64),
//         applyCh:     make(chan ApplyMsg, 1000),
//         stopCh:      make(chan struct{}),
//         storage:     config.Storage,
//     }

//     // Initialize from storage
//     if err := r.loadPersistedState(); err != nil {
//         return nil, err
//     }

//     // Start background routines
//     go r.run()

//     return r, nil
// }

// // Submit submits a new command to the Raft cluster
// func (r *Raft) Submit(command []byte) (uint64, error) {
//     r.mu.Lock()
//     if r.state != Leader {
//         r.mu.Unlock()
//         return 0, errors.New("not leader")
//     }

//     entry := LogEntry{
//         Term:    r.currentTerm,
//         Index:   uint64(len(r.log)),
//         Command: command,
//     }
    
//     r.log = append(r.log, entry)
//     r.persist()
//     r.mu.Unlock()

//     // Start replication to followers
//     r.replicateLog()

//     return entry.Index, nil
// }



// // startElection begins a new election round
// func (r *Raft) startElection() {
//     r.mu.Lock()
//     r.state = Candidate
//     r.currentTerm++
//     r.votedFor = r.config.ID
//     r.persist()
    
//     term := r.currentTerm
//     lastLogIndex := uint64(len(r.log) - 1)
//     lastLogTerm := uint64(0)
//     if lastLogIndex >= 0 {
//         lastLogTerm = r.log[lastLogIndex].Term
//     }
//     r.mu.Unlock()

//     votes := 1 // Vote for self
//     voteCh := make(chan bool, len(r.config.Peers))

//     // Request votes from all peers
//     for _, peer := range r.config.Peers {
//         if peer == r.config.ID {
//             continue
//         }
        
//         go func(peer string) {
//             args := &RequestVoteArgs{
//                 Term:         term,
//                 CandidateID:  r.config.ID,
//                 LastLogIndex: lastLogIndex,
//                 LastLogTerm:  lastLogTerm,
//             }
            
//             var reply RequestVoteReply
//             if err := r.sendRequestVote(peer, args, &reply); err != nil {
//                 voteCh <- false
//                 return
//             }
            
//             voteCh <- reply.VoteGranted
//         }(peer)
//     }

//     // Count votes
//     for i := 0; i < len(r.config.Peers)-1; i++ {
//         if <-voteCh {
//             votes++
//             if votes > len(r.config.Peers)/2 {
//                 r.becomeLeader()
//                 return
//             }
//         }
//     }
// }

// // becomeLeader transitions the node to leader state
// func (r *Raft) becomeLeader() {
//     r.mu.Lock()
//     defer r.mu.Unlock()

//     r.state = Leader
    
//     // Initialize leader state
//     lastLogIndex := uint64(len(r.log))
//     for _, peer := range r.config.Peers {
//         if peer != r.config.ID {
//             r.nextIndex[peer] = lastLogIndex
//             r.matchIndex[peer] = 0
//         }
//     }

//     // Start sending heartbeats
//     go r.sendHeartbeats()
// }

// // replicateLog replicates log entries to followers
// func (r *Raft) replicateLog() {
//     r.mu.RLock()
//     defer r.mu.RUnlock()

//     for _, peer := range r.config.Peers {
//         if peer == r.config.ID {
//             continue
//         }
        
//         go r.syncLog(peer)
//     }
// }

// // syncLog synchronizes log with a single follower
// func (r *Raft) syncLog(peer string) {
//     r.mu.RLock()
//     prevLogIndex := r.nextIndex[peer] - 1
//     prevLogTerm := uint64(0)
//     if prevLogIndex < uint64(len(r.log)) {
//         prevLogTerm = r.log[prevLogIndex].Term
//     }
    
//     entries := r.log[r.nextIndex[peer]:]
//     args := &AppendEntriesArgs{
//         Term:         r.currentTerm,
//         LeaderID:     r.config.ID,
//         PrevLogIndex: prevLogIndex,
//         PrevLogTerm:  prevLogTerm,
//         Entries:      entries,
//         LeaderCommit: r.commitIndex,
//     }
//     r.mu.RUnlock()

//     var reply AppendEntriesReply
//     if err := r.sendAppendEntries(peer, args, &reply); err != nil {
//         return
//     }

//     if reply.Success {
//         r.mu.Lock()
//         r.nextIndex[peer] += uint64(len(entries))
//         r.matchIndex[peer] = r.nextIndex[peer] - 1
//         r.updateCommitIndex()
//         r.mu.Unlock()
//     } else {
//         r.mu.Lock()
//         if r.nextIndex[peer] > 1 {
//             r.nextIndex[peer]--
//         }
//         r.mu.Unlock()
//     }
// }

// // persist saves Raft state to stable storage
// func (r *Raft) persist() error {
//     if err := r.storage.SaveState(r.currentTerm, r.votedFor); err != nil {
//         return err
//     }
//     return r.storage.SaveLog(r.log)
// }


// Package raft implements the Raft consensus protocol
package raft

import (
    "context"
    "sync"
    "time"
    "encoding/json"
    "errors"
    "net"
    "os"
    "path/filepath"
    pb "Distributed-Key-Value-Store/raft/proto"
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
    ID          string
    Peers       map[string]string // map[ID]Address
    DataDir     string
    RPCPort     string
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
    mu          sync.RWMutex
    config      Config
    
    // Persistent state
    currentTerm uint64
    votedFor    string
    log         []pb.LogEntry
    
    // Volatile state
    commitIndex uint64
    lastApplied uint64
    state       State
    
    // Leader state
    nextIndex   map[string]uint64
    matchIndex  map[string]uint64
    
    // Channels
    applyCh     chan ApplyMsg
    stopCh      chan struct{}
    
    // RPC clients
    peerClients map[string]pb.RaftClient
    rpcServer   *grpc.Server
    
    // Storage
    storage     *FileStorage
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
    
    return &FileStorage{
        stateFile: filepath.Join(dataDir, "state.json"),
        logFile:   filepath.Join(dataDir, "log.json"),
    }, nil
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
    data, err := json.Marshal(entries)
    if err != nil {
        return err
    }
    
    return os.WriteFile(fs.logFile, data, 0644)
}

// LoadLog loads the persisted Raft log
func (fs *FileStorage) LoadLog() ([]pb.LogEntry, error) {
    data, err := os.ReadFile(fs.logFile)
    if os.IsNotExist(err) {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    
    var entries []pb.LogEntry
    if err := json.Unmarshal(data, &entries); err != nil {
        return nil, err
    }
    
    return entries, nil
}
// loadPersistedState loads Raft state from stable storage
func (r *Raft) loadPersistedState() error {
    term, votedFor, err := r.storage.LoadState()
    if err != nil {
        return err
    }
    
    r.currentTerm = term
    r.votedFor = votedFor

    log, err := r.storage.LoadLog()
    if err != nil {
        return err
    }
    
    r.log = log
    return nil
}

// updateCommitIndex updates the commit index based on matchIndex values
func (r *Raft) updateCommitIndex() {
    for n := r.commitIndex + 1; n < uint64(len(r.log)); n++ {
        if r.log[n].Term != r.currentTerm {
            continue
        }
        
        count := 1
        for _, peer := range r.config.Peers {
            if peer != r.config.ID && r.matchIndex[peer] >= n {
                count++
            }
        }
        
        if count > len(r.config.Peers)/2 {
            r.commitIndex = n
            r.applyCommitted()
        }
    }
}

// applyCommitted applies committed log entries to the state machine
func (r *Raft) applyCommitted() {
    for r.lastApplied < r.commitIndex {
        r.lastApplied++
        r.applyCh <- ApplyMsg{
            CommandValid: true,
            Command:     r.log[r.lastApplied].Command,
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
        state:       Follower,
        nextIndex:   make(map[string]uint64),
        matchIndex:  make(map[string]uint64),
        applyCh:     make(chan ApplyMsg, 1000),
        stopCh:      make(chan struct{}),
        storage:     storage,
        peerClients: make(map[string]pb.RaftClient),
    }

    // Initialize from storage
    if err := r.loadPersistedState(); err != nil {
        return nil, err
    }

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

    if r.votedFor == "" || r.votedFor == req.CandidateId {
        lastLogIndex := uint64(len(r.log) - 1)
        lastLogTerm := uint64(0)
        if lastLogIndex >= 0 {
            lastLogTerm = r.log[lastLogIndex].Term
        }

        if req.LastLogTerm > lastLogTerm ||
           (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
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

    if req.Term < r.currentTerm {
        return &pb.EntriesResponse{Term: r.currentTerm, Success: false}, nil
    }

    if req.Term > r.currentTerm {
        r.stepDown(req.Term)
    }

    // Reset election timer as we received valid AppendEntries from current leader
    r.state = Follower

    // Check previous log entry
    if req.PrevLogIndex >= uint64(len(r.log)) ||
       (req.PrevLogIndex >= 0 && r.log[req.PrevLogIndex].Term != req.PrevLogTerm) {
        return &pb.EntriesResponse{Term: r.currentTerm, Success: false}, nil
    }

    // Append new entries, truncating conflicting entries
    if len(req.Entries) > 0 {
        newEntries := make([]pb.LogEntry, len(req.Entries))
        copy(newEntries, req.Entries)
        r.log = append(r.log[:req.PrevLogIndex+1], newEntries...)
        r.persist()
    }

    // Update commit index
    if req.LeaderCommit > r.commitIndex {
        r.commitIndex = min(req.LeaderCommit, uint64(len(r.log)-1))
        go r.applyCommitted()
    }

    return &pb.EntriesResponse{Term: r.currentTerm, Success: true}, nil
}

// Submit submits a new command to the Raft cluster
func (r *Raft) Submit(command string) (uint64, error) {
    r.mu.Lock()
    if r.state != Leader {
        r.mu.Unlock()
        return 0, errors.New("not leader")
    }

    entry := pb.LogEntry{
        Term:    r.currentTerm,
        Command: command,
    }
    
    r.log = append(r.log, entry)
    index := uint64(len(r.log) - 1)
    r.persist()
    r.mu.Unlock()

    // Start replication to followers
    r.replicateLog()

    return index, nil
}

// replicateLog replicates log entries to all followers
func (r *Raft) replicateLog() {
    for id := range r.peerClients {
        go r.syncLog(id)
    }
}

// syncLog synchronizes log with a single follower
func (r *Raft) syncLog(peerID string) {
    r.mu.RLock()
    if r.state != Leader {
        r.mu.RUnlock()
        return
    }

    prevLogIndex := r.nextIndex[peerID] - 1
    prevLogTerm := uint64(0)
    if prevLogIndex >= 0 && prevLogIndex < uint64(len(r.log)) {
        prevLogTerm = r.log[prevLogIndex].Term
    }

    entries := r.log[r.nextIndex[peerID]:]
    req := &pb.EntriesRequest{
        Term:         r.currentTerm,
        LeaderId:     r.config.ID,
        PrevLogIndex: prevLogIndex,
        PrevLogTerm:  prevLogTerm,
        Entries:      entries,
        LeaderCommit: r.commitIndex,
    }
    r.mu.RUnlock()

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
        r.nextIndex[peerID] += uint64(len(entries))
        r.matchIndex[peerID] = r.nextIndex[peerID] - 1
        r.updateCommitIndex()
    } else {
        // Decrement nextIndex and try again
        if r.nextIndex[peerID] > 1 {
            r.nextIndex[peerID]--
        }
    }
}

// Stop gracefully stops the Raft node
func (r *Raft) Stop() {
    close(r.stopCh)
    r.rpcServer.GracefulStop()
    
    for _, client := range r.peerClients {
        if conn, ok := client.(*grpc.ClientConn); ok {
            conn.Close()
        }
    }
}