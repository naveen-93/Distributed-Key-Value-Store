package raft

import (
	"sync"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type Node struct {
	mu sync.Mutex

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         *Log

	// Volatile state
	state    NodeState
	leaderId string

	// Configuration
	id    string
	peers []string

	// Channels for communication
	voteCh   chan bool
	appendCh chan bool
}

func NewNode(id string, peers []string) *Node {
	return &Node{
		id:    id,
		peers: peers,
		log:   NewLog(),
		state: Follower,
	}
}

// Log represents the log structure.
// Add log fields as needed.
type Log struct {
	// example field: entries []Entry
}

// NewLog creates a new Log instance.
func NewLog() *Log {
	return &Log{}
}
