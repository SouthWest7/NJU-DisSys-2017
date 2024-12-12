package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"a2/src/labrpc"
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

type State string

const (
	Leader    State = "Leader"
	Candidate State = "Candidate"
	Follower  State = "Follower"
)

const HeartbeatInterval = 60 * time.Millisecond

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// non persistent state
	state       State
	commitIndex int
	lastApplied int

	electionTimer *time.Timer
	online        bool
	// resetChan is the channel to reset electionTimer
	resetChan chan bool

	// only leader
	nextIndex  []int
	matchIndex []int
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil {
		return
	}
	if e.Encode(rf.votedFor) != nil {
		return
	}
	if e.Encode(rf.logs) != nil {
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil {
		return
	}
	if d.Decode(&rf.votedFor) != nil {
		return
	}
	if d.Decode(&rf.logs) != nil {
		return
	}
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	//lastLogIndex := rf.lastLog().Index
	//for i, _ := range rf.peers {
	//	rf.nextIndex[i] = lastLogIndex + 1
	//	rf.matchIndex[i] = 0
	//}
	rf.startLeader()
}

func (rf *Raft) convertToCandidate() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
}

func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
}

// RequestVoteArgs example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs example AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	// if the server's current term is less than term of the request, reject.
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	rf.resetChan <- true

	if rf.currentTerm < args.Term {
		rf.convertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// if the server vote for others, set VoteGranted false.
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// Vote for candidate and update
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
}

// AppendEntries example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false

	// if the server's current term is less than term of the request, reject.
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	rf.convertToFollower(args.Term)

	rf.resetChan <- true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	_, isLeader := rf.GetState()

	return index, term, isLeader
}

// Kill the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.online = false
	rf.persist()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.convertToCandidate()
	rf.electionTimer.Reset(randomElectionTimeout())
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	ch := make(chan int, len(rf.peers))
	rf.mu.Unlock()

	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}
		go func() {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(serverId, args, reply) {
				if rf.state != Candidate || rf.currentTerm != args.Term {
					ch <- 0
				} else if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.convertToFollower(reply.Term)
					rf.mu.Unlock()
					ch <- 0
				} else if !reply.VoteGranted {
					ch <- 0
				} else {
					ch <- 1
				}
			} else {
				ch <- 0
			}
		}()
	}

	voteCnt := 1
	totalVoteCnt := 1
	for ok := range ch {
		if ok == 1 {
			voteCnt++
		}
		totalVoteCnt++
		if voteCnt > len(rf.peers)/2 {
			rf.mu.Lock()
			rf.convertToLeader()
			rf.mu.Unlock()
		}
		if totalVoteCnt == len(rf.peers) {
			break
		}
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		currentTerm:   0,
		votedFor:      -1,
		logs:          make([]LogEntry, 0),
		state:         Follower,
		electionTimer: time.NewTimer(randomElectionTimeout()),
		online:        true,
		resetChan:     make(chan bool),
	}

	// Your initialization code here.
	go rf.runElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// Election loop
func (rf *Raft) runElectionTimer() {
	for rf.online {
		select {
		case <-rf.resetChan:
			rf.electionTimer.Reset(randomElectionTimeout())

		case <-rf.electionTimer.C:
			if rf.state != Leader {
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) startLeader() {
	go func() {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()

		for rf.state == Leader {
			rf.broadcastHeartbeat()
			<-ticker.C
		}
	}()
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	for serverId := range rf.peers {
		if serverId != rf.me && rf.state == Leader {
			go rf.sendAppendEntries(serverId, args, &AppendEntriesReply{})
		}
	}
}
