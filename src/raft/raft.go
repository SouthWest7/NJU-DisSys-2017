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

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// state
	state int // 0 follower 1 leader 2 candidate

	// channels
	chanApply chan ApplyMsg

	//vote count
	voteCount int

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// timer
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// Log Entry
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

const (
	Follower  = 0
	Leader    = 1
	Candidate = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.electionTimer.Reset(RandomElectionInterval())
	}
}

func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
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

// example AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.NextIndex = rf.getLastLogIndex() + 1
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.electionTimer.Reset(RandomElectionInterval())

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		reply.Success = false
		return
	}

	if args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		term := rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != term {
				reply.NextIndex = i + 1
				break
			}
		}
		reply.Success = false
		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	reply.NextIndex = args.PrevLogIndex + 1 + len(args.Entries)

	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		go rf.applyLogEntries()
	}

}

func (rf *Raft) applyLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				Index:   i,
				Command: rf.log[i].Command,
			}
			rf.chanApply <- msg
		}
	}
	rf.lastApplied = rf.commitIndex
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != Leader || args.Term != rf.currentTerm {
		return ok
	}
	if rf.currentTerm < reply.Term {
		rf.state = Follower
		rf.electionTimer.Reset(RandomElectionInterval())
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
	} else {
		rf.nextIndex[server] = reply.NextIndex
		if reply.Success {
			rf.matchIndex[server] = reply.NextIndex - 1
		}
		for n := rf.getLastLogIndex(); n > rf.commitIndex && rf.log[n].Term == rf.currentTerm; n-- {
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				go rf.applyLogEntries()
				break
			}
		}
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false
	if rf.state == Leader {
		index = rf.getLastLogIndex() + 1
		term = rf.currentTerm
		isLeader = true
		rf.log = append(rf.log, Entry{Index: index, Term: term, Command: command})
	}
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
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
		peers:          peers,
		persister:      persister,
		me:             me,
		state:          Follower,
		votedFor:       -1,
		log:            make([]Entry, 1),
		currentTerm:    0,
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(FixedHeartbeatInterval()),
		electionTimer:  time.NewTimer(RandomElectionInterval()),
		chanApply:      applyCh,
	}

	// Your initialization code here.
	go rf.start()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func FixedHeartbeatInterval() time.Duration {
	return time.Millisecond * 60
}

func RandomElectionInterval() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(300)+500)
}

func (rf *Raft) start() {
	for {
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat()
			}
			rf.heartbeatTimer.Reset(FixedHeartbeatInterval())
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.state = Candidate
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.StartElection()
				rf.electionTimer.Reset(RandomElectionInterval())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) BroadcastHeartbeat() {
	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[i] - 1,
			}
			if rf.nextIndex[i] == 0 {
				// args.PrevLogIndex = -1
				args.PrevLogTerm = -1
			} else {
				// args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].Term
			}
			if rf.nextIndex[i] <= rf.getLastLogIndex() {
				log := rf.log
				args.Entries = log[rf.nextIndex[i]:]
			}
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) StartElection() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {
			go func() {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(i, args, reply) {
					rf.mu.Lock()
					if rf.state != Candidate || rf.currentTerm != args.Term {
						rf.mu.Unlock()
						return
					}
					if rf.currentTerm < reply.Term {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}
					if reply.VoteGranted {
						rf.voteCount++
						if rf.voteCount > len(rf.peers)/2 && rf.state != Leader {
							rf.state = Leader
							rf.BroadcastHeartbeat()

							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							nextIndex := rf.getLastLogIndex() + 1
							for i := range rf.peers {
								rf.nextIndex[i] = nextIndex
							}
						}
					}
					rf.mu.Unlock()
				}
			}()
		}
	}
}
