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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type PeerRole byte

const (
	Follower  = 0x0
	Candidate = 0x1
	Leader    = 0x2
)

type CandidateVoted interface{}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                   sync.Mutex          // Lock to protect shared access to this peer's state
	peers                []*labrpc.ClientEnd // RPC end points of all peers
	persister            *Persister          // Object to hold this peer's persisted state
	me                   int                 // this peer's index into peers[]
	applyChan            chan ApplyMsg
	currentTerm          int
	isLeader             bool
	votedFor             CandidateVoted
	log                  []LogEntry
	role                 PeerRole
	electionTimeoutReset chan bool
	//volatile
	commitIndex int
	lastApplied int
	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//this is the heartbeat system

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (3A).
	go func() { rf.electionTimeoutReset <- true }()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Function skeleton for log persistence.
	// **Not required** for CS4740 Fall24 Lab3. You may skip.
	// (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Function skeleton for log persistence.
	// **Not required** for CS4740 Fall24 Lab3. You may skip.
	// (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

type AppendEntriesArgs struct {
	// all of these are leader's values
	Term         int // leader's term
	Id           int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	*reply = AppendEntriesReply{rf.currentTerm, false}
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		(*reply).Term = rf.currentTerm
	}
	logLastIndex := len(rf.log) - 1
	// election cleanup
	if rf.role != Follower {
		rf.role = Follower
	}
	if rf.votedFor != nil {
		rf.votedFor = nil
	}
	go func() { rf.electionTimeoutReset <- true }()

	if len(rf.log) == 0 {
		(*reply).Success =
			args.PrevLogIndex <= logLastIndex && rf.log[logLastIndex].Term == args.PrevLogTerm
		return
	}

	// Return failure if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex <= logLastIndex && //if log long enough to be able to check index
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	} //if index doesn't match
	for i, entry := range args.Entries {
		if entry.Index <= logLastIndex && //if log long enough to be able to check index
			entry.Term != rf.log[entry.Index].Term || entry.Command != rf.log[entry.Index].Command { //if it conflicts
			// removing indices in go https://stackoverflow.com/a/57213476
			newLog := make([]LogEntry, 0, entry.Index+len(args.Entries))
			// for given log [0:a, 1:b, 2:c, 3:d] and entries [2:c, 3:e, 4:f]
			// when i = 0, entry.Index is 2, and log[2] (2:c) == entries[0] (2:c)
			// when i = 1, entry.Index is 3, and log[3] (3:d) != entries[1] (3:e)
			// because log[3] does not equal entries[1],
			// newLog is set to a slice of rf.log of indices 0 to entryIndex - 1 (2)
			// then newLog, currently [0:a, 1:b, 2:c] has entries starting at i appended to it ([3:e, 4:f])
			// newLog is now [0:a, 1:b, 2:c, 3:e, 4:f], to which rf.log is set
			newLog = append(newLog, rf.log[:entry.Index]...)
			newLog = append(newLog, args.Entries[i:]...)
			rf.log = newLog
			break
		}
	}
	if args.CommitIndex > rf.commitIndex {
		lastEntryIndex := args.Entries[len(args.Entries)-1].Index
		if args.CommitIndex > lastEntryIndex { //int min
			rf.commitIndex = lastEntryIndex
		} else {
			rf.commitIndex = args.CommitIndex
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if args.Term > rf.currentTerm { //leader or candidate steps down
		rf.currentTerm = args.Term
		rf.role = Follower
	}

	*reply = RequestVoteReply{rf.currentTerm, false}
	if rf.votedFor != nil {
		return
	}
	last := rf.log[len(rf.log)-1]
	if last.Term > args.LastLogTerm || (last.Term == args.LastLogTerm && last.Index > args.LastLogIndex) {
		return
	}
	(*reply).VoteGranted = true
	rf.votedFor = args.CandidateId
	return

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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.role == Leader {
		return len(rf.log), rf.currentTerm, false
	}
	// Your code here (3B).
	go func() {}()
	return len(rf.log), rf.currentTerm, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) ElectionTimeout() {
	const min = 500
	const max = 1000
	for {
		duration := time.Duration(rand.Int()%(max+1-min) + min)
		select {
		case <-time.After(duration * time.Millisecond):
			if rf.role == Leader {
				continue
			} else {
				go rf.BeCandidate() // runs the election process, resets it if candidate
			}
		case <-rf.electionTimeoutReset:
			continue
		}
	}
}

func (rf *Raft) BeCandidate() {
	rf.electionTimeoutReset <- true
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.role = Candidate
	rf.mu.Unlock()
	timeout := 50 * time.Millisecond
	last := rf.log[len(rf.log)-1]
	voteChan := make(chan bool)
	request := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogIndex: last.Index,
		LastLogTerm:  last.Term,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func() { //send out vote requests in parallel
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &request, &reply)
			if !ok {
				reply.VoteGranted = false
			}
			voteChan <- reply.VoteGranted
		}()
	}

	// votes for itself
	rf.votedFor = rf.me
	votes := 1
	// collects votes
	length := len(rf.peers) - 1 //skip self
	i := 0
	for ; i < length; i++ {
		select {
		case m := <-voteChan:
			if m {
				votes += 1
			}
			if votes >= (len(rf.peers)/2 + 1) { // 5 / 2 = 2, 2 + 1 = 3, 3 is the majority
				go rf.BeLeader()
				break
			}
		case <-time.After(timeout):
			if rf.role != Candidate {
				break
			}
			i--
			continue
		}
	}
	for ; i < length; i++ {
		<-voteChan //cleans up threads
	}
}

func (rf *Raft) SendHeartbeat() {
	if rf.role != Leader {
		return
	}
	last := rf.log[len(rf.log)-1]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		Id:           rf.me,
		PrevLogIndex: last.Index,
		PrevLogTerm:  last.Term,
		Entries:      nil,
		CommitIndex:  0,
	}
	for i := 0; i < len(rf.peers)-1; i++ {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(i, &args, &reply)
	}
}

func (rf *Raft) BeLeader() {
	rf.role = Leader
	go rf.SendHeartbeat()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.isLeader = false
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.applyChan = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	// Your initialization code here (3A, 3B).
	go rf.ElectionTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
