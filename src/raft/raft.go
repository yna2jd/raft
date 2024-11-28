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
	"fmt"
	"math/rand"
	"sort"
	"strconv"
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

const minMs = 350
const maxMs = minMs * 2
const heartbeatMs = minMs / 2

//var muCounter sync.Mutex
//var counter = 0

func getRandomMs() time.Duration {
	return getRandomMsInRange(minMs, maxMs)
}

func getRandomMsInRange(inMin int, inMax int) time.Duration {
	return time.Duration(inMax+rand.Intn(inMax-inMin)) * time.Millisecond
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int         //the term the entry was logged in
	Index   int         //the index in the leader's log where the entry goes
	Command interface{} // the command to log
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
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	applyChan   chan ApplyMsg
	currentTerm int
	votedFor    CandidateVoted
	votes       int //additional
	log         []LogEntry
	role        PeerRole
	//volatile
	commitIndex int
	lastApplied int
	committed   []ApplyMsg
	//volatile, initialized on election
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server
	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	wonElection      chan bool
	stepDownChan     chan bool
	heartbeatChan    chan bool
	updateCommitChan chan bool
}

func (rf *Raft) getLast() LogEntry {
	if len(rf.log) > rf.commitIndex {
		return rf.log[len(rf.log)-1]
	}
	DPrintf(Test, "Error: forced to return blank entry from getLast")
	return LogEntry{
		Term:    0,
		Index:   0,
		Command: -1,
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	Entries      []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	CommitIndex  int
}

type AppendEntriesReply struct {
	Term      int
	LastIndex int
	LastTerm  int
	Success   bool
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newestIndex, term := len(rf.log), rf.currentTerm
	if rf.role != Leader {
		return newestIndex, term, false
	}
	DPrintf(State, "Term %v: --- Leader %v has new index %v for command %v ---", rf.currentTerm, rf.me, newestIndex, command)
	newEntry := LogEntry{
		Term:    term,
		Index:   newestIndex,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	rf.matchIndex[rf.me] = newestIndex
	rf.nextIndex[rf.me] = newestIndex + 1
	return newestIndex, term, true
}

func (rf *Raft) propagateFollowers() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return false
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		prev := rf.log[rf.nextIndex[peer]-1] //maybe switch to next - 1?
		if prev.Index != rf.nextIndex[peer]-1 {
			DPrintf(Test, "Error: mismatched prev and next: p: %v, n: %v", prev.Index, rf.nextIndex[peer]-1)
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			Id:           rf.me,
			PrevLogIndex: prev.Index,
			PrevLogTerm:  prev.Term,
			CommitIndex:  rf.commitIndex,
		}
		args.Entries = make([]LogEntry, 0)
		args.Entries = append(args.Entries, rf.log[rf.nextIndex[peer]:]...)
		go rf.propagateCommand(peer, args)
	}
	return true
}

func (rf *Raft) propagateCommand(peer int, args AppendEntriesArgs) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	entryType := "AppendEntries"
	contents := ""

	if len(args.Entries) == 0 {
		entryType = "Heartbeat"
	} else {
		entries := ""
		for i, entry := range args.Entries {
			entries += strconv.Itoa(entry.Index) + ": " + strconv.Itoa(entry.Term)
			if i != len(args.Entries)-1 {
				entries += ", "
			}
		}
		contents = fmt.Sprintf("length %v entries {%v}", len(args.Entries), entries)
	}
	DPrintf(Count, "Term %v: %v sent %v to %v %v", rf.currentTerm, rf.me, entryType, peer, contents)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply) //sendAppendEntries
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(Count, "Term %v: %v received %v response from %v", rf.currentTerm, rf.me, entryType, peer)
	if !ok {
		DPrintf(StateFine, "Term %v: %v's AppendEntries to %v timed out", rf.currentTerm, rf.me, peer)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term, "Append entries")
		return
	}
	if reply.Term < rf.currentTerm { //stale response
		DPrintf(Election, "Term %v: %v ignored stale append response from %v: term %v", rf.currentTerm, rf.me, peer, reply.Term)
		return
	}
	if rf.role != Leader {
		return
	}
	if reply.Success {
		verifiedIndex := args.PrevLogIndex + len(args.Entries)
		if verifiedIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = verifiedIndex
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		DPrintf(Accept, "Term %v: %v's state for %v's indices: match: %v, next: %v", rf.currentTerm, rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
		DPrintf(Accept, "Term %v: %v's entries for %v (%v to %v) accepted", rf.currentTerm, rf.me, peer, args.PrevLogIndex+1, verifiedIndex)
	} else { //
		DPrintf(Accept, "Term %v: %v append for %v rejected. match: %v next: %v", reply.Term, rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
		rf.nextIndex[peer]--
		if rf.nextIndex[peer] < 0 {
			DPrintf(Test, "Fatal: nextIndex below 0 for %v, killing %v", peer, rf.me)
			rf.Kill()
		}
	}
	lviCopy := make([]int, 0, len(rf.peers))
	lviCopy = append(lviCopy, rf.matchIndex...)
	sort.Ints(lviCopy)
	majorityAccepted := lviCopy[len(lviCopy)/2]
	/*
		If there exists an N such that N > commitIndex, a majority
		of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		set commitIndex = N (§5.3, §5.4).
	*/
	if majorityAccepted > rf.commitIndex && rf.log[majorityAccepted].Term == rf.currentTerm { //
		rf.commitIndex = majorityAccepted
		select {
		case rf.updateCommitChan <- true:
		default:
		}
	}
	rf.printLog()

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.stepDown(args.Term, "higher term in AppendEntries")
		DPrintf(State, "Term %v: %v updated term due to HB", rf.currentTerm, rf.me)
	} else if args.Term < rf.currentTerm {
		DPrintf(State, "Term %v: %v received outdated append from %v", rf.currentTerm, rf.me, args.Id)
		return
	}
	select {
	case rf.heartbeatChan <- true:
	default:
	}
	prev := rf.getLast()
	if args.PrevLogIndex > prev.Index {
		reply.LastIndex = prev.Index + 1
		DPrintf(State, "Term %v: %v received too large previous value (%v > %v) from %v", rf.currentTerm, rf.me, args.PrevLogIndex, len(rf.log)-1, args.Id)
		rf.printLog()
		return
	}
	//if args.PrevLogIndex > len(rf.log)-1 && //if log long enough to be able to check index
	//	rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	//	DPrintf(State, "Term %v: %v has longer log than %v: mine: [#%v, t:%v] < args: [#%v, t:%v]", rf.currentTerm, rf.me, args.Id, rf.log[rf.lastApplied].Index, rf.log[rf.lastApplied].Term, args.PrevLogIndex, args.PrevLogTerm)
	//	return
	//}
	entry := LogEntry{}
	index := 0
	for index, entry = range args.Entries {
		if entry.Index > len(rf.log)-1 { //if too long, go to append state
			// potential cause of errors with gaps?
			break
		} else if entry.Term != rf.log[entry.Index].Term || entry.Command != rf.log[entry.Index].Command {
			// for given log [0:nil, 1:b, 2:c, 3:d] and entries [2:c, 3:e, 4:f]
			// when i = 0, entry.Index is 2, and log[2] (2:c) == entries[0] (2:c)
			// when i = 1, entry.Index is 3, and log[3] (3:d) != entries[1] (3:e)
			// because log[3] does not equal entries[1],
			// newLog is set to a slice of rf.log of indices 0 to entryIndex - 1 (2)
			rf.log = rf.log[:entry.Index]
			break
		}
	}
	DPrintf(Accept, "Term %v: %v with index %v and entry index %v", rf.currentTerm, rf.me, index, entry.Index)
	rf.log = append(rf.log, args.Entries[index:]...) //append new logs
	if args.CommitIndex > rf.commitIndex {
		if prev.Index < args.CommitIndex { //int min
			rf.commitIndex = prev.Index
		} else {
			rf.commitIndex = args.CommitIndex
		}
		select {
		case rf.updateCommitChan <- true:
		default:
		}

	}

	reply.Success = true
	//if entry.Index > logNextIndex { //if too long, reject and try again
	//	return
	//}
	//if entry.Index == logNextIndex { //if entry is next entry
	//	rf.log = append(rf.log, entry)
	//	reply.Success = true
	//	DPrintf(Accept, "Term %v: %v added entry [#%v, t:%v] to index %v", rf.currentTerm, rf.me, entry.Index, entry.Term, len(rf.log)-1)
	//	continue
	//} else
	//if it conflicts
	//not properly handing case where 5 entries are sent: this is failing for some reason
	rf.printLog()
	//*reply = out
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return //stale vote
	}
	if args.Term > rf.currentTerm { //leader or candidate steps down
		rf.stepDown(args.Term, "higher term requested vote")
	}
	*reply = RequestVoteReply{rf.currentTerm, false}
	if rf.votedFor != nil && rf.votedFor != args.CandidateId {
		DPrintf(Election, "Term %v: %v did not vote for %v (Already voted for %v)", rf.currentTerm, rf.me, args.CandidateId, rf.votedFor)
		return
	}
	last := rf.log[rf.lastApplied]
	if last.Term > args.LastLogTerm || last.Index > args.LastLogIndex { // requester is unqualified
		DPrintf(Election, "Term %v: %v did not vote for %v, less qualified", rf.currentTerm, rf.me, args.CandidateId)

		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	DPrintf(Election, "Term %v: %v voted for %v", rf.currentTerm, rf.me, args.CandidateId)
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
	//muCounter.Lock()
	//counter++
	//localCounter := getCounter()
	//muCounter.Unlock()
	rf.mu.Lock()
	DPrintf(Count, "Term %v: %v sent request vote to %v", rf.currentTerm, rf.me, server)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	DPrintf(State, "Term %v: %v received response for vote from %v", rf.currentTerm, rf.me, server)
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) stepDown(newTerm int, reason string) {
	DPrintf(State, "Term %v: %v stepped down (%v)", rf.currentTerm, rf.me, reason)
	rf.currentTerm = newTerm
	rf.votedFor = nil
	rf.votes = 0
	rf.role = Follower
	select {
	case rf.stepDownChan <- true:
	default:
	}
}

func (rf *Raft) printLog() {
	s := "{"
	for _, entry := range rf.log {
		if entry.Index == rf.lastApplied {
			s += ">"
		}
		if entry.Index == rf.commitIndex {
			s += ")"
		}
		s += fmt.Sprintf("[#%v, t%v, c%v]", entry.Index, entry.Term, entry.Command)
		if entry.Index == rf.lastApplied {
			s += "<"
		}
		if entry.Index == rf.commitIndex {
			s += "("
		}
	}
	s += "}"
	DPrintf(State, "Term %v: Log for %v %v", rf.currentTerm, rf.me, s)
	if true {
		s = "{"
		for _, entry := range rf.committed {
			valid := "valid"
			if !entry.CommandValid {
				valid = "invalid"
			}
			s += fmt.Sprintf("[#%v, %v, c%v]", entry.CommandIndex, valid, entry.Command)
		}
		s += "}"
		DPrintf(Commit, "Term %v: Committed for %v %v", rf.currentTerm, rf.me, s)
	}

}

//func getCounter() string {
//	return fmt.Sprintf("(%.5v)", counter)
//}

func (rf *Raft) updateCommit() {
	for {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-rf.updateCommitChan:
		}
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			toApply := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
			rf.applyChan <- toApply
			rf.committed = append(rf.committed, toApply)
		}
		rf.mu.Unlock()
	}
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	DPrintf(StateFine, "-- Killed %v --", rf.me)
	rf.mu.Unlock()
	// Your code here, if desired.
}

func (rf *Raft) BeFollower() {
	random := getRandomMs()
	loop := 0
	for {
		loop++
		//DPrintf(StateFine, "Follower loop for %v: %v", rf.me, loop)
		select {
		case <-rf.heartbeatChan:
			//rf.mu.Lock()
			//DPrintf(Election, "Term %v: Reset timeout for %v", rf.currentTerm, rf.me)
			//rf.mu.Unlock()
			continue
		case <-time.After(random):
			rf.mu.Lock()
			//DPrintf(State, "%v became Candidate due to timeout", rf.me)
			rf.role = Candidate
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) BeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	//votes for self
	rf.votes = 1
	rf.votedFor = rf.me
	// we don't send a heartbeat because we're handling it differently
	last := rf.getLast()
	request := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         rf.currentTerm,
		LastLogIndex: last.Index,
		LastLogTerm:  last.Term,
	}
	DPrintf(Election, "Term %v: %v made it to election", rf.currentTerm, rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		DPrintf(Election, "Term %v: %v requesting vote from %v", rf.currentTerm, rf.me, peer)
		go func(peer int) { //send out vote requests in parallel
			rf.mu.Lock()
			if rf.role != Candidate {
				return
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &request, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term, "be candidate: got higher term")
				return
			}
			if reply.Term+1 < rf.currentTerm { //stale response, +1 because followers will be in previous term
				DPrintf(Election, "Term %v: %v ignored stale vote from %v: term %v", rf.currentTerm, rf.me, peer, reply.Term)
				return
			}
			if ok && reply.VoteGranted {
				rf.votes++
				if rf.votes >= (len(rf.peers)/2 + 1) { // 5 / 2 = 2, 2 + 1 = 3, 3 is the majority
					DPrintf(Election, "Term %v: %v received majority vote (%v>=%v/%v)", rf.currentTerm, rf.me, rf.votes, len(rf.peers)/2+1, len(rf.peers))
					select {
					case rf.wonElection <- true:
					default:
					}
					return
				}
				DPrintf(Election, "Term %v: %v was sent positive vote by %v in term %v", rf.currentTerm, rf.me, peer, reply.Term)
			}
		}(peer)
	}
}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(State, "%v became Leader", rf.me)
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	//next := rf.getLast().Index
	for i := range len(rf.peers) {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}
}

func (rf *Raft) StateTimer() {
	for {
		rf.mu.Lock()
		role := rf.role
		DPrintf(State, "Term %v: %v has role %v", rf.currentTerm, rf.me, []string{"Follower", "Candidate", "Leader"}[int(role)])
		rf.mu.Unlock()
		switch role {
		case Follower:
			rf.BeFollower()
		case Candidate:
			rf.BeCandidate()
			select {
			case <-rf.wonElection:
				rf.BecomeLeader()
				continue // leader return
			case <-rf.heartbeatChan:
				// should be follower now
				continue // follower return
			case <-rf.stepDownChan:
				continue
			case <-time.After(getRandomMs()):
				continue // if candidate (due to timeout), restarts election (candidate return)
			}
		case Leader:
			ok := rf.propagateFollowers()
			if !ok {
				continue
			}
			select {
			case <-time.After(heartbeatMs * time.Millisecond):
			case <-rf.heartbeatChan:
				continue
			case <-rf.stepDownChan:
				continue
			}

		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.log = []LogEntry{
		{
			Term:    0,
			Index:   0,
			Command: -1,
		},
	}
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.applyChan = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.committed = make([]ApplyMsg, 0)
	// Your initialization code here (3A, 3B).
	go rf.StateTimer()
	go rf.updateCommit()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
