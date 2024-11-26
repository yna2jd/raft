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
const maxMs = 700

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
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	applyChan     chan ApplyMsg
	currentTerm   int
	isLeader      bool
	votedFor      CandidateVoted
	log           []LogEntry
	role          PeerRole
	heartbeatChan chan bool
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

}

func (rf *Raft) getLast() LogEntry {
	if len(rf.log) > rf.commitIndex {
		return rf.log[len(rf.log)-1]
	}
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
	// Your code here (3A).
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	out := AppendEntriesReply{rf.currentTerm, false}
	outdated := args.Term < rf.currentTerm
	if outdated {
		*reply = out
		DPrintf(State, "Term %v: %v recieved outdated append from %v", rf.currentTerm, rf.me, args.Id)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		out.Term = args.Term
		DPrintf(State, "Term %v: %v updated term due to HB", rf.currentTerm, rf.me)
	}
	if rf.role != Follower {
		rf.stepDown(args.Term, "equal or higher term issue command")
	}
	// election cleanup
	if rf.votedFor != nil {
		rf.votedFor = nil
	}
	go func() { rf.heartbeatChan <- true }()
	//if index doesn't match, skipped for heartbeat
	if args.PrevLogIndex < len(rf.log) && //if log long enough to be able to check index
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf(State, "Term %v: %v has older log than %v: mine: [#%v, t:%v] < args: [#%v, t:%v]", rf.currentTerm, rf.me,
			args.Id, rf.log[rf.lastApplied].Index, rf.log[rf.lastApplied].Term, args.PrevLogIndex, args.PrevLogTerm)
		*reply = out
		return
	}
	// Return failure if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	//if len(args.Entries) == 0 {
	//	success := args.PrevLogIndex <= rf.log[rf.lastApplied].Index && args.PrevLogTerm == rf.log[rf.lastApplied].Term
	//	//should always exist
	//	if !success {
	//
	//	}
	//	out.Success = success
	//	*reply = out
	//	return
	//}

	for _, entry := range args.Entries {
		logNextIndex := len(rf.log)
		out.Success = false
		if entry.Index > logNextIndex { //if too long, reject and try again
			*reply = out
			return
		}
		if entry.Index == logNextIndex { //if entry is next entry
			rf.log = append(rf.log, entry)
			rf.lastApplied = logNextIndex
			out.Success = true
			DPrintf(Accept, "Last Applied for %v: %v", rf.me, rf.lastApplied)
			DPrintf(Accept, "Term %v: %v added entry [#%v, t:%v] to index %v", rf.currentTerm, rf.me, entry.Index, entry.Term, len(rf.log)-1)
			continue
		}
		//if it conflicts
		//not properly handing case where 5 entries are sent: this is failing for some reason
		//if entry.Term != rf.log[entry.Index].Term || entry.Command != rf.log[entry.Index].Command {
		//	// removing indices in go https://stackoverflow.com/a/57213476
		//	newLog := make([]LogEntry, 0, entry.Index+len(args.Entries))
		//	// for given log [0:nil, 1:b, 2:c, 3:d] and entries [2:c, 3:e, 4:f]
		//	// when i = 0, entry.Index is 2, and log[2] (2:c) == entries[0] (2:c)
		//	// when i = 1, entry.Index is 3, and log[3] (3:d) != entries[1] (3:e)
		//	// because log[3] does not equal entries[1],
		//	// newLog is set to a slice of rf.log of indices 0 to entryIndex - 1 (2)
		//	// then newLog, currently [0:nil, 1:b, 2:c] has entries starting at i appended to it ([3:e, 4:f])
		//	// newLog is now [0:a, 1:b, 2:c, 3:e, 4:f], to which rf.log is set
		//	newLog = append(newLog, rf.log[:entry.Index]...)
		//	newLog = append(newLog, args.Entries[entriesIndex:]...)
		//	rf.log = newLog
		//	rf.lastApplied = len(rf.log) - 1
		//	DPrintf(Accept, "Term %v: %v added entries to indices %v to %v", rf.currentTerm, rf.me, args.Entries[0].Index, len(rf.log)-1)
		//	out.Success = true
		//	break
		//}

	}
	if args.CommitIndex > rf.commitIndex && args.CommitIndex > 0 {
		oldCommitInd := rf.commitIndex
		prev := rf.getLast()
		if prev.Index < args.CommitIndex { //int min
			rf.commitIndex = prev.Index
		} else {
			rf.commitIndex = args.CommitIndex
		}
		slice := make([]LogEntry, 0)
		slice = append(slice, rf.log[oldCommitInd+1:rf.commitIndex+1]...)
		go func(slice []LogEntry) {
			for _, entry := range slice {
				DPrintf(Commit, "Tried to commit index %v for peer %v", entry.Index, rf.me)
				toApply := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.applyChan <- toApply
				rf.mu.Lock()
				DPrintf(Commit, "successfully committed index %v for peer %v", entry.Index, rf.me)
				rf.commitIndex = entry.Index
				rf.committed = append(rf.committed, toApply)
				rf.printLog()
				rf.mu.Unlock()
			}
		}(slice)
	}
	*reply = out
	rf.printLog()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return //stale vote
	}
	if args.Term > rf.currentTerm && rf.role != Follower { //leader or candidate steps down
		rf.stepDown(args.Term, "higher term requested vote")
		rf.votedFor = nil
	}
	*reply = RequestVoteReply{rf.currentTerm, false}
	alreadyVoted := rf.votedFor != nil
	if alreadyVoted {
		DPrintf(Election, "Term %v: %v did not vote for %v (Already voted for %v)", rf.currentTerm, rf.me, args.CandidateId, rf.votedFor)
		return
	}
	last := rf.getLast()
	if last.Term > args.LastLogTerm || last.Index > args.LastLogIndex {

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) stepDown(newTerm int, reason string) {
	DPrintf(State, "Term %v: %v stepped down (%v)", rf.currentTerm, rf.me, reason)
	rf.currentTerm = newTerm
	rf.role = Follower
}

func (rf *Raft) printLog() {
	s := "{"
	for _, entry := range rf.log {
		if entry.Index == rf.commitIndex {
			s += ">"
		}
		s += fmt.Sprintf("[#%v, t%v, c%v]", entry.Index, entry.Term, entry.Command)
		if entry.Index == rf.commitIndex {
			s += "<"
		}
	}
	s += "}"
	DPrintf(State, "Term %v: Log for %v %v", rf.currentTerm, rf.me, s)
	if true {
		s = "{"
		for _, entry := range rf.committed {
			if entry.CommandIndex == rf.commitIndex {
				s += ">"
			}
			valid := "valid"
			if !entry.CommandValid {
				valid = "invalid"
			}
			s += fmt.Sprintf("[#%v, %v, c%v]", entry.CommandIndex, valid, entry.Command)
			if entry.CommandIndex == rf.commitIndex {
				s += "<"
			}
		}
		s += "}"
		DPrintf(State, "Term %v: Committed for %v %v", rf.currentTerm, rf.me, s)
	}

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
	rf.lastApplied = newestIndex
	rf.matchIndex[rf.me] = newestIndex
	rf.nextIndex[rf.me] = newestIndex + 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.propagateCommand(peer)
	}
	return newestIndex, term, true
}

func (rf *Raft) propagateCommand(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	match := rf.matchIndex[peer]
	firstNew := match + 1
	prev := rf.log[match]
	args := AppendEntriesArgs{
		Term:         rf.log[rf.lastApplied].Term,
		Id:           rf.me,
		PrevLogIndex: prev.Index,
		PrevLogTerm:  prev.Term,
		Entries:      []LogEntry{},
		CommitIndex:  rf.commitIndex,
	}
	DPrintf(Accept, "Values for %v, previous val [#%v, t:%v], match: %v, newestIndex: %v", peer, prev.Index, prev.Term, match, rf.lastApplied)
	args.Entries = append(args.Entries, rf.log[firstNew:rf.lastApplied+1]...)
	DPrintf(Accept, "Term %v: %v sent AppendEntries of length {%v} to %v", rf.currentTerm, rf.me, len(args.Entries), peer)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {
		rf.matchIndex[peer] = rf.lastApplied
		rf.nextIndex[peer] = rf.lastApplied + 1
		DPrintf(Accept, "Term %v: %v's state for %v's indices: match: %v, next: %v",
			rf.currentTerm, rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
		entries := ""
		for i, entry := range args.Entries {
			entries += strconv.Itoa(entry.Index) + ": " + strconv.Itoa(entry.Term)
			if i != len(args.Entries)-1 {
				entries += ", "
			}
		}
		DPrintf(Accept, "Term %v: %v's entries for %v (%v to %v) accepted: [%v]",
			rf.currentTerm, rf.me, peer, firstNew, rf.lastApplied, entries)
		rf.updateCommit()
		return
	} else {
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term, "appendEntries")
			return
		}
		DPrintf(Accept, "Term %v: %v append for %v rejected [#%v, t%v]",
			reply.Term, rf.me, peer, firstNew, reply.Term)
		if rf.nextIndex[peer] > rf.matchIndex[peer]+1 {
			rf.nextIndex[peer]--
			defer rf.propagateCommand(peer)
		}

	}

}

func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lviCopy := make([]int, 0, len(rf.peers))
	lviCopy = append(lviCopy, rf.matchIndex...)
	sort.Ints(lviCopy)
	majorityAccepted := lviCopy[len(lviCopy)/2] //median, rounding up: len 5 -> 5/2 -> [2], which is middle
	if majorityAccepted > rf.commitIndex {
		currentTerm := rf.currentTerm
		slice := make([]LogEntry, 0, majorityAccepted-rf.commitIndex)
		slice = append(slice, rf.log[rf.commitIndex+1:majorityAccepted+1]...)
		go func(commitIndex int) {
			for _, entry := range slice {
				if entry.Term == currentTerm {
					toApply := ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: entry.Index,
					}
					rf.applyChan <- toApply
					rf.mu.Lock()
					rf.commitIndex = entry.Index
					DPrintf(Commit, "Leader %v successfully committed to index: %v", rf.me, entry.Index)
					rf.committed = append(rf.committed, toApply)
					isLeader := rf.role == Leader
					rf.printLog()
					rf.mu.Unlock()
					if !isLeader {
						break
					}

				}
			}
			s := "Follower match: ["
			for i, index := range rf.matchIndex {
				s += strconv.Itoa(index)
				if i != len(rf.peers)-1 {
					s += ", "
				}
			}
			s += "] follower next: ["
			for i, index := range rf.nextIndex {
				s += strconv.Itoa(index)
				if i != len(rf.peers)-1 {
					s += ", "
				}
			}
			DPrintf(Commit, "%v]", s)
		}(rf.commitIndex + 1)
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
		DPrintf(StateFine, "Follower loop for %v: %v", rf.me, loop)
		select {
		case <-rf.heartbeatChan:
			rf.mu.Lock()
			DPrintf(Election, "Term %v: Reset timeout for %v", rf.currentTerm, rf.me)
			rf.mu.Unlock()
			continue
		case <-time.After(random):
			rf.mu.Lock()
			DPrintf(State, "%v became Candidate due to timeout", rf.me)
			rf.role = Candidate
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) BeCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	//votes for self
	votes := 1
	wonElection := make(chan bool)
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
	rf.printLog()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		DPrintf(Election, "Term %v: %v requesting vote from %v", rf.currentTerm, rf.me, peer)
		go func() { //send out vote requests in parallel
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(peer, &request, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.role = Follower
				return
			}
			if ok && reply.VoteGranted {
				votes++
				if votes >= (len(rf.peers)/2 + 1) { // 5 / 2 = 2, 2 + 1 = 3, 3 is the majority
					DPrintf(Election, "Term %v: %v received majority vote (%v>=%v/%v)", rf.currentTerm, rf.me, votes, len(rf.peers)/2+1, len(rf.peers))
					rf.role = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					next := rf.getLast().Index + 1
					for i := range len(rf.peers) {
						rf.nextIndex[i] = next
						rf.matchIndex[i] = 0
						go rf.propagateCommand(i)
					}
					wonElection <- true
					return
				}
				DPrintf(Election, "Term %v: %v was sent positive vote by %v in term %v", rf.currentTerm, rf.me, peer, reply.Term)
			}
		}()
	}
	rf.mu.Unlock()
	for {
		select {
		case <-rf.heartbeatChan:
			return // follower return
		case <-time.After(getRandomMs()):
			return // if candidate (due to timeout), restarts election (candidate return)
		case <-wonElection:
			return // leader return
		}
	}
}

func (rf *Raft) StateTimer() {

	for {
		rf.mu.Lock()
		role := rf.role
		roles := [...]string{"Follower", "Candidate", "Leader"}
		DPrintf(State, "Term %v: %v has role %v", rf.currentTerm, rf.me, roles[int(role)])
		rf.mu.Unlock()
		switch role {
		case Follower:
			rf.BeFollower()
		case Candidate:
			rf.BeCandidate()
		case Leader:
			rf.SendHeartbeat()
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (rf *Raft) SendHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			prev := rf.log[rf.matchIndex[peer]]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				Id:           rf.me,
				PrevLogIndex: prev.Index,
				PrevLogTerm:  prev.Term,
				Entries:      nil,
				CommitIndex:  rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			DPrintf(StateFine, "Term %v: Leader %v sent HB to %v", rf.currentTerm, rf.me, peer)
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				DPrintf(Election, "Term %v: Leader %v received no response from %v", rf.currentTerm, rf.me, peer)
				return
			} else {
				DPrintf(Election, "Term %v: Leader %v received HB response from %v: {%v, %v}", rf.currentTerm, rf.me, peer, reply.Term, reply.Success)
			}
			if rf.role != Leader {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term, "heartbeat")
			}
		}(i)
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
	rf.isLeader = false
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
	rf.heartbeatChan = make(chan bool)
	rf.committed = make([]ApplyMsg, 0)
	// Your initialization code here (3A, 3B).
	go rf.StateTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
