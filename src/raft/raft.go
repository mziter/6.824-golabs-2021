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
	//	"bytes"

	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 500 * time.Millisecond
	heartbeat          time.Duration = 50 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (le LogEntry) String() string {
	cmdStr := fmt.Sprintf("%v", le.Command)
	if len(cmdStr) > 10 {
		cmdStr = cmdStr[:10] + "..."
	}
	return fmt.Sprintf("{Index: %d, Term: %d, Cmd: %s}", le.Index, le.Term, cmdStr)
}

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

func (r Role) String() string {
	switch r {
	case 0:
		return "LEADER"
	case 1:
		return "CANDIDATE"
	case 2:
		return "FOLLOWER"
	default:
		return "UNKNOWN"
	}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	majority int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastHeardFromLeader time.Time
	currentRole         Role
	logs                []LogEntry

	// PERSISTENT STATE
	currentTerm int // latest term server has seen
	votedFor    int //candidate ID that received vote in the current term

	// VOLATILE STATE ON ALL SERVERS
	commitIndex int
	lastApplied int

	// VOLATILE STATE ON LEADERS
	// Reinitialized after election
	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of the highest log etnry known to be replicated ton server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentRole == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.logs = []LogEntry{}
	}
	return
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // Current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var logMsg string
	defer func() {
		DPrintf(VotingDebug, logMsg)
	}()

	logMsg = fmt.Sprintf("[%d] Vote requested from %d, TERM=%d", rf.me, args.CandidateID, args.Term)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		logMsg = logMsg + " Have newer term. REJECTED"
		return
	}

	rf.checkTerm(args.Term)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if rf.asUpToDate(args.LastLogTerm, args.LastLogIndex) {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.lastHeardFromLeader = time.Now()
			logMsg = logMsg + " Candidate is up-to-date. GRANTED"
			return
		} else {
			logMsg = logMsg + " Inconsistent logs. REJECTED"
			return
		}
	}
	logMsg = logMsg + fmt.Sprintf(" Already voted for %d. TERM=%d. REJECTED", rf.votedFor, rf.currentTerm)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (ae AppendEntriesArgs) String() string {
	return fmt.Sprintf("{LeaderID=%d, Term=%d, PrevLogIndex=%d, PrevLogTerm=%d, LeaderCommit=%d, Entries=%v}",
		ae.LeaderID, ae.Term, ae.PrevLogIndex, ae.PrevLogTerm, ae.LeaderCommit, ae.Entries)
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	DPrintf(AppendingDebug, "[%d] received AppendEntries RPC %s\n", rf.me, args)
	if args.Term < rf.currentTerm {
		return
	}

	rf.checkTerm(args.Term)
	rf.lastHeardFromLeader = time.Now()
	prevIndex := args.PrevLogIndex
	prevTerm := args.PrevLogTerm

	// If we don't even have previous entry, or its inconsistent fail immediately
	if rf.shouldCheckLogHistory(prevIndex, prevTerm) && rf.havePreviousEntry(prevIndex) && rf.havePreviousMatchingTerms(prevIndex, prevTerm) {
		return
	}

	// For now, lets assume they are already in order
	// For each entry
	//   1. Check to make sure its same, if not blow it and everything after it away
	//   2. Append if new entry
	if prevIndex > len(rf.logs) {
		return
	}
	if prevIndex > 0 {
		prevLog := rf.logAtIndex(prevIndex)
		if prevTerm != prevLog.Term {
			return
		}
	}
	for _, e := range args.Entries {
		if rf.indexExists(e.Index) {
			if rf.logAtIndex(e.Index).Term != e.Term {
				rf.clearLogsStartingWithIndex(e.Index)
				return
			}
		} else {
			rf.logs = append(rf.logs, e)
			// DEBUG
			DPrintf(AppendingDebug, "[%d] entry: [%v]\n", rf.me, e)
			DPrintf(AppendingDebug, "[%d] logs after append: [%v]\n", rf.me, rf.logs)
			// DEBUG
			ll, _ := rf.lastLog()
			if ll.Index != e.Index {
				log.Panicf("Expected new log index to be [%d] but was [%d]", e.Index, ll.Index)
			}
			if rf.logs[len(rf.logs)-1].Index != e.Index {
				log.Panicf("Expected last inserted log index to be [%d] but was [%d]", e.Index, ll.Index)
			}
		}
	}

	lastNewIdx := 0
	ll, exists := rf.lastLog()
	if exists {
		lastNewIdx = ll.Index
	}
	// Finally, set commit index
	//log.Printf("[%d] leaderCommit:%d > commitIndex:%d => %v", rf.me, args.LeaderCommit, rf.commitIndex, args.LeaderCommit > rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		min := min(args.LeaderCommit, lastNewIdx)
		//log.Printf("[%d] leaderCommit:%d, commitIndex:%d, lastNewIndex:%d, min:%d, logs:%v", rf.me, args.LeaderCommit, rf.commitIndex, lastNewIdx, min, rf.logs)
		rf.setCommitIndex(min)
	}
	reply.Success = true
}

func (rf *Raft) shouldCheckLogHistory(lastIndex int, term int) bool {
	return lastIndex == 0 && term == 0
}

func (rf *Raft) havePreviousEntry(lastIndex int) bool {
	return len(rf.logs) < lastIndex
}

func (rf *Raft) havePreviousMatchingTerms(lastIndex int, lastTerm int) bool {
	return rf.logs[lastIndex-1].Term == lastTerm
}

func (rf *Raft) indexExists(index int) bool {
	return len(rf.logs) >= index
}

func (rf *Raft) logAtIndex(index int) LogEntry {
	return rf.logs[index-1]
}

func (rf *Raft) clearLogsStartingWithIndex(index int) {
	rf.logs = rf.logs[:index-1]
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	for commitIndex > rf.lastApplied {
		logEntry := rf.logAtIndex(rf.lastApplied + 1)
		applyCmd := ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
		rf.applyCh <- applyCmd
		rf.lastApplied++
	}
	DPrintf(AppendingDebug, "[%d] log state update commitIndex=[%d], lastApplied=[%d]\n", rf.me, rf.commitIndex, rf.lastApplied)
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) asUpToDate(term int, index int) bool {
	ll, exists := rf.lastLog()

	if !exists && term == 0 && index == 0 {
		return true
	}

	if term < ll.Term {
		return false
	}

	if term > ll.Term {
		return true
	}

	if index >= ll.Index {
		return true
	}

	return false
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf(VotingDebug, "[%d] Requesting vote from %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := 1
	ll, exists := rf.lastLog()
	if exists {
		index = ll.Index + 1
	}

	term := rf.currentTerm
	isLeader := rf.currentRole == Leader

	if !isLeader {
		rf.mu.Unlock()
		return index, term, false
	}
	logEntry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	rf.logs = append(rf.logs, logEntry)
	if len(rf.logs) != index {
		DPrintf(AppendingDebug, "[%d] expected length of logs [%d] to equal index [%d]\n", rf.me, len(rf.logs), index)
		DPrintf(AppendingDebug, "[%d] logs=%v\n", rf.me, rf.logs)
		rf.mu.Unlock()
		log.Panic("Something bad has happened!")
	}
	DPrintf(AppendingDebug, "[%d] sending cmd to majority\n", rf.me)
	rf.mu.Unlock()
	rf.sendEntries()
	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) lastLog() (LogEntry, bool) {
	if len(rf.logs) > 0 {
		return rf.logs[len(rf.logs)-1], true
	} else {
		return LogEntry{}, false
	}
}

func (rf *Raft) startElection() {
	DPrintf(ElectionDebug, "[%d] Starting election\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentRole = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	go rf.requestVotes()
}

func (rf *Raft) sendEntries() {
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	rf.mu.Unlock()

	for i := 0; i < len(peers); i++ {
		if i != me {
			go func(i int) {
				rf.mu.Lock()
				peerNextIndex := rf.nextIndex[i]
				prevIndex := peerNextIndex - 1
				if peerNextIndex <= 0 {
					prevIndex = 0
				}
				if peerNextIndex > len(rf.logs) {
					prevIndex = len(rf.logs)
				}
				prevTerm := 0
				if prevIndex > 0 {
					prevTerm = rf.logAtIndex(prevIndex).Term
				}
				logEntries := []LogEntry{}
				if len(rf.logs) >= peerNextIndex && len(rf.logs) > 0 {
					logEntries = rf.logs[peerNextIndex-1:]
					DPrintf(AppendingDebug, "[%d] Log len:[%d], peer:[%d], peerNextIndex:[%d], logEntries[%v]\n", rf.me, len(rf.logs), i, peerNextIndex, logEntries)
				}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      logEntries,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()

				ok := rf.sendAppendEntries(i, args, reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.checkTerm(reply.Term)
					rf.mu.Unlock()
					return
				}
				if !reply.Success {
					rf.nextIndex[i]--
				} else {
					lastIndex := prevIndex + len(args.Entries)
					rf.nextIndex[i] = lastIndex + 1
					rf.matchIndex[i] = lastIndex
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

func (rf *Raft) requestVotes() {
	rf.mu.Lock()
	DPrintf(ElectionDebug, "[%d] Requesting votes\n", rf.me)

	replies := make([]*RequestVoteReply, len(rf.peers))
	doneSignals := make(chan int, len(rf.peers)-1)
	wg := sync.WaitGroup{}
	defer func() {
		wg.Wait()
		close(doneSignals)
	}()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			lastIndex := 0
			lastTerm := 0
			ll, exists := rf.lastLog()
			if exists {
				lastIndex = ll.Index
				lastTerm = ll.Term
			}
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			replies[i] = reply
			go func(i int, args *RequestVoteArgs, reply *RequestVoteReply, wg *sync.WaitGroup) {
				rf.sendRequestVote(i, args, reply)
				doneSignals <- i
				wg.Done()
			}(i, args, reply, &wg)
		}
	}
	rf.mu.Unlock()

	// lets lock as we process each reply and compate/update state
	votes := 0
	for i := range doneSignals {
		rf.mu.Lock()

		reply := replies[i]

		rf.checkTerm(reply.Term)

		// our role changed since we performed term check and we no longer care to proceed
		if rf.currentRole != Candidate {
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			votes++
		}
		// I vote for myself :)
		if votes >= rf.majority-1 {
			rf.makeLeader()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) makeLeader() {
	DPrintf(ElectionDebug, "[%d] NEW LEADER\n", rf.me)
	rf.currentRole = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := 0
	ll, exists := rf.lastLog()
	if exists {
		lastIndex = ll.Index
	}
	for i := 0; i < len(rf.peers); i++ {
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) checkTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		if rf.currentRole != Follower {
			DPrintf(ElectionDebug, "[%d] Changed role from %s to %s\n", rf.me, rf.currentRole, Follower)
			rf.currentRole = Follower
		}
		rf.votedFor = -1
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		time.Sleep(NewElectionTimeout())

		rf.mu.Lock()
		role := rf.currentRole
		lastHeard := time.Since(rf.lastHeardFromLeader)
		rf.mu.Unlock()

		switch role {
		case Candidate:
			rf.startElection()
		case Follower:
			if lastHeard > electionTimeoutMax {
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {

		rf.mu.Lock()
		role := rf.currentRole
		rf.mu.Unlock()

		if role == Leader {
			rf.mu.Lock()
			rf.checkMajorityCommits()
			rf.mu.Unlock()

			rf.sendEntries()
		}

		time.Sleep(heartbeat)
	}
}

func (rf *Raft) checkMajorityCommits() {
	ll, exists := rf.lastLog()
	if !exists {
		return
	}
	index := rf.commitIndex + 1
	lastIndex := ll.Index
	for index <= lastIndex && rf.majoritySent(index) {
		rf.setCommitIndex(index)
		index++
	}
}

func (rf *Raft) majoritySent(index int) bool {
	required := rf.majority - 1
	count := 0
	for i, _ := range rf.peers {
		if rf.matchIndex[i] >= index {
			count++
			if count >= required {
				return true
			}
		}
	}
	return false
}

func NewElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	min := int(electionTimeoutMin / time.Millisecond)
	max := int(electionTimeoutMax / time.Millisecond)
	timeout := rand.Intn(max-min+1) + min
	return time.Duration(timeout) * time.Millisecond
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		majority:    (len(peers) / 2) + 1,
		currentRole: Candidate,
		currentTerm: 0,
		votedFor:    -1,
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     applyCh,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start elections after random timeout
	go rf.electionTicker()
	go rf.heartbeatTicker()

	return rf
}
