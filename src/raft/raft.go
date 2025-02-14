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

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 500 * time.Millisecond
	heartbeat          time.Duration = 50 * time.Millisecond
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dAppend  logTopic = "APND"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func (rf *Raft) Debug(topic logTopic, format string, a ...interface{}) {
	if rf.debugVerbosity >= 1 {
		time := time.Since(rf.debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v %s ", time, string(topic), "S"+fmt.Sprint(rf.me))
		format = prefix + format
		log.Printf(format, a...)
	}
}

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

	debugStart     time.Time
	debugVerbosity int
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// TODO uncomment when persist is less noisy
	//rf.Debug(dPersist, "persisted state")
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.Debug(dPersist, "reading persisted state")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.logs = []LogEntry{}
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Panic("Panic decoding persistent state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		rf.Debug(dVote, "received term=%d less than current term=%d", args.Term, rf.currentTerm)
		return
	}

	rf.checkTerm(args.Term)

	if rf.canVoteFor(args.CandidateID) {
		rf.Debug(dVote, "considering voting for=%d, votedFor=%d", args.CandidateID, rf.votedFor)
		if rf.asUpToDate(args.LastLogTerm, args.LastLogIndex) {
			rf.voteFor(args.CandidateID)
			reply.VoteGranted = true
			rf.Debug(dVote, "voted for CandidateID=%d", args.CandidateID)
			return
		} else {
			rf.Debug(dVote, "rejecting vote request. CandidateID=%d logs are not as current as mine", args.CandidateID)
			return
		}
	}
}

func (rf *Raft) canVoteFor(candidateID int) bool {
	return rf.votedFor == -1 || rf.votedFor == candidateID
}

func (rf *Raft) voteFor(candidateID int) {
	rf.votedFor = candidateID
	rf.lastHeardFromLeader = time.Now()
	rf.persist()
}

func (rf *Raft) resetVote() {
	rf.votedFor = -1
	rf.persist()
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
	rf.Debug(dAppend, "received AppendEntries args=%v", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		rf.Debug(dAppend, "received term=%d less than my term=%d", args.Term, rf.currentTerm)
		return
	}

	rf.checkTerm(args.Term)
	rf.lastHeardFromLeader = time.Now()
	prevIndex := args.PrevLogIndex
	prevTerm := args.PrevLogTerm

	// If we don't even have previous entry, or its inconsistent fail immediately
	if !rf.havePreviousMatchingTerms(prevIndex, prevTerm) {
		rf.Debug(dAppend, "log inconsistency found! prevIndex=%d, prevTerm=%d returning failure", prevIndex, prevTerm)
		return
	}

	rf.Debug(dAppend, "processing %d new entries", len(args.Entries))
	for _, e := range args.Entries {
		if rf.indexExists(e.Index) {
			if rf.logAtIndex(e.Index).Term != e.Term {
				rf.Debug(dAppend, "term of new=%d and existing log entry=%d don't match", e.Term, rf.logAtIndex(e.Index).Term)
				rf.clearLogsStartingWithIndex(e.Index)
				return
			}
			rf.Debug(dAppend, "already have index=%d", e.Index)
		} else {
			rf.logs = append(rf.logs, e)
			rf.Debug(dAppend, "appended entry: %v", e)
		}
	}
	rf.persist()

	lastNewIndex := rf.commitIndex
	ll, exists := rf.lastLog()
	if exists {
		lastNewIndex = ll.Index
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.Debug(dInfo, "getting min of leaderCommit=%d and lastNewIdx=%d", args.LeaderCommit, lastNewIndex)
		min := min(args.LeaderCommit, lastNewIndex)
		rf.setCommitIndex(min)
	}
	reply.Success = true
}

func (rf *Raft) havePreviousMatchingTerms(prevIndex int, prevTerm int) bool {
	if len(rf.logs) == 0 {
		rf.Debug(dAppend, "prev term matches since log length is zero")
		return true
	}
	if prevIndex == 0 {
		rf.Debug(dAppend, "prev term matches since previous index is 0")
		return true
	}
	if len(rf.logs) < prevIndex {
		rf.Debug(dAppend, "prev term doesn't match since it isn't in the logs")
		return false
	}
	return rf.logs[prevIndex-1].Term == prevTerm
}

func (rf *Raft) indexExists(index int) bool {
	return len(rf.logs) >= index
}

func (rf *Raft) logAtIndex(index int) LogEntry {
	return rf.logs[index-1]
}

func (rf *Raft) clearLogsStartingWithIndex(index int) {
	rf.logs = rf.logs[:index-1]
	rf.persist()
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.Debug(dCommit, "setting commitIndex to %d", commitIndex)
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.Debug(dCommit, "starting agreement for new log entry:%v", logEntry)
	rf.logs = append(rf.logs, logEntry)
	rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentRole = Candidate
	rf.currentTerm++
	rf.Debug(dTerm, "starting election with term=%d", rf.currentTerm)
	rf.voteFor(rf.me)
	go rf.requestVotes()
}

func (rf *Raft) sendEntries() {
	rf.mu.Lock()
	peers := rf.peers
	rf.mu.Unlock()

	for i := 0; i < len(peers); i++ {
		if i != rf.me {
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
				}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      logEntries,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
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
					rf.Debug(dCommit, "append failed for S%d, decrementing nextIndex", i)
					rf.nextIndex[i]--
				} else {
					lastIndex := prevIndex + len(args.Entries)
					rf.nextIndex[i] = lastIndex + 1
					rf.matchIndex[i] = lastIndex
					rf.Debug(dCommit, "append succeeded for S%d, nextIndex=%d, matchIndex=%d", i, rf.nextIndex[i], rf.matchIndex[i])
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

func (rf *Raft) requestVotes() {
	rf.mu.Lock()

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
			rf.sendEntries()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) makeLeader() {
	rf.Debug(dLeader, "making myself leader")
	rf.currentRole = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := 0
	ll, exists := rf.lastLog()
	if exists {
		lastIndex = ll.Index
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) checkTerm(term int) {
	if term > rf.currentTerm {
		rf.Debug(dTerm, "found term=%d is higher than mine=%d", term, rf.currentTerm)
		rf.currentTerm = term
		if rf.currentRole != Follower {
			rf.currentRole = Follower
		}
		rf.resetVote()
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
		if rf.currentRole == Leader {
			rf.checkMajorityCommits()
			rf.mu.Unlock()
			rf.sendEntries()
			rf.mu.Lock()
		}

		rf.mu.Unlock()
		time.Sleep(heartbeat)
	}
}

func (rf *Raft) checkMajorityCommits() {
	ll, exists := rf.lastLog()
	if !exists {
		return
	}
	nextIndex := rf.commitIndex + 1
	lastIndex := ll.Index
	for nextIndex <= lastIndex {
		if rf.majoritySent(nextIndex) {
			rf.Debug(dCommit, "majority has commitIndex=%d, going to commit", nextIndex)
			rf.setCommitIndex(nextIndex)
		}
		nextIndex++
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
		peers:          peers,
		persister:      persister,
		me:             me,
		majority:       (len(peers) / 2) + 1,
		currentRole:    Candidate,
		currentTerm:    0,
		votedFor:       -1,
		commitIndex:    0,
		lastApplied:    0,
		applyCh:        applyCh,
		debugVerbosity: getVerbosity(),
		debugStart:     time.Now(),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// set log and debug details
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	// start elections after random timeout
	go rf.electionTicker()
	go rf.heartbeatTicker()

	return rf
}
