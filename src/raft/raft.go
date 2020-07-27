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
	"../labgob"
	"../labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	InfoColor    = "\033[1;34m%s\033[0m"
	NoticeColor  = "\033[1;36m%s\033[0m"
	WarningColor = "\033[1;33m%s\033[0m"
	ErrorColor   = "\033[1;31m%s\033[0m"
	DebugColor   = "\033[0;36m%s\033[0m"
)

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
	Snapshot     []byte
}

type State string

const (
	Follower  State = "follower"
	Candidate       = "candidate"
	Leader          = "leader"
	Killed          = "killed"
)

const MinElectionInterval = 150
const HeartbeatInterval = 50
const ApplyInterval = 25

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // channel for sending back applied messages

	state    State     // server state
	lastTime time.Time // the most recent time of received heartbeat from leader

	currentTerm int        // latest term server has seen
	votedFor    int        // candidate id which receives the vote in current term or -1
	log         []LogEntry // log entries

	lastIncludedIndex int // last index in the snapshot
	lastIncludedTerm  int // term at last index in the snapshot

	commitIndex int // index of highest log entry commited
	lastApplied int // index of highest log entry applied to state machine

	nextIndex  []int // for each peer, index of the next log entry to send to that peer
	matchIndex []int // for each peer, index of the highest log entry replicated on that peer

	snapshot []byte // raw snapshot data
}

type LogEntry struct {
	Term    int         // leader term of log entry
	Index   int         // leader index of log entry
	Command interface{} // the command to apply to state machine
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate's id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // current term for leader to inspect
	VoteGranted bool // true if candidate recieved the vote
}

type AppendEntriesArgs struct {
	Term         int // current term
	LeaderId     int // leader's id
	PrevLogIndex int // index of log entry preceding new ones
	PrevLogTerm  int // term of PrevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // current term for leader to inspect
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	XLen    int  // log length
	XTerm   int  // term in the conflicting entry, if any
	XIndex  int  // index of first entry with that term, if any
}

type InstallSnapshotArgs struct {
	Term              int    // current term
	LeaderId          int    // leader's id
	LastIncludedIndex int    // snapshot replaces entries up to this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // raw bytes of snapshot
}

type InstallSnapshotReply struct {
	Term int // current term for leader to inspect
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.mu.Unlock()

	return term, isLeader
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) fullLogLen() int {
	return rf.fullIndex(len(rf.log))
}

func (rf *Raft) fullIndex(index int) int {
	if rf.lastIncludedIndex == 0 {
		return index
	}
	return rf.lastIncludedIndex + index + 1
}

func (rf *Raft) realIndex(index int) int {
	if rf.lastIncludedIndex == 0 {
		return index
	}
	return index - rf.lastIncludedIndex - 1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) recoverState() {
	rf.readSnapshot()
	rf.readPersist()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()

	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var vote int
	var log []LogEntry
	if d.Decode(&term) != nil || d.Decode(&vote) != nil || d.Decode(&log) != nil {
		DPrintf("[%d] Error decoding data...\n", rf.me)
	} else {
		rf.mu.Lock()
		DPrintf("[%d] (%s) Recovering state with currentTerm=%d, votedFor=%d, len(log)=%d\n", rf.me, rf.state, term, vote, len(log))

		rf.currentTerm = term
		rf.votedFor = vote
		rf.log = log
		rf.mu.Unlock()
	}
}

func (rf *Raft) readSnapshot() {
	data := rf.persister.ReadSnapshot()

	lastIndex, lastTerm, appData := rf.decodeSnapshot(data)
	if lastIndex != -1 && lastTerm != -1 && len(appData) > 0 {
		DPrintf("[%d] (%s) Recovering snapshot with lastIncludedIndex=%d, lastIncludedTerm=%d, len(appData)=%d\n",
			rf.me, rf.state, lastIndex, lastTerm, len(appData))

		rf.mu.Lock()
		rf.snapshot = data
		rf.applySnapshot(lastIndex, lastTerm, appData)
		rf.mu.Unlock()
	}
}

func (rf *Raft) encodeSnapshot(data []byte) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(data)

	return w.Bytes()
}

func (rf *Raft) decodeSnapshot(data []byte) (int, int, []byte) {
	lastIndex := -1
	lastTerm := -1
	appData := []byte{}

	if data == nil || len(data) < 1 {
		DPrintf("[%d] snapshot data is empty", rf.me)
		return lastIndex, lastTerm, appData
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&lastIndex) != nil || d.Decode(&lastTerm) != nil || d.Decode(&appData) != nil {
		DPrintf("[%d] Error decoding snapshot data", rf.me)
		return lastIndex, lastTerm, appData
	}

	return lastIndex, lastTerm, appData
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	return w.Bytes()
}

func (rf *Raft) applySnapshot(lastIncludedIndex int, lastIncludedTerm int, appData []byte) {
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastApplied
	rf.log = rf.log[:0]

	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		Snapshot:     appData,
	}
}

//
// Create new snapshot API.
//
func (rf *Raft) MakeSnapshot(data []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	realIndex := rf.realIndex(index)
	if realIndex < 0 {
		return
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[realIndex].Term
	rf.log = rf.log[realIndex+1:]

	rf.snapshot = rf.encodeSnapshot(data)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), rf.snapshot)

	DPrintf("[%d] Snapshot made: lastIncludedIndex=%d, lastIncludedTerm=%d, full log=%d, new log=%d",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.fullLogLen(), len(rf.log))
}

//
// RequestVote RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	voteGranted := false
	if args.Term < rf.currentTerm {
		DPrintf("[%d] (%s) not voting for candidate [%d] because their term is lower [%d<%d]\n",
			rf.me, rf.state, args.CandidateId, args.Term, rf.currentTerm)
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		if args.LastLogTerm > rf.getLastEntryTerm() ||
			args.LastLogTerm == rf.getLastEntryTerm() && args.LastLogIndex >= rf.getLastEntryFullIndex() {
			voteGranted = true
			rf.becomeFollower(args.Term, args.CandidateId)

			DPrintf("[%d] (%s) votes for candidate [%d] in term [%d]\n", rf.me, rf.state, args.CandidateId, args.Term)
		} else {
			DPrintf("[%d] (%s) not voting for candidate [%d] because their log is not up-to-date, their LL-index=%d, our LL-index=%d, their LL-term=%d, our LL-term=%d\n",
				rf.me, rf.state, args.CandidateId, args.LastLogIndex, rf.getLastEntryFullIndex(), args.LastLogTerm, rf.getLastEntryTerm())
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(ErrorColor, "[[[[[ APPEND ENTRIES ]]]]]")
	success := false
	if args.Term < rf.currentTerm {
		DPrintf("[%d] (%s) got HB from [%d], not following because args.Term=%d < currentTerm=%d.\n",
			rf.me, rf.state, args.LeaderId, args.Term, rf.currentTerm)
	} else {
		rf.becomeFollower(args.Term, -1)

		DPrintf("[%d] (%s) got HB from [%d] with %d entries, our log=%d, our commitIndex=%d, PrevLogIndex=%d, LeaderCommit=%d\n",
			rf.me, rf.state, args.LeaderId, len(args.Entries), rf.fullLogLen(), rf.commitIndex, args.PrevLogIndex, args.LeaderCommit)

		prevLogIndex := rf.realIndex(args.PrevLogIndex)
		// DPrintf("[%d] (%s) ---- PrevLogIndex=%d, prevLogIndex=%d, PrevLogTerm=%d, lastIncludedTerm=%d, len(log)=%d, realLen=%d",
		// 	rf.me, rf.state, args.PrevLogIndex, prevLogIndex, args.PrevLogTerm, rf.lastIncludedTerm, rf.fullLogLen(), len(rf.log))
		if (prevLogIndex < 0 || len(rf.log) == 0) && rf.lastIncludedTerm == args.PrevLogTerm || // for snapshot case
			prevLogIndex >= 0 && prevLogIndex < len(rf.log) && rf.log[prevLogIndex].Term == args.PrevLogTerm { // for normal case
			success = true
			rf.updateFollowerLog(args)
			reply.XLen = rf.fullLogLen()
		} else {
			xTerm, xIndex := rf.findReplyXArgs(prevLogIndex, reply)
			reply.XTerm = xTerm
			reply.XIndex = rf.fullIndex(xIndex)
			reply.XLen = rf.fullLogLen()

			trm := -1
			if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
				trm = rf.log[prevLogIndex].Term
			}
			DPrintf("[%d] (%s) Log indices do not match: PrevLogIndex=%d, len(log)=%d, PrevLogTerm=%d, our term at PrevLogTerm index=%d, len=%d\n",
				rf.me, rf.state, args.PrevLogIndex, rf.fullLogLen(), args.PrevLogTerm, trm, len(rf.log))
			DPrintf("[%d] (%s) Sending back XTerm=%d, XIndex=%d, XLen=%d\n", rf.me, rf.state, reply.XTerm, reply.XIndex, reply.XLen)
		}
	}
	DPrintf(ErrorColor, "[[[[[ /APPEND ENTRIES/ ]]]]]\n")

	reply.Term = rf.currentTerm
	reply.Success = success
}

func (rf *Raft) findReplyXArgs(prevLogIndex int, reply *AppendEntriesReply) (int, int) {
	term := -1
	index := -1
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		term = rf.log[prevLogIndex].Term
		index = prevLogIndex
		for index >= 0 && rf.log[index].Term == term {
			index--
		}
	}
	return term, index + 1
}

//
// InstallSnapshot RPC handler.
//
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		DPrintf("[%d] (%s) not installing snapshot from leader [%d] because their term is lower [%d<%d]",
			rf.me, rf.state, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	DPrintf("[%d] (%s) got snapshot from leader [%d], lastIncludedIndex=%d, lastIncludedTerm=%d",
		rf.me, rf.state, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)

	lastIndex, lastTerm, appData := rf.decodeSnapshot(args.Data)
	if lastIndex == args.LastIncludedIndex && lastTerm == args.LastIncludedTerm && len(appData) > 0 {
		DPrintf(InfoColor, "[[[[[ INSTALLING SNAPSHOT ]]]]]")

		rf.snapshot = args.Data
		rf.applySnapshot(args.LastIncludedIndex, args.LastIncludedTerm, appData)
		rf.persister.SaveStateAndSnapshot(rf.encodeState(), rf.snapshot)

		rf.becomeFollower(args.Term, -1)
		reply.Term = rf.currentTerm

		DPrintf("[%d] (%s) Snapshot install complete, sending term=%d", rf.me, rf.state, reply.Term)
		DPrintf(InfoColor, "[[[[[ /INSTALLING SNAPSHOT/ ]]]]]")
	} else {
		DPrintf("[%d] (%s) Decoded snapshot data does not match arguments, decodedIndex=%d, decodedTerm=%d, len(appData)=%d",
			rf.me, rf.state, lastIndex, lastTerm, len(appData))
	}
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
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.HandleInstallSnapshot", args, reply)
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
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		index = rf.fullLogLen()
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command, Index: index})
		DPrintf("[%d] (%s) Start: added (%v) to log, len(log)=%d\n", rf.me, rf.state, command, index)

		rf.persist()
	}

	return index, term, isLeader
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
	rf.mu.Lock()
	rf.state = Killed
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLastEntryFullIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastEntryIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.realIndex(rf.log[len(rf.log)-1].Index)
}

func (rf *Raft) getLastEntryTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) applyCommands() {
	for {
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		rf.mu.Unlock()

		for count := lastApplied; count < commitIndex; count++ {
			rf.mu.Lock()
			idx := rf.realIndex(rf.lastApplied + 1)
			if idx < 0 || idx >= len(rf.log) {
				rf.lastApplied = rf.lastIncludedIndex
				rf.commitIndex = rf.lastIncludedIndex
				rf.mu.Unlock()
				break
			}
			rf.lastApplied++
			entry := rf.log[idx]
			rf.mu.Unlock()

			//DPrintf("[%d] (%s) Applying command index=%d, lastApplied=%d, commitIndex=%d\n",
			//	rf.me, state, entry.Index, lastApplied, commitIndex)

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}

		time.Sleep(time.Duration(ApplyInterval) * time.Millisecond)

		if rf.killed() {
			DPrintf("[%d] was killed, stopping apply loop.\n", rf.me)
			return
		}
	}
}

func (rf *Raft) updateFollowerLog(args *AppendEntriesArgs) {
	if len(args.Entries) > 0 {
		DPrintf("[%d] (%s) Starting update to follower log with %d new entries, before len(log)=%d, commitIndex=%d, leaderCommit=%d\n",
			rf.me, rf.state, len(args.Entries), rf.fullLogLen(), rf.commitIndex, args.LeaderCommit)
	}

	i := 0
	prevIndex := rf.realIndex(args.PrevLogIndex + 1)
	for ; i < len(args.Entries) && prevIndex < len(rf.log); i++ {
		term := rf.lastIncludedTerm
		if prevIndex >= 0 {
			term = rf.log[prevIndex].Term
		}
		if args.Entries[i].Term != term {
			break
		}
		prevIndex++
	}
	if prevIndex < 0 {
		prevIndex = 0
	}

	if prevIndex < len(rf.log) {
		rf.log = rf.log[:prevIndex]
		DPrintf("[%d] (%s) Truncated our log from %d to %d entries, fullLog=%d\n", rf.me, rf.state, len(rf.log), prevIndex, rf.fullLogLen())
	}

	newEntries := len(args.Entries) - i
	if newEntries > 0 {
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()

		DPrintf("[%d] (%s) Appended %d new entries to our log, after len(log)=%d\n", rf.me, rf.state, newEntries, rf.fullLogLen())
	}
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := rf.getLastEntryFullIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		DPrintf("[%d] (%s) Updated follower commitIndex to=%d, len(log)=%d, leaderCommit=%d\n",
			rf.me, rf.state, rf.commitIndex, rf.fullLogLen(), args.LeaderCommit)
	}
	DPrintf("[%d] (%s) Sending back update saying we added %d new entries from %d args.Entries, after len(log)=%d",
		rf.me, rf.state, newEntries, len(args.Entries), rf.fullLogLen())
}

func (rf *Raft) becomeFollower(newTerm int, votedFor int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = votedFor
	rf.lastTime = time.Now()

	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.lastTime = time.Now()

	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.votedFor = -1
	rf.lastTime = time.Now()
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.getLastEntryFullIndex() + 1
		rf.matchIndex[peer] = 0
	}
}

func (rf *Raft) checkTerm(ourTerm int, theirTerm int) bool {
	if ourTerm < theirTerm {
		DPrintf("[%d] (%s) got a higher term in reply, stepping down.\n", rf.me, rf.state)
		rf.becomeFollower(theirTerm, -1)
		return false
	}
	return ourTerm == theirTerm
}

func (rf *Raft) updateCommitIndex() {
	prevCommitIndex := rf.realIndex(rf.commitIndex)
	for index := len(rf.log) - 1; index > prevCommitIndex; index-- {
		if rf.log[index].Term != rf.currentTerm {
			continue
		}
		count := 1
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer != rf.me && rf.realIndex(rf.matchIndex[peer]) >= index {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			DPrintf("[%d] (%s) Setting commitIndex from %d to %d", rf.me, rf.state, rf.commitIndex, rf.fullIndex(index))
			rf.commitIndex = rf.fullIndex(index)
			break
		}
	}
}

func (rf *Raft) processAppendEntriesReply(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if termOk := rf.checkTerm(args.Term, reply.Term); termOk {
		if reply.Success && len(args.Entries) > 0 && reply.XLen > 0 {
			DPrintf("[%d] (%s) Got success from peer=[%d] which has %d log entries / we sent %d entries before\n",
				rf.me, rf.state, peer, reply.XLen, len(args.Entries))

			rf.nextIndex[peer] = reply.XLen
			if rf.nextIndex[peer] > rf.fullLogLen() {
				DPrintf("[%d] (%s) [WARNING] Setting nextIndex[%d] to %d - follower's log has %d entries, our log=%d\n",
					rf.me, rf.state, peer, rf.nextIndex[peer], reply.XLen, rf.fullLogLen())
			}
			rf.matchIndex[peer] = args.Entries[len(args.Entries)-1].Index
			rf.updateCommitIndex()
		} else if !reply.Success {
			DPrintf("[%d] (%s) Got failure from peer=%d which has %d total entries / we sent %d entries before\n",
				rf.me, rf.state, peer, reply.XLen, len(args.Entries))
			if reply.XTerm != -1 {
				// find last entry for args.XTerm, if any
				index := rf.getLastEntryIndex()
				for index >= 0 && rf.log[index].Term > reply.XTerm {
					index--
				}
				if index >= 0 && rf.log[index].Term == reply.XTerm {
					// case 1: leader has XTerm
					rf.nextIndex[peer] = rf.fullIndex(index)
				} else {
					// case 2: leader does not have XTerm
					rf.nextIndex[peer] = reply.XIndex
				}
			} else {
				// case 3: follower's log is too short
				rf.nextIndex[peer] = reply.XLen
			}
		}
	}
}

func (rf *Raft) processInstallSnapshotReply(peer int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if ok := rf.checkTerm(args.Term, reply.Term); ok {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
}

func (rf *Raft) sendHeartbeats() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			DPrintf("[%d] (%s) realised it's not a leader anymore, will stop sending HB.\n", rf.me, rf.state)
			rf.mu.Unlock()
			return
		}

		DPrintf(WarningColor, "[[[[[ HEARTBEATS ]]]]]")
		DPrintf("[%d] (%s) sending out HB to all peers, log=%d, realLog=%d, commitIndex=%d",
			rf.me, rf.state, rf.fullLogLen(), len(rf.log), rf.commitIndex)
		lastIndex := rf.getLastEntryIndex()
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer == rf.me {
				continue
			}

			nextIndex := rf.realIndex(rf.nextIndex[peer])
			prevLogTerm := rf.lastIncludedTerm
			if nextIndex > 0 && nextIndex <= len(rf.log) {
				prevLogTerm = rf.log[nextIndex-1].Term
			}

			if nextIndex >= 0 {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[peer] - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      make([]LogEntry, 0),
					LeaderCommit: rf.commitIndex,
				}
				if lastIndex >= nextIndex {
					args.Entries = rf.log[nextIndex:]
				}

				go func(p int) {
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(p, &args, &reply); ok {
						rf.processAppendEntriesReply(p, args, reply)
					}
				}(peer)
			} else {
				DPrintf("[%d] (%s) Peer [%d] too far behind, sending snapshot instead of AppendEntries", rf.me, rf.state, peer)
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				go func(p int) {
					reply := InstallSnapshotReply{}
					if ok := rf.sendInstallSnapshot(p, &args, &reply); ok {
						rf.processInstallSnapshotReply(p, args, reply)
					}
				}(peer)
			}
		}
		rf.mu.Unlock()
		DPrintf(WarningColor, "[[[[[ /HEARTBEATS/ ]]]]]\n")

		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) attemptLeaderElection() {
	rf.mu.Lock()
	DPrintf("[%d] becomes a candidate.\n", rf.me)
	rf.becomeCandidate()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastEntryFullIndex(),
		LastLogTerm:  rf.getLastEntryTerm(),
	}
	rf.mu.Unlock()

	var votes int32 = 1
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(p, &args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.VoteGranted {
					total := int(atomic.AddInt32(&votes, 1))
					if total > len(rf.peers)/2 && rf.state == Candidate {
						DPrintf("[%d] got %d votes, becomes a leader.\n", rf.me, votes)
						rf.becomeLeader()
						DPrintf(NoticeColor, "[[[[[ /ELECTIONS/ ]]]]]")
						go rf.sendHeartbeats()
					}
				} else {
					rf.checkTerm(args.Term, reply.Term)
				}
			}
		}(peer)
	}
}

func (rf *Raft) periodicLeaderElection() {
	for {
		start := time.Now()
		interval := MinElectionInterval + rand.Intn(200)
		time.Sleep(time.Duration(interval) * time.Millisecond)

		if rf.killed() {
			DPrintf("[%d] was killed, stopping election loop.\n", rf.me)
			return
		}

		rf.mu.Lock()
		if rf.lastTime.Before(start) && rf.state != Leader {
			DPrintf(NoticeColor, "[[[[[ ELECTIONS ]]]]]")
			DPrintf("[%d] (%s) starting elections.\n", rf.me, rf.state)
			go rf.attemptLeaderElection()
		}
		rf.mu.Unlock()
	}
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
		log:         []LogEntry{LogEntry{Term: 0, Index: 0}},
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		lastTime:    time.Now(),
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
	}

	go rf.periodicLeaderElection()
	go rf.applyCommands()
	// initialize from state persisted before a crash
	go rf.recoverState()

	return rf
}
