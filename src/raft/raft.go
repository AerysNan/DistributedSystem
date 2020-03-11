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
	"bytes"
	"distributed/labgob"
	"distributed/labrpc"
	"distributed/util"
	"sync"
	"time"
)

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
	Command interface{}
	Term    int
}

const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

const (
	HeartbeatTimeout = 120
	ElectTimeoutMin  = 250
	ElectTimeoutMax  = 400
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	state       int

	logs          []LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	snapshotIndex int

	applyCh chan ApplyMsg

	electTimer *time.Timer
	heartTimer *time.Timer
}

func (rf *Raft) CurrentTerm() int {
	return rf.currentTerm
}

func (rf *Raft) SnapshotIndex() int {
	return rf.snapshotIndex
}

func (rf *Raft) toAbsolute(index int) int {
	return index + rf.snapshotIndex
}

func (rf *Raft) toRelative(index int) int {
	return index - rf.snapshotIndex
}

func (rf *Raft) transitState(state int) {
	if rf.state == state {
		return
	}
	util.DPrintf("[%vT%vS%v] transit from state %v to state %v", rf.me, rf.currentTerm, rf.snapshotIndex, StateToString(rf.state), StateToString(state))
	switch state {
	case StateFollower:
		rf.heartTimer.Stop()
		rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
		rf.votedFor = -1
		rf.persist()
	case StateCandidate:
	case StateLeader:
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.toAbsolute(len(rf.logs))
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = rf.snapshotIndex
		}
		rf.electTimer.Stop()
		rf.heartTimer.Reset(HeartbeatTimeout * time.Millisecond)
	}
	rf.state = state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, snapshotIndex int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&logs) != nil {
		util.DPrintf("[%vT%vS%v] read persist failed", rf.me, rf.currentTerm, rf.snapshotIndex)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.snapshotIndex = snapshotIndex
	rf.lastApplied = snapshotIndex
	rf.commitIndex = snapshotIndex
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) startElection() {
	rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.toAbsolute(len(rf.logs) - 1),
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	total := 1
	rf.votedFor = rf.me
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(receiver int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(receiver, &args, &reply) {
				rf.mu.Lock()
				if reply.VoteGranted && rf.state == StateCandidate {
					total += 1
					if total > len(rf.peers)/2 && rf.state == StateCandidate {
						rf.transitState(StateLeader)
						rf.broadcastHeartbeat()
					}
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.transitState(StateFollower)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	util.DPrintf("[%vT%vS%v] receive request vote from [%vT%v]", rf.me, rf.currentTerm, rf.snapshotIndex, args.CandidateId, args.Term)
	end := len(rf.logs) - 1
	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.transitState(StateFollower)
	}
	if rf.logs[end].Term > args.LastLogTerm ||
		(rf.logs[end].Term == args.LastLogTerm && args.LastLogIndex < rf.toAbsolute(end)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	rf.transitState(StateFollower)
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	util.DPrintf("[%vT%vS%v] with logs %v receive append entries %v from [%vT%v]", rf.me, rf.currentTerm, rf.snapshotIndex, EntriesToString(rf.logs), EntriesToString(args.Entries), args.LeaderId, args.Term)
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		util.DPrintf("[%vT%vS%v] reject append entries due to term", rf.me, rf.currentTerm, rf.snapshotIndex)
		return
	}
	rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	if args.PrevLogIndex <= rf.snapshotIndex {
		util.DPrintf("[%vT%vS%v] previous index %v before snapshot index %v", rf.me, rf.currentTerm, rf.snapshotIndex, args.PrevLogIndex, rf.snapshotIndex)
		reply.Success = true
		reply.Term = rf.currentTerm
		if args.PrevLogIndex+len(args.Entries) > rf.snapshotIndex {
			rf.logs = rf.logs[:1]
			rf.logs = append(rf.logs, args.Entries[rf.snapshotIndex-args.PrevLogIndex:]...)
		}
		if args.LeaderCommit > rf.commitIndex {
			end := rf.toAbsolute(len(rf.logs) - 1)
			if args.LeaderCommit <= end {
				rf.commitIndex = args.LeaderCommit
				util.DPrintf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, args.LeaderCommit)
			} else {
				rf.commitIndex = end
				util.DPrintf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, end)
			}
			rf.apply()
		}
		return
	}
	if rf.toAbsolute(len(rf.logs)-1) < args.PrevLogIndex ||
		rf.logs[rf.toRelative(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		if rf.toAbsolute(len(rf.logs)-1) < args.PrevLogIndex {
			reply.ConflictIndex = rf.toAbsolute(len(rf.logs))
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.logs[rf.toRelative(args.PrevLogIndex)].Term
			index := args.PrevLogIndex
			for rf.logs[rf.toRelative(index-1)].Term == reply.ConflictTerm && index > rf.snapshotIndex+1 {
				index--
			}
			reply.ConflictIndex = index
		}
		util.DPrintf("[%vT%vS%v] reject append entries due to log mismatch", rf.me, rf.currentTerm, rf.snapshotIndex)
		return
	}
	for index, entry := range args.Entries {
		if len(rf.logs) <= rf.toRelative(args.PrevLogIndex+index+1) ||
			rf.logs[rf.toRelative(args.PrevLogIndex+index+1)].Term != entry.Term {
			rf.logs = append(rf.logs[:rf.toRelative(args.PrevLogIndex+index+1)], args.Entries[index:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		end := rf.toAbsolute(len(rf.logs) - 1)
		if args.LeaderCommit <= end {
			rf.commitIndex = args.LeaderCommit
			util.DPrintf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, args.LeaderCommit)
		} else {
			rf.commitIndex = end
			util.DPrintf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, end)
		}
		rf.apply()
	}
	rf.currentTerm = args.Term
	reply.Success = true
	reply.Term = args.Term
	rf.transitState(StateFollower)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.snapshotIndex {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.transitState(StateFollower)
	}
	index := rf.toRelative(args.LastIncludedIndex)
	if len(rf.logs) > index &&
		rf.logs[index].Term == args.LastIncludedTerm {
		rf.logs = rf.logs[index:]
	} else {
		rf.logs = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}
	util.DPrintf("[%vT%vS%v] install snapshot to index %v", rf.me, rf.currentTerm, rf.snapshotIndex, args.LastIncludedIndex)
	rf.snapshotIndex = args.LastIncludedIndex
	if rf.commitIndex < rf.snapshotIndex {
		rf.commitIndex = rf.snapshotIndex
	}
	util.DPrintf("[%vT%vS%v] commit index now %v after install snapshot", rf.me, rf.currentTerm, rf.snapshotIndex, rf.commitIndex)
	if rf.lastApplied < rf.snapshotIndex {
		rf.lastApplied = rf.snapshotIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)
	if rf.lastApplied > rf.snapshotIndex {
		return
	}
	command := ApplyMsg{
		CommandValid: true,
		CommandIndex: rf.snapshotIndex,
		Command: util.Op{
			Type:  "Snapshot",
			Value: string(rf.persister.ReadSnapshot()),
		},
	}
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(command)
}

func (rf *Raft) apply() {
	if rf.commitIndex <= rf.lastApplied {
		return
	}
	entries := rf.logs[rf.toRelative(rf.lastApplied+1):rf.toRelative(rf.commitIndex+1)]
	go func(start int, entries []LogEntry) {
		for offset, entry := range entries {
			util.DPrintf("[%vT%vS%v] send command %v to channel", rf.me, rf.currentTerm, rf.snapshotIndex, entry.Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: start + offset,
			}

			rf.mu.Lock()
			if rf.lastApplied < start+offset {
				rf.lastApplied = start + offset
			}
			rf.mu.Unlock()
		}
	}(rf.lastApplied+1, entries)
	util.DPrintf("[%vT%vS%v] apply entries %v~%v", rf.me, rf.currentTerm, rf.snapshotIndex, rf.lastApplied+1, rf.commitIndex)
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	util.DPrintf("[%vT%vS%v] start command %v", rf.me, rf.currentTerm, rf.snapshotIndex, command)
	index := -1
	term, isLeader := rf.GetState()
	if isLeader {
		rf.mu.Lock()
		index = rf.toAbsolute(len(rf.logs))
		rf.logs = append(rf.logs, LogEntry{
			Command: command,
			Term:    term,
		})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.mu.Unlock()
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.broadcastHeartbeat()
		}()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) broadcastHeartbeat() {
	util.DPrintf("[%vT%vS%v] broadcast heartbeat with logs %v", rf.me, rf.currentTerm, rf.snapshotIndex, EntriesToString(rf.logs))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(receiver int) {
			rf.mu.Lock()
			if rf.state != StateLeader {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[receiver] - 1
			if prevLogIndex < rf.snapshotIndex {
				rf.mu.Unlock()
				rf.sendSnapshot(receiver)
				return
			}
			entries := make([]LogEntry, len(rf.logs[rf.toRelative(prevLogIndex+1):]))
			copy(entries, rf.logs[rf.toRelative(prevLogIndex+1):])
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[rf.toRelative(prevLogIndex)].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			if rf.sendAppendEntries(receiver, &args, &reply) {
				rf.mu.Lock()
				if rf.state != StateLeader {
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.matchIndex[receiver] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[receiver] = rf.matchIndex[receiver] + 1
					for j := rf.toAbsolute(len(rf.logs) - 1); j > rf.commitIndex; j-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= j {
								count += 1
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = j
							util.DPrintf("[%vT%vS%v] update commit index to %v", rf.me, rf.currentTerm, rf.snapshotIndex, j)
							rf.apply()
							break
						}
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.transitState(StateFollower)
						rf.persist()
					} else {
						rf.nextIndex[receiver] = reply.ConflictIndex
						if reply.ConflictTerm != -1 {
							for i := args.PrevLogIndex; i >= rf.snapshotIndex+1; i-- {
								if rf.logs[rf.toRelative(i-1)].Term == reply.ConflictTerm {
									rf.nextIndex[receiver] = i
									break
								}
							}
						}
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) startTimer() {
	for {
		select {
		case <-rf.electTimer.C:
			rf.mu.Lock()
			util.DPrintf("[%vT%vS%v] election timer timeout", rf.me, rf.currentTerm, rf.snapshotIndex)
			rf.transitState(StateCandidate)
			rf.currentTerm += 1
			rf.persist()
			rf.startElection()
			rf.mu.Unlock()
		case <-rf.heartTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.broadcastHeartbeat()
				rf.heartTimer.Reset(HeartbeatTimeout * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.logs)
	return w.Bytes()
}

func (rf *Raft) ReplaceLogWithSnapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex {
		return
	}
	util.DPrintf("[%vT%vS%v] replace log with snapshot until index %v", rf.me, rf.currentTerm, rf.snapshotIndex, index)
	rf.logs = rf.logs[rf.toRelative(index):]
	rf.snapshotIndex = index
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), data)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendSnapshot(i)
	}
}

func (rf *Raft) sendSnapshot(server int) {
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.logs[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if rf.sendInstallSnapshot(server, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.transitState(StateFollower)
			rf.persist()
		} else {
			if rf.matchIndex[server] < args.LastIncludedIndex {
				rf.matchIndex[server] = args.LastIncludedIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = StateFollower
	rf.applyCh = applyCh
	rf.logs = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	rf.matchIndex = make([]int, len(rf.peers))
	rf.heartTimer = time.NewTimer(HeartbeatTimeout * time.Millisecond)
	rf.electTimer = time.NewTimer(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	go rf.startTimer()
	return rf
}
