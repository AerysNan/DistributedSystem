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

	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	applyCh chan ApplyMsg

	electTimer *time.Timer
	heartTimer *time.Timer
}

func (rf *Raft) transitState(state int) {
	if rf.state == state {
		return
	}
	DPrintf("[%v(%v)] transit from state %v to state %v", rf.me, rf.currentTerm, StateToString(rf.state), StateToString(state))
	switch state {
	case StateFollower:
		rf.heartTimer.Stop()
		rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
		rf.votedFor = -1
		rf.persist()
	case StateCandidate:
	case StateLeader:
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.logs)
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("[%v(%v)] read persist failed", rf.me, rf.currentTerm)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
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

func (rf *Raft) startElection() {
	rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
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
	DPrintf("[%v(%v)] receive request vote from %v(%v)", rf.me, rf.currentTerm, args.CandidateId, args.Term)
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
		(rf.logs[end].Term == args.LastLogTerm && end > args.LastLogIndex) {
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
	DPrintf("[%v(%v)] with logs %v receive append entries %v from %v(%v)", rf.me, rf.currentTerm, EntriesToString(rf.logs), EntriesToString(args.Entries), args.LeaderId, args.Term)
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("[%v(%v)] reject append entries due to term", rf.me, rf.currentTerm)
		return
	}
	rf.electTimer.Reset(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	if len(rf.logs)-1 < args.PrevLogIndex ||
		rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		if len(rf.logs)-1 < args.PrevLogIndex {
			reply.ConflictIndex = len(rf.logs)
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
			index := args.PrevLogIndex
			for rf.logs[index-1].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		DPrintf("[%v(%v)] reject append entries due to log mismatch", rf.me, rf.currentTerm)
		return
	}
	for index, entry := range args.Entries {
		if len(rf.logs) <= args.PrevLogIndex+index+1 ||
			rf.logs[args.PrevLogIndex+index+1].Term != entry.Term {
			rf.logs = append(rf.logs[:args.PrevLogIndex+index+1], args.Entries[index:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		end := len(rf.logs) - 1
		if args.LeaderCommit <= end {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = end
		}
		rf.apply()
	}
	rf.currentTerm = args.Term
	reply.Success = true
	reply.Term = args.Term
	rf.transitState(StateFollower)
}

func (rf *Raft) apply() {
	if rf.commitIndex > rf.lastApplied {
		go func(start int, entries []LogEntry) {
			for offset, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: start + offset,
				}

				rf.mu.Lock()
				rf.lastApplied = start + offset
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, rf.logs[rf.lastApplied+1:rf.commitIndex+1])
		DPrintf("[%v(%v)] apply entries from %v to %v", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex+1)
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
	index := -1
	term, isLeader := rf.GetState()
	if isLeader {
		rf.mu.Lock()
		index = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{
			Command: command,
			Term:    term,
		})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.mu.Unlock()
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
	DPrintf("[%v(%v)] broadcast heartbeat with logs %v", rf.me, rf.currentTerm, EntriesToString(rf.logs))
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
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[receiver] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[receiver]-1].Term,
				Entries:      rf.logs[rf.nextIndex[receiver]:],
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			if rf.sendAppendEntries(receiver, &args, &reply) {
				rf.mu.Lock()
				if reply.Success {
					rf.matchIndex[receiver] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[receiver] = rf.matchIndex[receiver] + 1
					for j := len(rf.logs) - 1; j > rf.commitIndex; j-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= j {
								count += 1
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = j
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
							for i := args.PrevLogIndex; i >= 1; i-- {
								if rf.logs[i-1].Term == reply.ConflictTerm {
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
			rf.transitState(StateCandidate)
			rf.currentTerm += 1
			rf.persist()
			DPrintf("[%v(%v)] election timer timeout", rf.me, rf.currentTerm)
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
	rf.matchIndex = make([]int, len(rf.peers))
	rf.heartTimer = time.NewTimer(HeartbeatTimeout * time.Millisecond)
	rf.electTimer = time.NewTimer(RandomRange(ElectTimeoutMin, ElectTimeoutMax) * time.Millisecond)
	go rf.startTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
