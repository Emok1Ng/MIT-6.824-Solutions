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
	"math"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type State string

const (
	follower                   State = "Follower"
	candidate                  State = "Candidate"
	leader                     State = "Leader"
	heartBeatInterval                = 100 * time.Millisecond
	updateCommitIndexInterval        = 10 * time.Millisecond
	checkLastLogIndexInterval        = 10 * time.Millisecond
	appendEntriesRetryInterval       = 10 * time.Millisecond
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	state       State
	overtime    time.Duration
	heartBeat   bool
	applyCh     chan ApplyMsg
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == leader
	// Your code here (2A).
	return term, isLeader
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
		return
	}
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
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft[%v](%v term:%v) receive RequestVote request.\n", rf.me, rf.state, rf.currentTerm)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
		DPrintf("Raft[%v](%v term:%v) change to follower due to higher term %v.\n", rf.me, rf.state, rf.currentTerm, args.Term)
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if rf.state == follower {
		if args.Term < rf.currentTerm {
			return
		}
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateUpToDate(args.Term, args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			DPrintf("Raft[%v](%v term:%v) vote for Raft[%v].\n", rf.me, rf.state, rf.currentTerm, rf.votedFor)
			rf.heartBeat = true
		}
	}
}

func (rf *Raft) isCandidateUpToDate(term int, lastLogIndex int) bool {
	rfLastLogIndex := len(rf.log) - 1
	if term != rf.log[rfLastLogIndex].Term {
		return term > rf.log[rfLastLogIndex].Term
	}
	return lastLogIndex >= rfLastLogIndex
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("Raft[%v](%v term:%v) receive AppendEntries.\n", rf.me, rf.state, rf.currentTerm)
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
		DPrintf("Raft[%v](%v term:%v) change to follower due to higher term %v.\n", rf.me, rf.state, rf.currentTerm, args.Term)
	}
	reply.Success = false
	reply.Term = rf.currentTerm
	if rf.state == follower || rf.state == candidate {
		if args.Term < rf.currentTerm {
			return
		}
		if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}
		if rf.state == follower {
			rf.heartBeat = true
		}
		if rf.state == candidate {
			rf.currentTerm = args.Term
			rf.state = follower
			rf.votedFor = -1
			DPrintf("Raft[%v](%v term:%v) change to follower due to higher term %v.\n", rf.me, rf.state, rf.currentTerm, args.Term)
		}
		reply.Success = true
		DPrintf("Raft[%v](%v term:%v) starts matching logs.\n", rf.me, rf.state, rf.currentTerm)
		for i := range args.Entries {
			localI := args.PrevLogIndex + i + 1

			if localI >= len(rf.log) || (localI < len(rf.log) && rf.log[localI].Term != args.Entries[i].Term) {
				if localI < len(rf.log) {
					rf.log = rf.log[:localI]
				}
				break
			}
		}
		for i := range args.Entries {
			localI := args.PrevLogIndex + i + 1
			if localI >= len(rf.log) {
				rf.log = append(rf.log, args.Entries[i])
			} else {
				rf.log[localI] = args.Entries[i]
			}
		}
		prevCommitIndex := rf.commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(args.PrevLogIndex+len(args.Entries))))
		}
		for i := prevCommitIndex + 1; i <= rf.commitIndex; i++ {
			DPrintf("Raft[%v](%v term:%v) sending to applyCh, cmd:%v, cmdIndex:%v.\n", rf.me, rf.state, rf.currentTerm, rf.log[i].Command, i)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
		}
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return index, term, isLeader
	}
	isLeader = rf.state == leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		index = len(rf.log) - 1
		term = rf.currentTerm
	}
	DPrintf("Raft[%v](%v term:%v) return index:%v term:%v isLeader:%v.\n", rf.me, rf.state, rf.currentTerm, index, term, isLeader)
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(rf.overtime)
		rf.mu.Lock()
		DPrintf("Raft[%v](%v term:%v) timeout at %v.\n", rf.me, rf.state, rf.currentTerm, time.Now())
		if rf.heartBeat == false {
			if rf.state == leader {
				rf.mu.Unlock()
				continue
			}
			rf.overtime = time.Duration(200+rand.Intn(200)) * time.Millisecond
			DPrintf("Raft[%v](%v term:%v) set overtime to %v.\n", rf.me, rf.state, rf.currentTerm, rf.overtime)
			rf.startElection()
			rf.mu.Unlock()
		} else {
			rf.heartBeat = false
			rf.mu.Unlock()
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) startElection() {
	rf.state = candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.heartBeat = true
	DPrintf("Raft[%v](%v term:%v) start sending RequestVote RPC to all peers.\n", rf.me, rf.state, rf.currentTerm)
	voteCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		index := i
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		reply := &RequestVoteReply{}
		DPrintf("Raft[%v](%v term:%v) sending RequestVote RPC to Raft[%v].\n", rf.me, rf.state, rf.currentTerm, index)
		go func() {
			rf.sendRequestVote(index, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("Raft[%v](%v term:%v) got reply %v from Raft[%v].\n", rf.me, rf.state, rf.currentTerm, reply, index)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				rf.state = follower
				rf.votedFor = -1
				DPrintf("Raft[%v](%v term:%v) change to follower due to higher term %v.\n", rf.me, rf.state, rf.currentTerm, reply.Term)
				return
			}
			if reply.VoteGranted {
				DPrintf("Raft[%v](%v term:%v) get vote from Raft[%v].\n", rf.me, rf.state, rf.currentTerm, index)
				voteCount++
				if voteCount == len(rf.peers)/2+1 {
					rf.state = leader
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					DPrintf("Raft[%v](%v term:%v) get majority voting and become the leader, log:%v.\n", rf.me, rf.state, rf.currentTerm, rf.log)
					go rf.handleHeartBeats()
					go rf.checkLogIndex()
					go rf.updateCommitIndex()
				}
			}
		}()
	}
}

func (rf *Raft) checkLogIndex() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			return
		}
		if rf.state != leader {
			rf.mu.Unlock()
			time.Sleep(checkLastLogIndexInterval)
			continue
		}
		DPrintf("Raft[%v](%v term:%v) start checking last log index with followers.\n", rf.me, rf.state, rf.currentTerm)
		for i := range rf.peers {
			index := i
			if i == rf.me {
				continue
			}
			if len(rf.log)-1 >= rf.nextIndex[i] {
				DPrintf("Raft[%v](%v term:%v) send non-heartbeat AppendEntries RPC to Raft[%v].\n", rf.me, rf.state, rf.currentTerm, index)
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					Entries:      make([]LogEntry, len(rf.log[rf.nextIndex[i]:])),
					LeaderCommit: rf.commitIndex,
				}
				copy(args.Entries, rf.log[rf.nextIndex[i]:])
				reply := &AppendEntriesReply{}
				go func() {
					for {
						rf.mu.Lock()
						if rf.state != leader {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						ok := rf.sendAppendEntries(index, args, reply)
						rf.mu.Lock()
						if !ok {
							DPrintf("Raft[%v](%v term:%v) failed to send non-heartbeat AppendEntries RPC to Raft[%v].\n", rf.me, rf.state, rf.currentTerm, index)
							rf.mu.Unlock()
							return
						}
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = follower
							rf.votedFor = -1
							DPrintf("Raft[%v](%v term:%v) change to follower due to higher term %v.\n", rf.me, rf.state, rf.currentTerm, reply.Term)
							rf.mu.Unlock()
							return
						}
						if reply.Term > args.Term {
							rf.mu.Unlock()
							return
						}
						if reply.Success {
							break
						} else {
							DPrintf("Raft[%v](%v term:%v) retry sending non-heartbeat AppendEntries RPC to Raft[%v], prev args:%v.\n", rf.me, rf.state, rf.currentTerm, index, args)
							prevLogTerm := args.PrevLogTerm
							for rf.log[args.PrevLogIndex].Term == prevLogTerm {
								args.PrevLogTerm--
							}
							args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
							args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1:]))
							copy(args.Entries, rf.log[args.PrevLogIndex+1:])
							rf.mu.Unlock()
							time.Sleep(appendEntriesRetryInterval)
						}
					}
					DPrintf("Raft[%v](%v term:%v) successfully send non-heartbeat AppendEntries RPC to Raft[%v].\n", rf.me, rf.state, rf.currentTerm, index)
					rf.nextIndex[index] = int(math.Max(float64(args.PrevLogIndex+len(args.Entries)+1), float64(rf.nextIndex[index])))
					rf.matchIndex[index] = rf.nextIndex[index] - 1
					DPrintf("Raft[%v](%v term:%v) append non-heartbeat AppendEntries, Raft[%v] nextIndex:%v.\n", rf.me, rf.state, rf.currentTerm, index, rf.nextIndex[index])
					rf.mu.Unlock()
				}()
			}
		}
		rf.mu.Unlock()
		time.Sleep(checkLastLogIndexInterval)
	}
}

func (rf *Raft) updateCommitIndex() {
	for {
		rf.mu.Lock()
		if rf.state == leader {
			next := true
			for commitIndex := rf.commitIndex + 1; next; {
				next = false
				count := 1
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= commitIndex {
						next = true
						if rf.log[commitIndex].Term == rf.currentTerm {
							count++
						}
					}
				}
				if count > len(rf.peers)/2 {
					DPrintf("Raft[%v](%v term:%v) sending cmd:%v index:%v to be committed.\n", rf.me, rf.state, rf.currentTerm, rf.log[commitIndex].Command, commitIndex)
					for t := rf.commitIndex + 1; t <= commitIndex; t++ {
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.log[t].Command,
							CommandIndex: t,
						}
					}
					rf.commitIndex = commitIndex
				}
				commitIndex++
			}
		}
		rf.mu.Unlock()
		time.Sleep(updateCommitIndexInterval)
	}
}

func (rf *Raft) handleHeartBeats() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			DPrintf("Raft[%v](%v term:%v) is killed.\n", rf.me, rf.state, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		if rf.state != leader {
			DPrintf("Raft[%v](%v term:%v) is not leader.\n", rf.me, rf.state, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		DPrintf("Raft[%v](%v term:%v) start sending heartbeat.\n", rf.me, rf.state, rf.currentTerm)
		rf.sendHeartBeats()
		DPrintf("Raft[%v](%v term:%v) finish sending heartbeat.\n", rf.me, rf.state, rf.currentTerm)
		rf.mu.Unlock()
		time.Sleep(heartBeatInterval)
	}
}

func (rf *Raft) sendHeartBeats() {
	DPrintf("Raft[%v](%v term:%v) ready to send heartbeat.\n", rf.me, rf.state, rf.currentTerm)
	for i := range rf.peers {
		index := i
		reply := &AppendEntriesReply{}
		if index == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[index] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[index]-1].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		go func() {
			ok := rf.sendAppendEntries(index, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				DPrintf("Raft[%v](%v term:%v) failed to send heartbeats.\n", rf.me, rf.state, rf.currentTerm)
			}
			if rf.killed() {
				DPrintf("Raft[%v](%v term:%v) is killed.\n", rf.me, rf.state, rf.currentTerm)
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				rf.state = follower
				rf.votedFor = -1
				DPrintf("Raft[%v](%v term:%v) change to follower due to higher term %v.\n", rf.me, rf.state, rf.currentTerm, reply.Term)
				return
			}
		}()
	}
}

func (rf *Raft) sendAppendEntries(index int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[index].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.mu = sync.Mutex{}
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastApplied = 0
	rf.state = follower
	rf.heartBeat = false
	rf.overtime = time.Duration(200+rand.Intn(200)) * time.Millisecond
	rf.applyCh = applyCh

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
