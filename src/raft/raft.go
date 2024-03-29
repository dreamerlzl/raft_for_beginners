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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	logrus "github.com/sirupsen/logrus"

	"../labgob"
	"../labrpc"
)

var logLevel = logrus.DebugLevel
var heartbeatPeriod = 50
var electTimeoutBase = 250 // 250 - 500 ms for randomized election timeout
var electTimeoutRange = 250
var applyPeriod = 2 * heartbeatPeriod

// import "bytes"
// import "../labgob"

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
	CommandValid      bool
	Command           interface{}
	CommandIndex      int
	Snapshot          []byte
	LastIncludedIndex int
}

type LogEntry struct {
	EntryTerm int
	Command   interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	AcceptedLeader int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	log               []LogEntry
	lastIndex         int // the index of the last log entry
	currentTerm       int
	votedFor          int
	lastIncludedIndex int // the number of deleted log entries
	lastIncludedTerm  int

	// can be non-persistent
	numPeers       int
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	commitIndex    int
	lastSent       int
	applyCh        chan ApplyMsg
	roleLock       sync.Mutex

	// specific killing for this lab
	killedChan [3]chan bool
	killReply  chan bool
	downgrade  bool
	stale      bool // if true, then log is not up-to-date as a majority of other peers

	// leader only
	nextIndex  []int
	matchIndex []int

	// for debug
	debug bool
}

func (rf *Raft) Debugf(msg string, f ...interface{}) {
	if rf.debug {
		// log.Printf("[rf %d] %s", rf.me, fmt.Sprintf(msg, f...))
		logrus.Debugf("[rf %d] %s", rf.me, fmt.Sprintf(msg, f...))
	}
}

func (rf *Raft) Infof(msg string, f ...interface{}) {
	if rf.debug {
		// log.Printf("[rf %d] %s", rf.me, fmt.Sprintf(msg, f...))
		logrus.Infof("[rf %d] %s", rf.me, fmt.Sprintf(msg, f...))
	}
}

func (rf *Raft) Warnf(msg string, f ...interface{}) {
	if rf.debug {
		// log.Printf("[rf %d] %s", rf.me, fmt.Sprintf(msg, f...))
		logrus.Warnf("[rf %d] %s", rf.me, fmt.Sprintf(msg, f...))
	}
}

func (rf *Raft) Errorf(msg string, f ...interface{}) {
	if rf.debug {
		// log.Printf("[rf %d] %s", rf.me, fmt.Sprintf(msg, f...))
		logrus.Warnf("[rf %d] %s", rf.me, fmt.Sprintf(msg, f...))
	}
}

func (rf *Raft) lock(msg string, f ...interface{}) {
	rf.mu.Lock()
	rf.Debugf(msg, f...)
}

func (rf *Raft) unlock(msg string, f ...interface{}) {
	rf.Debugf(msg, f...)
	rf.mu.Unlock()
}

func (rf *Raft) newElectionTimer() *time.Timer {
	newElectionTimeout := rand.Intn(electTimeoutBase) + electTimeoutRange
	return time.NewTimer(time.Duration(newElectionTimeout) * time.Millisecond)
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	newElectionTimeout := rand.Intn(electTimeoutBase) + electTimeoutRange
	rf.electionTimer.Reset(time.Duration(newElectionTimeout) * time.Millisecond)
	rf.Debugf("resets election timer")
}

func (rf *Raft) newHeartbeatTimer() {
	rf.heartbeatTimer = time.NewTimer(time.Duration(heartbeatPeriod) * time.Millisecond)
}

func (rf *Raft) resetHeartbeatTimer() {
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C:
		default:
		}
	}
	rf.heartbeatTimer.Reset(time.Duration(heartbeatPeriod) * time.Millisecond)
	rf.Debugf("resets heartbeat timer")
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	return rf.log[index-rf.lastIncludedIndex]
}

func (rf *Raft) getLogEntries(start, end int) []LogEntry {
	if start < rf.lastIncludedIndex {
		rf.Errorf("lastIncludedIndex: %d start: %d end: %d", rf.lastIncludedIndex, start, end)
		panic("index out of bound")
	}
	return rf.log[start-rf.lastIncludedIndex : end-rf.lastIncludedIndex]
}

func (rf *Raft) getLogLength() int {
	return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) appendLogEntry(entry LogEntry) {
	rf.log = append(rf.log, entry)
}

func (rf *Raft) appendLogEntries(entries []LogEntry) {
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) replaceLogEntries(pos int, entries []LogEntry) {
	rf.log = append(rf.getLogEntries(rf.lastIncludedIndex, pos), entries...)
}

func (rf *Raft) trimEntries(index int) {
	rf.log = rf.log[index-rf.lastIncludedIndex:]
}

type State int

const (
	leader State = iota
	follower
	candidate
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = (rf.state == leader)
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) persist() {
	data := rf.encodeRaftState()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var currentTerm int
	var votedFor int
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&log) != nil {
		rf.Errorf("labgob decode error")
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIndex = lastIncludedIndex + len(rf.log) - 1
	}
	rf.resetElectionTimer()
	rf.Warnf("restarts with term %d, lastIncludedIndex: %d!", rf.currentTerm, rf.lastIncludedIndex)
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
	if rf.state == leader {
		isLeader = true
		term = rf.currentTerm
		rf.lastIndex++
		index = rf.lastIndex
		rf.appendLogEntry(LogEntry{Command: command, EntryTerm: term})
		rf.Infof("start to propose entry{command: %v, term: %d} on index %d", command, term, index)
		rf.persist()
		rf.mu.Unlock()
		// rf.bcastAppendEntries()
	} else {
		rf.mu.Unlock()
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
	// Your code here, if desired.
	rf.killedChan[rf.state] <- true
	rf.Warnf("has been killed!")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.AcceptedLeader = -1
	// Your initialization code here (2A, 2B, 2C).
	logrus.SetLevel(logLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: time.StampMilli,
		FullTimestamp:   true,
	})
	rf.killedChan[follower] = make(chan bool)
	rf.killedChan[leader] = make(chan bool)
	rf.killedChan[candidate] = make(chan bool)
	rf.killReply = make(chan bool)
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.appendLogEntry(LogEntry{EntryTerm: rf.currentTerm}) // the first entry is dummy
	rf.votedFor = -1                                       // -1 means vote for nothing
	rf.lastIndex = 0                                       // log index starts with 1
	rf.newHeartbeatTimer()
	rf.electionTimer = rf.newElectionTimer()
	rf.commitIndex = 0
	rf.lastSent = 0
	rf.numPeers = len(peers)
	rf.nextIndex = make([]int, rf.numPeers)
	rf.matchIndex = make([]int, rf.numPeers)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// send applyMsg
	go func() {
		for !rf.killed() {
			time.Sleep(time.Duration(applyPeriod))
			if rf.lastSent < rf.commitIndex {
				rf.lock(" starts to send applyMsg")
				rf.lastSent = MaxInt(rf.lastIncludedIndex, rf.lastSent)
				num := rf.commitIndex - rf.lastSent
				applyMsgs := make([]ApplyMsg, num)
				for i := 0; rf.lastSent < rf.commitIndex; i++ {
					rf.lastSent++
					applyMsg := ApplyMsg{
						Command:      rf.getLogEntry(rf.lastSent).Command,
						CommandIndex: rf.lastSent,
						CommandValid: true,
					}
					applyMsgs[i] = applyMsg
					rf.Infof("sent applyMsg with index %d", rf.lastSent)
					// rf.applyCh <- applyMsg
				}
				rf.unlock(" finished sending applyMsg")
				for i := 0; i < num; i++ {
					rf.applyCh <- applyMsgs[i]
				}
			}
		}
	}()
	// intially a follower
	rf.becomeFollower()
	return rf
}

func (rf *Raft) Log() {
	rf.debug = true
}
