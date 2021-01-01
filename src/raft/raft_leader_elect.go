package raft

import (
	"sync"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	SenderTerm   int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ReceiverTerm int
	ReceiverVote bool
	NotUpToDate  bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.NotUpToDate = false
	reply.ReceiverVote = false
	reply.ReceiverTerm = rf.currentTerm
	turnToFollower := rf.currentTerm < args.SenderTerm
	if turnToFollower || (rf.currentTerm == args.SenderTerm && rf.votedFor == -1) {
		lastTerm := rf.getLogEntry(rf.lastIndex).EntryTerm
		if turnToFollower {
			rf.Infof("(term %d) receives request vote from %d with larger term %d", rf.currentTerm, args.CandidateId, args.SenderTerm)
			rf.updateCurrentTerm(args.SenderTerm) // must update here
		}
		if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= rf.lastIndex) {
			if !rf.electionTimer.Stop() {
				// tries to be a candidate
				turnToFollower = true
			}
			defer rf.resetElectionTimer()
			reply.ReceiverVote = true
			rf.Debugf("votes for %d in term %d (original votedFor %d)", args.CandidateId, args.SenderTerm, rf.votedFor)
			rf.votedFor = args.CandidateId
			rf.persist()
		} else {
			reply.NotUpToDate = true
			rf.Debugf("[%d]'s log is not up-to-date as mine", args.CandidateId)
		}
		if turnToFollower {
			rf.killOldRole()
		}
	} else {
		rf.Debugf("(term %d) rejects %d(term %d)", rf.currentTerm, args.CandidateId, args.SenderTerm)
	}
}

func (rf *Raft) becomeFollower() {
	rf.Infof("becomes a follower with term %d!", rf.currentTerm)
	rf.state = follower
	go rf.checkHeartbeat()
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	// re-initialize nextIndex and matchIndex
	for i := 0; i < rf.numPeers; i++ {
		if i != rf.me {
			rf.nextIndex[i] = rf.lastIndex + 1
			rf.matchIndex[i] = 0
		}
	}
	// initial heartbeat
	go func() {
		rf.bcastAppendEntries()
		go rf.sendHeartbeat()
	}()
	rf.Infof("becomes a leader with term %d!", rf.currentTerm)
}

func (rf *Raft) becomeCandidate() {
	stopListen := make(chan bool)
	go func() {
		for { // before acquiring the lock and change to candidate,
			// might experience multiple times of receiving higher terms
			select {
			case kill := <-rf.killedChan[follower]:
				if kill {
					rf.Infof("receives kill signal")
					return
				} else {
					rf.downgrade = true
					rf.killReply <- true
				}
			case <-stopListen:
				return
			}
		}
	}()
	var localLock sync.Mutex
	for !rf.killed() {
		rf.Debugf("waiting for lock in becomeCandidate")
		rf.mu.Lock()
		close(stopListen)
		if rf.downgrade {
			rf.downgrade = false
			rf.Warnf("hears from new leader during transition to candidate")
			rf.becomeFollower()
			rf.mu.Unlock()
			return
		}
		if rf.stale { // if log is not up-to-date as a majority, doomed to fail
			rf.Warnf("becomes a follower because its log is stale")
			rf.becomeFollower()
			rf.mu.Unlock()
			return
		}
		rf.state = candidate
		rf.votedFor = rf.me
		rf.currentTerm = rf.currentTerm + 1
		rf.Infof("becomes a candidate with term: %d", rf.currentTerm)
		rf.resetElectionTimer()
		args := &RequestVoteArgs{CandidateId: rf.me, SenderTerm: rf.currentTerm}
		args.LastLogIndex = rf.lastIndex
		args.LastLogTerm = rf.getLogEntry(rf.lastIndex).EntryTerm
		numPeers := len(rf.peers)
		rf.persist()
		rf.mu.Unlock()

		finish := false
		vote := 1 // note that it vote for itself
		olderThan := 0
		for i := 0; i < numPeers; i++ {
			if i != rf.me {
				go func(server int) {
					reply := &RequestVoteReply{}
					ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
					localLock.Lock()
					defer localLock.Unlock()
					if finish {
						return
					}
					if ok {
						if reply.ReceiverVote {
							vote++
							rf.Debugf("vote count: %d", vote)
							if 2*vote > numPeers {
								finish = true
								rf.mu.Lock()
								if rf.state == candidate {
									rf.killedChan[candidate] <- true
									rf.becomeLeader()
									rf.Infof("succeeds in election")
									rf.mu.Unlock()
									return
								}
								rf.mu.Unlock()
							}
						} else if reply.ReceiverTerm > args.SenderTerm {
							rf.Debugf("'s term %d is < %d's term %d", args.SenderTerm, server, reply.ReceiverTerm)
							finish = true
							rf.mu.Lock()
							if rf.state == candidate {
								rf.killedChan[candidate] <- true
								rf.updateCurrentTerm(reply.ReceiverTerm)
								rf.becomeFollower()
								rf.Infof("turns to a follower")
								rf.mu.Unlock()
								return
							}
							rf.mu.Unlock()
						} else if reply.NotUpToDate {
							// rejected because log is not up-to-date
							olderThan++
							if 2*olderThan > numPeers {
								finish = true
								rf.mu.Lock()
								if rf.state == candidate {
									rf.killedChan[candidate] <- true
									rf.stale = true
									rf.becomeFollower()
									rf.mu.Unlock()
									rf.Infof("turns to a follower cause its log is stale")
									return
								}
								rf.mu.Unlock()
							}
						}
					} else {
						// rf.Debugf("failed to contact %d for vote", server)
					}
				}(i)
			}
		}

		localTimer := rf.newElectionTimer()
		go func() {
			select {
			case <-localTimer.C:
				localLock.Lock()
				if finish {
					rf.Infof("timeout but find that it already finished election")
					localLock.Unlock()
					return
				}
				finish = true
				localLock.Unlock()
				rf.mu.Lock()
				if rf.state == candidate {
					rf.killedChan[candidate] <- true
					rf.state = follower
					go rf.becomeCandidate()
					rf.mu.Unlock()
					rf.Infof("retries to be a candidate")
					return
				}
				rf.mu.Unlock()
			}
		}()

		<-rf.killedChan[candidate]
		rf.Infof("ends being a candidate")
		return
	}
}

func (rf *Raft) updateCurrentTerm(compareTerm int) {
	rf.currentTerm = compareTerm
	rf.votedFor = -1
	rf.stale = false
	rf.persist()
	var role string
	switch rf.state {
	case follower:
		role = "follower"
	case leader:
		role = "leader"
	case candidate:
		role = "candidate"
	}
	rf.Infof("updates term to %d; original role: %s", compareTerm, role)
}

// become a candidate if not receiving a heartbeat from the leader for rf.electionTimeout
func (rf *Raft) checkHeartbeat() {
	rf.Infof("starts to check heartbeat")
	rf.resetElectionTimer()
	defer rf.Infof("ends checking heartbeat")
	for {
		select {
		case <-rf.electionTimer.C:
			rf.Infof("tries to be a candidate")
			go rf.becomeCandidate()
			return
		case kill := <-rf.killedChan[follower]:
			if kill {
				rf.Infof("receives kill signal")
				return
			} else {
				rf.Infof("continues to be a follower")
				rf.killReply <- true
				continue
			}
		}
	}
}

// if myself becomes a leader then periodically(200ms) sends heartbeats to all peers
func (rf *Raft) sendHeartbeat() {
	rf.Infof("starts to send heartbeats")
	rf.resetHeartbeatTimer()
	defer rf.Infof("terminates sending heartbeat")
	for {
		select {
		case <-rf.heartbeatTimer.C:
			go rf.bcastAppendEntries()
		case <-rf.killedChan[leader]:
			rf.Infof("receives kill signal")
			return
		}
	}
}

// should be called under rf.mu.Lock()
func (rf *Raft) killOldRole() {
	switch rf.state {
	case follower: // transition from follower to candidate
		rf.killedChan[follower] <- false
		<-rf.killReply
	case candidate:
		rf.killedChan[candidate] <- true
		rf.becomeFollower()
		rf.Warnf("downgrades to follower during transition")
	case leader:
		rf.killedChan[leader] <- true
		rf.becomeFollower()
	}
}
