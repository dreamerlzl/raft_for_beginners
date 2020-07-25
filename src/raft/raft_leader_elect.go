package raft

import (
	"sync"

	logrus "github.com/sirupsen/logrus"
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
		lastTerm := rf.log[rf.lastIndex].EntryTerm
		if turnToFollower {
			logrus.Infof("[%d] (term %d) receives request vote from %d with larger term %d", rf.me, rf.currentTerm, args.CandidateId, args.SenderTerm)
			rf.updateCurrentTerm(args.SenderTerm) // must update here
		}
		if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= rf.lastIndex) {
			if !rf.electionTimer.Stop() {
				// tries to be a candidate
				turnToFollower = true
			}
			defer rf.resetElectionTimer()
			reply.ReceiverVote = true
			logrus.Debugf("[%d] votes for %d in term %d (original votedFor %d)", rf.me, args.CandidateId, args.SenderTerm, rf.votedFor)
			rf.votedFor = args.CandidateId
			rf.persist()
		} else {
			reply.NotUpToDate = true
			logrus.Debugf("[%d]'s log is not up-to-date as %d's", args.CandidateId, rf.me)
		}
		if turnToFollower {
			rf.killOldRole()
		}
	} else {
		logrus.Debugf("[%d](term %d) rejects %d(term %d)",
			rf.me, rf.currentTerm, args.CandidateId, args.SenderTerm)
	}
}

func (rf *Raft) becomeFollower() {
	logrus.Infof("[%d] becomes a follower with term %d!", rf.me, rf.currentTerm)
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
	go rf.bcastAppendEntries()
	go rf.sendHeartbeat()
	logrus.Infof("[%d] becomes a leader with term %d!", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeCandidate() {
	stopListen := make(chan bool)
	go func() {
		for { // before acquiring the locak and change to candidate,
			// might experience multiple times of receiving higher terms
			select {
			case kill := <-rf.killedChan[follower]:
				if kill {
					logrus.Infof("[%d] receives kill signal", rf.me)
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
		logrus.Debugf("[%d] waiting for lock", rf.me)
		rf.mu.Lock()
		close(stopListen)
		if rf.downgrade {
			rf.downgrade = false
			logrus.Warnf("[%d] hears from new leader during transition to candidate", rf.me)
			rf.becomeFollower()
			rf.mu.Unlock()
			return
		}
		if rf.stale { // if log is not up-to-date as a majority, doomed to fail
			logrus.Warnf("[%d] becomes a follower because its log is stale", rf.me)
			rf.becomeFollower()
			rf.mu.Unlock()
			return
		}
		rf.state = candidate
		rf.votedFor = rf.me
		rf.currentTerm = rf.currentTerm + 1
		logrus.Infof("[%d] becomes a candidate with term: %d", rf.me, rf.currentTerm)
		rf.resetElectionTimer()
		args := &RequestVoteArgs{CandidateId: rf.me, SenderTerm: rf.currentTerm}
		args.LastLogIndex = rf.lastIndex
		args.LastLogTerm = rf.log[rf.lastIndex].EntryTerm
		numPeers := len(rf.peers)
		rf.mu.Unlock()
		rf.persist()

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
							logrus.Debugf("[%d] vote count: %d", rf.me, vote)
							if 2*vote > numPeers {
								finish = true
								rf.mu.Lock()
								if rf.state == candidate {
									rf.killedChan[candidate] <- true
									rf.becomeLeader()
									logrus.Infof("[%d] succeeds in election", rf.me)
									rf.mu.Unlock()
									return
								}
								rf.mu.Unlock()
							}
						} else if reply.ReceiverTerm > args.SenderTerm {
							logrus.Debugf("[%d]'s term %d is < %d's term %d", rf.me, args.SenderTerm, server, reply.ReceiverTerm)
							finish = true
							rf.mu.Lock()
							if rf.state == candidate {
								rf.killedChan[candidate] <- true
								rf.updateCurrentTerm(reply.ReceiverTerm)
								rf.becomeFollower()
								logrus.Infof("[%d] turns to a follower", rf.me)
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
									logrus.Infof("[%d] turns to a follower cause its log is stale", rf.me)
									return
								}
								rf.mu.Unlock()
							}
						}
					} else {
						// logrus.Debugf("[%d] failed to contact %d for vote", rf.me, server)
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
					logrus.Infof("[%d] timeout but find that it already finished election", rf.me)
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
					logrus.Infof("[%d] retries to be a candidate", rf.me)
					return
				}
				rf.mu.Unlock()
			}
		}()

		<-rf.killedChan[candidate]
		logrus.Infof("[%d] ends being a candidate", rf.me)
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
	logrus.Infof("[%d] updates term to %d; original role: %s", rf.me, compareTerm, role)
}

// become a candidate if not receiving a heartbeat from the leader for rf.electionTimeout
func (rf *Raft) checkHeartbeat() {
	logrus.Infof("[%d] starts to check heartbeat", rf.me)
	rf.resetElectionTimer()
	defer logrus.Infof("[%d] ends checking heartbeat", rf.me)
	for {
		select {
		case <-rf.electionTimer.C:
			logrus.Infof("[%d] tries to be a candidate", rf.me)
			go rf.becomeCandidate()
			return
		case kill := <-rf.killedChan[follower]:
			if kill {
				logrus.Infof("[%d] receives kill signal", rf.me)
				return
			} else {
				logrus.Infof("[%d] continues to be a follower", rf.me)
				rf.killReply <- true
				continue
			}
		}
	}
}

// if myself becomes a leader then periodically(200ms) sends heartbeats to all peers
func (rf *Raft) sendHeartbeat() {
	logrus.Infof("[%d] starts to send heartbeats", rf.me)
	rf.resetHeartbeatTimer()
	defer logrus.Infof("[%d] terminates sending heartbeat", rf.me)
	for {
		select {
		case <-rf.heartbeatTimer.C:
			go rf.bcastAppendEntries()
		case <-rf.killedChan[leader]:
			logrus.Infof("[%d] receives kill signal", rf.me)
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
		logrus.Warnf("[%d] downgrades to follower during transition", rf.me)
	case leader:
		rf.killedChan[leader] <- true
		rf.becomeFollower()
	}
}
