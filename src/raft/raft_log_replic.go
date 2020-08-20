package raft

import (
	"sort"

	logrus "github.com/sirupsen/logrus"
)

type AppendEntriesArgs struct {
	LeaderId     int
	Entries      []LogEntry
	SenderTerm   int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	ReceiverTerm  int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
	NeedSnapshot  bool
}

func (rf *Raft) bcastAppendEntries() {
	rf.heartbeatTimer.Stop()
	defer rf.resetHeartbeatTimer()
	args := AppendEntriesArgs{}
	args.LeaderId = rf.me
	// rf.mu.Lock()
	args.SenderTerm = rf.currentTerm
	args.LeaderCommit = rf.commitIndex
	// rf.mu.Unlock()
	logrus.Infof("[%d] starts to send appendEntries", rf.me)
	for i := 0; i < rf.numPeers; i++ {
		if i != rf.me {
			go rf.sendAppendEntries(i, args)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		} // a leader may issue sendAppendEntries but then immediately become a follower
		// with log truncated. So must check identity before using nextIndex
		if rf.nextIndex[server] > rf.lastIndex+1 {
			logrus.Errorf("[%d] nextIndex[%d]=%d > lastIndex+1=%d", rf.me, server, rf.nextIndex[server], rf.lastIndex+1)
		}

		if rf.nextIndex[server] <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			go rf.sendInstallSnapshot(server)
			return
		}

		args.Entries = make([]LogEntry, rf.lastIndex+1-rf.nextIndex[server])
		copy(args.Entries, rf.getLogEntries(rf.nextIndex[server], rf.lastIndex+1))
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.getLogEntry(args.PrevLogIndex).EntryTerm
		lastIndex := rf.lastIndex
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if ok {
			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				if len(args.Entries) > 0 {
					rf.nextIndex[server] = MaxInt(rf.nextIndex[server], rf.lastIndex+1)
					rf.matchIndex[server] = MaxInt(rf.matchIndex[server], lastIndex)

					// logrus.Debugf("[%d] matches %d with follower %d", rf.me, rf.matchIndex[server], server)
					// logrus.Debugf("[%d] lastIndex: %d", rf.me, rf.lastIndex)
					logrus.Debugf("[%d] new nextIndex[%d]: %d after reply success", rf.me, server, rf.nextIndex[server])

					rf.updateCommitIndex(server)
				} else {
					logrus.Debugf("[%d] sent heartbeat to %d", rf.me, server)
				}
				rf.mu.Unlock()
				return
			} else if reply.ReceiverTerm > args.SenderTerm {
				logrus.Debugf("[%d]'s term %d is < %d's term %d", rf.me, args.SenderTerm, server, reply.ReceiverTerm)
				rf.updateCurrentTerm(reply.ReceiverTerm)
				rf.killOldRole()
				rf.mu.Unlock()
				return
			} else {
				if !reply.NeedSnapshot {
					found := false
					if rf.nextIndex[server]-1 > rf.lastIndex {
						logrus.Errorf("[%d] lastIndex: %d, nextIndex[%d]: %d", rf.me, rf.lastIndex, server, rf.nextIndex[server])
					}
					for i := rf.nextIndex[server] - 1; i >= rf.lastIncludedIndex; i-- {
						if rf.getLogEntry(i).EntryTerm == reply.ConflictTerm {
							rf.nextIndex[server] = i + 1
							logrus.Debugf("[%d] new nextIndex[%d]: %d after fail", rf.me, server, i+1)
							found = true
							break
						}
					}
					if !found {
						rf.nextIndex[server] = reply.ConflictIndex
						logrus.Debugf("[%d] new nextIndex[%d]: %d after fail", rf.me, server, reply.ConflictIndex)
					}
				}
				if rf.nextIndex[server] < rf.lastIncludedIndex || reply.NeedSnapshot {
					// installSnapshot RPC
					go rf.sendSnapshot(server)
				}
				// logrus.Debugf("[%d] new nextIndex[%d]: %d after reply fail", rf.me, server, rf.nextIndex[server])
			}
			rf.mu.Unlock()
		} else {
			// logrus.Debugf("[%d] failed to append entries on %d", rf.me, server)
			return
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ReceiverTerm = rf.currentTerm
	reply.Success = false
	if args.SenderTerm < rf.currentTerm {
		logrus.Debugf("[%d]'s term %d > leader %d's term %d", rf.me, rf.currentTerm, args.LeaderId, args.SenderTerm)
		return
	}
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// a stale appendEntry
		reply.Success = true
		logrus.Infof("[%d] receives a stale appendEntry:\n", rf.me)
		logrus.Infof("commitId: %d  lastIncludedIndex: %d leader %d commit: %d prevLogIndex: %d", rf.commitIndex, rf.lastIncludedIndex, args.LeaderId, args.LeaderCommit, args.PrevLogIndex)
		// panic("unexpected situation")
		return
	}

	rf.electionTimer.Stop()
	defer rf.resetElectionTimer()

	l := len(rf.log)
	if rf.lastIndex >= args.PrevLogIndex && args.PrevLogIndex-rf.lastIncludedIndex >= l {
		logrus.Errorf("[%d] lastIncludedIndex: %d, lastIndex: %d, prevIndex: %d\nlog len: %d, %v", rf.me, rf.lastIncludedIndex, rf.lastIndex, args.PrevLogIndex, l, rf.log)
		panic("index out of range")
	}

	if rf.lastIndex < args.PrevLogIndex {
		logrus.Debugf("[%d] log inconsistent with leader %d; lastIndex: %d, PrevLogIndex: %d", rf.me, args.LeaderId, rf.lastIndex, args.PrevLogIndex)
		reply.ConflictIndex = rf.lastIndex + 1
		reply.ConflictTerm = -1
	} else if rf.getLogEntry(args.PrevLogIndex).EntryTerm != args.PrevLogTerm {
		logrus.Debugf("[%d] log inconsistent with leader %d; index: %d,  my term: %d, leader term: %d", rf.me, args.LeaderId, args.PrevLogIndex, rf.getLogEntry(args.PrevLogIndex).EntryTerm, args.PrevLogTerm)
		reply.ConflictTerm = rf.getLogEntry(args.PrevLogIndex).EntryTerm
		reply.NeedSnapshot = true
		for i := args.PrevLogIndex; i >= rf.lastIncludedIndex; i-- {
			if rf.getLogEntry(i).EntryTerm < reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				reply.NeedSnapshot = false
				break
			}
		}
	} else {
		if rf.state == leader && args.SenderTerm == rf.currentTerm {
			logrus.Errorf("[%d] leader receives appendEntries with the same term %d from %d", rf.me, rf.currentTerm, args.LeaderId)
		}

		reply.Success = true
		// logrus.Debugf("[%d] successfully receives leader %d's appendEntries", rf.me, args.LeaderId)
		myLogLength := rf.getLogLength()
		base := args.PrevLogIndex + 1
		firstConflictIndex := myLogLength // by default assume no conflicts
		lenAppendEntries := len(args.Entries)
		for i := 0; i < MinInt(lenAppendEntries, myLogLength-base); i++ {
			if rf.getLogEntry(i+base).EntryTerm != args.Entries[i].EntryTerm {
				firstConflictIndex = i + base
				break
			}
		}

		appendedEntries := args.Entries[MinInt(firstConflictIndex-base, lenAppendEntries):]
		rf.replaceLogEntries(firstConflictIndex, appendedEntries)
		rf.persist()
		// if !(firstConflictIndex == myLogLength && myLogLength-base > lenAppendEntries) {
		// 	rf.lastIndex = args.PrevLogIndex + lenAppendEntries
		// 	rf.stale = false
		// }

		// if rf.lastIndex != rf.lastIncludedIndex+len(rf.log)-1 {
		// 	panic("inconsistent lastIndex!")
		// }
		rf.lastIndex = rf.lastIncludedIndex + len(rf.log) - 1

		if args.LeaderCommit > rf.commitIndex {
			// rf.commitIndex = MinInt(args.LeaderCommit, firstConflictIndex+len(appendedEntries)-1)
			rf.commitIndex = MinInt(args.LeaderCommit, rf.lastIndex)
			logrus.Debugf("[%d]'s commitIndex: %d", rf.me, rf.commitIndex)
		}
		rf.AcceptedLeader = args.LeaderId
		if lenAppendEntries == 0 {
			logrus.Debugf("[%d] receives heartbeat with term %d from leader %d",
				rf.me, args.SenderTerm, args.LeaderId)
		} else {
			// startIndex := MinInt(firstConflictIndex-base, lenAppendEntries)
			startIndex := firstConflictIndex
			if firstConflictIndex >= base+lenAppendEntries {
				startIndex = base
			}
			logrus.Debugf("[%d] receives appendEntries starting %d ending %d with term %d from leader %d",
				rf.me, startIndex, lenAppendEntries+base, args.SenderTerm, args.LeaderId)
		}

	}
	if args.SenderTerm > rf.currentTerm {
		// logrus.Infof("[%d] (term %d) receive appendEntries from leader %d (term %d)", rf.me, rf.currentTerm, args.LeaderId, args.SenderTerm)
		rf.updateCurrentTerm(args.SenderTerm)
		rf.killOldRole()
	} else {
		// equal
		rf.killOldRole()
	}
}

func (rf *Raft) updateCommitIndex(server int) {
	rf.matchIndex[rf.me] = rf.lastIndex
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	n := (rf.numPeers - 1) / 2
	N := tmp[n]

	// logrus.Debugf("[%d] N: %d commitIndex: %d term: %d currentTerm: %d", rf.me, N, rf.commitIndex, rf.getLogEntry(N).EntryTerm, rf.currentTerm)

	if N > rf.commitIndex && rf.getLogEntry(N).EntryTerm == rf.currentTerm {
		rf.commitIndex = tmp[n]
		logrus.Infof("[%d]'s commitIndex updates to %d", rf.me, tmp[n])
	}
}
