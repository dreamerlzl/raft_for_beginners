package raft

import (
	"bytes"

	"../labgob"
	logrus "github.com/sirupsen/logrus"
)

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) TakeSnapshot(snapshot []byte, lastApplied int) {
	if rf.commitIndex > rf.lastIncludedIndex {
		rf.lock("[%d] starts to take snapshot!", rf.me)
		// only committed entries will be bundled as a snapshot
		if lastApplied < rf.lastIncludedIndex {
			rf.unlock("[%d] installs a snapshot with lastIncludedIndex %d when preparing to take one with %d", rf.me, rf.lastIncludedIndex, lastApplied)
			return
		}
		rf.trimEntries(lastApplied)

		rf.lastIncludedIndex = lastApplied
		rf.lastIncludedTerm = rf.getLogEntry(rf.lastIncludedIndex).EntryTerm

		// copy the src code here so that no memory reorder will be applied
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.lastIncludedIndex)
		e.Encode(rf.lastIncludedTerm)
		e.Encode(rf.log)
		data := w.Bytes()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
		rf.unlock("[%d] finished taking snapshot with lastIncludedIndex %d", rf.me, rf.lastIncludedIndex)
	}
}

func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) sendInstallSnapshot(server int) {

}

type InstallSnapshotArgs struct {
	LeaderTerm       int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendSnapshot(server int) {
	for !rf.killed() {
		rf.lock("[%d] starts to prepare snapshot to %d", rf.me, server)
		args := InstallSnapshotArgs{
			Data:             rf.GetSnapshot(),
			LeaderId:         rf.me,
			LeaderTerm:       rf.currentTerm,
			LastIncludeIndex: rf.lastIncludedIndex,
			LastIncludeTerm:  rf.lastIncludedTerm,
		}
		rf.unlock("[%d] finishes preparing snapshot for %d with lastIncludedIndex: %d", rf.me, server, rf.lastIncludedIndex)
		reply := InstallSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		if ok {
			rf.lock("[%d] receives reply of sendSnapshot from %d", rf.me, server)
			if reply.Term > rf.currentTerm {
				logrus.Debugf("[%d]'s term %d is < %d's term %d", rf.me, args.LeaderTerm, server, reply.Term)
				rf.updateCurrentTerm(reply.Term)
				rf.killOldRole()
			} else {
				rf.nextIndex[server] = rf.lastIncludedIndex + 1
				logrus.Debugf("[%d] new nextIndex[%d]: %d", rf.me, server, rf.nextIndex[server])
			}
			rf.unlock("[%d] finishes sendSnapshot to %d", rf.me, server)
			return
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("[%d] starts to install snapshot", rf.me)
	defer rf.unlock("[%d] finishes installing snapshot", rf.me)
	reply.Term = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		logrus.Debugf("[%d]'s term %d > leader %d's term %d", rf.me, rf.currentTerm, args.LeaderId, args.LeaderTerm)
		return
	} else if args.LastIncludeIndex <= rf.lastIncludedIndex {
		logrus.Infof("[%d] (lastIncludedIndex %d) receives a stale snapshot with lastIncludeIndex: %d!", rf.me, rf.lastIncludedIndex, args.LastIncludeIndex)
		return
	}

	logrus.Infof("[%d] before installing snapshot: lastIncludedIndex %d, commitIndex %d, lastSent %d, log %d", rf.me, rf.lastIncludedIndex, rf.commitIndex, rf.lastSent, len(rf.log))

	data := rf.encodeRaftState()
	rf.persister.SaveStateAndSnapshot(data, args.Data)

	sendSS2kv := false
	if args.LastIncludeIndex <= rf.lastIndex && rf.getLogEntry(args.LastIncludeIndex).EntryTerm == args.LastIncludeTerm {
		rf.trimEntries(args.LastIncludeIndex)
		if rf.lastSent < args.LastIncludeIndex {
			sendSS2kv = true
			rf.commitIndex = MaxInt(rf.commitIndex, args.LastIncludeIndex)
		}
	} else {
		rf.log = nil
		rf.appendLogEntry(LogEntry{EntryTerm: args.LastIncludeTerm})
		rf.commitIndex = args.LastIncludeIndex
		rf.lastIndex = args.LastIncludeIndex
		sendSS2kv = true
	}

	if sendSS2kv {
		applyMsg := ApplyMsg{
			CommandValid:      false,
			Snapshot:          args.Data,
			LastIncludedIndex: args.LastIncludeIndex,
		}
		logrus.Infof("[%d] sends a snapshot with lastIncludedIndex: %d to its kv peer!", rf.me, args.LastIncludeIndex)
		rf.applyCh <- applyMsg
		rf.lastSent = args.LastIncludeIndex
	}

	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.lastIncludedTerm = args.LastIncludeTerm

	logrus.Infof("[%d] new lastIncludeIndex: %d, commitIndex: %d, lastSent: %d, log: %d", rf.me, rf.lastIncludedIndex, rf.commitIndex, rf.lastSent, len(rf.log))
}
