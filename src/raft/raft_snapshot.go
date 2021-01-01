package raft

import (
	"bytes"

	"../labgob"
)

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) TakeSnapshot(snapshot []byte, lastApplied int) {
	if rf.commitIndex > rf.lastIncludedIndex {
		rf.lock(" starts to take snapshot!")
		// only committed entries will be bundled as a snapshot
		if lastApplied < rf.lastIncludedIndex {
			rf.unlock(" installs a snapshot with lastIncludedIndex %d when preparing to take one with %d", rf.lastIncludedIndex, lastApplied)
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
		rf.unlock(" finished taking snapshot with lastIncludedIndex %d", rf.lastIncludedIndex)
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
		rf.lock(" starts to prepare snapshot to %d", server)
		args := InstallSnapshotArgs{
			Data:             rf.GetSnapshot(),
			LeaderId:         rf.me,
			LeaderTerm:       rf.currentTerm,
			LastIncludeIndex: rf.lastIncludedIndex,
			LastIncludeTerm:  rf.lastIncludedTerm,
		}
		rf.unlock(" finishes preparing snapshot for %d with lastIncludedIndex: %d", server, rf.lastIncludedIndex)
		reply := InstallSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		if ok {
			rf.lock(" receives reply of sendSnapshot from %d", server)
			if reply.Term > rf.currentTerm {
				rf.Debugf(" term %d is < %d's term %d", args.LeaderTerm, server, reply.Term)
				rf.updateCurrentTerm(reply.Term)
				rf.killOldRole()
			} else {
				rf.nextIndex[server] = rf.lastIncludedIndex + 1
				rf.Debugf(" new nextIndex[%d]: %d", server, rf.nextIndex[server])
			}
			rf.unlock(" finishes sendSnapshot to %d", server)
			return
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock(" starts to install snapshot")
	defer rf.unlock(" finishes installing snapshot")
	reply.Term = rf.currentTerm
	if args.LeaderTerm < rf.currentTerm {
		rf.Debugf(" term %d > leader %d's term %d", rf.currentTerm, args.LeaderId, args.LeaderTerm)
		return
	} else if args.LastIncludeIndex <= rf.lastIncludedIndex {
		rf.Infof("(lastIncludedIndex %d) receives a stale snapshot with lastIncludeIndex: %d!", rf.lastIncludedIndex, args.LastIncludeIndex)
		return
	}

	rf.Infof(" before installing snapshot: lastIncludedIndex %d, commitIndex %d, lastSent %d, log %d", rf.lastIncludedIndex, rf.commitIndex, rf.lastSent, len(rf.log))

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
		rf.Infof(" sends a snapshot with lastIncludedIndex: %d to its kv peer!", args.LastIncludeIndex)
		rf.applyCh <- applyMsg
		rf.lastSent = args.LastIncludeIndex
	}

	rf.lastIncludedIndex = args.LastIncludeIndex
	rf.lastIncludedTerm = args.LastIncludeTerm

	rf.Infof(" new lastIncludeIndex: %d, commitIndex: %d, lastSent: %d, log: %d", rf.lastIncludedIndex, rf.commitIndex, rf.lastSent, len(rf.log))
}
