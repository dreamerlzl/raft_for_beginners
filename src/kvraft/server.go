package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1
const checkLeaderPeriod = 20
const checkSnapshotPeriod = 150
const ratio float32 = 0.95 // when rf.RaftStateSize >= ratio * kv.maxraftestatesize, take a snapshot

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	RequestId int64
	ClerkId   int64
}

func op2string(op Op) string {
	switch op.Type {
	case "Get":
		return fmt.Sprintf("{Get %s, RequestId: %d, ClerkId: %d}", op.Key, op.RequestId, op.ClerkId)
	default:
		return fmt.Sprintf("{%s %s with %s, RequestID: %d, ClerkId: %d}", op.Type, op.Key, op.Value, op.RequestId, op.ClerkId)
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data          map[string]string
	lastRequestId map[int64]int64 // ClerkId -> last finished RequestId
	applyResult   map[int]string
	applyIndex    int
}

type KVSnapshot struct {
	Data          map[string]string
	LastRequestId map[int64]string
}

func (kv *KVServer) lock(msg string, f ...interface{}) {
	kv.mu.Lock()
	// logrus.Infof("[%d]"+msg, kv.me)
	DPrintf(msg, f...)
}

func (kv *KVServer) unlock(msg string, f ...interface{}) {
	kv.mu.Unlock()
	// logrus.Infof("[%d]"+msg, kv.me)
	DPrintf(msg, f...)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		RequestId: args.RequestId,
		ClerkId:   args.ClerkId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[kv %d] issues %s at index %d", kv.me, op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		// periodically check the currentTerm and apply result
		time.Sleep(period)
		currentTerm, isleader := kv.rf.GetState()
		if !(term == currentTerm && isleader) {
			reply.Err = ErrWrongLeader
			return
		}
		if kv.applyIndex >= index {
			kv.lock("[kv %d] is reading index %d's result", kv.me, index)
			result := kv.applyResult[index]
			delete(kv.applyResult, index)
			kv.unlock("[kv %d] finished reading index %d's result", kv.me, index)
			if result == "" {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = result
			}
			return
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		RequestId: args.RequestId,
		ClerkId:   args.ClerkId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[kv %d] issues %s at index %d", kv.me, op2string(op), index)
	// as said in hints, a client will make only one call into a Clerk at a time.
	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		// periodically check the currentTerm and apply result
		time.Sleep(period)
		currentTerm, isleader := kv.rf.GetState()
		if !(term == currentTerm && isleader) {
			reply.Err = ErrWrongLeader
			return
		}
		if kv.applyIndex >= index {
			reply.Err = OK
			return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.lastRequestId = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applyIndex = 0 // as raft, 1 is the first meaingful index
	kv.applyResult = make(map[int]string)

	// read snapshot
	kv.loadSnapshot(kv.rf.GetSnapshot())

	// You may need initialization code here.
	go func() {
		for {
			applyMsg := <-kv.applyCh
			if applyMsg.CommandValid {
				if kv.applyIndex+1 != applyMsg.CommandIndex {
					DPrintf("[kv %d] application not in order! expected: %d, given: %d", kv.me, kv.applyIndex+1, applyMsg.CommandIndex)
					continue
				}
				op := applyMsg.Command.(Op)
				if kv.lastRequestId[op.ClerkId] == op.RequestId {
					// duplicate execution
					DPrintf("[kv %d] detects duplicate request %d", kv.me, op.RequestId)
				} else {
					value := kv.applyOp(op)
					if op.Type == "Get" {
						kv.lock("[kv %d] writes result for index %d", kv.me, applyMsg.CommandIndex)
						kv.applyResult[applyMsg.CommandIndex] = value
						kv.unlock("[kv %d] finished writing result for index %d", kv.me, applyMsg.CommandIndex)
					}
					kv.lastRequestId[op.ClerkId] = op.RequestId
				}
				kv.applyIndex++
			} else {
				// update the data with snapshot
				kv.applyIndex = applyMsg.LastIncludedIndex
				kv.loadSnapshot(applyMsg.Snapshot)
			}
		}
	}()

	// periodically check whether need to take a snapshot
	go func() {
		for {
			if kv.rf.GetStateSize() >= int(ratio*float32(kv.maxraftstate)) {
				snapshot := kv.encodeSnapshot()
				kv.rf.TakeSnapshot(snapshot)
			}
			time.Sleep(time.Duration(checkSnapshotPeriod) * time.Millisecond)
		}
	}()
	return kv
}

func (kv *KVServer) applyOp(op Op) string {
	switch op.Type {
	case "Get":
		v, ok := kv.data[op.Key]
		if ok {
			return v
		} else {
			return ""
		}
	case "Put":
		kv.data[op.Key] = op.Value
	case "Append":
		kv.data[op.Key] += op.Value
	}
	return ""
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.applyIndex) != nil {
		panic("fail to encode kv.applyIndex!")
	}
	if e.Encode(kv.data) != nil {
		panic("fail to encode kv.data!")
	}
	if e.Encode(kv.lastRequestId) != nil {
		panic("fail to encode kv.lastRequestId!")
	}
	return w.Bytes()
}

func (kv *KVServer) loadSnapshot(snapshot []byte) {
	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var data map[string]string
		var lastRequestId map[int64]int64
		var applyIndex int
		if d.Decode(&applyIndex) != nil ||
			d.Decode(&data) != nil ||
			d.Decode(&lastRequestId) != nil {
			DPrintf("[%d] fails to read snapshot!", kv.me)
			panic("fail to read snapshot")
		}
		kv.applyIndex = applyIndex
		kv.data = data
		kv.lastRequestId = lastRequestId
		DPrintf("[kv %d] successfully load snapshot!", kv.me)
	}
}
