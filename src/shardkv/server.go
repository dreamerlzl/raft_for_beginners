package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
	"github.com/sirupsen/logrus"
)

const Debug = true
const (
	checkLeaderPeriod   = 20
	checkSnapshotPeriod = 150
	rpcTimeout          = 100
)
const logLevel = logrus.DebugLevel
const ratio float32 = 0.90 // when rf.RaftStateSize >= ratio * kv.maxraftestatesize, take a snapshot

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	RequestId int64
	ClerkId   int64

	// for loadShard
	Shard     map[string]string
	ShardNum  int
	ConfigVer int

	// for sendShard
	Config shardmaster.Config

	// for init shards
	Shards []int
	Num    int
}

func op2string(op Op) (r string) {
	switch op.Type {
	case "Get":
		r = fmt.Sprintf("get %s, clerk %d, request %d", op.Key, op.ClerkId, op.RequestId)
	case "Put":
		r = fmt.Sprintf("put %s %v, clerk %d, request %d", op.Key, op.Value, op.ClerkId, op.RequestId)
	case "Append":
		r = fmt.Sprintf("append %s %v, clerk %d, request %d", op.Key, op.Value, op.ClerkId, op.RequestId)
	case "LoadShard":
		r = fmt.Sprintf("load shard %d", op.ShardNum)
	case "Init":
		r = fmt.Sprintf("init shards %v", op.Shards)
	case "SendShard":
		r = fmt.Sprintf("update config to %d: %v", op.Config.Num, op.Config.Shards)
	default:
		fmt.Printf("%v:", op.Type)
		panic("unexpected type!")
	}
	return
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastConfigVer int
	sm_ck         *shardmaster.Clerk
	data          map[int]map[string]string // shard -> key -> value
	lastRequestId map[int64]int64           // ClerkId -> last finished RequestId
	applyResult   map[int]interface{}
	applyIndex    int
	shardVersion  map[int]int
}

func (kv *ShardKV) DPrintf(msg string, f ...interface{}) {
	if Debug {
		log.Printf("[gid %d, kv %d] %s", kv.gid, kv.me, fmt.Sprintf(msg, f...))
		logrus.Debugf("[gid %d, kv %d] %s", kv.gid, kv.me, fmt.Sprintf(msg, f...))
	}
}

func (kv *ShardKV) lock(msg string, f ...interface{}) {
	kv.DPrintf(msg, f...)
	kv.mu.Lock()
	// kv.DPrintf("gain the lock")
}

func (kv *ShardKV) unlock(msg string, f ...interface{}) {
	kv.mu.Unlock()
	kv.DPrintf(msg, f...)
}

//TODO
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
			kv.lock("is reading index %d's result", index)
			result := kv.applyResult[index]
			delete(kv.applyResult, index)
			kv.unlock("finished reading index %d's result %v", index, result)
			switch result.(type) {
			case Err:
				reply.Err = result.(Err) // ErrNoKey or ErrWrongGroup
				kv.DPrintf("%v <- %s", reply.Err, op2string(op))
				if reply.Err == ErrWrongGroup {
					kv.DPrintf("owns shard %v", mapKeys(kv.data))
				}
			case string:
				reply.Err = OK
				reply.Value = result.(string)
				kv.DPrintf("%v <- %s", reply.Value, op2string(op))
			}
			return
		}
	}
}

//TODO
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
			kv.lock("is reading index %d's result", index)
			result := kv.applyResult[index]
			delete(kv.applyResult, index)
			kv.unlock("finished reading index %d's result", index)
			switch result.(type) {
			case string:
				fmt.Printf(result.(string))
				panic("unexpected string!\n")
			default:
				reply.Err = result.(Err)
				kv.DPrintf("result of %d %s: %s", index, op2string(op), reply.Err)
				if reply.Err == ErrWrongGroup {
					kv.DPrintf("owns shard %v", mapKeys(kv.data))
				}
			}
			return
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	if maxraftstate == -1 {
		kv.maxraftstate = math.MaxInt32
	} else {
		kv.maxraftstate = maxraftstate
	}
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	logrus.SetLevel(logLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: time.StampMilli,
		FullTimestamp:   true,
	})

	kv.lastConfigVer = 0
	kv.sm_ck = shardmaster.MakeClerk(masters)
	kv.data = make(map[int]map[string]string)

	kv.lastRequestId = make(map[int64]int64)
	kv.applyIndex = 0 // as raft, 1 is the first meaingful index
	kv.applyResult = make(map[int]interface{})
	kv.shardVersion = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// read snapshot
	kv.loadSnapshot(kv.rf.GetSnapshot())

	go kv.checkApply()
	go kv.pollConfig()
	// periodically check whether need to take a snapshot
	go func() {
		for {
			if kv.rf.GetStateSize() >= int(ratio*float32(kv.maxraftstate)) {
				kv.lock("starts to encode snapshot")
				snapshot := kv.encodeSnapshot()
				applyIndex := kv.applyIndex
				kv.unlock("finishes encoding snapshot with applyIndex %d", applyIndex)
				kv.rf.TakeSnapshot(snapshot, applyIndex)
			}
			time.Sleep(time.Duration(checkSnapshotPeriod) * time.Millisecond)
		}
	}()

	return kv
}

func (kv *ShardKV) checkApply() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			if kv.applyIndex+1 != applyMsg.CommandIndex {
				kv.DPrintf("application not in order! expected: %d, given: %d", kv.applyIndex+1, applyMsg.CommandIndex)
				panic("application not in order")
			}
			op := applyMsg.Command.(Op)
			kv.lock("receives applyMsg index %v %s", applyMsg.CommandIndex, op2string(op))
			if op.Type == "Get" {
				kv.applyResult[applyMsg.CommandIndex] = kv.applyOp(op)
			} else {
				if op.RequestId > -1 && kv.lastRequestId[op.ClerkId] == op.RequestId {
					// duplicate execution
					kv.DPrintf("detects duplicate request %d for index %d", op.RequestId, applyMsg.CommandIndex)
					kv.applyResult[applyMsg.CommandIndex] = ErrDuplicate
				} else {
					kv.applyResult[applyMsg.CommandIndex] = kv.applyOp(op)
					if kv.applyResult[applyMsg.CommandIndex].(Err) != ErrWrongGroup {
						kv.lastRequestId[op.ClerkId] = op.RequestId
					}
				}
			}
			kv.applyIndex++
			kv.unlock("finishes applying op with index: %d", applyMsg.CommandIndex)
		} else {
			// update the data with snapshot
			kv.loadSnapshot(applyMsg.Snapshot)
		}
	}
}

func (kv *ShardKV) applyOp(op Op) interface{} {
	// any string/OK (Err) for success, others for failure
	shard := key2shard(op.Key)
	hasShard := true
	_, ok := kv.data[shard]
	if !ok {
		hasShard = false
	}
	kv.DPrintf("starts to apply op %s", op2string(op))
	defer kv.DPrintf("finish applying op %s", op2string(op))
	switch op.Type {
	case "Get":
		if !hasShard {
			return ErrWrongGroup
		}
		v, ok := kv.data[shard][op.Key]
		if ok {
			return v
		} else {
			return ErrNoKey
		}
	case "Put":
		if !hasShard {
			return ErrWrongGroup
		}
		kv.data[shard][op.Key] = op.Value
	case "Append":
		if !hasShard {
			return ErrWrongGroup
		}
		kv.data[shard][op.Key] += op.Value
	case "SendShard":
		kv.sendShard(op)
	case "LoadShard":
		kv.loadShard(op)
	case "Init":
		for _, shardNum := range op.Shards {
			kv.data[shardNum] = make(map[string]string)
			kv.shardVersion[shardNum] = op.Num
		}
		kv.lastConfigVer = op.Num
		kv.DPrintf("finish init")
	default:
		panic("unknown op type!")
	}
	return OK
}

func (kv *ShardKV) pollConfig() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			config := kv.sm_ck.Query(1)
			if config.Num != kv.lastConfigVer {
				kv.DPrintf("index:%d, shards:%v", config.Num, config.Shards)
				var shards []int
				for shardNum, gid := range config.Shards {
					if gid == kv.gid {
						shards = append(shards, shardNum)
					}
				}
				op := Op{
					Type:      "Init",
					Shards:    shards,
					Num:       config.Num,
					RequestId: -1,
					ClerkId:   -1,
				}
				kv.rf.Start(op)
			}
		}
		time.Sleep(100 * time.Millisecond)
		if kv.lastConfigVer > 0 {
			break
		}
	}

	for {
		time.Sleep(100 * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		// only issue update when the configuration changes
		// it's OK to have stale leaders doing the same job
		if isLeader {
			// kv.DPrintf("leader!")
			config := kv.sm_ck.Query(-1)
			if config.Num != kv.lastConfigVer {
				if config.Num > kv.lastConfigVer+1 {
					kv.DPrintf("skips config %d", kv.lastConfigVer+1)
				}

				op := Op{
					Type:      "SendShard",
					Config:    config,
					RequestId: -1,
					ClerkId:   int64(kv.me),
				}
				kv.rf.Start(op)
			}
		}
	}
}

func (kv *ShardKV) sendShard(op Op) {
	config := op.Config
	// kv.DPrintf("start to update config %d", config.Num)
	if op.ClerkId == int64(kv.me) {
		numShards := len(config.Shards)
		for i := 0; i < numShards; i++ {
			if _, ok := kv.data[i]; ok && config.Shards[i] != kv.gid {
				// me would need to send this shard to group with gid config.Shards[i]
				destGid := config.Shards[i]

				kv.DPrintf("starts to send shard %d", i)
				args := GetShardArgs{
					ConfigVer: config.Num,
					ShardNum:  i,
					Shard:     shardCopy(kv.data[i]),
				}
				delete(kv.data, i)

				if servers, ok := config.Groups[destGid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply GetShardReply
						ok := srv.Call("ShardKV.GetShard", &args, &reply)
						if ok && reply.Err == OK {
							break
						}
					}
				}
			}
		}
	} else {
		numShards := len(config.Shards)
		for i := 0; i < numShards; i++ {
			if _, ok := kv.data[i]; ok && config.Shards[i] != kv.gid {
				delete(kv.data, i)
			}
		}
	}
	kv.lastConfigVer = config.Num
}

// TODO;
func (kv *ShardKV) loadShard(op Op) {
	if _, ok := kv.shardVersion[op.ShardNum]; !ok || op.ConfigVer > kv.shardVersion[op.ShardNum] {
		kv.data[op.ShardNum] = op.Shard
		kv.shardVersion[op.ShardNum] = op.ConfigVer
		kv.DPrintf("receives shard %d, %v of version %d", op.ShardNum, op.Shard, op.ConfigVer)
	} else {
		kv.DPrintf("receives stale shard %d of version %d", op.ShardNum, op.ConfigVer)
	}
}

//TODO; rpc for get a shard from another replica group
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	// only group leader should handle this rpc
	op := Op{
		Type:      "LoadShard",
		ShardNum:  args.ShardNum,
		Shard:     args.Shard,
		ConfigVer: args.ConfigVer,
		ClerkId:   -1,
		RequestId: -1,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

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

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.applyIndex) != nil {
		panic("fail to encode kv.applyIndex!")
	}
	if e.Encode(kv.lastConfigVer) != nil {
		panic("fail to encode kv.lastConfigVer!")
	}
	if e.Encode(kv.data) != nil {
		panic("fail to encode kv.data!")
	}
	if e.Encode(kv.lastRequestId) != nil {
		panic("fail to encode kv.lastRequestId!")
	}
	if e.Encode(kv.shardVersion) != nil {
		panic("fail to encode kv.shardVersion!")
	}
	return w.Bytes()
}

//TODO; add fields
func (kv *ShardKV) loadSnapshot(snapshot []byte) {
	if len(snapshot) > 0 {
		kv.lock("starts to load snapshot...")
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var data map[int]map[string]string
		var lastRequestId map[int64]int64
		var shardVersion map[int]int
		var applyIndex int
		var lastConfigVer int
		if d.Decode(&applyIndex) != nil ||
			d.Decode(&lastConfigVer) != nil ||
			d.Decode(&data) != nil ||
			d.Decode(&lastRequestId) != nil ||
			d.Decode(&shardVersion) != nil {
			kv.DPrintf("fails to read snapshot!")
			panic("fail to read snapshot")
		}
		kv.applyIndex = applyIndex
		kv.data = data
		kv.lastRequestId = lastRequestId
		kv.lastConfigVer = lastConfigVer
		kv.shardVersion = shardVersion
		kv.DPrintf("loads applyIndex %d, data: %v", kv.applyIndex, kv.data)
		kv.unlock("load snapshot with applyIndex: %d", kv.applyIndex)
	}
}
