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
	maxShards           = 10
	opChannelBufferSize = 10
	maxGetShardTime     = 100
	waitLagReplicaTime  = 100
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

	// for updateConfig and Init
	Config shardmaster.Config

	// for update shards
	Data    map[string]string
	Shard   int
	Ver     int
	Changed bool
}

type ShardOp interface {
	sop2string() string
}

type Get struct {
	key    string
	result chan interface{}
}

func (g Get) sop2string() string {
	return fmt.Sprintf("get %s", g.key)
}

type Put struct {
	key    string
	value  string
	result chan interface{}
}

func (p Put) sop2string() string {
	return fmt.Sprintf("put %s %s", p.key, p.value)
}

type Append struct {
	key    string
	value  string
	result chan interface{}
}

func (a Append) sop2string() string {
	return fmt.Sprintf("append %s %s", a.key, a.value)
}

type Abandon struct {
	ver int
}

func (a Abandon) sop2string() string {
	return "abandon"
}

type Pull struct {
	gid int
	ver int
}

func (p Pull) sop2string() string {
	return fmt.Sprintf("pull from %d", p.gid)
}

type GetShard struct {
	ver    int
	from   int
	result chan interface{}
}

func (g GetShard) sop2string() string {
	return fmt.Sprintf("%d wants to get version %d", g.from, g.ver)
}

type UpdateShard struct {
	ver     int
	data    map[string]string
	changed bool
}

func (u UpdateShard) sop2string() string {
	return fmt.Sprintf("update to v %d, %v", u.ver, u.data)
}

func op2string(op Op) (r string) {
	switch op.Type {
	case "Get":
		r = fmt.Sprintf("get %s, clerk %d, request %d", op.Key, op.ClerkId, op.RequestId)
	case "Put":
		r = fmt.Sprintf("put %s %v, clerk %d, request %d", op.Key, op.Value, op.ClerkId, op.RequestId)
	case "Append":
		r = fmt.Sprintf("append %s %v, clerk %d, request %d", op.Key, op.Value, op.ClerkId, op.RequestId)
	case "Init":
		r = fmt.Sprintf("init shards %v", op.Config)
	case "UpdateConfig":
		r = fmt.Sprintf("update config to %d: %v", op.Config.Num, op.Config.Shards)
	case "UpdateShard":
		r = fmt.Sprintf("update shard %d v %d as %v", op.Shard, op.Ver, op.Data)
	case "":
		r = fmt.Sprintf("dummy op for pull")
	default:
		fmt.Printf("unexpected type %v:", op.Type)
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
	lastConfig        shardmaster.Config
	newConfig         shardmaster.Config
	lastPollConfigNum int
	sm_ck             *shardmaster.Clerk
	data              []map[string]string // shard -> key -> value
	ver               []int
	lastRequestId     map[int64]int64 // ClerkId -> last finished RequestId
	applyResult       map[int]interface{}
	applyIndex        int
	shardOp           map[int]chan ShardOp
	mu2               sync.Mutex //to avoid concurrent hashmap write
	// to ensure that when the leader fails during a reconfig,
	// the new leader can retry the reconfig
	toGet int
}

func (kv *ShardKV) giveShardOp(shard int, op ShardOp) {
	_, ok := kv.shardOp[shard]
	if !ok {
		kv.mu2.Lock()
		_, ok := kv.shardOp[shard]
		if !ok {
			kv.shardOp[shard] = make(chan ShardOp, opChannelBufferSize)
			if kv.data[shard] == nil {
				kv.data[shard] = make(map[string]string)
			}
		}
		kv.mu2.Unlock()
		go kv.handleShardOp(shard, kv.shardOp[shard])
	}
	kv.shardOp[shard] <- op
}

func (kv *ShardKV) handleShardOp(me int, ops chan ShardOp) {
	// if ver is 1, then it means that if a client also realizes config version 1,
	// then the client can contact this kv for requests.
	valid := kv.lastConfig.Num > 0 && (kv.lastConfig.Shards[me] == kv.gid)
	for {
		op := <-ops
		kv.DPrintf(fmt.Sprintf("shard %d, ver %d: ", me, kv.ver[me]) + op.sop2string())
		switch op.(type) {
		case Pull:
			sop := op.(Pull)
			if sop.ver > kv.ver[me]+1 {
				kv.DPrintf("lagging for %d", kv.ver[me]+1)
			}
			var data map[string]string
			_, isLeader := kv.rf.GetState()
			if isLeader {
				if sop.gid != kv.gid {
					data = kv.pullFrom(sop.gid, me, sop.ver)
				}
				op := Op{
					Type:      "UpdateShard",
					ClerkId:   -1,
					RequestId: -1,
					Data:      data,
					Ver:       sop.ver,
					Shard:     me,
					// the shard belongs to me in the last and the current version;
					// no need to pull from another replica group
					Changed: sop.gid != kv.gid,
				}
				kv.rf.Start(op)
			}
		case Get:
			sop := op.(Get)
			if !valid {
				sop.result <- ErrWrongGroup
			} else {
				if _, ok := kv.data[me][sop.key]; !ok {
					sop.result <- ErrNoKey
					kv.DPrintf("no key %s: %v", sop.key, kv.data[me])
				} else {
					sop.result <- kv.data[me][sop.key]
				}
			}
		case Put:
			sop := op.(Put)
			if !valid {
				sop.result <- ErrWrongGroup
			} else {
				kv.data[me][sop.key] = sop.value
				sop.result <- OK
			}
		case Append:
			sop := op.(Append)
			if !valid {
				sop.result <- ErrWrongGroup
			} else {
				if _, ok := kv.data[me][sop.key]; !ok {
					sop.result <- ErrNoKey
				} else {
					kv.data[me][sop.key] += sop.value
					sop.result <- OK
				}
			}
		case Abandon:
			valid = false
			kv.ver[me] = op.(Abandon).ver
		case GetShard:
			sop := op.(GetShard)
			// if sop.ver > ver+1 {
			// 	sop.result <- ErrLagConfig
			// } else if !valid {
			// 	msg := fmt.Sprintf("while %d doesn't own it", kv.gid)
			// 	kv.DPrintf("%d thinks shard %d ver %d is from %d, \n"+msg, sop.from, me, sop.ver, kv.gid)
			// 	panic("unexpected getshard")
			// } else {
			// 	sop.result <- data
			// }
			if sop.ver > kv.ver[me]+1 {
				sop.result <- ErrLagConfig
			} else {
				copy := shardCopy(kv.data[me])
				sop.result <- copy
			}
		case UpdateShard:
			sop := op.(UpdateShard)
			if sop.ver > kv.ver[me] {
				if sop.ver != kv.ver[me]+1 {
					kv.DPrintf("skip update version %d for shard %d", kv.ver[me]+1, me)
					panic("unexpected update version")
				}
				kv.ver[me] = sop.ver
				if sop.changed {
					kv.data[me] = sop.data
				}
				valid = true
				kv.DPrintf("updates shard %d v %d as %v", me, kv.ver[me], sop.data)
			}
		default:
			panic("Unexpected op type ")
		}
	}
}

func (kv *ShardKV) pullFrom(gid int, shard int, ver int) map[string]string {
	args := PullArgs{
		Shard: shard,
		Ver:   ver,
		From:  kv.gid,
	}
	var reply PullReply
	if servers, ok := kv.lastConfig.Groups[gid]; ok {
		for {
			for si := 0; si < len(servers); {
				srv := kv.make_end(servers[si])
				ok := srv.Call("ShardKV.Pull", &args, &reply)
				if ok {
					switch reply.Err {
					case ErrWrongLeader:
						kv.DPrintf("[pull s %d v %d from %d] wrong leader", shard, ver, gid)
						si++
					case ErrLagConfig:
						kv.DPrintf("[pull s %d v %d from %d] lagging", shard, ver, gid)
						time.Sleep(waitLagReplicaTime * time.Millisecond)
					case OK:
						return reply.Data
					}
				} else {
					si++
				}
			}
			kv.DPrintf("retry pulling shard %d version %d from %d...", shard, ver, gid)
		}
	} else {
		kv.DPrintf("can't find group %d's peers: %d, %v", gid, kv.lastConfig.Num, kv.lastConfig.Groups)
		panic("unexpected group setting")
	}
}

func (kv *ShardKV) Pull(args *PullArgs, reply *PullReply) {
	// a dummy Op
	index, _, isLeader := kv.rf.Start(Op{})
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	kv.DPrintf("notices that %d wants to get shard %d for %v", args.From, args.Shard, args.Ver)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		if kv.applyIndex >= index {
			// we can confirm that only leader can arrive here
			sop := GetShard{
				ver:  args.Ver,
				from: args.From,
			}
			sop.result = make(chan interface{}, 1)
			timer := time.NewTimer(maxGetShardTime * time.Millisecond)
			defer timer.Stop()
			kv.giveShardOp(args.Shard, sop)
			reply.Err = ErrLagConfig
			select {
			case r := <-sop.result:
				switch r.(type) {
				case Err:
					kv.DPrintf("fail to get shard %d v %d due to %v", args.Shard, args.Ver, r.(Err))
				case map[string]string:
					reply.Data = r.(map[string]string)
					reply.Err = OK
				}
			case <-timer.C:
				kv.DPrintf("fail to get shard %d v %d due to  timeout", args.Shard, args.Ver)
			}
		}
	}
}

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("15:04:05.999") + string(bytes))
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
	index, _, isLeader := kv.rf.Start(op)
	// by default, let the client retry
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		// periodically check the currentTerm and apply result
		time.Sleep(period)
		// currentTerm, isleader := kv.rf.GetState()
		// if !(term == currentTerm && isleader) {
		// 	reply.Err = ErrWrongLeader
		// 	return
		// }
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
					keys := make([]int, len(kv.data))
					i := 0
					for k := range kv.data {
						keys[i] = k
						i++
					}
					kv.DPrintf("owns shard %v with config %d", keys, kv.lastConfig.Num)
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
	index, _, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		// periodically check the currentTerm and apply result
		time.Sleep(period)
		// currentTerm, isleader := kv.rf.GetState()
		// if !(term == currentTerm && isleader) {
		// 	reply.Err = ErrWrongLeader
		// 	return
		// }
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
					keys := make([]int, len(kv.data))
					i := 0
					for k := range kv.data {
						keys[i] = k
						i++
					}
					kv.DPrintf("owns shard %v with config %d", keys, kv.lastConfig.Num)
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
	log.SetFlags(0)
	log.SetOutput(new(logWriter))

	kv.lastConfig = shardmaster.Config{}
	kv.newConfig = shardmaster.Config{}
	for i := 0; i < len(kv.lastConfig.Shards); i++ {
		kv.lastConfig.Shards[i] = kv.gid
	}
	kv.sm_ck = shardmaster.MakeClerk(masters)
	kv.data = make([]map[string]string, shardmaster.NShards)
	kv.ver = make([]int, shardmaster.NShards)

	kv.lastRequestId = make(map[int64]int64)
	kv.applyIndex = 0 // as raft, 1 is the first meaingful index
	kv.applyResult = make(map[int]interface{})
	kv.shardOp = make(map[int]chan ShardOp)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// read snapshot
	kv.loadSnapshot(kv.rf.GetSnapshot())
	kv.DPrintf("is started with lastPollConfigNum: %d", kv.lastPollConfigNum)

	go kv.checkApply()
	go kv.pollConfig()
	// periodically check whether need to take a snapshot
	go func() {
		for {
			if kv.rf.GetStateSize() >= int(ratio*float32(kv.maxraftstate)) {
				kv.lock("start to encode snapshot")
				snapshot := kv.encodeSnapshot()
				applyIndex := kv.applyIndex
				kv.unlock("finish encoding snapshot with applyIndex %d", applyIndex)
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
			if op.Type == "" {
				// dummy op, see Pull
			} else if op.Type == "Get" {
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
			kv.unlock("finish applying op with index: %d", applyMsg.CommandIndex)
		} else {
			// update the data with snapshot
			kv.DPrintf("receives snapshot from rf")
			kv.loadSnapshot(applyMsg.Snapshot)
		}
	}
}

func (kv *ShardKV) applyOp(op Op) interface{} {
	// any string/OK (Err) for success, others for failure
	shard := key2shard(op.Key)
	// No need to wait for Init and UpdateConfig
	switch op.Type {
	case "Get":
		sop := Get{
			key: op.Key,
		}
		sop.result = make(chan interface{})
		kv.giveShardOp(shard, sop)
		return <-sop.result
	case "Put":
		sop := Put{
			key:   op.Key,
			value: op.Value,
		}
		sop.result = make(chan interface{})
		kv.giveShardOp(shard, sop)
		return <-sop.result
	case "Append":
		sop := Append{
			key:   op.Key,
			value: op.Value,
		}
		sop.result = make(chan interface{})
		kv.giveShardOp(shard, sop)
		return <-sop.result
	case "UpdateConfig":
		config := op.Config
		kv.toGet = 0
		for i := 0; i < len(config.Shards); i++ {
			if config.Shards[i] == kv.gid {
				sop := Pull{
					gid: kv.lastConfig.Shards[i], //maybe take from myself!
					ver: config.Num,
				}
				kv.toGet++
				kv.giveShardOp(i, sop)
			} else {
				// to advance the shard version
				kv.giveShardOp(i, Abandon{ver: config.Num})
			}
		}
		if config.Num > kv.lastConfig.Num {
			kv.newConfig = config
		}
		if kv.toGet == 0 {
			kv.lastPollConfigNum = config.Num
			kv.lastConfig = config
		}
		return OK
	case "UpdateShard":
		sop := UpdateShard{
			ver:     op.Ver,
			data:    op.Data,
			changed: op.Changed,
		}
		kv.giveShardOp(op.Shard, sop)
		kv.toGet--
		if kv.toGet == 0 {
			kv.lastPollConfigNum = kv.newConfig.Num
			kv.lastConfig = kv.newConfig
		}
		return OK
	default:
		panic("unknown op type!")
	}
}

func (kv *ShardKV) pollConfig() {
	for {
		_, isLeader := kv.rf.GetState()
		// only issue update when the configuration changes
		// it's OK to have stale leaders doing the same job
		if isLeader {
			// config := kv.sm_ck.Query(-1)
			// if config.Num > kv.lastPollConfigNum {
			// 	kv.lastPollConfigNum = config.Num
			// 	kv.DPrintf("sees config %d", config.Num)
			// 	op := Op{
			// 		Type:      "UpdateConfig",
			// 		Config:    config,
			// 		RequestId: -1,
			// 		ClerkId:   -1,
			// 	}
			// 	kv.rf.Start(op)
			// }

			// always poll for the next configuration, not skipping any
			config := kv.sm_ck.Query(kv.lastPollConfigNum + 1)
			if config.Num == kv.lastPollConfigNum+1 {
				kv.lastPollConfigNum++
				kv.DPrintf("sees config %d", config.Num)
				op := Op{
					Type:      "UpdateConfig",
					Config:    config,
					RequestId: -1,
					ClerkId:   -1,
				}
				kv.rf.Start(op)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.applyIndex) != nil {
		panic("fail to encode kv.applyIndex!")
	}
	if e.Encode(kv.lastConfig) != nil {
		panic("fail to encode kv.lastConfigVer!")
	}
	if e.Encode(kv.lastPollConfigNum) != nil {
		panic("fail to encode kv.lastConfigVer!")
	}
	if e.Encode(kv.data) != nil {
		panic("fail to encode kv.data!")
	}
	if e.Encode(kv.lastRequestId) != nil {
		panic("fail to encode kv.lastRequestId!")
	}
	if e.Encode(kv.ver) != nil {
		panic("fail to encode kv.ver!")
	}
	return w.Bytes()
}

//TODO; add fields
func (kv *ShardKV) loadSnapshot(snapshot []byte) {
	if len(snapshot) > 0 {
		kv.lock("starts to load snapshot...")
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var data []map[string]string
		var lastRequestId map[int64]int64
		var applyIndex int
		var lastPollConfigNum int
		var lastConfig shardmaster.Config
		var ver []int
		if d.Decode(&applyIndex) != nil ||
			d.Decode(&lastConfig) != nil ||
			d.Decode(&lastPollConfigNum) != nil ||
			d.Decode(&data) != nil ||
			d.Decode(&lastRequestId) != nil ||
			d.Decode(&ver) != nil {
			kv.DPrintf("fails to read snapshot!")
			panic("fail to read snapshot")
		}
		kv.applyIndex = applyIndex
		kv.data = data
		kv.lastRequestId = lastRequestId
		kv.lastConfig = lastConfig
		kv.lastPollConfigNum = lastPollConfigNum
		kv.ver = ver
		kv.DPrintf("loads applyIndex %d, data: %v", kv.applyIndex, kv.data)
		kv.unlock("load snapshot with applyIndex: %d", kv.applyIndex)
	}
}
