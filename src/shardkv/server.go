package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
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
	requestTimeout      = 200
	opChannelBufferSize = 10
	maxGetShardTime     = 100
	waitLagReplicaTime  = 100
	retryPullTime       = 50
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
	ClerkId   int64
	RequestId int64
	Me        int
	Uid       int64 // different each startup
	Result    chan interface{}

	// for updateConfig and Init
	Config shardmaster.Config

	// for update shards
	ShardInfo ShardInfo
	Shard     int
	Ver       int
	Changed   bool

	// for GetShard
	From int
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
	return fmt.Sprintf("abandon %d", a.ver)
}

type Pull struct {
	ver int
}

func (p Pull) sop2string() string {
	return fmt.Sprintf("pull ver %v", p.ver)
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
	ver       int
	shardinfo ShardInfo
	changed   bool
}

func (u UpdateShard) sop2string() string {
	return fmt.Sprintf("update to v %d\ndata: %v\nlastRequestId: %v", u.ver, u.shardinfo.Data, u.shardinfo.LastRequestId)
}

type Terminate struct {
}

func (t Terminate) sop2string() string {
	return fmt.Sprintf("get killed")
}

func op2string(op Op) (r string) {
	switch op.Type {
	case "Get":
		r = fmt.Sprintf("get %s, clerk %d, request %d", op.Key, op.ClerkId, op.RequestId)
	case "Put":
		r = fmt.Sprintf("put %s %s, clerk %d, request %d", op.Key, op.Value, op.ClerkId, op.RequestId)
	case "Append":
		r = fmt.Sprintf("append %s %s, clerk %d, request %d", op.Key, op.Value, op.ClerkId, op.RequestId)
	case "UpdateConfig":
		r = fmt.Sprintf("update config to %d: %v", op.Config.Num, op.Config.Shards)
	case "UpdateShard":
		r = fmt.Sprintf("update shard %d v %d as %v", op.Shard, op.Ver, op.ShardInfo)
	case "DelayedAbandon":
		r = fmt.Sprintf("delayed abandon shard %d v %d", op.Shard, op.Ver)
	case "GetShard":
		r = fmt.Sprintf("%d wants to pull shard %d v %d", op.From, op.Shard, op.Ver)
	default:
		fmt.Printf("unexpected type: %v\n", op.Type)
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
	configs           []shardmaster.Config
	configIndex       int
	lastPollConfigNum int
	sm_ck             *shardmaster.Clerk
	data              []map[string]string // shard -> key -> value
	ver               []int
	valid             []bool
	lastRequestId     []map[int64]map[int64]bool // ClerkId -> last finished RequestId
	applyIndex        int
	shardOp           map[int]chan ShardOp
	mu2               sync.Mutex //to avoid concurrent hashmap write
	// to ensure that when the leader fails during a reconfig,
	// the new leader can retry the reconfig
	toGet      map[int]int
	dead       int32
	killedChan chan bool
	uid        int64
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
	kv.valid[me] = kv.configIndex > 0 && (kv.configs[kv.ver[me]].Shards[me] == kv.gid)
	kv.DPrintf("starts handle shard %d v %d valid %v", me, kv.ver[me], kv.valid[me])
	// for pulling data from other groups
	pullChan := make(chan Pull, 10)
	go func(sops chan Pull) {
		for sop := range sops {
			var shardinfo ShardInfo
			var killed bool
			lastConfig := kv.configs[sop.ver-1]
			gid := lastConfig.Shards[me]
			servers := lastConfig.Groups[gid]
			if gid != kv.gid {
				shardinfo, killed = kv.pullFrom(gid, me, sop.ver, servers)
			} else {
				killed = kv.killed()
			}
			if killed {
				return
			}
			op := Op{
				Type:      "UpdateShard",
				ShardInfo: shardinfo,
				Ver:       sop.ver,
				Shard:     me,
				// the shard belongs to me in the last and the current version;
				// no need to pull from another replica group
				Changed: gid != kv.gid,
			}
			kv.rf.Start(op)
		}
	}(pullChan)

	for {
		op := <-ops
		kv.DPrintf(fmt.Sprintf("shard %d, ver %d: ", me, kv.ver[me]) + op.sop2string())
		switch op.(type) {
		case Terminate:
			close(pullChan)
			return
		case Pull:
			sop := op.(Pull)
			if sop.ver > kv.ver[me] {
				_, isLeader := kv.rf.GetState()
				if isLeader {
					if sop.ver > kv.ver[me]+1 {
						kv.DPrintf("lagging for %d", kv.ver[me]+1)
					}
					pullChan <- sop
				}
			} else {
				kv.DPrintf("sees outdated pull %d", sop.ver)
			}
		case Get:
			sop := op.(Get)
			if !kv.valid[me] {
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
			if !kv.valid[me] {
				sop.result <- ErrWrongGroup
			} else {
				kv.data[me][sop.key] = sop.value
				sop.result <- OK
			}
		case Append:
			sop := op.(Append)
			if !kv.valid[me] {
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
			sop := op.(Abandon)
			if sop.ver > kv.ver[me]+1 {
				// this abandon appears earlier than the finish of last update
				// needs to delay a bit
				go func() {
					time.Sleep(100 * time.Millisecond)
					op := Op{
						Type:  "DelayedAbandon",
						Ver:   sop.ver,
						Shard: me,
					}
					kv.rf.Start(op)
				}()
			} else if sop.ver == kv.ver[me]+1 {
				kv.valid[me] = false
				kv.ver[me] = sop.ver
				kv.DPrintf("updates shard %d to ver %d: %v", me, sop.ver, kv.data[me])
			} else {
				kv.DPrintf("sees outdated abandon: %d", sop.ver)
			}
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
				kv.DPrintf("ErrLagConfig: my version: %d, expected version: %d", kv.ver[me], sop.ver)
				sop.result <- ErrLagConfig
			} else {
				copy := shardCopy(kv.data[me])
				sop.result <- copy
				kv.valid[me] = false // ownership transfer!
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
					kv.DPrintf("updates shard %d v %d from %v to %v\nlastRequestId: %v",
						me, kv.ver[me], kv.data[me], sop.shardinfo.Data, sop.shardinfo.LastRequestId)
					kv.data[me] = shardCopy(sop.shardinfo.Data)
					kv.lastRequestId[me] = sop.shardinfo.LastRequestId
				}
				kv.valid[me] = true
			} else {
				kv.DPrintf("notices stale update shard %v", sop.ver)
			}
		default:
			panic("Unexpected op type ")
		}
	}
}

func (kv *ShardKV) pullFrom(gid int, shard int, ver int, servers []string) (ShardInfo, bool) {
	args := PullArgs{
		Shard: shard,
		Ver:   ver,
		From:  kv.gid,
	}
	var reply PullReply

	for {
		for si := 0; si < len(servers); {
			srv := kv.make_end(servers[si])
			ok := srv.Call("ShardKV.GetShard", &args, &reply)
			if ok {
				switch reply.Err {
				case ErrWrongLeader:
					kv.DPrintf("[pull s %d v %d from %d] wrong leader: %s", shard, ver, gid, servers[si])
					si++
				case ErrLagConfig:
					kv.DPrintf("[pull s %d v %d from %d] lagging: %s", shard, ver, gid, servers[si])
					time.Sleep(waitLagReplicaTime * time.Millisecond)
				case OK:
					kv.DPrintf("fetch s %d v %d from %d!", shard, ver, gid)
					return reply.Data, false
				}
			} else {
				si++
			}
		}
		time.Sleep(time.Duration(retryPullTime) * time.Millisecond)
		if kv.killed() {
			return ShardInfo{}, true
		}
		kv.DPrintf("retry pulling shard %d version %d from %d...", shard, ver, gid)
	}
}

func (kv *ShardKV) GetShard(args *PullArgs, reply *PullReply) {
	op := Op{
		Type:   "GetShard",
		From:   args.From,
		Ver:    args.Ver,
		Shard:  args.Shard,
		Me:     kv.me,
		Uid:    kv.uid,
		Result: make(chan interface{}, 1),
	}
	index, _, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	kv.DPrintf("notices that %d wants to get shard %d for %v", args.From, args.Shard, args.Ver)

	timer := time.NewTimer(time.Millisecond * time.Duration(requestTimeout))
	defer timer.Stop()
	select {
	case <-timer.C:
		kv.DPrintf("timeout for index %d, let %d get shard %d v %d", index, args.From, args.Shard, args.Ver)
	case result := <-op.Result:
		switch result.(type) {
		case Err:
			kv.DPrintf("fail to let %d get shard %d v %d, due to %v", args.From, args.Shard, args.Ver, result.(Err))
			reply.Err = result.(Err)
		case ShardInfo:
			reply.Data = result.(ShardInfo)
			reply.Err = OK
		}
	}
}

//TODO
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
		Me:        kv.me,
		Uid:       kv.uid,
		Result:    make(chan interface{}, 1),
	}
	index, _, isLeader := kv.rf.Start(op)
	// by default, let the client retry
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	// kv.DPrintf("issues index %d, %s", index, op2string(op))

	timer := time.NewTimer(time.Millisecond * time.Duration(requestTimeout))
	defer timer.Stop()
	select {
	case <-timer.C:
		kv.DPrintf("timeout for index %d get %s, ck %d request %d", index, args.Key, args.ClerkId, args.RequestId)
	case result := <-op.Result:
		switch result.(type) {
		case Err:
			reply.Err = result.(Err)
		case string:
			reply.Err = OK
			reply.Value = result.(string)
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
		ClerkId:   args.ClerkId,
		RequestId: args.RequestId,
		Me:        kv.me,
		Uid:       kv.uid,
		Result:    make(chan interface{}, 1),
	}
	index, _, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	//kv.DPrintf("issues index %d, %s", index, op2string(op))

	timer := time.NewTimer(time.Millisecond * time.Duration(requestTimeout))
	defer timer.Stop()
	select {
	case <-timer.C:
		kv.DPrintf("timeout for index %d, %s %s %s, ck %d request %d", index, args.Op, args.Key, args.Value, args.ClerkId, args.RequestId)
	case result := <-op.Result:
		reply.Err = result.(Err)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.DPrintf("begins to shutdown...")
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	kv.killedChan <- true
	kv.DPrintf("is killed with ver %d!", kv.configIndex)
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
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
	kv.uid = nrand()
	log.SetFlags(0)
	log.SetOutput(new(logWriter))

	firstConfig := shardmaster.Config{}
	for i := 0; i < len(firstConfig.Shards); i++ {
		firstConfig.Shards[i] = kv.gid
	}
	firstConfig.Groups = make(map[int][]string)
	firstConfig.Groups[kv.gid] = nil
	kv.configs = append(kv.configs, firstConfig)
	kv.configIndex = 0

	kv.sm_ck = shardmaster.MakeClerk(masters)
	kv.data = make([]map[string]string, shardmaster.NShards)
	kv.ver = make([]int, shardmaster.NShards)
	kv.valid = make([]bool, shardmaster.NShards)

	kv.lastRequestId = make([]map[int64]map[int64]bool, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.lastRequestId[i] = make(map[int64]map[int64]bool)
	}
	kv.applyIndex = 0 // as raft, 1 is the first meaingful index
	kv.shardOp = make(map[int]chan ShardOp)
	kv.toGet = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killedChan = make(chan bool)
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
				kv.unlock("finish copying snapshot with applyIndex %d", applyIndex)
				kv.rf.TakeSnapshot(snapshot, applyIndex)
				kv.DPrintf("finish encoding snapshot with applyIndex %d", applyIndex)
			}
			time.Sleep(time.Duration(checkSnapshotPeriod) * time.Millisecond)
			if kv.killed() {
				return
			}
		}
	}()

	return kv
}

func (kv *ShardKV) checkApply() {
	for {
		select {
		case <-kv.killedChan:
			for i := 0; i < len(kv.configs[kv.configIndex].Shards); i++ {
				kv.giveShardOp(i, Terminate{})
				close(kv.shardOp[i])
			}
			return
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				if kv.applyIndex+1 != applyMsg.CommandIndex {
					kv.DPrintf("application not in order! expected: %d, given: %d", kv.applyIndex+1, applyMsg.CommandIndex)
					panic("application not in order")
				}
				op := applyMsg.Command.(Op)
				kv.lock("receives applyMsg index %v %s", applyMsg.CommandIndex, op2string(op))
				// not client request
				result := kv.applyOp(op)
				kv.DPrintf("%v <- %s with index %d", result, op2string(op), applyMsg.CommandIndex)
				kv.applyIndex++
				kv.unlock("finish applying %s with index: %d", op2string(op), applyMsg.CommandIndex)
			} else {
				// update the data with snapshot
				kv.DPrintf("receives snapshot from rf")
				kv.loadSnapshot(applyMsg.Snapshot)
			}
		}
	}
}

func (kv *ShardKV) applyOp(op Op) interface{} {
	// any string/OK (Err) for success, others for failure
	shard := key2shard(op.Key)
	var result interface{} = OK
	// No need to wait for Init and UpdateConfig
	if kv.lastRequestId[shard][op.ClerkId] == nil {
		kv.lastRequestId[shard][op.ClerkId] = make(map[int64]bool)
	}
	switch op.Type {
	case "GetShard":
		sop := GetShard{
			ver:  op.Ver,
			from: op.From,
		}
		sop.result = make(chan interface{}, 1)
		kv.giveShardOp(op.Shard, sop)
		result = <-sop.result
		if kv.me == op.Me && kv.uid == op.Uid {
			switch result.(type) {
			case map[string]string:
				op.Result <- ShardInfo{
					Data:          result.(map[string]string),
					LastRequestId: kv.lastRequestId[op.Shard],
				}
			case Err:
				op.Result <- result
			}
		}
	case "Get":
		sop := Get{
			key: op.Key,
		}
		sop.result = make(chan interface{})
		kv.giveShardOp(shard, sop)
		result = <-sop.result
		if kv.me == op.Me && kv.uid == op.Uid {
			op.Result <- result
		}
	case "Put":
		if kv.lastRequestId[shard][op.ClerkId][op.RequestId] {
			result = ErrDuplicate
		} else {
			sop := Put{
				key:   op.Key,
				value: op.Value,
			}
			sop.result = make(chan interface{})
			kv.giveShardOp(shard, sop)
			result = <-sop.result
			if result.(Err) == OK {
				if kv.lastRequestId[shard][op.ClerkId] == nil {
					kv.lastRequestId[shard][op.ClerkId] = make(map[int64]bool)
				}
				kv.lastRequestId[shard][op.ClerkId][op.RequestId] = true
			}
		}
		if kv.me == op.Me && kv.uid == op.Uid {
			op.Result <- result
		}
	case "Append":
		if kv.lastRequestId[shard][op.ClerkId][op.RequestId] {
			result = ErrDuplicate
		} else {
			sop := Append{
				key:   op.Key,
				value: op.Value,
			}
			sop.result = make(chan interface{})
			kv.giveShardOp(shard, sop)
			result = <-sop.result
			if result.(Err) == OK {
				if kv.lastRequestId[shard][op.ClerkId] == nil {
					kv.lastRequestId[shard][op.ClerkId] = make(map[int64]bool)
				}
				kv.lastRequestId[shard][op.ClerkId][op.RequestId] = true
			}
		}
		if kv.me == op.Me && kv.uid == op.Uid {
			op.Result <- result
		}
	case "UpdateConfig":
		config := shardmaster.CopyConfig(op.Config)
		if config.Num <= kv.configIndex || kv.toGet[config.Num] > 0 {
			kv.DPrintf("sees outdated config v %d", config.Num)
			return OK
		}
		kv.configs = append(kv.configs, config)
		kv.toGet[config.Num] = 0
		for i := 0; i < len(config.Shards); i++ {
			if config.Shards[i] == kv.gid {
				sop := Pull{
					ver: config.Num,
				}
				kv.toGet[config.Num]++
				kv.giveShardOp(i, sop)
			} else {
				// to advance the shard version
				kv.giveShardOp(i, Abandon{ver: config.Num})
			}
		}
		if kv.toGet[config.Num] == 0 {
			if config.Num > kv.lastPollConfigNum {
				kv.lastPollConfigNum = config.Num
			}
			kv.configIndex++
			kv.DPrintf("successfully updates config from %d to %d", kv.configIndex-1, kv.configIndex)
		}
	case "UpdateShard":
		sop := UpdateShard{
			ver:       op.Ver,
			shardinfo: op.ShardInfo,
			changed:   op.Changed,
		}
		kv.giveShardOp(op.Shard, sop)
		kv.toGet[op.Ver]--
		if kv.toGet[op.Ver] == 0 {
			if op.Ver > kv.lastPollConfigNum {
				delete(kv.toGet, op.Ver)
				kv.lastPollConfigNum = op.Ver
			}
			kv.configIndex++
			kv.DPrintf("successfully updates config from %d to %d", kv.configIndex-1, kv.configIndex)
		}
	case "DelayedAbandon":
		sop := Abandon{
			ver: op.Ver,
		}
		kv.giveShardOp(op.Shard, sop)
	default:
		kv.DPrintf("unexpected op type: %v\n", op.Type)
		panic("unknown op type!")
	}
	return result
}

func (kv *ShardKV) pollConfig() {
	if kv.lastPollConfigNum > kv.configIndex {
		// for restart with snapshot; maybe killed during updating to a new config
		kv.lastPollConfigNum = kv.configIndex
	}
	for {
		_, isLeader := kv.rf.GetState()
		// only issue update when the configuration changes
		// it's OK to have stale leaders doing the same job
		if isLeader {
			// always poll for the next configuration, not skipping any
			config := kv.sm_ck.Query(kv.lastPollConfigNum + 1)
			if config.Num == kv.lastPollConfigNum+1 {
				kv.lastPollConfigNum++
				kv.DPrintf("sees config %d: %v", config.Num, config.Shards)
				op := Op{
					Type:   "UpdateConfig",
					Config: config,
				}
				index, _, isLeader := kv.rf.Start(op)
				if isLeader {
					kv.DPrintf("issues index %d, %s", index, op2string(op))
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		if kv.killed() {
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
	if e.Encode(kv.configIndex) != nil {
		panic("fail to encode kv.lastConfigVer!")
	}
	if e.Encode(kv.lastPollConfigNum) != nil {
		panic("fail to encode kv.lastConfigVer!")
	}
	if e.Encode(kv.configs) != nil {
		panic("fail to encode kv.configs")
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
	if e.Encode(kv.valid) != nil {
		panic("fail to encode kv.valid")
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
		var lastRequestId []map[int64]map[int64]bool
		var applyIndex int
		var lastPollConfigNum int
		var configIndex int
		var configs []shardmaster.Config
		var ver []int
		var valid []bool
		if d.Decode(&applyIndex) != nil ||
			d.Decode(&configIndex) != nil ||
			d.Decode(&lastPollConfigNum) != nil ||
			d.Decode(&configs) != nil ||
			d.Decode(&data) != nil ||
			d.Decode(&lastRequestId) != nil ||
			d.Decode(&ver) != nil ||
			d.Decode(&valid) != nil {
			kv.DPrintf("fails to read snapshot!")
			panic("fail to read snapshot")
		}
		kv.applyIndex = applyIndex
		kv.data = data
		kv.lastRequestId = lastRequestId
		kv.configIndex = configIndex
		kv.configs = configs
		kv.lastPollConfigNum = lastPollConfigNum
		kv.ver = ver
		kv.valid = valid
		kv.DPrintf("loads config: %v, \ndata: %v", kv.configs[configIndex], kv.data)
		kv.unlock("load snapshot with applyIndex: %d", kv.applyIndex)
	}
}

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("15:04:05.999") + string(bytes))
}

func (kv *ShardKV) DPrintf(msg string, f ...interface{}) {
	if Debug {
		log.Printf("[gid %d, kv %d, %d] %s", kv.gid, kv.me, kv.uid, fmt.Sprintf(msg, f...))
		// logrus.Debugf("[gid %d, kv %d, %d] %s", kv.gid, kv.me, fmt.Sprintf(msg, f...))
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
