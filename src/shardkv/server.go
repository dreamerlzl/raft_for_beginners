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
	checkSnapshotPeriod = 300
	requestTimeout      = 300
	opChannelBufferSize = 10
	waitLagReplicaTime  = 300
	retryTime           = 300
	delayAbandonTime    = 200
)
const logLevel = logrus.DebugLevel
const ratio float32 = 0.95 // when rf.RaftStateSize >= ratio * kv.maxraftestatesize, take a snapshot

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

	// for update/modify shards
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
	ver    int
	result chan interface{}
}

func (g Get) sop2string() string {
	return fmt.Sprintf("get %s", g.key)
}

type Put struct {
	key    string
	value  string
	ver    int
	result chan interface{}
}

func (p Put) sop2string() string {
	return fmt.Sprintf("put %s %s, v %d", p.key, p.value, p.ver)
}

type Append struct {
	key    string
	value  string
	ver    int
	result chan interface{}
}

func (a Append) sop2string() string {
	return fmt.Sprintf("append %s %s, v %d", a.key, a.value, a.ver)
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
	ver         int
	shardinfo   ShardInfo
	changed     bool
	providerGid int
	result      chan bool
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
	lastPollConfigNum int
	lastUpdatedConfig int
	sm_ck             *shardmaster.Clerk
	data              []map[string]string // shard -> key -> value
	ver               []int
	lastValid         []int
	lastRequestId     []map[int64]map[int64]bool // ClerkId -> last finished RequestId
	applyIndex        int
	shardOp           map[int]chan ShardOp
	updateChan        []chan int
	mu2               sync.Mutex //to avoid concurrent hashmap write of kv.shardOp and kv.lastLeader
	mu3               sync.Mutex // sync toGet
	lastLeader        map[int]int
	toGet             map[int]int
	// to ensure that when the leader fails during a reconfig,
	// the new leader can retry the reconfig
	dead       int32
	killedChan chan bool
	uid        int64
}

func (kv *ShardKV) updateConfig() {
	for i := kv.lastUpdatedConfig + 1; ; i++ {
		if v, ok := kv.toGet[i]; !ok || v > 0 {
			break
		}
		kv.lastUpdatedConfig = i
	}
	kv.lastPollConfigNum = kv.lastUpdatedConfig
	kv.DPrintf("finish update config %d", kv.lastUpdatedConfig)
}

func (kv *ShardKV) getNumShards() int {
	return shardmaster.NShards
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

	// for pulling data from other groups
	pullChan := make(chan Pull, 10)
	go func(sops chan Pull) {
		for sop := range sops {
			var shardinfo ShardInfo
			var status PullStatus
			lastConfig := kv.configs[sop.ver-1]
			if lastConfig.Num != sop.ver-1 {
				kv.DPrintf("inconsistent config ver; wanted %d, given %d\n%v", sop.ver-1, lastConfig.Num, kv.configs)
				panic("wrong config ver")
			}
			gid := lastConfig.Shards[me]
			servers := lastConfig.Groups[gid]

			finish := false
			retryPull := 500
			for {
				if gid != kv.gid {
					_, isLeader := kv.rf.GetState()
					if isLeader {
						kv.DPrintf("shard %d v %d: start pulling from %d: %v", me, sop.ver, gid, kv.configs)
						shardinfo, status = kv.pullFrom(gid, me, sop.ver, servers)
						if status == Killed || status == Stopped {
							break
						}
					}
				} else {
					kv.DPrintf("pull shard %d v %d from myself", me, sop.ver)
				}

				op := Op{
					Type:      "UpdateShard",
					ShardInfo: shardinfo,
					Ver:       sop.ver,
					Shard:     me,
					From:      gid,
					Uid:       kv.uid,
					// the shard belongs to me in the last and the current version;
					// no need to pull from another replica group
					Changed: gid != kv.gid,
				}
				index, _, isLeader := kv.rf.Start(op) // only leader would succeed
				if isLeader {
					kv.DPrintf("issues update shard %d, v %d at index %d", me, sop.ver, index)
				}

				timer := time.NewTimer(time.Millisecond * time.Duration(retryPull))
				defer timer.Stop()
				select {
				case <-timer.C:
					kv.DPrintf("timeout: pull shard %d for v %d", me, sop.ver)
					if 2*retryPull < 2000 {
						retryPull = 2 * retryPull
					}
				case ver := <-kv.updateChan[me]:
					kv.DPrintf("realize update shard %d v %d is issued", me, sop.ver)
					if ver != sop.ver {
						kv.DPrintf("shard %d wanted update ver: %d; given update ver: %d", me, sop.ver, ver)
						// panic("unexpected update ver")
					}
					finish = true
				}
				if finish {
					break
				}
				if kv.killed() {
					return
				}
			}
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
				if sop.ver > kv.ver[me]+1 {
					kv.DPrintf("shard %d, skip v %d", me, kv.ver[me]+1)
					// panic("unexpected pull")
				}
				kv.DPrintf("begins to pull shard %d for v %d", me, sop.ver)
				pullChan <- sop
			} else {
				kv.DPrintf("sees outdated pull %d", sop.ver)
			}
		case Get:
			sop := op.(Get)
			if sop.ver > kv.lastValid[me] {
				sop.result <- ErrLagConfig
			} else if sop.ver == kv.lastValid[me] && kv.lastValid[me] == kv.ver[me] {
				if _, ok := kv.data[me][sop.key]; !ok {
					sop.result <- ErrNoKey
					kv.DPrintf("no key %s: %v", sop.key, kv.data[me])
				} else {
					sop.result <- kv.data[me][sop.key]
				}
			} else {
				sop.result <- ErrWrongGroup
			}
		case Put:
			sop := op.(Put)
			if sop.ver > kv.lastValid[me] {
				sop.result <- ErrLagConfig
			} else if sop.ver == kv.lastValid[me] && kv.lastValid[me] == kv.ver[me] {
				kv.data[me][sop.key] = sop.value
				sop.result <- OK
			} else {
				sop.result <- ErrWrongGroup
			}
		case Append:
			sop := op.(Append)
			if sop.ver > kv.lastValid[me] {
				sop.result <- ErrLagConfig
			} else if sop.ver == kv.lastValid[me] && kv.lastValid[me] == kv.ver[me] {
				if _, ok := kv.data[me][sop.key]; !ok {
					sop.result <- ErrNoKey
				} else {
					kv.data[me][sop.key] += sop.value
					sop.result <- OK
				}
			} else {
				sop.result <- ErrWrongGroup
			}
		case Abandon:
			sop := op.(Abandon)
			if sop.ver > kv.ver[me]+1 {
				// this abandon appears earlier than the finish of last update
				// needs to delay a bit
				kv.DPrintf("unexpected abandon for shard %d: %d; current v %d", me, sop.ver, kv.ver[me])
				// panic("unexpected abandon")
				go func() {
					time.Sleep(time.Millisecond * time.Duration(delayAbandonTime))
					op := Op{
						Type:   "DelayedAbandon",
						Ver:    sop.ver,
						Shard:  me,
						Result: nil,
					}
					kv.rf.Start(op)
				}()
			} else if sop.ver == kv.ver[me]+1 {
				kv.ver[me] = sop.ver
				kv.DPrintf("updates shard %d to ver %d: %v", me, sop.ver, kv.data[me])
			} else {
				kv.DPrintf("sees outdated abandon: %d", sop.ver)
			}
		case GetShard:
			sop := op.(GetShard)
			if sop.ver-1 > kv.lastValid[me] {
				kv.DPrintf("ErrLagConfig: shard %d last valid version: %d, expected version: %d", me, kv.lastValid[me], sop.ver-1)
				sop.result <- ErrLagConfig
			} else {
				copy := shardCopy(kv.data[me])
				sop.result <- copy
				if sop.ver == kv.lastValid[me]+1 {
					if sop.ver > kv.ver[me] {
						kv.ver[me] = sop.ver
					}
					kv.DPrintf("transfers shard %d to %d for v %d!", me, sop.from, sop.ver)
				} else {
					kv.DPrintf("gives meaningless shard %d to %d for v %d", me, sop.from, sop.ver)
				}
			}
		case UpdateShard:
			sop := op.(UpdateShard)
			if sop.ver == kv.ver[me]+1 {
				if sop.changed {
					if sop.shardinfo.Data == nil {
						kv.DPrintf("shard %d v %d rare case: first no leader pull data, but then one issues this log entry successfully", me, sop.ver)
						break // skip this update
					}
					kv.data[me] = shardCopy(sop.shardinfo.Data)
					kv.lastRequestId[me] = lastRequestCopy(sop.shardinfo.LastRequestId)
				}
				kv.DPrintf("waiting for receiving update shard %d v %d", me, sop.ver)
				kv.updateChan[me] <- sop.ver
				kv.ver[me] = sop.ver
				kv.lastValid[me] = sop.ver
				kv.mu3.Lock()
				if _, ok := kv.toGet[sop.ver]; ok {
					kv.toGet[sop.ver]--
					kv.DPrintf("to get %d for config %d", kv.toGet[sop.ver], sop.ver)
					if kv.toGet[sop.ver] == 0 && sop.ver == kv.lastUpdatedConfig+1 {
						kv.updateConfig()
					}
				} else {
					kv.DPrintf("shard %d, v %d: sees unfinished update v %d from last instance", me, kv.ver[me], sop.ver)
				}
				kv.mu3.Unlock()
				kv.DPrintf("shard %d, v %d: becomes valid", me, kv.ver[me])
			} else if sop.ver > kv.ver[me]+1 {
				kv.DPrintf("notices unexpected update shard %d v %d", me, sop.ver)
				panic("unexpected update shard")
			} else {
				kv.DPrintf("notices outdated update shard %d v %d", me, sop.ver)
			}
			sop.result <- true
		default:
			panic("Unexpected op type ")
		}
	}
}

type PullStatus int

const (
	Done    PullStatus = 0
	Killed  PullStatus = 1
	Stopped PullStatus = 2
)

func (kv *ShardKV) pullFrom(gid int, shard int, ver int, servers []string) (ShardInfo, PullStatus) {
	args := PullArgs{
		Shard: shard,
		Ver:   ver,
		From:  kv.gid,
	}
	var reply PullReply
	num := len(servers)
	for {
		j := 0
		kv.mu2.Lock()
		si := kv.lastLeader[gid]
		kv.mu2.Unlock()
		for j < num {
			srv := kv.make_end(servers[si])
			ok := srv.Call("ShardKV.GetShard", &args, &reply)
			if ok {
				switch reply.Err {
				case ErrTimeout:
					kv.DPrintf("shard %d v %d from %d: timeout: %s", shard, ver, gid, servers[si])
					si = (si + 1) % num
					j++
				case ErrWrongLeader:
					kv.DPrintf("shard %d v %d from %d: wrong leader: %s", shard, ver, gid, servers[si])
					si = (si + 1) % num
					j++
				case ErrLagConfig:
					kv.DPrintf("shard %d v %d from %d: lagging: %s", shard, ver, gid, servers[si])
					time.Sleep(waitLagReplicaTime * time.Millisecond)
				case OK:
					kv.mu2.Lock()
					kv.lastLeader[gid] = si
					kv.mu2.Unlock()
					kv.DPrintf("shard %d v %d from %d: fetch success", shard, ver, gid)
					return reply.Data, Done
				}
			} else {
				si = (si + 1) % num
				j++
			}
		}
		time.Sleep(time.Duration(retryTime) * time.Millisecond)
		select {
		case v := <-kv.updateChan[shard]:
			if v != ver {
				kv.DPrintf("unexpected update shard %d v %d in pullFrom", shard, ver)
				panic("unexpected update shard in pullFrom")
			}
			kv.DPrintf("stops pulling shard %d v %d from %d", shard, ver, gid)
			return ShardInfo{}, Stopped
		default:
		}
		if kv.killed() {
			return ShardInfo{}, Killed
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
	kv.DPrintf("notices that %d wants to get shard %d for %v at index %d", args.From, args.Shard, args.Ver, index)

	timeout := kv.applyIndex - index
	if timeout < requestTimeout {
		timeout = requestTimeout
	}
	timer := time.NewTimer(time.Millisecond * time.Duration(timeout))
	defer timer.Stop()
	select {
	case <-timer.C:
		reply.Err = ErrTimeout
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
		Ver:       args.Ver,
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
	kv.DPrintf("issues index %d, %s", index, op2string(op))

	timeout := kv.applyIndex - index
	if timeout < requestTimeout {
		timeout = requestTimeout
	}
	timer := time.NewTimer(time.Millisecond * time.Duration(timeout))
	defer timer.Stop()
	select {
	case <-timer.C:
		reply.Err = ErrTimeout
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
		Ver:       args.Ver,
		Me:        kv.me,
		Uid:       kv.uid,
		Result:    make(chan interface{}, 1),
	}
	index, _, isLeader := kv.rf.Start(op)
	reply.Err = ErrWrongLeader
	if !isLeader {
		return
	}
	kv.DPrintf("issues index %d, %s", index, op2string(op))
	timeout := kv.applyIndex - index
	if timeout < requestTimeout {
		timeout = requestTimeout
	}

	timer := time.NewTimer(time.Millisecond * time.Duration(timeout))
	defer timer.Stop()
	select {
	case <-timer.C:
		reply.Err = ErrTimeout
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
	kv.DPrintf("is killed!")
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
		firstConfig.Shards[i] = kv.gid // todo
	}
	firstConfig.Groups = make(map[int][]string)
	firstConfig.Groups[kv.gid] = nil
	kv.configs = append(kv.configs, firstConfig)

	kv.sm_ck = shardmaster.MakeClerk(masters)
	kv.data = make([]map[string]string, shardmaster.NShards)
	kv.ver = make([]int, shardmaster.NShards)
	kv.lastValid = make([]int, shardmaster.NShards)
	kv.updateChan = make([]chan int, shardmaster.NShards)
	kv.toGet = make(map[int]int)

	kv.lastRequestId = make([]map[int64]map[int64]bool, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.lastRequestId[i] = make(map[int64]map[int64]bool)
		kv.updateChan[i] = make(chan int, 1) // avoid deadlock, handling kill signal
	}
	kv.applyIndex = 0 // as raft, 1 is the first meaingful index
	kv.shardOp = make(map[int]chan ShardOp)
	kv.lastLeader = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killedChan = make(chan bool, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// read snapshot
	kv.loadSnapshot(kv.rf.GetSnapshot())
	kv.DPrintf("is started with lastPollConfigNum: %d", kv.lastPollConfigNum)

	go kv.checkApply()
	go kv.pollConfig()
	// periodically check whether need to take a snapshot
	go func() {
		for {
			size := kv.rf.GetStateSize()
			if size >= int(ratio*float32(kv.maxraftstate)) {
				kv.lock("start to encode snapshot")
				snapshot := kv.encodeSnapshot()
				applyIndex := kv.applyIndex
				kv.unlock("finish copying snapshot with applyIndex %d", applyIndex)
				kv.rf.TakeSnapshot(snapshot, applyIndex)
				kv.DPrintf("finish encoding snapshot with applyIndex %d", applyIndex)
			} else {
				kv.DPrintf("snapshot size: %d/%d", size, kv.maxraftstate)
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
			for i := 0; i < kv.getNumShards(); i++ {
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
	// toCheck := false
	// No need to wait for Init and UpdateConfig
	if kv.lastRequestId[shard][op.ClerkId] == nil {
		kv.lastRequestId[shard][op.ClerkId] = make(map[int64]bool)
	}
	switch op.Type {
	case "GetShard":
		// toCheck = true
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
					LastRequestId: lastRequestCopy(kv.lastRequestId[op.Shard]),
				}
			case Err:
				op.Result <- result
			}
		}
	case "Get":
		sop := Get{
			key: op.Key,
			ver: op.Ver,
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
				ver:   op.Ver,
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
				ver:   op.Ver,
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
		kv.mu3.Lock()
		defer kv.mu3.Unlock()
		if _, ok := kv.toGet[config.Num]; config.Num <= kv.lastUpdatedConfig || ok {
			kv.DPrintf("sees outdated config v %d; last finished config: %d", config.Num, kv.lastUpdatedConfig)
			return OK
		}
		kv.toGet[config.Num] = 0
		if config.Num > len(kv.configs)-1 {
			kv.configs = append(kv.configs, config)
		}
		kv.DPrintf("adds config %d: %v\n%v", config.Num, config.Shards, kv.configs[config.Num])
		for i := 0; i < len(config.Shards); i++ {
			if config.Shards[i] == kv.gid {
				sop := Pull{
					ver: config.Num,
				}
				kv.giveShardOp(i, sop)
				kv.toGet[config.Num]++
			} else {
				// to advance the shard version
				kv.giveShardOp(i, Abandon{ver: config.Num})
			}
		}
		kv.DPrintf("to get %d shards for config %d", kv.toGet[config.Num], config.Num)
		if kv.toGet[config.Num] == 0 && config.Num == kv.lastUpdatedConfig+1 {
			kv.updateConfig()
		}
	case "UpdateShard":
		sop := UpdateShard{
			ver:         op.Ver,
			shardinfo:   op.ShardInfo,
			changed:     op.Changed,
			providerGid: op.From,
			result:      make(chan bool),
		}
		kv.giveShardOp(op.Shard, sop)
		<-sop.result // just for sync
	case "DelayedAbandon":
		sop := Abandon{
			ver: op.Ver,
		}
		kv.giveShardOp(op.Shard, sop)
		if op.Result != nil && op.Uid == kv.uid && op.Me == kv.me {
			op.Result <- OK
		}
	default:
		kv.DPrintf("unexpected op type: %v\n", op.Type)
		panic("unknown op type!")
	}
	return result
}

func (kv *ShardKV) pollConfig() {
	if kv.lastPollConfigNum > kv.lastUpdatedConfig {
		// for restart with snapshot; maybe killed during updating to a new config
		kv.lastPollConfigNum = kv.lastUpdatedConfig
	}
	for {
		_, isLeader := kv.rf.GetState()
		// only issue update when the configuration changes
		// it's OK to have stale leaders doing the same job
		if isLeader {
			// always poll for the next configuration, not skipping any
			config := kv.sm_ck.Query(kv.lastPollConfigNum + 1)
			if config.Num == kv.lastPollConfigNum+1 {
				kv.DPrintf("sees config %d: %v", config.Num, config.Shards)
				op := Op{
					Type:   "UpdateConfig",
					Config: config,
					Uid:    kv.uid,
				}
				index, _, isLeader := kv.rf.Start(op)
				if isLeader {
					kv.lastPollConfigNum++
					kv.DPrintf("issues %s at index %d", op2string(op), index)
				} else {
					kv.DPrintf("fail to issue %s at index %d", op2string(op), index)
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
	if e.Encode(kv.lastUpdatedConfig) != nil {
		panic("fail to encode kv.lastUpdatedConfig")
	}
	if e.Encode(kv.applyIndex) != nil {
		panic("fail to encode kv.applyIndex!")
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
	if e.Encode(kv.lastValid) != nil {
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
		var lastUpdatedConfig int
		var applyIndex int
		var lastPollConfigNum int
		var configs []shardmaster.Config
		var ver []int
		var lastValid []int
		if d.Decode(&lastUpdatedConfig) != nil ||
			d.Decode(&applyIndex) != nil ||
			d.Decode(&lastPollConfigNum) != nil ||
			d.Decode(&configs) != nil ||
			d.Decode(&data) != nil ||
			d.Decode(&lastRequestId) != nil ||
			d.Decode(&ver) != nil ||
			d.Decode(&lastValid) != nil {
			kv.DPrintf("fails to read snapshot!")
			panic("fail to read snapshot")
		}
		kv.lastUpdatedConfig = lastUpdatedConfig
		kv.applyIndex = applyIndex
		kv.data = data
		kv.lastRequestId = lastRequestId
		kv.configs = configs
		kv.lastPollConfigNum = lastPollConfigNum
		kv.ver = ver
		kv.lastValid = lastValid
		// kv.DPrintf("loads config: %v, \ndata: %v", kv.configs, kv.data)
		kv.unlock("load snapshot with applyIndex: %d, last updated config: %d", kv.applyIndex, kv.lastUpdatedConfig)
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
