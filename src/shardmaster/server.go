package shardmaster

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const debug = 1
const (
	checkLeaderPeriod   = 50
	checkSnapshotPeriod = 150
	rpcTimeout          = 500
)
const ratio float32 = 0.90 // when rf.RaftStateSize >= ratio * kv.maxraftestatesize, take a snapshot

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs       []Config // indexed by config num
	lastNum       int
	lastRequestId map[int64]int64 // clerk id -> last finished request id
	applyIndex    int
	gid2shards    map[int][]int // configs[-1]'s mappings of gid -> shards
	applyResult   map[int]interface{}
}

func (sm *ShardMaster) DPrintf(msg string, f ...interface{}) {
	DPrintf(fmt.Sprintf("[sm %d] %s", sm.me, msg), f...)
}

func (sm *ShardMaster) lock(msg string, f ...interface{}) {
	sm.mu.Lock()
	sm.DPrintf(msg, f...)
}

func (sm *ShardMaster) unlock(msg string, f ...interface{}) {
	sm.mu.Unlock()
	sm.DPrintf(msg, f...)
}

type RequestType int

const (
	join RequestType = iota
	leave
	move
	query
)

type Op struct {
	// Your data here.
	Type      RequestType
	RequestId int64
	ClerkId   int64
	Shard     int
	GID       int
	Num       int
	GIDs      []int
	Servers   map[int][]string
}

func op2string(op Op) string {
	var s string
	switch op.Type {
	case move:
		s = fmt.Sprintf("Move{shard: %d, gid: %d}, request: %d, clerk: %d", op.Shard, op.GID, op.RequestId, op.ClerkId)
	case join:
		s = fmt.Sprintf("Join{assignment: %v}, request: %d, clerk: %d", op.Servers, op.RequestId, op.ClerkId)
	case leave:
		s = fmt.Sprintf("Leave{leaving groups: %v}, request: %d, clerk: %d", op.GIDs, op.RequestId, op.ClerkId)
	case query:
		s = fmt.Sprintf("Query{config: %v}, request: %d, clerk: %d", op.Num, op.RequestId, op.ClerkId)
	}
	return s
}

// func (sm *ShardMaster) isDuplicate(clerkId, requestId int64) bool {
// 	duplicate := false
// 	sm.lock("checks whether the request %d by %d is duplicate", requestId, clerkId)
// 	if sm.lastRequestId[clerkId] == requestId {
// 		duplicate = true
// 	}
// 	sm.unlock("finished check duplication")
// 	return duplicate
// }

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// if sm.isDuplicate(args.ClerkId, args.RequestId) {
	// 	reply.Err = "duplicate request"
	// 	DPrintf("[sm %d] duplicate request %d by %d", sm.me, args.RequestId, args.ClerkId)
	// 	return
	// }
	op := Op{
		Type:      join,
		RequestId: args.RequestId,
		ClerkId:   args.ClerkId,
		Servers:   args.Servers,
	}
	index, term, isLeader := sm.rf.Start(op)
	reply.Err = WrongLeader
	if isLeader == false {
		return
	}

	sm.DPrintf("issues %s at index %d", op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		currentTerm, isleader := sm.rf.GetState()
		if !(term == currentTerm && isleader) {
			return
		}
		if sm.applyIndex >= index {
			reply.Err = OK
			return
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// if sm.isDuplicate(args.ClerkId, args.RequestId) {
	// 	reply.Err = "duplicate request"
	// 	DPrintf("[sm %d] duplicate request %d by %d", sm.me, args.RequestId, args.ClerkId)
	// 	return
	// }
	op := Op{
		Type:      leave,
		RequestId: args.RequestId,
		ClerkId:   args.ClerkId,
		GIDs:      args.GIDs,
	}
	index, _, isLeader := sm.rf.Start(op)
	reply.Err = WrongLeader
	if isLeader == false {
		return
	}

	sm.DPrintf("issues %s at index %d", op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		// currentTerm, isleader := sm.rf.GetState()
		// if !(term == currentTerm && isleader) {
		// 	return
		// }
		if sm.applyIndex >= index {
			reply.Err = OK
			return
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// if sm.isDuplicate(args.ClerkId, args.RequestId) {
	// 	reply.Err = "duplicate request"
	// 	DPrintf("[sm %d] duplicate request %d by %d", sm.me, args.RequestId, args.ClerkId)
	// 	return
	// }
	op := Op{
		Type:      move,
		RequestId: args.RequestId,
		ClerkId:   args.ClerkId,
		GID:       args.GID,
		Shard:     args.Shard,
	}
	index, _, isLeader := sm.rf.Start(op)
	reply.Err = WrongLeader
	if isLeader == false {
		return
	}

	sm.DPrintf("issues %s at index %d", op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		// currentTerm, isleader := sm.rf.GetState()
		// if !(term == currentTerm && isleader) {
		// 	return
		// }
		if sm.applyIndex >= index {
			reply.Err = OK
			return
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// if sm.isDuplicate(args.ClerkId, args.RequestId) {
	// 	reply.Err = "duplicate request"
	// 	DPrintf("[sm %d] duplicate request %d by %d", sm.me, args.RequestId, args.ClerkId)
	// 	return
	// }
	op := Op{
		Type:      query,
		RequestId: args.RequestId,
		ClerkId:   args.ClerkId,
		Num:       args.Num,
	}
	index, _, isLeader := sm.rf.Start(op)
	reply.Err = WrongLeader
	if isLeader == false {
		return
	}

	sm.DPrintf("issues %s at index %d", op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		// currentTerm, isleader := sm.rf.GetState()
		// if !(term == currentTerm && isleader) {
		// 	return
		// }
		if sm.applyIndex >= index {
			sm.lock("is reading config %d", args.Num)
			result := sm.applyResult[index]
			delete(sm.applyResult, index)
			sm.unlock("finished reading config %d: %v", args.Num, result)
			switch result.(type) {
			case Err:
				reply.Err = result.(Err)
			case Config:
				reply.Err = OK
				reply.Config = result.(Config)
			}
			return
		}
	}
}

func (sm *ShardMaster) checkApplyMsg() {
	for {
		applyMsg := <-sm.applyCh
		if applyMsg.CommandValid {
			if sm.applyIndex+1 != applyMsg.CommandIndex {
				sm.DPrintf("application not in order! expected: %d, given: %d", sm.applyIndex+1, applyMsg.CommandIndex)
				panic("application not in order")
			}
			op := applyMsg.Command.(Op)
			sm.lock("starts to apply index %d, op %s", applyMsg.CommandIndex, op2string(op))
			if op.Type == query {
				var r interface{} = InvalidNum
				if op.Num < len(sm.configs) && op.Num >= 0 {
					r = sm.configs[op.Num]
				} else if op.Num == -1 {
					r = sm.configs[sm.lastNum]
				}
				sm.applyResult[applyMsg.CommandIndex] = r
			} else {
				if sm.lastRequestId[op.ClerkId] == op.RequestId {
					sm.DPrintf("duplicate request %d by %d", op.RequestId, op.ClerkId)
				} else {
					sm.applyOp(op)
					sm.lastRequestId[op.ClerkId] = op.RequestId
				}
			}
			sm.applyIndex++
			sm.unlock("ends applying index %d, op %s", applyMsg.CommandIndex, op2string(op))
		} else {
			// reserved for snapshots!
			sm.loadSnapshot(applyMsg.Snapshot)
		}
	}
}

func copyConfig(o Config) Config {
	ng := make(map[int][]string)
	for k, v := range o.Groups {
		ng[k] = v
	}

	var s [NShards]int
	for i, v := range o.Shards {
		s[i] = v
	}

	return Config{
		Num:    o.Num,
		Shards: s,
		Groups: ng,
	}
}

// note that when the following function executes, it's under a mutex lock
func (sm *ShardMaster) applyOp(op Op) {
	c := copyConfig(sm.configs[sm.lastNum])
	c.Num = sm.lastNum + 1
	gid2shards := make(map[int][]int)
	for shard, gid := range c.Shards {
		if gid != 0 {
			gid2shards[gid] = append(gid2shards[gid], shard)
		}
	}

	for gid, shards := range gid2shards {
		if !equalShards(shards, sm.gid2shards[gid]) {
			sm.DPrintf("inconsistent shards %v and gid2shards %v", c.Shards, sm.gid2shards)
			panic("inconsistent shards and gid2shards")
		}
	}

	switch op.Type {
	case join:
		sm.DPrintf("before join %v; %v, %v, %v", op.Servers, len(c.Groups), sm.gid2shards, c.Shards)
		c = sm.rebalanceJoin(c, op.Servers) // in-place modification
		sm.DPrintf("after join %v; %v, %v,%v", op.Servers, len(c.Groups), sm.gid2shards, c.Shards)
	case leave:
		sm.DPrintf("before leave %v: %v, %v, %v", op.GIDs, len(c.Groups), sm.gid2shards, c.Shards)
		c = sm.rebalanceLeave(c, op.GIDs) // in-place modification
		sm.DPrintf("after leave %v: %v, %v, %v", op.GIDs, len(c.Groups), sm.gid2shards, c.Shards)
	case move:
		oldGid := c.Shards[op.Shard]
		sm.gid2shards[oldGid] = deleteShard(sm.gid2shards[oldGid], op.Shard)
		c.Shards[op.Shard] = op.GID
		sm.gid2shards[op.GID] = append(sm.gid2shards[op.GID], op.Shard)
	}
	sm.configs = append(sm.configs, c)
	sm.lastNum++
}

// modify c's Group and shards in-place
func (sm *ShardMaster) rebalanceJoin(c Config, servers map[int][]string) Config {
	oldSize := len(c.Groups)
	newSize := len(servers) + oldSize
	averageFloor := NShards / newSize
	takeFromOld := Max(newSize*averageFloor, NShards-oldSize) - oldSize*averageFloor

	var thisTake int
	l := oldSize
	pool := make([]int, 0, takeFromOld)
	if oldSize == 0 {
		for i := 0; i < NShards; i++ {
			pool = append(pool, i)
		}
	} else {
		// need ordering, from small to large
		t := takeFromOld
		for _, pair := range sortByValue(sm.gid2shards, false) {
			gid := pair.Key
			shards := sm.gid2shards[gid]
			thisTake = t / l
			pool = append(pool, shards[:thisTake]...)
			// DPrintf("[sm %d] pool add %v from group %d", sm.me, shards[:thisTake], gid)
			sm.gid2shards[gid] = shards[thisTake:]
			t -= thisTake
			l--
		}
	}

	// DPrintf("[sm %d] pool: %v", sm.me, pool)

	l = len(servers)
	for gid, names := range servers {
		thisTake = takeFromOld / l
		// DPrintf("[sm %d] group %d takes %d shards: %v", sm.me, gid, thisTake, pool[:thisTake])
		sm.gid2shards[gid] = make([]int, len(pool[:thisTake]))
		copy(sm.gid2shards[gid], pool[:thisTake])
		for _, shard := range sm.gid2shards[gid] {
			c.Shards[shard] = gid
		}
		pool = pool[thisTake:]
		c.Groups[gid] = names
		takeFromOld -= thisTake
		l--
	}
	return c
}

func (sm *ShardMaster) rebalanceLeave(c Config, gids []int) Config {
	oldSize := len(c.Groups)
	newSize := oldSize - len(gids)
	pool := make([]int, 0, NShards-newSize*NShards/oldSize)
	for _, gid := range gids {
		pool = append(pool, sm.gid2shards[gid]...)
		// sm.DPrintf("add %v from %d to pool", sm.gid2shards[gid], gid)
		delete(sm.gid2shards, gid)
		delete(c.Groups, gid)
	}

	var thisTake int
	l := newSize
	ps := len(pool)
	if newSize == 0 {
		for _, shard := range pool {
			c.Shards[shard] = 0
		}
	} else {
		// need ordering of c.Groups, from large to small
		for _, pair := range sortByValue(sm.gid2shards, true) {
			gid := pair.Key
			thisTake = ps / l
			sm.gid2shards[gid] = append(sm.gid2shards[gid], pool[:thisTake]...)
			// sm.DPrintf("%d takes %v from pool", gid, pool[:thisTake])
			for _, shard := range pool[:thisTake] {
				c.Shards[shard] = gid
			}
			pool = pool[thisTake:]
			ps -= thisTake
			if ps == 0 {
				break
			}
			l--
		}
	}
	return c
}

func sortByValue(m map[int][]int, reverse bool) PairList {
	pl := make(PairList, len(m))
	i := 0
	for k, v := range m {
		pl[i] = Pair{k, len(v)}
		i++
	}
	if reverse {
		sort.Sort(sort.Reverse(pl))
	} else {
		sort.Sort(pl)
	}
	return pl
}

type Pair struct {
	Key   int
	Value int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.lastNum = 0
	sm.applyIndex = 0
	sm.lastRequestId = make(map[int64]int64)
	sm.gid2shards = make(map[int][]int)
	sm.applyResult = make(map[int]interface{})
	sm.loadSnapshot(sm.rf.GetSnapshot())

	// a function for receiving notifications from the applyCh
	go sm.checkApplyMsg()

	go func() {
		for {
			if sm.rf.GetStateSize() >= int(ratio*float32(1000)) {
				sm.lock("starts to encode snapshot")
				snapshot := sm.encodeSnapshot()
				applyIndex := sm.applyIndex
				sm.unlock("finishes encoding snapshot with applyIndex %d", applyIndex)
				sm.rf.TakeSnapshot(snapshot, applyIndex)
			}
			time.Sleep(time.Duration(checkSnapshotPeriod) * time.Millisecond)
		}
	}()
	return sm
}

func (sm *ShardMaster) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(sm.applyIndex) != nil {
		panic("fail to encode sm.applyIndex!")
	}
	if e.Encode(sm.lastNum) != nil {
		panic("fail to encode sm.lastConfigVer!")
	}
	if e.Encode(sm.configs) != nil {
		panic("fail to encode sm.data!")
	}
	if e.Encode(sm.lastRequestId) != nil {
		panic("fail to encode sm.lastRequestId!")
	}
	if e.Encode(sm.gid2shards) != nil {
		panic("fail to encode sm.shardVersion!")
	}
	return w.Bytes()
}

//TODO; add fields
func (sm *ShardMaster) loadSnapshot(snapshot []byte) {
	if len(snapshot) > 0 {
		sm.lock("starts to load snapshot...")
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var configs []Config
		var lastRequestId map[int64]int64
		var gid2shard map[int][]int
		var applyIndex int
		var lastNum int
		if d.Decode(&applyIndex) != nil ||
			d.Decode(&lastNum) != nil ||
			d.Decode(&configs) != nil ||
			d.Decode(&lastRequestId) != nil ||
			d.Decode(&gid2shard) != nil {
			sm.DPrintf("fails to read snapshot!")
			panic("fail to read snapshot")
		}
		sm.applyIndex = applyIndex
		sm.configs = configs
		sm.lastRequestId = lastRequestId
		sm.lastNum = lastNum
		sm.gid2shards = gid2shard
		sm.unlock("load snapshot with applyIndex: %d", sm.applyIndex)
	}
}
