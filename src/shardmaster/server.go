package shardmaster

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const debug = 1
const checkLeaderPeriod = 50
const rpcTimeout = 500

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
}

func (sm *ShardMaster) DPrintf(msg string, f ...interface{}) {
	DPrintf("[sm"+string(sm.me)+"]"+msg, f...)
}

func (sm *ShardMaster) lock(msg string, f ...interface{}) {
	sm.mu.Lock()
	DPrintf(msg, f...)
}

func (sm *ShardMaster) unlock(msg string, f ...interface{}) {
	sm.mu.Unlock()
	DPrintf(msg, f...)
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

func (sm *ShardMaster) isDuplicate(clerkId, requestId int64) bool {
	duplicate := false
	sm.lock("[sm %d] checks whether the request %d by %d is duplicate", sm.me, requestId, clerkId)
	if sm.lastRequestId[clerkId] == requestId {
		duplicate = true
	}
	sm.unlock("[sm %d] finished check duplication", sm.me)
	return duplicate
}

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
	reply.WrongLeader = true
	reply.Err = "wrong shardmaster leader; try another!\n"
	if isLeader == false {
		return
	}

	DPrintf("[sm %d] issues %s at index %d", sm.me, op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		currentTerm, isleader := sm.rf.GetState()
		if !(term == currentTerm && isleader) {
			return
		}
		if sm.applyIndex >= index {
			reply.Err = OK
			reply.WrongLeader = false
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
	index, term, isLeader := sm.rf.Start(op)
	reply.WrongLeader = true
	reply.Err = "wrong shardmaster leader; try another!\n"
	if isLeader == false {
		return
	}

	DPrintf("[sm %d] issues %s at index %d", sm.me, op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		currentTerm, isleader := sm.rf.GetState()
		if !(term == currentTerm && isleader) {
			return
		}
		if sm.applyIndex >= index {
			reply.Err = OK
			reply.WrongLeader = false
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
	index, term, isLeader := sm.rf.Start(op)
	reply.WrongLeader = true
	reply.Err = "wrong shardmaster leader; try another!\n"
	if isLeader == false {
		return
	}

	DPrintf("[sm %d] issues %s at index %d", sm.me, op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		currentTerm, isleader := sm.rf.GetState()
		if !(term == currentTerm && isleader) {
			return
		}
		if sm.applyIndex >= index {
			reply.Err = OK
			reply.WrongLeader = false
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
	index, term, isLeader := sm.rf.Start(op)
	reply.WrongLeader = true
	reply.Err = "wrong shardmaster leader; try another!\n"
	if isLeader == false {
		return
	}

	DPrintf("[sm %d] issues %s at index %d", sm.me, op2string(op), index)

	period := time.Duration(checkLeaderPeriod) * time.Millisecond
	for iter := 0; iter < rpcTimeout/checkLeaderPeriod; iter++ {
		time.Sleep(period)
		currentTerm, isleader := sm.rf.GetState()
		if !(term == currentTerm && isleader) {
			return
		}
		if sm.applyIndex >= index {
			sm.lock("[sm %d] is reading config %d", sm.me, args.Num)
			if args.Num < len(sm.configs) && args.Num > 0 {
				reply.Config = sm.configs[args.Num]
			} else if args.Num == -1 {
				reply.Config = sm.configs[sm.lastNum]
			} else {
				reply.Config = Config{}
				reply.Err = "Invalid configuration number"
			}
			sm.unlock("[sm %d] finished reading config %d", sm.me, args.Num)
			reply.Err = OK
			reply.WrongLeader = false
			return
		}
	}
}

func (sm *ShardMaster) checkApplyMsg() {
	for {
		applyMsg := <-sm.applyCh
		if applyMsg.CommandValid {
			if sm.applyIndex+1 != applyMsg.CommandIndex {
				DPrintf("[sm %d] application not in order! expected: %d, given: %d", sm.me, sm.applyIndex+1, applyMsg.CommandIndex)
				panic("application not in order")
			}
			op := applyMsg.Command.(Op)
			sm.lock("[sm %d] starts to apply op %s", sm.me, op2string(op))
			if sm.lastRequestId[op.ClerkId] == op.RequestId {
				DPrintf("[sm %d] duplicate request %d by %d", sm.me, op.RequestId, op.ClerkId)
			} else {
				sm.applyOp(op)
				sm.lastRequestId[op.ClerkId] = op.RequestId
				sm.applyIndex++
			}
			sm.unlock("[sm %d] ends applying op %s", sm.me, op2string(op))
		} else {
			// reserved for snapshots!
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
	if op.Type == query {
		return
	}
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
			DPrintf("[sm %d] inconsistent shards %v and gid2shards %v", sm.me, c.Shards, sm.gid2shards)
			panic("inconsistent shards and gid2shards")
		}
	}

	switch op.Type {
	case join:
		DPrintf("[sm %d] before join %v; %v, %v, %v", sm.me, op.Servers, len(c.Groups), sm.gid2shards, c.Shards)
		c = sm.rebalanceJoin(c, op.Servers) // in-place modification
		DPrintf("[sm %d] after join %v; %v, %v,%v", sm.me, op.Servers, len(c.Groups), sm.gid2shards, c.Shards)
	case leave:
		DPrintf("[sm %d] before leave %v: %v, %v, %v", sm.me, op.GIDs, len(c.Groups), sm.gid2shards, c.Shards)
		c = sm.rebalanceLeave(c, op.GIDs) // in-place modification
		DPrintf("[sm %d] after leave %v: %v, %v, %v", sm.me, op.GIDs, len(c.Groups), sm.gid2shards, c.Shards)
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
		sm.DPrintf("add %v from %d to pool", sm.gid2shards[gid], gid)
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
			sm.DPrintf("%d takes %v from pool", gid, pool[:thisTake])
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

	// a function for receiving notifications from the applyCh
	go sm.checkApplyMsg()
	return sm
}
