package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func equalShards(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	as := make(map[int]bool)
	for _, v := range a {
		as[v] = true
	}

	bs := make(map[int]bool)
	for _, v := range b {
		bs[v] = true
	}

	for k, _ := range as {
		_, ok := bs[k]
		if !ok {
			return false
		}
		delete(bs, k)
	}

	for k, _ := range bs {
		_, ok := as[k]
		if !ok {
			return false
		}
	}
	return true
}

func deleteShard(shards []int, shard int) []int {
	index := -1
	for i, v := range shards {
		if v == shard {
			index = i
		}
	}
	if index == -1 {
		return shards
	}
	l := len(shards)
	shards[l-1], shards[index] = shards[index], shards[l-1]
	shards = shards[:l-1]
	return shards
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func CopyConfig(c Config) Config {
	r := c
	r.Groups = make(map[int][]string)
	for k, v := range c.Groups {
		r.Groups[k] = v
	}
	return r
}

type Err string

const (
	OK          Err = "OK"
	InvalidNum  Err = "InvalidNum"
	WrongLeader Err = "WrongLeader"
)

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClerkId   int64
	RequestId int64
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs      []int
	ClerkId   int64
	RequestId int64
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClerkId   int64
	RequestId int64
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClerkId   int64
	RequestId int64
}

type QueryReply struct {
	Err    Err
	Config Config
}
