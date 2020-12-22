package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrDuplicate   Err = "ErrDuplicate"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId   int64
	RequestId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId   int64
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	ShardNum int
	Shard    map[string]string
}

type GetShardReply struct {
	Err Err
}

func shardCopy(data map[string]string) map[string]string {
	r := make(map[string]string)
	for k, v := range data {
		r[k] = v
	}
	return r
}
