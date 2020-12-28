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
	ErrLagConfig   Err = "ErrLagConfig"
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

type PullArgs struct {
	Ver   int
	Shard int
	From  int
}

type ShardInfo struct {
	Data          map[string]string
	LastRequestId map[int64]map[int64]bool
}

type PullReply struct {
	Data ShardInfo
	Err  Err
}

func shardCopy(data map[string]string) map[string]string {
	r := make(map[string]string)
	for k, v := range data {
		r[k] = v
	}
	return r
}

func mapKeys(data map[int]map[string]string) []int {
	keys := make([]int, len(data))
	i := 0
	for k := range data {
		keys[i] = k
		i++
	}
	return keys
}
