package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"

	"../labrpc"
	"../shardmaster"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	clerkId    int64
	lastLeader map[int]int
	mu         sync.Mutex // for sync lastLeader
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.config = ck.sm.Query(-1)
	ck.lastLeader = make(map[int]int)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		ClerkId:   ck.clerkId,
		RequestId: nrand(),
	}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		ck.mu.Lock()
		if _, ok := ck.lastLeader[gid]; !ok {
			ck.lastLeader[gid] = 0
		}
		si := ck.lastLeader[gid]
		ck.mu.Unlock()
		log.Printf("[ck %d][get] sees key %s (shard %d) belong to groupd %d with config %d, %v", ck.clerkId, key, shard, gid, ck.config.Num, ck.config.Shards)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			num := len(servers)
			for j := 0; j < num; {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.mu.Lock()
					ck.lastLeader[gid] = si
					ck.mu.Unlock()
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					ck.mu.Lock()
					ck.lastLeader[gid] = si
					ck.mu.Unlock()
					log.Printf("[ck %d][get] errwronggroup: shard %d for group %d", ck.clerkId, shard, gid)
					break
				}
				j++
				si = (si + 1) % num
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		ClerkId:   ck.clerkId,
		RequestId: nrand(),
	}
	args.Key = key
	args.Value = value
	args.Op = op

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		var si int
		ck.mu.Lock()
		if _, ok := ck.lastLeader[gid]; !ok {
			ck.lastLeader[gid] = 0
			si = ck.lastLeader[gid]
		}
		ck.mu.Unlock()
		log.Printf("[ck %d][pa] sees key %s (shard %d) belong to groupd %d with config %d, %v", ck.clerkId, key, shard, gid, ck.config.Num, ck.config.Shards)
		var nextConfig bool
		if servers, ok := ck.config.Groups[gid]; ok {
			num := len(servers)
			for j := 0; j < num; {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				args.Ver = ck.config.Num
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok {
					switch reply.Err {
					case OK:
						ck.mu.Lock()
						ck.lastLeader[gid] = si
						ck.mu.Unlock()
						log.Printf("[ck %d][pa] finish %s %s %s at %d, %s", ck.clerkId, op, key, value, gid, servers[si])
						return
					case ErrDuplicate:
						ck.mu.Lock()
						ck.lastLeader[gid] = si
						ck.mu.Unlock()
						log.Printf("[ck %d][pa] %s %s %s duplicate request at %d, %s", ck.clerkId, op, key, value, gid, servers[si])
						return
					case ErrWrongGroup:
						nextConfig = true
						log.Printf("[ck %d][pa] %s %s %s wrong group at %d, %s", ck.clerkId, op, key, value, gid, servers[si])
						break
					case ErrLagConfig:
						time.Sleep(time.Millisecond * waitLagReplicaTime)
					default:
						j++
						si = (si + 1) % num
						log.Printf("[ck %d][pa] %s %s %s err: %v at %d, %s", ck.clerkId, op, key, value, reply.Err, gid, servers[si])
					}
					if nextConfig {
						break
					}
				} else {
					j++
					si = (si + 1) % num
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
