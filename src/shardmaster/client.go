package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"../labrpc"
)

const (
	queryRetryTime int = 100
	waitTime           = 20
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clerkId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clerkId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.Num = num
	args.RequestId = nrand()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok {
				if reply.Err == OK {
					return reply.Config
				} else if reply.Err == InvalidNum {
					log.Printf("[ck %d] invalid config num: %d", ck.clerkId, num)
					time.Sleep(time.Millisecond * time.Duration(waitTime))
				}
			}
		}
		time.Sleep(time.Duration(queryRetryTime) * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClerkId = ck.clerkId
	args.RequestId = nrand()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok {
				if reply.Err == OK {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClerkId = ck.clerkId
	args.RequestId = nrand()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok {
				if reply.Err == OK {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClerkId = ck.clerkId
	args.RequestId = nrand()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok {
				if reply.Err == OK {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
