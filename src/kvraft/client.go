package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
	logrus "github.com/sirupsen/logrus"
)

var kvlogLevel = logrus.InfoLevel
var rpcTimeout = 500

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId    int64
	lastLeader int
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
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.lastLeader = 0
	// time.Sleep(2 * time.Second) // wait for leader election
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		RequestId: nrand(),
	}
	reply := GetReply{}

	for i := ck.lastLeader; ; i = (i + 1) % len(ck.servers) {
		ok := ck.getTimeout(i, &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.lastLeader = i
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.lastLeader = i
				return ""
			}
		}
	}
}

func (ck *Clerk) getTimeout(kvserver int, args *GetArgs, reply *GetReply) bool {
	ch := make(chan bool, 1)
	rpcTimer := time.NewTimer(time.Duration(rpcTimeout) * time.Millisecond)
	go func() {
		ok := ck.servers[kvserver].Call("KVServer.Get", args, reply)
		ch <- ok
		close(ch)
	}()
	select {
	case <-rpcTimer.C:
		return false
	case ok := <-ch:
		return ok
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Op:        op,
		Key:       key,
		Value:     value,
		ClerkId:   ck.clerkId,
		RequestId: nrand(),
	}
	reply := PutAppendReply{}

	for i := ck.lastLeader; ; i = (i + 1) % len(ck.servers) {
		ok := ck.putAppendTimeout(i, &args, &reply)
		if ok && reply.Err == OK {
			ck.lastLeader = i
			return
		}
	}
}

func (ck *Clerk) putAppendTimeout(kvserver int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ch := make(chan bool, 1)
	rpcTimer := time.NewTimer(time.Duration(rpcTimeout) * time.Millisecond)
	go func() {
		ok := ck.servers[kvserver].Call("KVServer.PutAppend", args, reply)
		ch <- ok
		close(ch)
	}()

	select {
	case <-rpcTimer.C:
		return false
	case ok := <-ch:
		return ok
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
