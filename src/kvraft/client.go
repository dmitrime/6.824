package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int
	id      int64
	counter int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers: servers,
		leader:  0,
		id:      nrand(),
		counter: 0,
	}
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
	DPrintf("Clerk(%d) started Get(key=%s)\n", ck.id%100, key)
	defer DPrintf("Clerk(%d) finished Get(key=%s)\n", ck.id%100, key)

	ck.counter += 1
	var value string
	peer := ck.leader
	args := GetArgs{Key: key, OpNum: ck.counter, ClerkId: ck.id}
	reply := GetReply{}
	for quit, timeout := false, time.After(10*time.Second); !quit; {
		select {
		case <-timeout:
			DPrintf("Clerk(%d)@%d timing out for Get(key=%s)", ck.id%100, ck.counter, key)
			quit = true
		default:
			// DPrintf("Clerk(%d)@%d: starting Get(key=%s) to peer=%d", ck.id%100, ck.counter, key, peer)
			ok := ck.servers[peer].Call("KVServer.Get", &args, &reply)
			if !ok {
				// DPrintf("Clerk(%d)@%d failed connecting to server %d", ck.id%100, ck.counter, peer)
			} else {
				if reply.Err == OK {
					ck.leader = peer
					value = reply.Value
					DPrintf("Clerk(%d)@%d: finished Get(key=%s) to peer=%d", ck.id%100, ck.counter, key, peer)
					quit = true
				} else if reply.Err == ErrNoKey {
					DPrintf("Key not found: %s", args.Key)
					quit = true
				} else if reply.Err != ErrWrongLeader {
					DPrintf("Unknown error: %s", reply.Err)
					quit = true
				}
			}
			peer = (peer + 1) % len(ck.servers)
			// DPrintf("Clerk(%d)@%d for Get(key=%s) moving on to peer=%d because err=%s", ck.id%100, ck.counter, key, peer, reply.Err)
			time.Sleep(time.Duration(5) * time.Millisecond)
		}
	}
	return value
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
	DPrintf("Clerk(%d) started %s(key=%s,val='%s')\n", ck.id%100, op, key, value)
	defer DPrintf("Clerk(%d) finished %s(key=%s,val='%s')\n", ck.id%100, op, key, value)

	ck.counter += 1

	peer := ck.leader
	args := PutAppendArgs{Key: key, Value: value, Op: op, OpNum: ck.counter, ClerkId: ck.id}
	reply := PutAppendReply{}

	for quit, timeout := false, time.After(10*time.Second); !quit; {
		select {
		case <-timeout:
			DPrintf("Clerk(%d)@%d timing out for %s(key=%s)", ck.id%100, ck.counter, op, key)
			quit = true
		default:
			// DPrintf("Clerk(%d)@%d: starting %s(key=%s) to peer=%d", ck.id%100, ck.counter, op, key, peer)
			ok := ck.servers[peer].Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				// DPrintf("Clerk(%d)@%d failed connecting to server %d", ck.id%100, ck.counter, peer)
			} else {
				if reply.Err == OK {
					ck.leader = peer
					quit = true
				} else if reply.Err == ErrDuplicate {
					DPrintf("Got duplicate for key %s, server %d", args.Key, peer)
					quit = true
				} else if reply.Err != ErrWrongLeader {
					DPrintf("Unknown error: %s", reply.Err)
					quit = true
				}
			}
			peer = (peer + 1) % len(ck.servers)
			// DPrintf("Clerk(%d)@%d for %s(key=%s) moving on to peer=%d because err=%s", ck.id%100, ck.counter, op, key, peer, reply.Err)
			time.Sleep(time.Duration(5) * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
