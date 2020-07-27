package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Op      string
	Key     string
	Value   string
	OpNum   int64
	ClerkId int64
}

type KVServer struct {
	mu         sync.Mutex
	me         int
	peers      int
	raft       *raft.Raft
	applyCh    chan raft.ApplyMsg
	dead       int32 // set by Kill()
	shutdownCh chan interface{}

	maxraftstate int // snapshot if log grows this big

	cond        *sync.Cond        // condition used for waiting on raft
	store       map[string]string // key-value store
	inFlight    int               // number of messages this server is processing
	commited    map[int64]int64   // for deduplication
	lastApplied []raft.ApplyMsg   // last applied messages from raft
}

func (kv *KVServer) checkLeader(startTerm int) bool {
	newTerm, isLeader := kv.raft.GetState()
	return isLeader && newTerm == startTerm
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.raft.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.inFlight += 1
	defer func() { kv.inFlight -= 1 }()

	op := Op{Op: GET, Key: args.Key, ClerkId: args.ClerkId, OpNum: args.OpNum}
	index, startTerm, isLeader := kv.raft.Start(op)

	DPrintf("[%d] Leader called with %s(key=%s), promised index=%d, term=%d\n", kv.me, op.Op, op.Key, index, startTerm)

	for count := 1; ; count++ {
		DPrintf("[%d] Gonna wait %d...%s(key=%s), buf=%d, inFlight=%d, index=%d, applied=%v",
			kv.me, count, GET, op.Key, len(kv.lastApplied), kv.inFlight, index, kv.lastApplied)
		kv.cond.Wait()
		DPrintf("Done wait %d...", count)

		if len(kv.lastApplied) > 0 && index == kv.lastApplied[0].CommandIndex {
			DPrintf("Got our message, consuming index=%d", index)
			kv.lastApplied = kv.lastApplied[1:]
			break
		}

		if !kv.checkLeader(startTerm) {
			DPrintf("[%d] Leader changed after waiting for %s(key=%s) to finish", kv.me, op.Op, op.Key)
			reply.Err = ErrWrongLeader
			return
		}

		// use only last messages
		if kv.inFlight < len(kv.lastApplied) {
			n := len(kv.lastApplied) - kv.inFlight
			kv.lastApplied = kv.lastApplied[n:]
			DPrintf("[%d] Removed last %d applied messages", kv.me, n)
		}

		if kv.killed() {
			DPrintf("KVServer[%d] killed while waiting for %s(key=%s) to finish", kv.me, op.Op, op.Key)
			reply.Err = ErrKilled
			return
		}
	}

	if val, ok := kv.store[op.Key]; ok {
		reply.Value = val
		reply.Err = OK
		DPrintf("[%d] Leader result for %s(key=%s)='%s'", kv.me, op.Op, op.Key, reply.Value)
	} else {
		reply.Err = ErrNoKey
		DPrintf("[%d] Leader has no results for %s(key=%s)", kv.me, op.Op, op.Key)
	}
	// avoid bloking
	kv.cond.Broadcast()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.raft.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.inFlight += 1
	defer func() { kv.inFlight -= 1 }()

	op := Op{Op: args.Op, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, OpNum: args.OpNum}
	index, startTerm, isLeader := kv.raft.Start(op)

	DPrintf("[%d] Leader called with %s(key=%s, val=%s, c=%d, n=%d), promised index=%d, term=%d\n",
		kv.me, op.Op, op.Key, op.Value, args.ClerkId%100, args.OpNum, index, startTerm)

	for count := 1; ; count++ {
		DPrintf("Gonna wait %d...%s(key=%s), buf=%d, index=%d", count, op.Op, op.Key, len(kv.lastApplied), index)
		kv.cond.Wait()
		DPrintf("Done wait %d...%s(key=%s)", count, op.Op, op.Key)

		if len(kv.lastApplied) > 0 && index == kv.lastApplied[0].CommandIndex {
			DPrintf("Got our message, consuming index=%d", index)
			kv.lastApplied = kv.lastApplied[1:]
			break
		}

		if !kv.checkLeader(startTerm) {
			DPrintf("[%d] Leader changed after waiting for %s(key=%s) to finish", kv.me, op.Op, op.Key)
			reply.Err = ErrWrongLeader
			return
		}

		// use only last messages
		if kv.inFlight < len(kv.lastApplied) {
			n := len(kv.lastApplied) - kv.inFlight
			kv.lastApplied = kv.lastApplied[n:]
			DPrintf("[%d] Removed last %d applied messages", kv.me, n)
		}

		if kv.killed() {
			DPrintf("KVServer[%d] killed while waiting for %s(key=%s) to finish", kv.me, op.Op, op.Key)
			reply.Err = ErrKilled
			return
		}
	}
	reply.Err = OK
	// avoid bloking
	kv.cond.Broadcast()
}

func (kv *KVServer) isDuplicate(cmd Op) bool {
	if val, ok := kv.commited[cmd.ClerkId]; ok && val == cmd.OpNum {
		DPrintf("[%d] Clerk(%d)@%d sent duplicate %s(key=%s)", kv.me, cmd.ClerkId%100, cmd.OpNum, cmd.Op, cmd.Key)
		return true
	}
	return false
}

func (kv *KVServer) applyCommand(cmd Op) {
	if !kv.isDuplicate(cmd) {
		if cmd.Op == PUT {
			kv.store[cmd.Key] = cmd.Value
		} else if cmd.Op == APPEND {
			kv.store[cmd.Key] += cmd.Value
		}
		// for duplicate detection
		kv.commited[cmd.ClerkId] = cmd.OpNum
	}
	// DPrintf("[%d] Server applied %s(key=%s, val=%s)\n", kv.me, cmd.Op, cmd.Key, cmd.Value)
}

func (kv *KVServer) makeSnapshot(lastIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.commited)

	kv.raft.MakeSnapshot(w.Bytes(), lastIndex)
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	DPrintf("[%d] Installing snapshot on KVServer", kv.me)

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var store map[string]string
	var commited map[int64]int64
	if d.Decode(&store) != nil || d.Decode(&commited) != nil {
		DPrintf("[%d] Error decoding snapshot...\n", kv.me)
	} else {
		kv.store = store
		kv.commited = commited
		DPrintf("[%d] Loaded snapshot with len(store)=%d, len(commited)=%d", kv.me, len(store), len(commited))
	}
}

func (kv *KVServer) processAppliedCommands() {
	DPrintf("[%d] Starting up...", kv.me)
	for msg := range kv.applyCh {
		select {
		case <-kv.shutdownCh:
			DPrintf("KVServer[%d].processAppliedCommands() shutting down...", kv.me)
			return
		default:
			if msg.CommandValid {
				// normal command
				kv.mu.Lock()
				cmd := msg.Command.(Op)
				kv.applyCommand(cmd)

				if kv.maxraftstate != -1 && kv.maxraftstate <= kv.raft.GetRaftStateSize() {
					DPrintf("[%d] Making snapshot, index=%d, mxRS=%d, stateSize=%d",
						kv.me, msg.CommandIndex, kv.maxraftstate, kv.raft.GetRaftStateSize())
					kv.makeSnapshot(msg.CommandIndex)
				}

				if _, isLeader := kv.raft.GetState(); isLeader {
					kv.lastApplied = append(kv.lastApplied, msg)
					DPrintf("[%d] Broadcasting apply for %v, index=%d", kv.me, cmd, msg.CommandIndex)
				}

				kv.mu.Unlock()
				kv.cond.Broadcast()
			} else {
				// install snapshot
				kv.mu.Lock()
				kv.installSnapshot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) refreshBroadcast() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("KVServer[%d].refreshBroadcast() shutting down...", kv.me)
			return
		default:
			// periodically wake up server waiting on cond to unblock client calls
			time.Sleep(250 * time.Millisecond)
			kv.cond.Broadcast()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set raft.dead (without needing a lock),
// and a killed() method to test raft.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.raft.Kill()

	close(kv.shutdownCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:           me,
		peers:        len(servers),
		maxraftstate: maxraftstate,
		applyCh:      applyCh,
		shutdownCh:   make(chan interface{}),
		raft:         raft.Make(servers, me, persister, applyCh),
		store:        make(map[string]string),
		commited:     make(map[int64]int64),
	}
	kv.cond = sync.NewCond(&kv.mu)

	go kv.processAppliedCommands()
	go kv.refreshBroadcast()

	return kv
}
