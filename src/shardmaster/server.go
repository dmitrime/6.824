package shardmaster

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	dead    int32
	raft    *raft.Raft
	applyCh chan raft.ApplyMsg

	shutdownCh  chan interface{}
	cond        *sync.Cond
	lastApplied []raft.ApplyMsg
	commited    map[int64]int64
	inFlight    int

	configs []Config // indexed by config num
}

type Op struct {
	Op string

	Servers map[int][]string // used for Join
	GIDs    []int            // used for Leave
	Shard   int              // used for Move
	GID     int              // used for Move
	Num     int              // used for Query

	OpNum   int64
	ClerkId int64
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) checkLeader(startTerm int) bool {
	newTerm, isLeader := sm.raft.GetState()
	return isLeader && newTerm == startTerm
}

func (sm *ShardMaster) submit(op Op) (bool, Err) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	_, isLeader := sm.raft.GetState()
	if !isLeader {
		return true, "WrongLeader"
	}

	sm.inFlight += 1
	defer func() { sm.inFlight -= 1 }()

	index, startTerm, isLeader := sm.raft.Start(op)
	DPrintf("[%d] Leader called with %s(%+v), promised index=%d, term=%d\n", sm.me, op.Op, op, index, startTerm)

	for count := 1; ; count++ {
		// DPrintf("[%d] Gonna wait %d...%s(), buf=%d, index=%d, applied=%v", sm.me, count, op.Op, len(sm.lastApplied), index, sm.lastApplied)
		sm.cond.Wait()
		// DPrintf("Done wait %d...", count)

		if len(sm.lastApplied) > 0 && index == sm.lastApplied[0].CommandIndex {
			DPrintf("Got our message, consuming index=%d", index)
			sm.lastApplied = sm.lastApplied[1:]
			break
		}

		// use only last messages
		if sm.inFlight < len(sm.lastApplied) {
			n := len(sm.lastApplied) - sm.inFlight
			sm.lastApplied = sm.lastApplied[n:]
			DPrintf("[%d] Removed last %d applied messages", sm.me, n)
		}

		if !sm.checkLeader(startTerm) || sm.killed() {
			DPrintf("[%d] Leader changed after waiting for %s() to finish", sm.me, op.Op)
			return true, "WrongLeader"
		}
	}
	// avoid bloking
	sm.cond.Broadcast()
	return false, ""
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{Op: JOIN, Servers: args.Servers, ClerkId: args.ClerkId, OpNum: args.OpNum}

	if wrongLeader, err := sm.submit(op); wrongLeader || err != "" {
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{Op: LEAVE, GIDs: args.GIDs, ClerkId: args.ClerkId, OpNum: args.OpNum}

	if wrongLeader, err := sm.submit(op); wrongLeader || err != "" {
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{Op: MOVE, Shard: args.Shard, GID: args.GID, ClerkId: args.ClerkId, OpNum: args.OpNum}

	if wrongLeader, err := sm.submit(op); wrongLeader || err != "" {
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{Op: QUERY, Num: args.Num, ClerkId: args.ClerkId, OpNum: args.OpNum}

	if wrongLeader, err := sm.submit(op); wrongLeader || err != "" {
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}

	sm.mu.Lock()
	if op.Num < 0 || op.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[op.Num]
	}
	DPrintf("[%d] Query result: %+v", sm.me, reply.Config)
	reply.Err = OK
	sm.mu.Unlock()
}

func (sm *ShardMaster) isDuplicate(cmd *Op) bool {
	if val, ok := sm.commited[cmd.ClerkId]; ok && val == cmd.OpNum {
		DPrintf("[%d] Clerk(%d)@%d sent duplicate %s(%+v)", sm.me, cmd.ClerkId%100, cmd.OpNum, cmd.Op, cmd)
		return true
	}
	return false
}

func rebalanceShards(groups map[int][]string) [NShards]int {
	keys := make([]int, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	shards := [NShards]int{}
	if len(keys) > 0 {
		for i := 0; i < NShards; i++ {
			shards[i] = keys[i%len(keys)]
		}
	}
	return shards
}

func (sm *ShardMaster) applyLeave(cmd *Op) {
	skip := map[int]bool{} // skip those in GIDs
	for i := 0; i < len(cmd.GIDs); i++ {
		skip[cmd.GIDs[i]] = true
	}
	groups := map[int][]string{}
	for k, v := range sm.configs[len(sm.configs)-1].Groups {
		if _, ok := skip[k]; !ok {
			groups[k] = v
		}
	}
	conf := Config{Num: len(sm.configs), Shards: rebalanceShards(groups), Groups: groups}
	sm.configs = append(sm.configs, conf)
	// DPrintf("[%d] Added new config-%d after LEAVE: %+v", sm.me, len(sm.configs), conf)
}

func (sm *ShardMaster) applyJoin(cmd *Op) {
	groups := map[int][]string{}
	for k, v := range sm.configs[len(sm.configs)-1].Groups {
		groups[k] = v
	}
	for k, v := range cmd.Servers {
		groups[k] = v
	}
	conf := Config{Num: len(sm.configs), Shards: rebalanceShards(groups), Groups: groups}
	sm.configs = append(sm.configs, conf)
	// DPrintf("[%d] Added new config-%d after JOIN: %+v", sm.me, len(sm.configs), conf)
}

func (sm *ShardMaster) applyMove(cmd *Op) {
	prev := sm.configs[len(sm.configs)-1]

	groups := map[int][]string{}
	for k, v := range prev.Groups {
		groups[k] = v
	}

	shards := [NShards]int{}
	for i := 0; i < NShards; i++ {
		shards[i] = prev.Shards[i]
	}
	// move this shard
	shards[cmd.Shard] = cmd.GID

	conf := Config{Num: len(sm.configs), Shards: shards, Groups: groups}
	sm.configs = append(sm.configs, conf)
}

func (sm *ShardMaster) applyCommand(cmd *Op) {
	if !sm.isDuplicate(cmd) {
		if cmd.Op == JOIN {
			sm.applyJoin(cmd)
		} else if cmd.Op == LEAVE {
			sm.applyLeave(cmd)
		} else if cmd.Op == MOVE {
			sm.applyMove(cmd)
		}
		// for duplicate detection
		sm.commited[cmd.ClerkId] = cmd.OpNum
	}
}

func (sm *ShardMaster) processAppliedCommands() {
	for msg := range sm.applyCh {
		select {
		case <-sm.shutdownCh:
			DPrintf("ShardMaster[%d].processAppliedCommands() shutting down...", sm.me)
			return
		default:
			if msg.CommandValid {
				// normal command
				sm.mu.Lock()
				cmd := msg.Command.(Op)
				sm.applyCommand(&cmd)

				if _, isLeader := sm.raft.GetState(); isLeader {
					sm.lastApplied = append(sm.lastApplied, msg)
					DPrintf("[%d] Broadcasting apply for %s@index=%d", sm.me, cmd.Op, msg.CommandIndex)
				}

				sm.mu.Unlock()
				sm.cond.Broadcast()
			}
		}
	}
}

func (sm *ShardMaster) refreshBroadcast() {
	for {
		select {
		case <-sm.shutdownCh:
			DPrintf("ShardMaster[%d].refreshBroadcast() shutting down...", sm.me)
			return
		default:
			// periodically wake up server waiting on cond to unblock client calls
			time.Sleep(250 * time.Millisecond)
			sm.cond.Broadcast()
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.raft.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.raft
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)
	sm := &ShardMaster{
		me:         me,
		configs:    make([]Config, 1),
		applyCh:    applyCh,
		raft:       raft.Make(servers, me, persister, applyCh),
		shutdownCh: make(chan interface{}),
		commited:   make(map[int64]int64),
	}
	sm.configs[0].Groups = map[int][]string{}
	sm.cond = sync.NewCond(&sm.mu)

	go sm.processAppliedCommands()
	go sm.refreshBroadcast()

	return sm
}
