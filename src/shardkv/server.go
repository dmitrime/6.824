package shardkv

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Op    string
	Key   string
	Value string

	OpNum   int64
	ClerkId int64

	Shard    int
	Store    map[string]string
	Commited map[int64]int64

	Config     *shardmaster.Config
	ConfigNum  int
	ShardReady bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	dead         int32 // set by Kill()
	raft         *raft.Raft
	applyCh      chan raft.ApplyMsg
	shutdownCh   chan interface{}
	make_end     func(string) *labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	gid            int
	sm             *shardmaster.Clerk
	config         shardmaster.Config
	shardConfigNum [shardmaster.NShards]int  // shard is being transferred
	isShardReady   [shardmaster.NShards]bool // can use this shard
	isShardMoving  [shardmaster.NShards]bool

	cond        *sync.Cond                             // condition used for waiting on raft
	inFlight    int                                    // number of messages this server is processing
	store       [shardmaster.NShards]map[string]string // key-value store
	commited    [shardmaster.NShards]map[int64]int64   // for deduplication
	lastApplied []raft.ApplyMsg                        // last applied messages from raft
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) checkLeader(startTerm int) bool {
	newTerm, isLeader := kv.raft.GetState()
	return isLeader && newTerm == startTerm
}

func (kv *ShardKV) submit(op Op) bool {
	if _, isLeader := kv.raft.GetState(); !isLeader {
		return false
	}

	kv.inFlight += 1
	defer func() { kv.inFlight -= 1 }()

	index, startTerm, _ := kv.raft.Start(op)
	DPrintf("[%d][g=%d](shard=%d) Leader called with %s(%+v), promised index=%d, term=%d",
		kv.me, kv.gid, op.Shard, op.Op, op, index, startTerm)
	if op.Shard >= 0 {
		DPrintf("[%d][g=%d](shard=%d) Leader called with %s, map[shard]=%+v",
			kv.me, kv.gid, op.Shard, op.Op, kv.store[op.Shard])
	}

	for count := 1; ; count++ {
		// DPrintf("[%d] Gonna wait %d...%s(), buf=%d, index=%d, applied=%v", kv.me, count, op.Op, len(kv.lastApplied), index, kv.lastApplied)
		kv.cond.Wait()
		// DPrintf("Done wait %d...", count)

		if len(kv.lastApplied) > 0 && index == kv.lastApplied[0].CommandIndex {
			DPrintf("[%d][g=%d] Got our message, consuming index=%d", kv.me, kv.gid, index)
			kv.lastApplied = kv.lastApplied[1:]
			break
		}

		// use only last messages
		if kv.inFlight < len(kv.lastApplied) {
			n := len(kv.lastApplied) - kv.inFlight
			kv.lastApplied = kv.lastApplied[n:]
			DPrintf("[%d][g=%d] Removed last %d applied messages", kv.me, kv.gid, n)
		}

		if !kv.checkLeader(startTerm) || kv.killed() {
			DPrintf("[%d][g=%d] Leader changed after waiting for %s(%s:%s) to finish", kv.me, kv.gid, op.Op, op.Key, op.Value)
			return false
		}
	}
	// avoid bloking
	kv.cond.Broadcast()
	return true
}

func makeOp(
	op string,
	key string,
	value string,
	shard int,
	clerkId int64,
	opNum int64,
	store map[string]string,
	commited map[int64]int64,
	config *shardmaster.Config,
	configNum int,
	isShardReady bool,
) Op {
	return Op{
		Op:         op,
		Key:        key,
		Value:      value,
		Shard:      shard,
		ClerkId:    clerkId,
		OpNum:      opNum,
		Store:      store,
		Commited:   commited,
		Config:     config,
		ConfigNum:  configNum,
		ShardReady: isShardReady,
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	gid, shard := args.GID, args.Shard
	if err := kv.checkGidAndShard(gid, shard); err != OK {
		DPrintf("[%d][g=%d] [GET] Error(%s) serving shard(%d), configNum=%d, shards=%+v, configNums=%+v, shardReady=%+v",
			kv.me, kv.gid, err, shard, kv.config.Num, getShardsForGid(&kv.config, kv.gid), kv.shardConfigNum, kv.isShardReady)
		reply.Err = err
		return
	}

	op := makeOp(GET, args.Key, "", args.Shard, args.ClerkId, args.OpNum, nil, nil, nil, -1, false)
	if ok := kv.submit(op); !ok {
		reply.Err = ErrWrongLeader
		return
	}

	if err := kv.checkGidAndShard(gid, shard); err != OK {
		DPrintf("[%d][g=%d] [GET] Shard(%d) was moved while replicating %s(%s), our new shards=%+v",
			kv.me, kv.gid, shard, op.Op, op.Key, getShardsForGid(&kv.config, kv.gid))
		reply.Err = err
		return
	}

	if val, ok := kv.store[shard][op.Key]; ok {
		reply.Value = val
		reply.Err = OK
		DPrintf("[%d][g=%d] (shard=%d) Leader result for %s(key=%s)='%s'", kv.me, kv.gid, shard, op.Op, op.Key, reply.Value)
	} else {
		reply.Err = ErrNoKey
		DPrintf("[%d][g=%d] (shard=%d) Leader has no results for %s(key=%s)", kv.me, kv.gid, shard, op.Op, op.Key)
	}
	reply.Err = OK
	DPrintf("[%d][g=%d] (shard=%d) Get result: %+v", kv.me, kv.gid, shard, reply.Value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	gid, shard := args.GID, args.Shard
	if err := kv.checkGidAndShard(gid, shard); err != OK {
		DPrintf("[%d][g=%d] [PUT] Error(%s) serving shard(%d), configNum=%d, shards=%+v, configNums=%+v, shardReady=%+v",
			kv.me, kv.gid, err, shard, kv.config.Num, getShardsForGid(&kv.config, kv.gid), kv.shardConfigNum, kv.isShardReady)
		reply.Err = err
		return
	}

	op := makeOp(args.Op, args.Key, args.Value, args.Shard, args.ClerkId, args.OpNum, nil, nil, nil, -1, false)
	if ok := kv.submit(op); !ok {
		reply.Err = ErrWrongLeader
		return
	}

	if err := kv.checkGidAndShard(gid, shard); err != OK {
		DPrintf("[%d][g=%d] [PUT] Shard(%d) was moved while replicating %s(%s:%s), our new shards=%+v",
			kv.me, kv.gid, shard, op.Op, op.Key, op.Value, getShardsForGid(&kv.config, kv.gid))
		reply.Err = err
		return
	}

	reply.Err = OK
}

func (kv *ShardKV) checkGidAndShard(gid int, shard int) Err {
	shards := getShardsForGid(&kv.config, kv.gid)
	if kv.gid == gid {
		_, ok := shards[shard]
		if ok && kv.isShardReady[shard] {
			return OK
		} else if !ok {
			return ErrWrongGroup
		} else if !kv.isShardReady[shard] {
			return ErrShardReplicating
		}
	}
	return ErrWrongGroup
}

func (kv *ShardKV) makeSnapshot(lastIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.commited)
	e.Encode(kv.config)
	e.Encode(kv.shardConfigNum)
	e.Encode(kv.isShardReady)

	kv.raft.MakeSnapshot(w.Bytes(), lastIndex)
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	d.Decode(&kv.store)
	d.Decode(&kv.commited)
	d.Decode(&kv.config)
	d.Decode(&kv.shardConfigNum)
	d.Decode(&kv.isShardReady)

	DPrintf("[%d][g=%d] Loaded snapshot with config=%+v", kv.me, kv.gid, kv.config)
}

func (kv *ShardKV) isDuplicate(cmd *Op) bool {
	if val, ok := kv.commited[cmd.Shard][cmd.ClerkId]; ok && val == cmd.OpNum {
		// DPrintf("[%d] Clerk(%d)@%d sent duplicate %s(%+v)", kv.me, cmd.ClerkId%100, cmd.OpNum, cmd.Op, cmd)
		return true
	}
	return false
}

func (kv *ShardKV) applyCommand(cmd *Op) {
	shard := cmd.Shard
	if cmd.Op == MOVE {
		if cmd.ConfigNum > kv.shardConfigNum[shard] {
			kv.shardConfigNum[shard] = cmd.ConfigNum
			kv.store[shard] = copyStringMap(&cmd.Store)
			kv.commited[shard] = copyInt64Map(&cmd.Commited)
			kv.isShardReady[shard] = cmd.ShardReady
			DPrintf("[%d][g=%d] Applied shard(%d), configNum=%d, ShardReady=%t", kv.me, kv.gid, shard, cmd.ConfigNum, cmd.ShardReady)
		}
	} else if cmd.Op == CONFIG {
		kv.config = *cmd.Config

		if kv.config.Num == 1 {
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] == kv.gid {
					kv.shardConfigNum[shard] = 1
					kv.isShardReady[shard] = true
				}
			}
		}
		DPrintf("[%d][g=%d] Applied new config=%+v", kv.me, kv.gid, kv.config)
	} else if !kv.isDuplicate(cmd) {
		if cmd.Op == PUT {
			kv.store[shard][cmd.Key] = cmd.Value
		} else if cmd.Op == APPEND {
			DPrintf("[%d][g=%d] Appended to shard(%d): %s:%s + %s", kv.me, kv.gid, shard, cmd.Key, kv.store[shard][cmd.Key], cmd.Value)
			kv.store[shard][cmd.Key] += cmd.Value
		}
		// for duplicate detection
		kv.commited[shard][cmd.ClerkId] = cmd.OpNum
	}
}

func (kv *ShardKV) processAppliedCommands() {
	for msg := range kv.applyCh {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d][g=%d] ShardMaster.processAppliedCommands() shutting down...", kv.me, kv.gid)
			return
		default:
			if msg.CommandValid {
				// normal command
				kv.mu.Lock()
				cmd := msg.Command.(Op)
				kv.applyCommand(&cmd)

				if kv.maxraftstate != -1 && kv.maxraftstate <= kv.raft.GetRaftStateSize() {
					DPrintf("[%d][g=%d] Making snapshot, index=%d, mxRS=%d, stateSize=%d",
						kv.me, kv.gid, msg.CommandIndex, kv.maxraftstate, kv.raft.GetRaftStateSize())
					kv.makeSnapshot(msg.CommandIndex)
				}

				if _, isLeader := kv.raft.GetState(); isLeader {
					kv.lastApplied = append(kv.lastApplied, msg)
					DPrintf("[%d][g=%d] Broadcasting apply for %s@index=%d", kv.me, kv.gid, cmd.Op, msg.CommandIndex)
				}

				kv.mu.Unlock()
				kv.cond.Broadcast()
			} else if msg.Snapshot != nil && len(msg.Snapshot) > 0 {
				kv.mu.Lock()
				DPrintf("[%d][g=%d] Installing snapshot on KVServer", kv.me, kv.gid)
				kv.installSnapshot(msg.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) HandleInstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	if args.GID != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

	if _, isLeader := kv.raft.GetState(); !isLeader {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[%d][g=%d] Receiving(conf=%d) shard(%d) to our group(%d)", kv.me, kv.gid, kv.config.Num, args.Shard, args.GID)
	op := makeOp(MOVE, "", "", args.Shard, -1, -1, args.Store, args.Commited, nil, args.ConfigNum, true)
	if ok := kv.submit(op); !ok {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%d][g=%d] Done replicating new shard(%d) to group(%d)", kv.me, kv.gid, args.Shard, args.GID)
	reply.Err = OK
}

func copyStringMap(m *map[string]string) map[string]string {
	store := make(map[string]string)
	for k, v := range *m {
		store[k] = v
	}
	return store
}

func copyInt64Map(m *map[int64]int64) map[int64]int64 {
	commited := make(map[int64]int64)
	for k, v := range *m {
		commited[k] = v
	}
	return commited
}

func (kv *ShardKV) transferShard(shard int, group int) {
	if _, isLeader := kv.raft.GetState(); !isLeader {
		return
	}

	kv.mu.Lock()
	kv.isShardMoving[shard] = true
	args := InstallShardArgs{
		GID:       group,
		Shard:     shard,
		Store:     copyStringMap(&kv.store[shard]),
		Commited:  copyInt64Map(&kv.commited[shard]),
		ConfigNum: kv.config.Num,
	}
	servers := kv.config.Groups[group]
	kv.mu.Unlock()

	DPrintf("[%d][g=%d] Attempting to transfer shard(%d) to group(%d)...", kv.me, kv.gid, shard, group)
	success := false
	for s := 0; s < len(servers); s++ {
		srv := kv.make_end(servers[s])
		var reply InstallShardReply
		ok := srv.Call("ShardKV.HandleInstallShard", &args, &reply)
		if ok && reply.Err == OK {
			DPrintf("[%d][g=%d] Done transfer for shard(%d) to group(%d)", kv.me, kv.gid, shard, group)
			success = true
			break
		}
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer func() { kv.isShardMoving[shard] = false }()

	if !success {
		DPrintf("[%d][g=%d] FAILED to transfer for shard(%d) to group(%d)", kv.me, kv.gid, shard, group)
		return
	} else {
		op := makeOp(MOVE, "", "", args.Shard, -1, -1, make(map[string]string), make(map[int64]int64), nil, args.ConfigNum, false)
		if ok := kv.submit(op); !ok {
			return
		}
	}
}

func getShardsForGid(config *shardmaster.Config, gid int) map[int]int {
	shards := map[int]int{}
	for idx, group := range config.Shards {
		if group == gid {
			shards[idx] = gid
		}
	}
	return shards
}

func (kv *ShardKV) shardsToMove() map[int]int {
	move := map[int]int{}
	for shard := 0; shard < shardmaster.NShards; shard++ {
		if kv.config.Num > kv.shardConfigNum[shard] && kv.isShardReady[shard] && kv.config.Shards[shard] != kv.gid {
			move[shard] = kv.config.Shards[shard]
		}
	}
	if len(move) > 0 {
		DPrintf("[%d][g=%d] Found shards to move: %+v", kv.me, kv.gid, move)
	}
	return move
}

func (kv *ShardKV) reconfigure() {
	if _, isLeader := kv.raft.GetState(); !isLeader {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	latest := -1
	if kv.config.Num == 0 {
		latest = 1
	}

	conf := kv.sm.Query(latest)
	if conf.Num > kv.config.Num {
		DPrintf("[%d][g=%d] Replicating new config: %+v, old config: %+v", kv.me, kv.gid, conf, kv.config)

		// replicate new config
		op := makeOp(CONFIG, "", "", -1, -1, -1, nil, nil, &conf, -1, false)
		if ok := kv.submit(op); !ok {
			return
		}
	}

	// if needed, move shards from this group to others
	for shard, group := range kv.shardsToMove() {
		if !kv.isShardMoving[shard] {
			DPrintf("[%d][g=%d] Reconfigure(%d): moving shard(%d) to group(%d)", kv.me, kv.gid, kv.config.Num, shard, group)
			go kv.transferShard(shard, group)
		}
	}
}

func (kv *ShardKV) refreshConfig() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d][g=%d] ShardKV.refreshConfig() shutting down...", kv.me, kv.gid)
			return
		default:
			// periodically poll the shardmaster for new config
			time.Sleep(100 * time.Millisecond)
			kv.reconfigure()
		}
	}
}

func (kv *ShardKV) refreshBroadcast() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d][g=%d] ShardKV.refreshBroadcast() shutting down...", kv.me, kv.gid)
			return
		default:
			// periodically wake up server waiting on cond to unblock client calls
			time.Sleep(250 * time.Millisecond)
			kv.cond.Broadcast()
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.raft.Kill()
	close(kv.shutdownCh)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.shutdownCh = make(chan interface{})
	kv.raft = raft.Make(servers, me, persister, kv.applyCh)
	kv.cond = sync.NewCond(&kv.mu)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.store[i] = make(map[string]string)
		kv.commited[i] = make(map[int64]int64)
	}
	kv.sm = shardmaster.MakeClerk(masters)
	kv.config = shardmaster.Config{Num: 0}

	go kv.processAppliedCommands()
	go kv.refreshBroadcast()
	go kv.refreshConfig()

	return kv
}
