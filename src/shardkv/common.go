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
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrShardReplicating = "ErrShardReplicating"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
	MOVE   = "Move"
	CONFIG = "Config"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Op    string // "Put" or "Append"
	Key   string
	Value string

	Shard int
	GID   int

	OpNum   int64
	ClerkId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	Shard int
	GID   int

	OpNum   int64
	ClerkId int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Install shard
type InstallShardArgs struct {
	Shard     int
	GID       int
	Store     map[string]string
	Commited  map[int64]int64
	ConfigNum int
}

type InstallShardReply struct {
	Err Err
}
