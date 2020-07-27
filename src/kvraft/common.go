package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrBadOp       = "ErrBadOp"
	ErrDuplicate   = "ErrDuplicate"
	ErrKilled      = "ErrKilled"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	OpNum   int64
	ClerkId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	OpNum   int64
	ClerkId int64
}

type GetReply struct {
	Err   Err
	Value string
}
