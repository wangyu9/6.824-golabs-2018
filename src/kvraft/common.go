package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	// wangyu added:
	ErrTimeOut = "ErrTimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientID	ClientIndexType
	RequestID	RequestIndexType
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	// Err == "" if no error.
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientID	ClientIndexType
	RequestID	RequestIndexType
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
