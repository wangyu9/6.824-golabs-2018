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
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

const ErrStartOpRaftTimesOut = "StartOpRaftTimesOut"

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// wangyu:
	ClientID	ClientIndexType
	RequestID	RequestIndexType
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	// wangyu:
	ClientID	ClientIndexType
	RequestID	RequestIndexType
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}


type ShardDetachArgs struct {
	ShardID		int

	ConfigNum	int
	NewGroup	[]string
}

type ShardDetachReply struct {
	ShardID		int
	ShardDatabase	map[string] string
	ShouldSend	bool

	MostRecentWrite map[ClientIndexType] RequestIndexType
}

type ShardAttachArgs struct {
	ShardID		int
	ShardDatabase	map[string] string

	MostRecentWrite map[ClientIndexType] RequestIndexType

	// This is to avoid duplicated attach:
	ClientID	ClientIndexType
	RequestID	RequestIndexType

	IsInit		bool

}

type ShardAttachReply struct {

	WrongLeader bool
	Err         Err
}
