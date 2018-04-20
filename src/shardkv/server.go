package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"labgob"
	"shardmaster"
	"fmt"
	"time"
)

type OpType int
//type ServerSeqIndexType int

const (  // iota is reset to 0
	OP_TYPE_DEFAULT OpType = iota  //  == 0
	OP_TYPE_PUTAPPEND OpType = iota  //  == 1
	OP_TYPE_GET OpType = iota  //  == 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Type OpType

	ClientID	ClientIndexType
	RequestID	RequestIndexType

	Key		interface{}
	Value	interface{}

}

type fn func(op *Op) (interface{})

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	mck *shardmaster.Clerk

	database	[shardmaster.NShards]map[string] string

	pendingOps	map[int] chan interface{}
	mostRecentWrite map[ClientIndexType] RequestIndexType
}

func (kv *ShardKV) GetHandler (op *Op) (interface{}) {

	return ""
}

func (kv *ShardKV) PutAppendHandler (op *Op) (interface{}) {

	return ""
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *ShardKV)  GetOpIDs(op *Op) (clientID ClientIndexType, requestID RequestIndexType){


	switch op.Type {
	case OP_TYPE_PUTAPPEND:
		clientID = op.ClientID
		requestID = op.RequestID
	case OP_TYPE_GET:
		clientID = op.ClientID
		requestID = op.RequestID
	}

	return
}


func (kv *ShardKV) StartOpRaft(op Op, opHandler fn) (wrongLeader bool, err Err, reply interface{}) {

	wrongLeader = false
	err = OK

	// fmt.Println("Start() called:", op, "at server:", sm.me)

	index, startTerm, isLeader := kv.rf.Start(op)

	if !isLeader {
		wrongLeader = true
		return
	}


	//clientID, requestID := sm.GetOpIDs(&op)

	kv.mu.Lock()

	newChan := make(chan interface{}, 1)
	//sm.insertToPendingOps(clientID, requestID, newChan)

	kv.pendingOps[index] = newChan


	kv.mu.Unlock()

	select {
	case r := <- newChan:

		// Same as kvraft lab3.
		// TODO: actually I am not able to handle this situation:
		// to handle the case in which a leader has called Start()
		// for a Clerk's RPC, but loses its leadership before the request is committed to the log.
		// In this case you should arrange for the Clerk to re-send the request to other servers
		// until it finds the new leader.

		endTerm := kv.rf.GetCurrentTerm()
		if endTerm == startTerm {

			reply = r
			err = OK

		} else {
			wrongLeader = true
			fmt.Println("Does this ever happen?")
		}

	case <- time.After( 4000*time.Millisecond):
		err = "StartOpRaftTimesOut"
		fmt.Println("Warning: StartOpRaft() times out.")
	}
	return
}


func (kv *ShardKV) tryApplyOp(op *Op) (r interface{}) {


	// In the face of unreliable connections and server failures,
	// a Clerk may send an RPC multiple times until it finds a kvserver that replies positively.
	// If a leader fails just after committing an entry to the Raft log,
	// the Clerk may not receive a reply, and thus may re-send the request to another leader.
	// Each call to Clerk.Put() or Clerk.Append() should result in just a single execution,
	// so you will have to ensure that the re-send doesn't result in the servers executing
	// the request twice.

	clientID, requestID := kv.GetOpIDs(op)


	recentReqID, ok := kv.mostRecentWrite[clientID]

	if !ok || recentReqID<requestID {
		// Apply the non-duplicated Op to the database.

		switch op.Type {
		case OP_TYPE_PUTAPPEND:
			r = kv.PutAppendHandler(op)
		case OP_TYPE_GET:
			r = kv.GetHandler(op)
		}
		// update the table
		kv.mostRecentWrite[clientID] = requestID


	} else {
		// the op is duplicated, but still reply ok
		//if debug_getputappend {
		//	fmt.Println("Duplicated to PutAppend", op)
		//}
	}



	return
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) MainLoop() {
	for {
		msg := <-kv.applyCh

		//fmt.Println("MainLoop(): ", msg)

		if msg.CommandValid {


			if msg.Command==nil {
				fmt.Println("Error: msg.Command==nil:", msg, "for server", kv.me, "i.e. raft server", kv.rf.GetServerID())
			} else {


				// Type assertion, see https://stackoverflow.com/questions/18041334/convert-interface-to-int-in-golang
				op := msg.Command.(Op)

				kv.mu.Lock()

				//clientID, requestID := sm.GetOpIDs(&op)

				reply := kv.tryApplyOp(&op)

				ch, ok := kv.pendingOps[msg.CommandIndex]

				if ok {
					go func() {
						ch <- reply
					}()
				} else {

				}

				kv.mu.Unlock() // avoid deadlock
			}
		}
	}
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
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mck = shardmaster.MakeClerk(kv.masters)

	for i:=0; i<shardmaster.NShards; i++ {
		kv.database[i] = make(map[string] string)
	}

	kv.pendingOps = make(map[int] chan interface{})
	kv.mostRecentWrite = make(map[ClientIndexType] RequestIndexType)

	// Given Code:
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	go kv.MainLoop()

	return kv
}
