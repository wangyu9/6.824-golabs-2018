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
	OP_TYPE_SHARD_DETACH = iota //  == 3
	OP_TYPE_SHARD_ATTACH = iota //  == 4
	OP_TYPE_SHARD_INIT = iota // == 5
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

	ArgsShardDetach ShardDetachArgs // used if Type == OP_TYPE_SHARD_DETACH
	ArgsShardAttach ShardAttachArgs // used if Type == OP_TYPE_SHARD_ATTACH
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

	// Together are the state of the KV.
	database	[shardmaster.NShards]map[string] string
	responsibleShards [shardmaster.NShards] bool


	pendingOps	map[int] chan interface{}
	mostRecentWrite map[ClientIndexType] RequestIndexType

	config shardmaster.Config

	// The "client part": remember to persist the client part.
	clientID	ClientIndexType
	requestID	RequestIndexType // the number of requests have been made.
}

func copyMapTo (originalMap* map [string]string, targetMap* map [string]string) {

	// Clear the target map
	// targetMap = nil
	*targetMap = make(map [string]string)

	// Copy from the original map to the target map
	for key, value := range *originalMap {
		(*targetMap)[key] = value
	}

	return
}


func (kv *ShardKV) ShardDetachHandler (op * Op) (interface{}) {

	// TODO: Detach the shard

	args := op.ArgsShardDetach
	shardDatabase := make(map[string] string)

	if kv.responsibleShards[args.ShardID]==true {
		kv.responsibleShards[args.ShardID] = false
		copyMapTo(&kv.database[args.ShardID], &shardDatabase)
		kv.database[args.ShardID] = nil
	} else {
		fmt.Println("Error: ShardDetachHandler(): this should not happen, probably duplication detection fails.")
	}

	reply := ShardDetachReply{args.ShardID, shardDatabase}

	return reply
}

func (kv *ShardKV) ShardAttachHandler (op * Op) (interface{}) {

	// TODO: Attach the shard.

	args := op.ArgsShardAttach

	if kv.responsibleShards[args.ShardID]==false {
		kv.responsibleShards[args.ShardID] = true
		kv.database[args.ShardID] = make(map[string] string)
		copyMapTo(&args.ShardDatabase, &kv.database[args.ShardID])
		fmt.Println("ShardAttachHandler() attach map=", args.ShardDatabase)
	} else {
		fmt.Println("Fattal Error: ShardAttachHandler(): this should not happen, probably duplication detection fails.")
	}

	reply := ShardAttachReply{false, OK}
	return reply
}


func (kv *ShardKV) GetHandler (op *Op) (interface{}) {

	shardID := key2shard(op.Key.(string))
	key := op.Key.(string)

	var err Err
	value := ""

	if kv.responsibleShards[shardID]==true {
		v, ok := kv.database[shardID][key]
		if ok {
			value = v
			err = OK
			fmt.Println("GetHandler() successful get key=", key, "value=", value)
		} else {
			err = ErrNoKey
		}
	} else {
		err = ErrWrongGroup
	}

	reply := GetReply{false,err,value} // The first one, wrong Leader is not set here.

	return reply
}

func (kv *ShardKV) PutAppendHandler (op *Op) (interface{}) {

	shardID := key2shard(op.Key.(string))

	key := op.Key.(string)
	value := op.Value.(string)

	var err Err

	if kv.responsibleShards[shardID]==true {
		oldValue, ok := kv.database[shardID][key]
		if ok {// Append if exists
			kv.database[shardID][key] = oldValue + value
		} else {// Put if not exists
			kv.database[shardID][key] = value
		}
		err = OK
		fmt.Println("PutAppendHandler() successful putappend key=", key, "value=", value)
	} else {
		err = ErrWrongGroup
	}

	reply := PutAppendReply{false, err}

	return reply
}

func (kv *ShardKV) SendShard(args ShardAttachArgs, newGroup []string) {

	for {

		for i:=0; i<len(newGroup); i++ {
			server := kv.make_end(newGroup[i])

			reply := ShardAttachReply{}

			ok := server.Call("ShardKV.ShardAttach", &args, &reply)
			if ok && reply.WrongLeader == false {
				fmt.Println("SendShard successful")
				break
			} else {
				fmt.Println("SendShard fails, retry at a different server.")
			}
		}

		time.Sleep(200*time.Millisecond)
	}
}

func (kv *ShardKV) ShardAttach (args *ShardAttachArgs, reply *ShardAttachReply) {

	op := Op{}
	op.Type = OP_TYPE_SHARD_ATTACH

	op.ClientID = args.ClientID
	op.RequestID = args.RequestID

	wrongLeader, err, r := kv.StartOpRaft(op)

	reply.WrongLeader = wrongLeader
	if !wrongLeader {
		if err == ErrStartOpRaftTimesOut {
			// Handle in the same way of wrong leader
			reply.WrongLeader = true
		} else {
			reply.Err = r.(ShardAttachReply).Err
		}
	}
}

func (kv *ShardKV) ShardDetach(shardID int, newGroup []string) {

	op := Op {}
	op.Type = OP_TYPE_SHARD_DETACH

	kv.mu.Lock()
	op.ClientID = kv.clientID
	op.RequestID = kv.requestID
	kv.requestID++
	kv.mu.Unlock()

	op.ArgsShardDetach = ShardDetachArgs{ShardID:shardID}

	wrongLeader, err, r := kv.StartOpRaft(op)
	// kv.StartOpRaft(op)

	if !wrongLeader && err==OK {
		// TODO: initialize the RPC to send the shard.

		args := ShardAttachArgs{}
		args.ShardID = op.ArgsShardDetach.ShardID
		args.ClientID = op.ClientID
		args.RequestID = op.RequestID

		//shardDatabase := make( map[string] string)
		//copyMapTo(& (r.(ShardDetachReply)), &shardDatabase)
		shardDatabase := r.(ShardDetachReply)

		copyMapTo(&shardDatabase.ShardDatabase, &args.ShardDatabase)


		kv.SendShard(args, newGroup)
	}
}

func (kv *ShardKV) InitShard(shardID int) {

	op := Op{}
	op.Type = OP_TYPE_SHARD_ATTACH

	kv.mu.Lock()
	op.ClientID = kv.clientID
	op.RequestID = kv.requestID
	kv.requestID++
	kv.mu.Unlock()

	op.ArgsShardAttach.ShardID = shardID
	// op.ArgsShardAttach.ShardDatabase // empty map

	// wrongLeader, err, r := kv.StartOpRaft(op)
	kv.StartOpRaft(op)

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{}
	op.Type = OP_TYPE_GET
	op.Key = args.Key

	op.ClientID = args.ClientID
	op.RequestID = args.RequestID

	wrongLeader, err, r := kv.StartOpRaft(op)

	reply.WrongLeader = wrongLeader
	if !wrongLeader {
		if err == ErrStartOpRaftTimesOut {
			// Handle in the same way of wrong leader
			reply.WrongLeader = true
		} else {
			reply.Err = r.(GetReply).Err
			reply.Value = r.(GetReply).Value
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{}
	op.Type = OP_TYPE_PUTAPPEND
	op.Key = args.Key
	op.Value = args.Value

	op.ClientID = args.ClientID
	op.RequestID = args.RequestID

	wrongLeader, err, r := kv.StartOpRaft(op)

	reply.WrongLeader = wrongLeader
	if !wrongLeader {
		if err == ErrStartOpRaftTimesOut {
			// Handle in the same way of wrong leader
			reply.WrongLeader = true
		} else {
			reply.Err = r.(PutAppendReply).Err
		}
	}
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


func (kv *ShardKV) StartOpRaft(op Op) (wrongLeader bool, err Err, reply interface{}) {

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
		// to handle the case in which a leader has called Start()
		// for a Clerk's RPC, but loses its leadership before the request is committed to the log.
		// In this case you should arrange for the Clerk to re-send the request to other servers
		// until it finds the new leader.

		endTerm := kv.rf.GetCurrentTerm()

		if endTerm == startTerm {

			reply = r
			err = OK

			switch op.Type {
			case OP_TYPE_GET:
				reply = GetReply{false, r.(GetReply).Err, r.(GetReply).Value}
			case OP_TYPE_PUTAPPEND:
				reply = PutAppendReply{false, r.(PutAppendReply).Err}
			case OP_TYPE_SHARD_ATTACH:
				reply = r.(ShardAttachReply)
			case OP_TYPE_SHARD_DETACH:
				reply = r.(ShardDetachReply)
			}

		} else {
			wrongLeader = true
			fmt.Println("Does this ever happen?")
		}

	case <- time.After( 4000*time.Millisecond):
		err = ErrStartOpRaftTimesOut
		fmt.Println("Warning: StartOpRaft() times out.")
	}
	return
}

const EventDuplicatedOp = "EventDuplicatedOp"

func (kv *ShardKV) tryApplyOp(op *Op) (r interface{}) {


	// In the face of unreliable connections and server failures,
	// a Clerk may send an RPC multiple times until it finds a kvserver that replies positively.
	// If a leader fails just after committing an entry to the Raft log,
	// the Clerk may not receive a reply, and thus may re-send the request to another leader.
	// Each call to Clerk.Put() or Clerk.Append() should result in just a single execution,
	// so you will have to ensure that the re-send doesn't result in the servers executing
	// the request twice.

	clientID, requestID := kv.GetOpIDs(op)

	switch op.Type {
	case OP_TYPE_PUTAPPEND:

		recentReqID, ok := kv.mostRecentWrite[clientID]
		if !ok || recentReqID<requestID {
			// Apply the non-duplicated Op to the database.
			r = kv.PutAppendHandler(op)
			// update the table
			kv.mostRecentWrite[clientID] = requestID
			fmt.Println("Succeed to PutAppend()", op)
		} else {
			r = PutAppendReply{false, OK}
			// the op is duplicated, but still reply ok
			//if debug_getputappend {
			fmt.Println("Duplicated to tryApplyOp()::PutAppend()", op)
			//}
		}
	case OP_TYPE_GET:
		r = kv.GetHandler(op)
	case OP_TYPE_SHARD_DETACH:

		// TODO: duplicated detection

		r = kv.ShardDetachHandler(op)


	case OP_TYPE_SHARD_ATTACH:


		recentReqID, ok := kv.mostRecentWrite[clientID]
		if !ok || recentReqID<requestID {
			// Apply the non-duplicated Op to the database.
			r = kv.ShardAttachHandler(op)
			// update the table
			kv.mostRecentWrite[clientID] = requestID
		} else {
			r = ShardAttachReply{false, OK}
			// the op is duplicated, but still reply ok
			//if debug_getputappend {
			fmt.Println("Duplicated to tryApplyOp()::ShardAttach()", op)
			//}
		}

	default:
		fmt.Println("Fattal error: tryApplyOp() unrecognized op")
	}

	return r
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

func (kv *ShardKV) PoolLoop() {
	for {

		// replica groups consult the master in order to find out what shards to serve
		// particularly, I made the leader responsible for consulting the master.
		newConfig := kv.mck.Query(-1)


		// The replica group currently holding a shard is responsible for initializing the transfer to the new host replica group.

		kv.mu.Lock()

		oldShards := kv.config.Shards
		newShards := newConfig.Shards

		for i:=0; i<len(oldShards); i++ {
			if kv.responsibleShards[i] && newShards[i]!=kv.gid {
				// TODO: move the shard to the new replica group.
				// This should be updated in handlers, not here: kv.responsibleShards[i] = false
				kv.mu.Unlock()
				groupToSend := kv.config.Groups[newShards[i]]
				kv.ShardDetach(i, groupToSend)
				kv.mu.Lock()
			}
		}

		// But if there was no current replica group, the server is responsible for the initialization.
		for i:=0; i<len(newShards); i++ {
			if !kv.responsibleShards[i] && newShards[i]==kv.gid && oldShards[i]==0 {
				// This should be updated in handlers, not here: kv.responsibleShards[i] = true
				kv.mu.Unlock()
				kv.InitShard(i)
				kv.mu.Lock()
			}
		}


		kv.config = newConfig
		kv.mu.Unlock()

		time.Sleep(100*time.Millisecond)
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

	// Don't do this until attach the shard.
	//for i:=0; i<shardmaster.NShards; i++ {
	//	kv.database[i] = make(map[string] string)
	//}
	// kv.responsibleShards // probably not necessary to initialize a fixed size array.
	for i:=0; i<shardmaster.NShards; i++ {
		kv.responsibleShards[i] = false
	}
	kv.pendingOps = make(map[int] chan interface{})
	kv.mostRecentWrite = make(map[ClientIndexType] RequestIndexType)
	// TODO: read these info from persister()

	// Given Code:
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	kv.clientID = ClientIndexType(int(nrand())) // assigned a random number and *hope* there is no conflict... ideally this should be assigned by the kvserver...
	kv.requestID = 0
	// TODO: read the clientID and requestID from persister()

	go kv.MainLoop()
	go kv.PoolLoop()

	return kv
}
