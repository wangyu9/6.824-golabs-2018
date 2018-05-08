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
	"bytes"
)

type OpType int
//type ServerSeqIndexType int

const enable_debug_lab4b = false
const enable_warning_lab4b = true

const (  // iota is reset to 0
	OP_TYPE_DEFAULT OpType = iota  //  == 0
	//OP_TYPE_PUTAPPEND OpType = iota  //  == 1
	OP_TYPE_APPEND OpType = iota
	OP_TYPE_GET OpType = iota  //  ==
	OP_TYPE_SHARD_DETACH = iota //  ==
	OP_TYPE_SHARD_ATTACH = iota //  ==
	OP_TYPE_SHARD_INIT = iota // ==
	OP_TYPE_PUT OpType = iota
)

const shardmaster_client_index = 1234567654321 // The magic number assigned as the ClientIndexType for the config master.

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

	hasSeenFirstConfig [shardmaster.NShards]bool

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

func copyMapTo2 (originalMap* map [ClientIndexType] RequestIndexType, targetMap* map [ClientIndexType] RequestIndexType) {

	// Clear the target map
	// targetMap = nil
	*targetMap = make(map [ClientIndexType] RequestIndexType)

	// Copy from the original map to the target map
	for key, value := range *originalMap {
		(*targetMap)[key] = value
	}

	return
}

func (kv *ShardKV) checkDatabaseInvariant() {

	if !enable_debug_lab4b && !enable_warning_lab4b {
		return
	}

	for k :=0; k < shardmaster.NShards; k++ {
		if (kv.database[k] == nil && kv.responsibleShards[k]) || (kv.database[k]!=nil && !kv.responsibleShards[k]) {
			fmt.Println("checkDatabaseInvariant() fails.")
			{
				for k2 :=0; k2 < shardmaster.NShards; k2++ {
					fmt.Println("Server", kv.gid, "-", kv.me, "-",k2,":", len(kv.database[k2]))
				}
				fmt.Println("Server", kv.gid, "-", kv.me,":", kv.responsibleShards)
				fmt.Println("here")
			}
		}
	}
}

// This should be called Try ShardDetachAndSend
func (kv *ShardKV) ShardDetachHandler (op * Op) (interface{}) {

	// TODO: Detach the shard

	args := op.ArgsShardDetach
	shardDatabase := make(map[string] string)
	mostRecentWrite := make(map[ClientIndexType] RequestIndexType)

	kv.checkDatabaseInvariant()

	reply := ShardDetachReply{}

	if kv.responsibleShards[args.ShardID]==true {
		kv.responsibleShards[args.ShardID] = false
		copyMapTo(&kv.database[args.ShardID], &shardDatabase)
		copyMapTo2(&kv.mostRecentWrite, &mostRecentWrite)
		kv.database[args.ShardID] = nil
		kv.database[args.ShardID] = make(map[string] string)
		kv.database[args.ShardID] = nil
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, " ShardDetachHandler() succeed to detach shardID", args.ShardID)
			kv.checkDatabaseInvariant()
		}
		reply = ShardDetachReply{args.ShardID, shardDatabase, true, mostRecentWrite}

	} else {
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, "ShardDetachHandler(): no such shardID", args.ShardID)
		}
		reply.ShouldSend = false
	}

	// TODO: only necessary for the leader to send the shard.
	if reply.ShouldSend { // TODO: a better determination of leader: this one does not work && kv.me==0
		args := ShardAttachArgs{}
		args.ShardID = op.ArgsShardDetach.ShardID


		args.ClientID = makeShardClientID( args.ShardID)
		args.RequestID = makeShardRequestID( op.ArgsShardDetach.ConfigNum, true)
		args.IsInit = false


		copyMapTo(&reply.ShardDatabase, &args.ShardDatabase)
		copyMapTo2(&reply.MostRecentWrite, &args.MostRecentWrite)


		// TODO: make sure that the shard is not lost, if the kv is killed. Currently cannot guarantee this.
		// Critical, this should be done in a go routine, or it may have a cycle of sending.
		go kv.SendShard(&args, op.ArgsShardDetach.NewGroup)
	}


	return reply
}

func max(a RequestIndexType, b RequestIndexType) (RequestIndexType) {
	if a > b {
		return a
	} else {
		return b
	}
}

func (kv *ShardKV) ShardAttachHandler (op * Op) (interface{}) {

	// TODO: Attach the shard.


	kv.checkDatabaseInvariant()

	args := op.ArgsShardAttach

	if op.ArgsShardAttach.IsInit {
		kv.hasSeenFirstConfig[args.ShardID] = true
	}

	if kv.responsibleShards[args.ShardID]==false {

		kv.responsibleShards[args.ShardID] = true
		kv.database[args.ShardID] = nil
		kv.database[args.ShardID] = make(map[string] string)
		copyMapTo(&args.ShardDatabase, &kv.database[args.ShardID])
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, "ShardAttachHandler() attach successfully, shardID", args.ShardID, " map=", args.ShardDatabase)
			fmt.Println("table: ", kv.responsibleShards)
		}

		// Hint: Be careful about implementing at-most-once semantics (duplicate detection)
		// for client requests. When groups move shards, they need to move some duplicate
		// detection state as well as the key/value data. Think about how the receiver of
		// a shard should update its own duplicate detection state. Is it correct for the
		// receiver to entirely replace its state with the received one?

		for key, value := range op.ArgsShardAttach.MostRecentWrite {
			currentValue, ok := kv.mostRecentWrite[key]
			if ok {
				kv.mostRecentWrite[key] = max(currentValue, value)
			} else {
				kv.mostRecentWrite[key] = value
			}
		}
	} else {
		fmt.Println("Server",kv.gid,"-",kv.me,"Fattal Error: ShardAttachHandler(): this should not happen, probably duplication detection fails. Op:", op)
		fmt.Println("Server",kv.gid,"-",kv.me,"shardID", args.ShardID, "table: ", kv.responsibleShards)
	}

	kv.checkDatabaseInvariant()

	reply := ShardAttachReply{false, OK}
	return reply
}


func (kv *ShardKV) GetHandler (op *Op) (interface{}) {

	shardID := key2shard(op.Key.(string))
	key := op.Key.(string)

	kv.checkDatabaseInvariant()

	var err Err
	value := ""

	if kv.responsibleShards[shardID]==true {
		v, ok := kv.database[shardID][key]
		if ok {
			value = v
			err = OK
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "GetHandler() successful get key=", key, "value=", value)
			}
		} else {
			err = ErrNoKey
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "GetHandler() ErrNoKey")
			}
		}
	} else {
		err = ErrWrongGroup
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, "GetHandler() ErrWrongGroup. table:", kv.responsibleShards)
		}
	}

	reply := GetReply{false,err,value} // The first one, wrong Leader is not set here.

	kv.checkDatabaseInvariant()

	return reply
}

func (kv *ShardKV) PutAppendHandler (op *Op) (interface{}) {

	shardID := key2shard(op.Key.(string))

	key := op.Key.(string)
	value := op.Value.(string)

	kv.checkDatabaseInvariant()

	var err Err

	if kv.responsibleShards[shardID]==true {
		if false {
			// my previous understanding.
			oldValue, ok := kv.database[shardID][key]
			if ok {// Append if exists
				kv.database[shardID][key] = oldValue + value
			} else {// Put if not exists
				kv.database[shardID][key] = value
			}
		} else {
			oldValue, ok := kv.database[shardID][key]
			if op.Type==OP_TYPE_PUT || !ok {// Put in case to Append but not exists
				kv.database[shardID][key] = value
			} else {
				kv.database[shardID][key] = oldValue + value
			}
		}

		err = OK
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, "PutAppendHandler() successful putappend key=", key, "value=", value)
		}
	} else {
		err = ErrWrongGroup
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, "PutAppendHandler() fails due to ErrWrongGroup to putappend key=", key, "value=", value, "shardID", shardID, "kv.responsibleShards", kv.responsibleShards)
		}
	}

	reply := PutAppendReply{false, err}

	kv.checkDatabaseInvariant()

	return reply
}

func (kv *ShardKV) SendShard(args* ShardAttachArgs, newGroup []string) {
	if enable_debug_lab4b {
		fmt.Println("Server", kv.gid, "-", kv.me, "SendShard id=", args.ShardID, "started to")
	}
	for {

		for i:=0; i<len(newGroup); i++ {
			server := kv.make_end(newGroup[i])

			reply := ShardAttachReply{}

			ok := server.Call("ShardKV.ShardAttach", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err==OK {
				if enable_debug_lab4b {
					fmt.Println("Server", kv.gid, "-", kv.me, "SendShard id=", args.ShardID, "successful.")
				}
				return
			} else {
				if enable_debug_lab4b {
					fmt.Println("Server", kv.gid, "-", kv.me, "SendShard id=", args.ShardID, "fails", "WrongLeader=", reply.WrongLeader, "Err=", reply.Err, ", retry at a different server.")
				}
			}
		}

		time.Sleep(200*time.Millisecond)
	}
}

func (kv *ShardKV) ShardAttach (args *ShardAttachArgs, reply *ShardAttachReply) {

	op := Op{}
	op.Type = OP_TYPE_SHARD_ATTACH

	op.ArgsShardAttach.ShardID = args.ShardID
	copyMapTo(&args.ShardDatabase, &op.ArgsShardAttach.ShardDatabase)
	copyMapTo2(&args.MostRecentWrite, &op.ArgsShardAttach.MostRecentWrite)

	op.ClientID = args.ClientID
	op.RequestID = args.RequestID
	op.ArgsShardAttach.IsInit = args.IsInit

	wrongLeader, err, r := kv.StartOpRaft(op)

	reply.WrongLeader = wrongLeader
	if !wrongLeader {
		if err == ErrStartOpRaftTimesOut {
			// Handle in the same way of wrong leader
			reply.WrongLeader = true
		} else {
			*reply = r.(ShardAttachReply)
			//reply.Err = r.(ShardAttachReply).Err
		}
	} else {
		//reply.WrongLeader = true
	}
}

func (kv *ShardKV) ShardDetachAndSend(shardID int, newGroup []string, configNum int) {

	op := Op {}
	op.Type = OP_TYPE_SHARD_DETACH

	if enable_debug_lab4b {
		fmt.Println("Server", kv.gid, "-", kv.me, ": ShardDetachAndSend() initialized shardID:", shardID)
	}
	// Do not need duplicated detection.
	//kv.mu.Lock()
	//op.ClientID = kv.clientID
	//op.RequestID = kv.requestID
	//kv.requestID++
	//kv.mu.Unlock()

	op.ClientID = makeShardClientID( shardID)
	op.RequestID = makeShardRequestID( configNum, false)

	op.ArgsShardDetach = ShardDetachArgs{ShardID:shardID, ConfigNum: configNum, NewGroup: newGroup}

	wrongLeader, err, _ := kv.StartOpRaft(op)
	// wrongLeader, err, r := kv.StartOpRaft(op)


	if !wrongLeader && err==OK {
		// TODO: initialize the RPC to send the shard.

		// reply := r.(ShardDetachReply)

		/* moved to detach handler.
		if reply.ShouldSend {
			args := ShardAttachArgs{}
			args.ShardID = op.ArgsShardDetach.ShardID

			// RPC call does not need duplication detection.
			// args.ClientID = op.ClientID
			// args.RequestID = op.RequestID

			args.ClientID = makeShardClientID( shardID)
			args.RequestID = makeShardRequestID( configNum, true)

			//shardDatabase := make( map[string] string)
			//copyMapTo(& (r.(ShardDetachReply)), &shardDatabase)


			copyMapTo(&reply.ShardDatabase, &args.ShardDatabase)
			copyMapTo2(&reply.MostRecentWrite, &args.MostRecentWrite)

			// Critical, this should be done in a go routine, or it may have a cycle of sending.
			go kv.SendShard(&args, newGroup)
		}
		*/

	} else {
		// TODO
	}

}

func makeShardClientID(shardID int) (ClientIndexType) {
	return shardmaster_client_index * shardmaster.NShards + ClientIndexType(shardID)
}

func makeShardRequestID(configNum int, isAttach bool) (RequestIndexType) {
	r := RequestIndexType( configNum ) * 2
	if isAttach {
		r = r + 1
		// detach for 0, attach for 1
	}
	return r
}

func (kv *ShardKV) InitShard(shardID int, configNum int)(success bool) {
	if enable_debug_lab4b {
		fmt.Println("Server", kv.gid, "-", kv.me, "InitShard(", shardID, ") called by server", kv.me)
	}
	op := Op{}
	op.Type = OP_TYPE_SHARD_ATTACH

	op.ClientID = makeShardClientID( shardID)
	op.RequestID = makeShardRequestID( configNum, true)

	/*
	kv.mu.Lock()
	op.ClientID = kv.clientID
	op.RequestID = kv.requestID
	kv.requestID++
	kv.mu.Unlock()
	*/
	op.ArgsShardAttach.ShardID = shardID
	op.ArgsShardAttach.IsInit = true
	// op.ArgsShardAttach.ShardDatabase // empty map

	// TODO: put this in a loop to make sure it is really initialized.

	wrongLeader, err, _ := kv.StartOpRaft(op)
	//kv.StartOpRaft(op)

	if wrongLeader || err!=OK {
		return false
	} else {
		return true
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if enable_debug_lab4b {
		fmt.Println("Server", kv.gid, "-", kv.me, "Get() called with arg=", *args)
	}

	if false {
		// An Optional Optimization to avoid no necessary Get
		//
		kv.mu.Lock()

		shardID := key2shard(args.Key)
		if !kv.responsibleShards[shardID] {
			// The data might be stale, but this is fine: the kv will figure out eventually and the client will retry later.
			// This avoid inserting not necessary entries to the log.
			reply.WrongLeader = true // reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}


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
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "Get() ErrStartOpRaftTimesOut")
			}
		} else {
			reply.Err = r.(GetReply).Err
			reply.Value = r.(GetReply).Value
		}
	} else {
		//reply.WrongLeader = true
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, "Get() wrong leader")
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if enable_debug_lab4b {
		fmt.Println("Server", kv.gid, "-", kv.me, "PutAppend() called with arg=", *args)
	}

	if false {
		// An Optional Optimization to avoid no necessary PutAppend
		// Cannot put this too earlier:
		kv.mu.Lock()

		shardID := key2shard(args.Key)
		if !kv.responsibleShards[shardID] {
			// The data might be stale, but this is fine: the kv will figure out eventually and the client will retry later.
			// This avoid inserting not necessary entries to the log.
			reply.WrongLeader = true// reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}


	op := Op{}
	//op.Type = OP_TYPE_PUTAPPEND
	if args.Op == "Put" {
		op.Type = OP_TYPE_PUT
	} else {
		op.Type = OP_TYPE_APPEND
	}
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
			*reply = r.(PutAppendReply)
		}
	}
}

func (kv *ShardKV) GetOpIDs(op *Op) (clientID ClientIndexType, requestID RequestIndexType){


	switch op.Type {
	//case OP_TYPE_PUTAPPEND:
	//	clientID = op.ClientID
	//		requestID = op.RequestID
	case OP_TYPE_PUT:
		clientID = op.ClientID
		requestID = op.RequestID
	case OP_TYPE_APPEND:
		clientID = op.ClientID
		requestID = op.RequestID
	case OP_TYPE_GET:
		clientID = op.ClientID
		requestID = op.RequestID
	case OP_TYPE_SHARD_ATTACH:
		clientID = op.ClientID
		requestID = op.RequestID
	case OP_TYPE_SHARD_DETACH:
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
			//case OP_TYPE_PUTAPPEND:
			//	reply = PutAppendReply{false, r.(PutAppendReply).Err}
			case OP_TYPE_PUT:
				reply = PutAppendReply{false, r.(PutAppendReply).Err}
			case OP_TYPE_APPEND:
				reply = PutAppendReply{false, r.(PutAppendReply).Err}
			case OP_TYPE_SHARD_ATTACH:
				reply = ShardAttachReply{false, r.(ShardAttachReply).Err} //r.(ShardAttachReply)
			case OP_TYPE_SHARD_DETACH:
				reply = r.(ShardDetachReply)
			}

		} else {
			wrongLeader = true
			if enable_warning_lab4b {
				// TODO: this might be OK, but I did not check it carefully.
				fmt.Println("Server", kv.gid, "-", kv.me, "Does this ever happen?")
			}
		}

	case <- time.After( 600*time.Millisecond):
		err = ErrStartOpRaftTimesOut
		// if enable_warning_lab4b
		{
			// TODO: I think this should be OK, but I did not check it carefully.
			fmt.Println("Server", kv.gid, "-", kv.me, "Warning: StartOpRaft() times out. ")
		}
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

	switch {
	//case op.Type == OP_TYPE_PUTAPPEND:
	case op.Type == OP_TYPE_PUT || op.Type == OP_TYPE_APPEND:
		recentReqID, ok := kv.mostRecentWrite[clientID]
		if !ok || recentReqID<requestID {
			// Apply the non-duplicated Op to the database.
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "try to PutAppend()", op, "to group", kv.gid)
			}
			r = kv.PutAppendHandler(op)
			// update the table
			if r.(PutAppendReply).Err == OK {
				// Critical to mark it as written only it really had written something,
				// query but realized it is the wrong group should not be marked as written.
				kv.mostRecentWrite[clientID] = requestID
			}

		} else {
			r = PutAppendReply{false, OK}
			// the op is duplicated, but still reply ok
			//if debug_getputappend {
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "Duplicated to tryApplyOp()::PutAppend()", op)
			}
			//}
		}
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, "OP_TYPE_PUTAPPEND table:", kv.responsibleShards)
		}
	case op.Type == OP_TYPE_GET:
		r = kv.GetHandler(op)
		if enable_debug_lab4b {
			fmt.Println("Server", kv.gid, "-", kv.me, "OP_TYPE_GET table:", kv.responsibleShards)
		}
	case op.Type == OP_TYPE_SHARD_DETACH:

		// Do not need duplicated detection.

		// r = kv.ShardDetachHandler(op)

		recentReqID, ok := kv.mostRecentWrite[clientID]
		if !ok || recentReqID<requestID {
			// Apply the non-duplicated Op to the database.
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "try to Detach()", op, "to group", kv.gid)
			}
			r = kv.ShardDetachHandler(op)
			// update the table
			kv.mostRecentWrite[clientID] = requestID

		} else {
			r = ShardDetachReply{ShouldSend: false}
			// the op is duplicated, but still reply ok
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "Duplicated to tryApplyOp()::Detach()", op)
			}
		}


	case op.Type == OP_TYPE_SHARD_ATTACH:

		// Do not need duplicated detection.
		// Since this is from a RPC call.
		// r = kv.ShardAttachHandler(op)

		recentReqID, ok := kv.mostRecentWrite[clientID]
		if !ok || recentReqID<requestID {
			// Apply the non-duplicated Op to the database.
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "try to Attach()", op, "to group", kv.gid)
			}
			r = kv.ShardAttachHandler(op)
			// update the table
			kv.mostRecentWrite[clientID] = requestID

		} else {
			// the op is duplicated, but still reply ok
			r = ShardAttachReply{false, OK}
			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "Duplicated to tryApplyOp()::Attach()", op)
			}
		}


		// Do need duplicated detection.
		/*
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
			fmt.Println("Duplicated to tryApplyOp()::ShardAttach()", op, "ShardID:", op.ArgsShardAttach.ShardID)
			//}
		}*/

	default:
		fmt.Println("Server",kv.gid,"-",kv.me,"Fattal error: tryApplyOp() unrecognized op")
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
	if enable_debug_lab4b {
		fmt.Println("Server", kv.gid, "-", kv.me, "kill()")
	}
}

func (kv *ShardKV) encodeDatabase() (upperData []byte) {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if enable_debug_lab4b {
		fmt.Println("Server",kv.gid,"-",kv.me,"Encoded Database:", kv.database)
	}

	kv.checkDatabaseInvariant()

	e.Encode(kv.database)
	e.Encode(kv.hasSeenFirstConfig)
	e.Encode(kv.responsibleShards)
	e.Encode(kv.mostRecentWrite)

	kv.checkDatabaseInvariant()

	upperData = w.Bytes()

	if false {
		fmt.Println("upperData size:", len(upperData))
		w2 := new(bytes.Buffer)
		e2 := labgob.NewEncoder(w2)
		e2.Encode(upperData)
		upperData2 := w2.Bytes()
		fmt.Println("upperData2 size:", len(upperData2))

		fmt.Println("Server",kv.gid,"-",kv.me,"Encoded Database:", kv.database, "\nmostRecentWrite", kv.mostRecentWrite)
	}

	return upperData
}

func (kv *ShardKV) MainLoop() {
	for {
		msg := <-kv.applyCh

		//fmt.Println("MainLoop(): ", msg)

		if msg.CommandValid {


			if msg.Command==nil {
				fmt.Println("Server",kv.gid,"-",kv.me,"Error: msg.Command==nil:", msg, "for server", kv.me, "i.e. raft server", kv.rf.GetServerID())
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
		} else {


			switch msg.Command.(type) {
			case raft.InstallSnapshotMsg:
				{


					upperData := msg.Command.(raft.InstallSnapshotMsg).SnapshotData

					kv.mu.Lock()

					kv.checkDatabaseInvariant()

					r := bytes.NewBuffer(upperData)
					d := labgob.NewDecoder(r)

					var database	[shardmaster.NShards]map[string] string
					var hasSeenFirstConfig [shardmaster.NShards]bool
					var responsibleShards [shardmaster.NShards] bool
					var mostRecentWrite map[ClientIndexType] RequestIndexType


					if d.Decode(&database) != nil ||
						d.Decode(&hasSeenFirstConfig) != nil ||
						d.Decode(&responsibleShards) != nil ||
						d.Decode(&mostRecentWrite) != nil {
						fmt.Println("Decode Snapshot fails.")
						//Success = false

					} else {
						//Success = true

						for k:=0; k<shardmaster.NShards; k++ {
							copyMapTo(&database[k], &kv.database[k])
							kv.hasSeenFirstConfig[k] = hasSeenFirstConfig[k]
							kv.responsibleShards[k] = responsibleShards[k]
						}
						copyMapTo2(&mostRecentWrite, & kv.mostRecentWrite)


						if enable_debug_lab4b {
							fmt.Println("Decoded Database:", kv.database, "\n")
						}
						// I do not understand why the code is necessary, but without it my code fails to pass the Test: shard deletion (challenge 1) ...
						for k :=0; k < shardmaster.NShards; k++ {
							if kv.database[k]!=nil && !kv.responsibleShards[k] {
								// This does not hold wiredly:
								if len(kv.database[k])>0 {
									fmt.Println("Server",kv.gid,"-",kv.me, "Error: database does not match with the responsibleShards")
								}
								kv.database[k] = nil
							}
						}
					}

					kv.checkDatabaseInvariant()

					//for i:=0; i<shardmaster.NShards; i++ {
					//	kv.hasSeenFirstConfig[i] = true // do not need to initialize shards if started from Snapshot.
					//}

					kv.mu.Unlock()
				}
			case raft.SaveSnapshotMsg:
				{// rf.Lock() is Lock() until LogCompactionEnd


					kv.mu.Lock()

					upperData := kv.encodeDatabase()

					kv.mu.Unlock()

					// no need to worry kv.database is updated during gap, since rf.mu is locked.

					kv.rf.LogCompactionEnd(upperData)

				}
			default:
				{
					fmt.Println("Error: ApplyMsgListener() unknown type of command msg.")
				}
			}


		}
	}
}

func (kv *ShardKV) PoolLoop() {

	//time.Sleep(1000*time.Millisecond)

	for {


		_, isleader := kv.rf.GetState()

		if isleader {
		//if true {
			// replica groups consult the master in order to find out what shards to serve
			// particularly, I made the leader responsible for consulting the master.

			if enable_debug_lab4b {
				fmt.Println("Server", kv.gid, "-", kv.me, "Pre Query()")
			}

			newConfig := kv.mck.Query(-1)

			if newConfig.Num > 0 {

				if enable_debug_lab4b {
					fmt.Println("Server", kv.gid, "-", kv.me, "Post Query()")
				}

				kv.mu.Lock()

				firstConfig := kv.mck.Query(1)

				var shouldInitShards [shardmaster.NShards]bool
				// shouldInitShards := false
				// For it to have seen the first config
				for i := 0; i < shardmaster.NShards; i++ {
					if !kv.hasSeenFirstConfig[i] && firstConfig.Shards[i] == kv.gid {
						if newConfig.Num > 1 {
							newConfig = kv.mck.Query(1)
							// kv.hasSeenFirstConfig[i] = true
							shouldInitShards[i] = true
						} else if newConfig.Num == 1 {
							// kv.hasSeenFirstConfig[i] = true
							shouldInitShards[i] = true
						} else {
							// Do nothing.
						}
					}
				}

				// The replica group currently holding a shard is responsible for initializing the transfer to the new host replica group.

				//oldShards := kv.config.Shards
				newShards := newConfig.Shards

				if enable_debug_lab4b {
					fmt.Println("Server", kv.gid, "-", kv.me, "PoolLoop() kv.responsibleShards:", kv.responsibleShards, "NewShards:", newShards, "kv.gid:", kv.gid)
				}

				for i := 0; i < shardmaster.NShards; i++ {

					// But if there was no current replica group, the server is responsible for the initialization.
					if shouldInitShards[i] {
						//for i := 0; i < len(newShards); i++ {
						//if newShards[i] == kv.gid // moved this up
						{ // !kv.responsibleShards[i] &&  TODO: this is not reliable, if the first config is missed; should be put in init function.
							// This should be updated in handlers, not here: kv.responsibleShards[i] = true
							kv.mu.Unlock()
							//succeed := kv.InitShard(i, newConfig.Num)
							kv.InitShard(i, newConfig.Num)
							kv.mu.Lock()
							//if succeed {
							//	kv.hasSeenFirstConfig[i] = true
							//}
						}
						//}
					} else {
						//for i := 0; i < len(oldShards); i++ {
						// kv.responsibleShards[i]  : this reads from kv.responsibleShards, probably not safe without.
						// if oldShards[i] == kv.gid && newShards[i] != kv.gid { // Critical: this is wrong, since oldShards[i] is out of date, and the server may miss the info,
						// blocking to try ShardDetachAndSend
						if kv.responsibleShards[i] && newShards[i] != kv.gid {
							// detach and move the shard to the new replica group.
							// This should be updated in handlers, not here: kv.responsibleShards[i] = false
							kv.mu.Unlock()
							groupToSend := newConfig.Groups[newShards[i]]
							if len(groupToSend) > 0 {
								// TODO optional optimize: read from local kv.responsibleShards before insert to raft.
								kv.ShardDetachAndSend(i, groupToSend, newConfig.Num)
							} else {
								fmt.Println("Server", kv.gid, "-", kv.me, "Warning: groupToSend is empty, this should be impossible")
							}
							kv.mu.Lock()
						}
						//}
					}
				}
				kv.config = newConfig
				kv.mu.Unlock()

			}

		}

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

	// wangyu
	//if maxraftstate>=0 && maxraftstate<(1+2)*2 {
	//	maxraftstate = (1+2)*2
	//}

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
		kv.hasSeenFirstConfig[i] = false
	}
	kv.pendingOps = make(map[int] chan interface{})
	kv.mostRecentWrite = make(map[ClientIndexType] RequestIndexType)
	// These info will be read and overwitten from persister() latter in MainLoop.

	// Given Code:
	kv.applyCh = make(chan raft.ApplyMsg)


	go kv.MainLoop() // put it before raft init

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	kv.clientID = ClientIndexType(int(nrand())) // assigned a random number and *hope* there is no conflict... ideally this should be assigned by the kvserver...
	kv.requestID = 0
	// TODO: read the clientID and requestID from persister()

	go kv.PoolLoop()

	kv.rf.SetMaxLogSize( maxraftstate )

	if enable_debug_lab4b {
		fmt.Println("Server", kv.gid, "-", kv.me, "StartServer()")
	}

	return kv
}
