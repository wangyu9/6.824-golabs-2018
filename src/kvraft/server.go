package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
	"bytes"
	"strconv"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const debug_getputappend = false
const enable_warning_lab3 = true
const enable_debug_lab3b = false

type OpType int
//type ServerSeqIndexType int

const (  // iota is reset to 0
	OP_TYPE_DEFAULT OpType = iota  //  == 0
	OP_TYPE_PUT OpType = iota  //  == 1
	OP_TYPE_APPEND OpType = iota  //  == 2
	OP_TYPE_GET OpType = iota  //  == 3
)

const TimeOutListenFromApplyCh = 1000

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Type OpType

	ClientID	ClientIndexType
	RequestID	RequestIndexType

	Key		interface{}
	Value	interface{}

	//ServerID int
	//ServerSeqID ServerSeqIndexType
	// Canno use req sequence number, since the server restarts will erase it.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	database	map[string] string

	//totalReqReceived	ServerSeqIndexType
	//waitingOpChan	map[ServerSeqIndexType] chan Op
	pendingOps	map[ClientIndexType] map[RequestIndexType] chan Op
	mostRecentWrite map[ClientIndexType] RequestIndexType
	// the most recent put or append from each client
}


func (kv *KVServer) insertToPendingOps(cid ClientIndexType, rid RequestIndexType, value chan Op) {
	// map[cid][rid] = value
	if kv.pendingOps[cid]==nil {
		kv.pendingOps[cid] = make(map[RequestIndexType] chan Op)
	}
	kv.pendingOps[cid][rid] = value
}



func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.


	opEntry := Op{}

	opEntry.Type = OP_TYPE_GET
	dataHandler := kv.getHandler

	opEntry.Key = args.Key

	// After this point, the code for PutAppend or Get should be the same.

	//opEntry.ServerID = kv.me

	kv.mu.Lock()
	opEntry.ClientID = args.ClientID
	opEntry.RequestID = args.RequestID
	//opEntry.ServerSeqID = kv.totalReqReceived
	//kv.totalReqReceived++
	kv.mu.Unlock()

	// These handlers should enter an Op in the Raft log using Start();
	// you should fill in the Op struct definition in server.go so that
	// it describes a Put/Append/Get operation.

	_, startTerm, isLeader := kv.rf.Start(opEntry)

	if !isLeader {

		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()

	opChan := make(chan Op, 1)

	kv.insertToPendingOps(opEntry.ClientID, opEntry.RequestID, opChan)
	//kv.waitingOpChan[opEntry.ServerSeqID] = opChan

	kv.mu.Unlock()

	// Each server should execute Op commands as Raft commits them,
	// i.e. as they appear on the applyCh.

	// Wait until the Op is committed by the Raft or timeout.
	select {
	case op := <- opChan:

		// Your solution needs to handle the case in which a leader has called Start()
		// for a Clerk's RPC, but loses its leadership before the request is committed to the log.
		// In this case you should arrange for the Clerk to re-send the request to other servers
		// until it finds the new leader.

		// One way to do this is for the server to detect that it has lost leadership, by
		// noticing that a different request has appeared at the index returned by Start(),
		// or that Raft's term has changed.
		endTerm := kv.rf.GetCurrentTerm()
		if endTerm == startTerm {
			kv.mu.Lock()

			dataHandler(&op, reply)

			if enable_debug_lab3b {
				if op.Key == strconv.Itoa(0) {
					fmt.Println("Get:----", op.Value)
				}
			}

			kv.mu.Unlock()

		} else {
			reply.WrongLeader = true
			//reply.Err
		}

		// If the ex-leader is partitioned by itself, it won't know about new leaders;
		// but any client in the same partition won't be able to talk to a new leader either,
		// so it's OK in this case for the server and client to wait indefinitely until the
		// partition heals.

		kv.mu.Lock()
		//close(kv.waitingOpChan[op.ServerSeqID])
		//delete(kv.waitingOpChan, op.ServerSeqID)
		kv.mu.Unlock()

	case <- time.After(TimeOutListenFromApplyCh*time.Millisecond):
		// timeout to listen from applyChan
		reply.Err = ErrTimeOut
	}

	// An RPC handler should notice when Raft commits its Op, and then reply to the RPC.

}

func (kv *KVServer) getHandler(op *Op, reply *GetReply) {
	switch op.Type {
	case OP_TYPE_GET:
		value, ok := kv.database[op.Key.(string)]
		if ok {
			reply.Err = OK
			reply.Value = value
 		} else {
 			reply.Err = ErrNoKey
		}
	default:
		fmt.Println("Error: getHandler() unrecognized operation type")
	}
}

func (kv *KVServer) putAppendHandler(op *Op, reply *PutAppendReply) {
	reply.Err = OK
}

func (kv *KVServer) tryApplyPutAppend(op *Op) {

	// In the face of unreliable connections and server failures,
	// a Clerk may send an RPC multiple times until it finds a kvserver that replies positively.
	// If a leader fails just after committing an entry to the Raft log,
	// the Clerk may not receive a reply, and thus may re-send the request to another leader.
	// Each call to Clerk.Put() or Clerk.Append() should result in just a single execution,
	// so you will have to ensure that the re-send doesn't result in the servers executing
	// the request twice.

	recentReqID, ok := kv.mostRecentWrite[op.ClientID]

	if !ok || recentReqID<op.RequestID {
		// Apply the non-duplicated Op to the database.

		switch op.Type {
		case OP_TYPE_APPEND:
			// Append(key, arg) appends arg to key's value, and Get() fetches the current value for a key.
			// An Append to a non-existant key should act like Put.
			value, ok := kv.database[op.Key.(string)]
			if ok {
				kv.database[op.Key.(string)] = value + op.Value.(string)
			} else {
				kv.database[op.Key.(string)] = op.Value.(string)
			}
		case OP_TYPE_PUT:
			// save as above
			kv.database[op.Key.(string)] = op.Value.(string)
			// should reply.Err set as ErrNoKey when the key does not exist?
			// I do not think so.
		default:
			fmt.Println("Error: putAppendHandler() unrecognized operation type")
		}

		// update the table
		kv.mostRecentWrite[op.ClientID] = op.RequestID

		if debug_getputappend {
			fmt.Println("Succeed to PutAppend", op)
		}

	} else {
		// the op is duplicated, but still reply ok
		if debug_getputappend {
			fmt.Println("Duplicated to PutAppend", op)
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.


	opEntry := Op{}
	if args.Op == "Put" {
		opEntry.Type = OP_TYPE_PUT
	} else {
		// Then args.Op == "Append"
		opEntry.Type = OP_TYPE_APPEND
	}

	dataHandler := kv.putAppendHandler

	opEntry.Key = args.Key
	opEntry.Value = args.Value

	// After this point, the code for PutAppend or Get should be the same.

	//opEntry.ServerID = kv.me

	kv.mu.Lock()
	opEntry.ClientID = args.ClientID
	opEntry.RequestID = args.RequestID
	//opEntry.ServerSeqID = kv.totalReqReceived
	//kv.totalReqReceived++
	kv.mu.Unlock()

	// These handlers should enter an Op in the Raft log using Start();
	// you should fill in the Op struct definition in server.go so that
	// it describes a Put/Append/Get operation.

	_, startTerm, isLeader := kv.rf.Start(opEntry)

	if !isLeader {

		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()

	opChan := make(chan Op, 1)

	// kv.waitingOpChan[opEntry.ServerSeqID] = opChan
	kv.insertToPendingOps(opEntry.ClientID, opEntry.RequestID, opChan)

	kv.mu.Unlock()

	// Each server should execute Op commands as Raft commits them,
	// i.e. as they appear on the applyCh.

	// Wait until the Op is committed by the Raft or timeout.
	select {
	case op := <- opChan:

		// Your solution needs to handle the case in which a leader has called Start()
		// for a Clerk's RPC, but loses its leadership before the request is committed to the log.
		// In this case you should arrange for the Clerk to re-send the request to other servers
		// until it finds the new leader.

		// One way to do this is for the server to detect that it has lost leadership, by
		// noticing that a different request has appeared at the index returned by Start(),
		// or that Raft's term has changed.
		endTerm := kv.rf.GetCurrentTerm()
		if endTerm == startTerm {
			kv.mu.Lock()

			dataHandler(&op, reply)

			if enable_debug_lab3b {
				if op.Key == strconv.Itoa(0) {
					fmt.Println("PutAppend:----", args.Op, " ", op.Value)
				}
			}

			kv.mu.Unlock()

			reply.Err = OK


		} else {
			reply.WrongLeader = true
			//reply.Err
		}

		// If the ex-leader is partitioned by itself, it won't know about new leaders;
		// but any client in the same partition won't be able to talk to a new leader either,
		// so it's OK in this case for the server and client to wait indefinitely until the
		// partition heals.

		kv.mu.Lock()
		//close(kv.waitingOpChan[op.ServerSeqID])
		//delete(kv.waitingOpChan, op.ServerSeqID)
		kv.mu.Unlock()

	case <- time.After(TimeOutListenFromApplyCh*time.Millisecond):
		reply.Err = ErrTimeOut
	}

	// An RPC handler should notice when Raft commits its Op, and then reply to the RPC.
}

/*
// do not use this function, since it is tricky to figure out when to close and delete the channel, due to possible restart.
// Maybe add it to Kill() as well for all channel will help. but do not have time to try it.
func (kv *KVServer) closeAndDeletePeningOps(cid ClientIndexType, rid RequestIndexType) {
	close( kv.pendingOps[cid][rid]) // close the channel no long used.
	// delete [cid,rid] from the 2D map
	delete( kv.pendingOps[cid], rid)
}
*/

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.

	if enable_debug_lab3b {
		kv.mu.Lock()
		fmt.Println("Kill() KVserver: server=", kv.me)
		fmt.Println("Decoded Database:", kv.database, "\n")
		kv.mu.Unlock()
	}
}

// wangyu:
func (kv *KVServer) ApplyMsgListener() {

	for {
		msg := <- kv.applyCh

		DPrintf("ApplyMsgListener(): ", msg)


		//fmt.Println("ApplyMsgListener(): ", msg)


		if msg.CommandValid {

			// Type assertion, see https://stackoverflow.com/questions/18041334/convert-interface-to-int-in-golang
			op := msg.Command.(Op)

			switch op.Type {
			case OP_TYPE_DEFAULT:
				fmt.Println("ApplyMsgListener(): Error")
				//case OP_TYPE_APPEND:
				//case OP_TYPE_PUT:
				//case OP_TYPE_GET:
			default: // these three cases are handled in the same way.
				kv.mu.Lock()
				if op.Type != OP_TYPE_GET {
					kv.tryApplyPutAppend(&op)
				}

				ch, ok := kv.pendingOps[op.ClientID][op.RequestID] // kv.pendingOps[op.ClientID] must exist, no need to worry about it.
				if ok {
					go func() {
						// TODO: cannot put this in a go routine since we need to be sure to get back to the client.
						// this may cause duplicated entries.
						// However I got deadlock without go routine. Figure out the reason.
						ch <- op
					}()
				} else {
				}
				kv.mu.Unlock()                                     // avoid deadlock
				/*if ok {
					go func() {
						// somehow need this to pass 3a linearizability.
						select {
						case ch <- op:
							return
						//case <-time.After((100 + TimeOutListenFromApplyCh) * time.Millisecond):
						//	if enable_warning_lab3 {
						//		fmt.Println("Warning!!!! Ops are lost due to timing out.")
						//	}
							// save to close and delete ch, since Get/PutAppend has the same timeout amount and it must have returned already.
							//kv.mu.Lock()
							//kv.mu.Unlock()
							return
						}
					}()
				}*/
			}

		} else {


			switch msg.Command.(type) {
			case raft.InstallSnapshotMsg:
				{
					upperData := msg.Command.(raft.InstallSnapshotMsg).SnapshotData

					kv.mu.Lock()

					r := bytes.NewBuffer(upperData)
					d := labgob.NewDecoder(r)

					if d.Decode(&kv.database) != nil ||
						d.Decode(&kv.mostRecentWrite) != nil {
						fmt.Println("Decode Snapshot fails.")
						//Success = false
					} else {
						//Success = true
						if enable_debug_lab3b {
							fmt.Println("Decoded Database:", kv.database, "\n")
						}
					}

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


func (kv *KVServer) CheckRaftSize() {// TODO delete
	for {

		// TODO: add the Lock back.
		kv.mu.Lock()
		kv.takeSnapshot()
		kv.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}



// wangyu:
func (kv *KVServer) encodeDatabase() (upperData []byte) {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if enable_debug_lab3b {
		fmt.Println("Encoded Database:", kv.database)
	}

	e.Encode(kv.database)
	e.Encode(kv.mostRecentWrite)
	upperData = w.Bytes()

	return upperData
}


func (kv *KVServer) takeSnapshot() {

	upperData := kv.encodeDatabase()

	kv.rf.TakeSnapshot(upperData, 13)//TODO kv.maxraftstate)
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.database = make(map[string] string)
	kv.pendingOps = make(map[ClientIndexType] map[RequestIndexType] chan Op)
	// kv.waitingOpChan = make(map[ServerSeqIndexType] chan Op)
	kv.mostRecentWrite = make(map[ClientIndexType] RequestIndexType)

	// Given code:
	kv.applyCh = make(chan raft.ApplyMsg)

	go kv.ApplyMsgListener() // put it before raft init
	//

	// Given code:
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	//kv.takeSnapshot()

	// You may need initialization code here.


	//if enable_lab_3b {
	/*{
		kv.rf.SetMaxLogSize( -1 )
		kv.rf.SetMaxLogSize( 100 )
		go func() {
			for {
				time.Sleep(10 * time.Millisecond)
				kv.rf.LogCompactionStart()
			}
		}()
	}*/

	{
		//maxraftstate = 1000 // TODO remove this later, change it back in final submission!!!

		kv.rf.SetMaxLogSize( maxraftstate/2 )
	}


	return kv
}


