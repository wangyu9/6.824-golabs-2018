package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"labgob"
	"time"
	"fmt"
)


type OpIndexType int

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.


	// config

	currentConfigNum int


	// Given:
	configs []Config // indexed by config num

	pendingOps	map[ClientIndexType] map[RequestIndexType] chan Op
	mostRecentWrite map[ClientIndexType] RequestIndexType
}

func (sm *ShardMaster) insertToPendingOps(cid ClientIndexType, rid RequestIndexType, value chan Op) {
	// map[cid][rid] = value
	if sm.pendingOps[cid]==nil {
		sm.pendingOps[cid] = make(map[RequestIndexType] chan Op)
	}
	sm.pendingOps[cid][rid] = value
}


type OpType int

const (  // iota is reset to 0
	OP_TYPE_JOIN OpType = iota  //  == 0
	OP_TYPE_LEAVE OpType = iota  //  == 1
	OP_TYPE_MOVE OpType = iota  //  == 2
	OP_TYPE_QUERY OpType = iota //  == 3
)

type Op struct {
	// Your data here.
	Type OpType
	Args interface{}

}

//type OpResult struct {
//	Reply interface{}
//}

type fn func(op *Op) (interface{})

/*
func copyMap (originalMap map [interface{}] []interface{}) (targetMap map [interface{}] []interface{}) {

	// Create the target map
	targetMap = make(map [interface{}] []interface{})

	// Copy from the original map to the target map
	for key, value := range originalMap {
		targetMap[key] = value
	}

	return
}
*/

func copyMap (originalMap map [int] []string) (targetMap map [int] []string) {

	// Create the target map
	targetMap = make(map [int] []string)

	// Copy from the original map to the target map
	for key, value := range originalMap {
		targetMap[key] = value
	}

	return
}


func copyNShards (origin [NShards] int) (target [NShards] int) {


	// https://stackoverflow.com/questions/30182538/why-can-not-i-duplicate-a-slice-with-copy-in-golang/30182622
	//copied.Shards = copy()
	//tmp := make([]int, NShards)

	//var tmp [NShards] int
	//copy(tmp, config.Shards)

	for i:=0; i<NShards; i++ {
		target[i] = origin[i]
	}
	return
}

func copyConfig (config Config) (copied Config) {

	copied = Config{}

	copied.Num = config.Num
	copied.Shards = copyNShards(config.Shards)
	copied.Groups = copyMap(config.Groups)

	return
}

func (config *Config) Rebalance() {
	gids := make([] int, len(config.Groups))
	i := 0
	for key, _ := range config.Groups {
		gids[i] = key
		i++
	}

	if len(gids)>0 {
		for i:=0; i<len(config.Shards); i++ {
			config.Shards[i] = gids[i%len(gids)]
		}
	} else {
		for i:=0; i<len(config.Shards); i++ {
			config.Shards[i] = 0
		}
	}


}

func (sm *ShardMaster) JoinHandler (op *Op) (interface{}) {

	// The Join RPC is used by an administrator to add new replica groups.
	// Its argument is a set of mappings from unique, non-zero replica group
	// identifiers (GIDs) to lists of server names. The shardmaster should
	// react by creating a new configuration that includes the new replica groups.
	// The new configuration should divide the shards as evenly as possible among
	// the full set of groups, and should move as few shards as possible to achieve
	// that goal. The shardmaster should allow re-use of a GID if it's not part of
	// the current configuration (i.e. a GID should be allowed to Join, then Leave,
	// then Join again).

	newServers := op.Args.(JoinArgs).Servers

	fmt.Println("Servers joined", newServers)
	fmt.Println("Servers joined2", copyMap(newServers))

	config := copyConfig(sm.configs[sm.currentConfigNum])// copy, not references!!

	for gid, servers := range newServers {
		_, exists := config.Groups[gid]
		if exists {
			fmt.Println("Error: JoinHandler(): GID=",gid,"already exists")
		} else {
			fmt.Println("JoinHandler(): GID=", gid,"added")
			config.Groups[gid] = servers
		}
	}

	config.Rebalance()

	sm.currentConfigNum++

	fmt.Println("config:",config)

	newConfig := copyConfig(config)//Config{sm.currentConfigNum, shards, 100}

	fmt.Println("newConfig:",newConfig)

	newConfig.Num = sm.currentConfigNum



	sm.configs = append(sm.configs, newConfig)

	fmt.Println("Configs:",sm.configs)

	return ""
}

func (sm *ShardMaster) LeaveHandler (op *Op) (interface{}) {


	// The Leave RPC's argument is a list of GIDs of previously joined groups.
	// The shardmaster should create a new configuration that does not include
	// those groups, and that assigns those groups' shards to the remaining groups.
	// The new configuration should divide the shards as evenly as possible among
	// the groups, and should move as few shards as possible to achieve that goal.

	GIDs := op.Args.(LeaveArgs).GIDs

	fmt.Println("GIDs leaves", GIDs)


	config := copyConfig(sm.configs[sm.currentConfigNum])// copy, not references!!


	for _, gid := range GIDs {
		_, exists := config.Groups[gid]
		if exists {
			delete(config.Groups, gid)
		} else {
			fmt.Println("Error: LeaveHandler(): GID=",gid,"does not exist")
		}
	}

	config.Rebalance()

	sm.currentConfigNum++

	fmt.Println("config:",config)

	newConfig := copyConfig(config)//Config{sm.currentConfigNum, shards, 100}

	fmt.Println("newConfig:",newConfig)

	newConfig.Num = sm.currentConfigNum

	sm.configs = append(sm.configs, newConfig)

	return ""
}

func (sm *ShardMaster) QueryHandler (op *Op) (interface{}) {


	// The Leave RPC's argument is a list of GIDs of previously joined groups.
	// The shardmaster should create a new configuration that does not include
	// those groups, and that assigns those groups' shards to the remaining groups.
	// The new configuration should divide the shards as evenly as possible among
	// the groups, and should move as few shards as possible to achieve that goal.

	Num := op.Args.(QueryArgs).Num

	//  If the number is -1 or bigger than the biggest known configuration number,
	// the shardmaster should reply with the latest configuration.
	if Num==-1 || Num>sm.currentConfigNum{
		Num = sm.currentConfigNum
	}

	fmt.Println("GIDs Query", Num)

	return copyConfig(sm.configs[Num])
}


//The Query RPC's argument is a configuration number. The shardmaster replies
// with the configuration that has that number. If the number is -1 or bigger
// than the biggest known configuration number, the shardmaster should reply
// with the latest configuration. The result of Query(-1) should reflect every
// Join, Leave, or Move RPC that the shardmaster finished handling before it
// received the Query(-1) RPC.

func (sm *ShardMaster) GetOpIDs(op *Op) (clientID ClientIndexType, requestID RequestIndexType){
	//var clientID ClientIndexType
	//var requestID RequestIndexType

	switch op.Type {
	case OP_TYPE_JOIN:
		clientID = op.Args.(JoinArgs).ClientID
		requestID = op.Args.(JoinArgs).RequestID
	case OP_TYPE_LEAVE:
		clientID = op.Args.(LeaveArgs).ClientID
		requestID = op.Args.(LeaveArgs).RequestID
	case OP_TYPE_MOVE:
		clientID = op.Args.(MoveArgs).ClientID
		requestID = op.Args.(MoveArgs).RequestID
	case OP_TYPE_QUERY:
		clientID = op.Args.(QueryArgs).ClientID
		requestID = op.Args.(QueryArgs).RequestID
	}

	return
}

func (sm *ShardMaster) StartOpRaft(op Op, opHandler fn) (wrongLeader bool, err Err, reply interface{}) {

	wrongLeader = false
	err = OK

	//fmt.Println("Start() called:", op)

	_, startTerm, isLeader := sm.rf.Start(op)

	if !isLeader {
		wrongLeader = true
		return
	}


	clientID, requestID := sm.GetOpIDs(&op)

	sm.mu.Lock()

	newChan := make(chan Op, 1)
	sm.insertToPendingOps(clientID, requestID, newChan)

	sm.mu.Unlock()

	select {
	case op2 := <- newChan:

		// Same as kvraft lab3.
		// to handle the case in which a leader has called Start()
		// for a Clerk's RPC, but loses its leadership before the request is committed to the log.
		// In this case you should arrange for the Clerk to re-send the request to other servers
		// until it finds the new leader.

		endTerm := sm.rf.GetCurrentTerm()
		if endTerm == startTerm {
			sm.mu.Lock()

			reply = opHandler(&op2)

			sm.mu.Unlock()

			err = OK

		} else {
			wrongLeader = true
		}

	case <- time.After( 600*time.Millisecond):
		err = "StartOpRaftTimesOut"
		fmt.Println("Warning: StartOpRaft() times out.")
	}
	return
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	op := Op{OP_TYPE_JOIN, *args}

	wrongLeader, err, _ := sm.StartOpRaft(op, sm.JoinHandler)

	reply.WrongLeader = wrongLeader
	reply.Err = err

	// TODO: execute the op

	//
	// The next configuration (created in response to a Join RPC) should be numbered 1, &c.
	// There will usually be significantly more shards than groups (i.e., each group will
	// serve more than one shard), in order that load can be shifted at a fairly fine granularity.

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	op := Op{OP_TYPE_LEAVE, *args}

	wrongLeader, err, _ := sm.StartOpRaft(op, sm.LeaveHandler)

	reply.WrongLeader = wrongLeader
	reply.Err = err

	// TODO: execute the op


}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	// The Move RPC's arguments are a shard number and a GID. The shardmaster
	// should create a new configuration in which the shard is assigned to the
	// group. The purpose of Move is to allow us to test your software. A Join
	// or Leave following a Move will likely un-do the Move, since Join and
	// Leave re-balance.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	op := Op{OP_TYPE_QUERY, *args}

	wrongLeader, err, r := sm.StartOpRaft(op, sm.QueryHandler)

	if !wrongLeader && err==OK {
		//*reply = r.(QueryReply)
		reply.Config = r.(Config)
	}

	reply.WrongLeader = wrongLeader
	reply.Err = err
}


func (sm *ShardMaster) MainLoop() {
	for {
		msg := <-sm.applyCh

		//fmt.Println("MainLoop(): ", msg)

		if msg.CommandValid {

			// Type assertion, see https://stackoverflow.com/questions/18041334/convert-interface-to-int-in-golang
			op := msg.Command.(Op)

			sm.mu.Lock()

			clientID, requestID := sm.GetOpIDs(&op)

			ch, ok := sm.pendingOps[clientID][requestID]

			if ok {
				ch <- op
			} else {

			}
			sm.mu.Unlock() // avoid deadlock
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
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.pendingOps = make(map[ClientIndexType] map[RequestIndexType] chan Op)
	sm.mostRecentWrite = make(map[ClientIndexType] RequestIndexType)

	sm.currentConfigNum = 0
	// The very first configuration should be numbered zero.
	// It should contain no groups, and all shards should be assigned to GID zero (an invalid GID).
	// This is true when make Config


	go sm.MainLoop()



	return sm
}
