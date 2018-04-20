package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"labgob"
	"time"
	"fmt"
	"sort"
)


const enable_debug_lab4a = false
const enable_debug_rebalance = false

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

	pendingOps	map[int] chan interface{}
	mostRecentWrite map[ClientIndexType] RequestIndexType
}

//func (sm *ShardMaster) insertToPendingOps(cid ClientIndexType, rid RequestIndexType, value chan interface{}) {
	// map[cid][rid] = value
//	if sm.pendingOps[cid]==nil {
//		sm.pendingOps[cid] = make(map[RequestIndexType] chan interface{})
//	}
//	sm.pendingOps[cid][rid] = value
//}


type OpType int

const (  // iota is reset to 0
	OP_TYPE_JOIN OpType = iota  //  == 0
	OP_TYPE_LEAVE OpType = iota  //  == 1
	OP_TYPE_MOVE OpType = iota  //  == 2
	OP_TYPE_QUERY OpType = iota //  == 3
)

// Critical: wired though, but the labrpc cannot pass Op with interface, the received Op is always nil.
//type Op struct {
	// Your data here.
//	Type OpType
//	Args interface{}
//}

// So I have to use the following one instead.

type Op struct {
	// Your data here.
	Type OpType
	ArgsJoin JoinArgs // Used if Type==OP_TYPE_JOIN
	ArgsLeave LeaveArgs // Used if Type==OP_TYPE_LEAVE
	ArgsMove MoveArgs // Used if Type==OP_TYPE_MOVE
	ArgsQuery QueryArgs // Used if Type==OP_TYPE_QUERY
}

//type OpResult struct {
//	Reply interface{}
//}

type fn func(op *Op) (interface{})


func copyMapTo (originalMap* map [int][]string, targetMap* map [int][]string) {

	// Clear the target map
	// targetMap = nil
	*targetMap = make(map [int][]string)

	// Copy from the original map to the target map
	for key, value := range *originalMap {
		(*targetMap)[key] = value
	}

	return
}


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

func copyNShardsTo (origin* [NShards] int, target* [NShards] int) {

	for i:=0; i<NShards; i++ {
		target[i] = origin[i]
	}
	return
}

func copyConfig (config* Config) (copied Config) {

	copied = Config{}
	copied.Groups = map[int][]string{}

	copied.Num = config.Num
	// copied.Shards = copyNShards(config.Shards)
	// copied.Groups = copyMap(config.Groups)
	copyNShardsTo(&config.Shards, &copied.Shards)
	copyMapTo(&config.Groups, &copied.Groups)

	return
}


// An important rule for the Rebalance is that the result should be identical whenever executed, and on whichever machine.

// Naive rebalance: slow, but passing all lab4a.
func (config *Config) Rebalance() {
	gids := make([] int, len(config.Groups))
	i := 0
	for key, _ := range config.Groups {
		gids[i] = key
		i++
	}

	sort.Ints(gids) // Important: this is critical: so the same results will obtained on different sdmaster server!!
	// Since the keys in map is not ordered!!!!!

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


func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// This one does not pass, max-min can be 2 or larger.
func (config *Config) JoinRebalance(newJoinID int) {

	gids := make([] int, len(config.Groups))
	i := 0
	for key, _ := range config.Groups {
		gids[i] = key
		i++
	}

	if len(gids)==1 {
		config.Rebalance()
		return
	}

	if enable_debug_rebalance {
		fmt.Println("JoinRebalance()")
	}

	sort.Ints(gids) // Important: this is critical: so the same results will obtained on different sdmaster server!!
	// Since the keys in map is not ordered!!!!!


	idxNew := -1
	for k:=0; k<len(gids); k++ {
		if gids[k]==newJoinID {
			idxNew=k
		}
	}

	// len(gids)==0 is impossible due to the new join.

	currentLoad := make([]int, len(gids))
	// The k-th one that gids[k]==newJOinID is not used.

	idx := make([]int, len(config.Shards))


	for s:=0; s<len(config.Shards); s++ {

		// find the k
		k := 0
		for k=0; k<len(gids); k++ {
			if gids[k]==config.Shards[s] {
				break
			}
		}
		idx[s] = k

		currentLoad[k]++

		if gids[k]==newJoinID {
			fmt.Println("Critical Error: this should be impossible!")
		}
	}

	if enable_debug_rebalance {
		fmt.Println("oldLoad:", currentLoad)
	}

	bn := len(config.Shards)/len(gids)
	// ln := len(config.Shards) - len(gids)*bn

	assignedToNew := 0


	for k:=0; k<len(gids); k++ {
		if gids[k]==newJoinID {
			continue
		}
		if assignedToNew >= bn+1 {
			break
		}
		if currentLoad[k] >= bn+2 {
			// Find the first ones in group gids[k]
			s := 0
			for ; s<len(config.Shards); s++ {
				if config.Shards[s]==gids[k]{
					config.Shards[s] = newJoinID// previously was gids[k]
					currentLoad[k]--
					assignedToNew++
					if currentLoad[k] < bn+2 {
						break
					}
					if assignedToNew >= bn+1 {
						goto finish
					}
				}
			}
		}
	}


	// The second round.
	for k:=0; k<len(gids); k++ {
		if gids[k]==newJoinID {
			continue
		}
		if assignedToNew >= bn+1 { // break condition
			break
		}
		if currentLoad[k] >= bn+1 {// Note here is different.
			// Find the first ones in group gids[k]
			s := 0
			for ; s<len(config.Shards); s++ {
				if config.Shards[s]==gids[k]{
					config.Shards[s] = newJoinID// previously was gids[k]
					currentLoad[k]--
					assignedToNew++
					if currentLoad[k] < bn+1 {// Note here is different.
						break
					}
					if assignedToNew >= bn+1 { // break condition
						goto finish
					}
				}
			}
		}
	}

	finish:

	currentLoad[idxNew] = assignedToNew

	if enable_debug_rebalance {
		fmt.Println("Finishing Load:", currentLoad)
	}

}

func (config *Config) LeaveRebalance(newLeaveID int) {
	gids := make([] int, len(config.Groups))
	i := 0
	for key, _ := range config.Groups {
		gids[i] = key
		i++
	}

	if len(gids)==0 {
		config.Rebalance()
		return
	}

	if enable_debug_rebalance {
		fmt.Println("LeaveRebalance()")
	}

	sort.Ints(gids) // Important: this is critical: so the same results will obtained on different sdmaster server!!
	// Since the keys in map is not ordered!!!!!


	currentLoad := make([]int, len(gids))
	idxShardToAssign := make([]int, 0)
	// The k-th one that gids[k]==newJOinID is not used.

	idx := make([]int, len(config.Shards))

	for s:=0; s<len(config.Shards); s++ {

		// find the k
		k := 0
		for k=0; k<len(gids); k++ {
			if gids[k]==config.Shards[s] {
				break
			}
		}
		idx[s] = k

		if k==len(gids) { // cannot find, since it is removed from Groups already.
			idxShardToAssign = append(idxShardToAssign, s)
		} else {
			currentLoad[k]++
		}
	}

	if enable_debug_rebalance {
		fmt.Println("oldLoad:", currentLoad)
	}

	bn := len(config.Shards)/len(gids)
	// ln := len(config.Shards) - len(gids)*bn

	idxAssigned := 0

	for k:=0; k<len(gids); k++ {
		if idxAssigned >= len(idxShardToAssign) {
			break
		}
		for currentLoad[k] <= bn-1 {
			if config.Shards[idxShardToAssign[idxAssigned]]!=newLeaveID {
				fmt.Println("Critical Error 4: this should be impossible!")
			}

			config.Shards[idxShardToAssign[idxAssigned]]= gids[k]
			currentLoad[k]++
			idxAssigned++

			if idxAssigned >= len(idxShardToAssign) {
				goto finish
			}
		}
	}

	// Note here is different.
	// The second round.
	for k:=0; k<len(gids); k++ {
		if idxAssigned >= len(idxShardToAssign) {
			break
		}
		for currentLoad[k] <= bn { // Note here is different.
			if config.Shards[idxShardToAssign[idxAssigned]]!=newLeaveID {
				fmt.Println("Critical Error 4: this should be impossible!")
			}

			config.Shards[idxShardToAssign[idxAssigned]]= gids[k]
			currentLoad[k]++
			idxAssigned++

			if idxAssigned >= len(idxShardToAssign) {
				goto finish
			}
		}
	}

finish:

	if enable_debug_rebalance {
		fmt.Println("Finishing Load:", currentLoad)
	}
}



// I think this one is not robust to reordering.
func (config *Config) JoinRebalance_old(newJoinID int) {
	gids := make([] int, len(config.Groups))
	i := 0
	for key, _ := range config.Groups {
		gids[i] = key
		i++
	}

	if len(gids)==1 {
		config.Rebalance()
		return
	}

	sort.Ints(gids) // Important: this is critical: so the same results will obtained on different sdmaster server!!
	// Since the keys in map is not ordered!!!!!


	// len(gids)==0 is impossible due to the new join.

	currentLoad := make([]int, len(gids))
	// The k-th one that gids[k]==newJOinID is not used.

	idx := make([]int, len(config.Shards))

	for s:=0; s<len(config.Shards); s++ {

		// find the k
		k := 0
		for k=0; k<len(gids); k++ {
			if gids[k]==config.Shards[s] {
				break
			}
		}
		idx[s] = k

		currentLoad[k]++

		if gids[k]==newJoinID {
			fmt.Println("Critical Error: this should be impossible!")
		}
	}

	fmt.Println("oldLoad:", currentLoad)

	newLoad := make([]int, len(gids))
	bn := len(config.Shards)/len(gids)
	ln := len(config.Shards) - len(gids)*bn // the first ln ones take one more
	for k:=0; k<len(gids); k++ {
		newLoad[k] = bn
		if k<ln {
			newLoad[k]++
		}
		if newLoad[k]>currentLoad[k] && gids[k]!=newJoinID {
			fmt.Println("Critical Error2: this should be impossible!")
		}
	}
	fmt.Println("newLoad:", newLoad)

	// makes currentLoad match new load
	for i:=0; i<len(config.Shards); i++ {
		k := idx[i]
		if currentLoad[k] > newLoad[k] { // this will not touch the neJoinID one
			config.Shards[i] = newJoinID// previously was gids[k]
			currentLoad[k]--
		}
	}

	fmt.Println("Finishing Load:", currentLoad)

	for k:=0; k<len(gids); k++ {
		if newLoad[k]!=currentLoad[k] && gids[k]!=newJoinID {
			fmt.Println("Fattal Error3: this should be impossible!")
		}
	}

}

// This one does not pass, max-min can be 2 or larger.
func (config *Config) JoinRebalance_old2(newJoinID int) {
	gids := make([] int, len(config.Groups))
	i := 0
	for key, _ := range config.Groups {
		gids[i] = key
		i++
	}

	if len(gids)==1 {
		config.Rebalance()
		return
	}

	sort.Ints(gids) // Important: this is critical: so the same results will obtained on different sdmaster server!!
	// Since the keys in map is not ordered!!!!!


	// len(gids)==0 is impossible due to the new join.

	currentLoad := make([]int, len(gids))
	// The k-th one that gids[k]==newJOinID is not used.

	idx := make([]int, len(config.Shards))

	for s:=0; s<len(config.Shards); s++ {

		// find the k
		k := 0
		for k=0; k<len(gids); k++ {
			if gids[k]==config.Shards[s] {
				break
			}
		}
		idx[s] = k

		currentLoad[k]++

		if gids[k]==newJoinID {
			fmt.Println("Critical Error: this should be impossible!")
		}
	}

	fmt.Println("oldLoad:", currentLoad)

	bn := len(config.Shards)/len(gids)
  	// ln := len(config.Shards) - len(gids)*bn

	for i:=0; i<len(config.Shards); i++ {
		k := idx[i]
		if currentLoad[k] > bn+1 { // this will not touch the neJoinID one
			config.Shards[i] = newJoinID// previously was gids[k]
			currentLoad[k]--
		}
	}

	fmt.Println("Finishing Load:", currentLoad)

}

// Untested, probably max-min is too large
func (config *Config) LeaveRebalance_old(newLeaveID int) {
	gids := make([] int, len(config.Groups))
	i := 0
	for key, _ := range config.Groups {
		gids[i] = key
		i++
	}

	if len(gids)==1 {
		config.Rebalance()
		return
	}

	sort.Ints(gids) // Important: this is critical: so the same results will obtained on different sdmaster server!!
	// Since the keys in map is not ordered!!!!!


	// len(gids)==0 is impossible due to the new join.

	currentLoad := make([]int, len(gids))
	idxLoadToAssign := make([]int, 0)
	// The k-th one that gids[k]==newJOinID is not used.

	idx := make([]int, len(config.Shards))

	for s:=0; s<len(config.Shards); s++ {

		// find the k
		k := 0
		for k=0; k<len(gids); k++ {
			if gids[k]==config.Shards[s] {
				break
			}
		}
		idx[s] = k

		if gids[k]==newLeaveID {
			idxLoadToAssign = append(idxLoadToAssign, k)
		}

		currentLoad[k]++

	}

	fmt.Println("oldLoad:", currentLoad)

	bn := len(config.Shards)/len(gids)
	// ln := len(config.Shards) - len(gids)*bn

	idxAssigned := 0
	for k:=0; k<len(gids); k++ {
		if idxAssigned >= len(idxLoadToAssign) {
			break
		}
		if currentLoad[k] < bn+1 {
			if config.Shards[idxLoadToAssign[idxAssigned]]!=newLeaveID {
				fmt.Println("Critical Error 4: this should be impossible!")
			}

			config.Shards[idxLoadToAssign[idxAssigned]]= gids[k]
			currentLoad[k]++
		}
	}

	fmt.Println("Finishing Load:", currentLoad)

}



// An important rule for the Handler is that the result should be identical whenever executed, and on whichever machine.

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

	// newServers := op.Args.(JoinArgs).Servers
	newServers := op.ArgsJoin.Servers

	if enable_debug_lab4a {
		fmt.Println("Servers joined", newServers)
	}
	//fmt.Println("Servers joined2", copyMap(newServers))

	newConfig := copyConfig(&sm.configs[sm.currentConfigNum])// copy, not references!!

	for gid, servers := range newServers {
		_, exists := newConfig.Groups[gid]
		if exists {
			fmt.Println("Warning: JoinHandler(): GID=",gid,"already exists")
		} else {
			//fmt.Println("JoinHandler(): GID=", gid,"added")
			newConfig.Groups[gid] = servers
			//newConfig.Rebalance()
			newConfig.JoinRebalance(gid)
		}
	}

	sm.currentConfigNum++

	//fmt.Println("config:",config)

	//fmt.Println("newConfig:",newConfig)

	newConfig.Num = sm.currentConfigNum

	sm.configs = append(sm.configs, newConfig)

	// fmt.Println("Configs:",sm.configs)

	return ""
}

func (sm *ShardMaster) LeaveHandler (op *Op) (interface{}) {


	// The Leave RPC's argument is a list of GIDs of previously joined groups.
	// The shardmaster should create a new configuration that does not include
	// those groups, and that assigns those groups' shards to the remaining groups.
	// The new configuration should divide the shards as evenly as possible among
	// the groups, and should move as few shards as possible to achieve that goal.

	//GIDs := op.Args.(LeaveArgs).GIDs
	GIDs := op.ArgsLeave.GIDs

	if enable_debug_lab4a {
		fmt.Println("GIDs leaves", GIDs)
	}

	newConfig := copyConfig(&sm.configs[sm.currentConfigNum])// copy, not references!!


	for _, gid := range GIDs {
		_, exists := newConfig.Groups[gid]
		if exists {
			delete(newConfig.Groups, gid)
			//newConfig.Rebalance()
			newConfig.LeaveRebalance(gid)
		} else {
			fmt.Println("Error: LeaveHandler(): GID=",gid,"does not exist")
		}
	}

	sm.currentConfigNum++

	//fmt.Println("config:",config)

	//fmt.Println("newConfig:",newConfig)

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

	//Num := op.Args.(QueryArgs).Num
	Num := op.ArgsQuery.Num

	if enable_debug_lab4a {
		fmt.Println("GIDs Query", Num)
	}

	//  If the number is -1 or bigger than the biggest known configuration number,
	// the shardmaster should reply with the latest configuration.
	if Num==-1{
		Num = sm.currentConfigNum
	}

	if Num>sm.currentConfigNum{
		fmt.Println("Warning: Query Config", Num, "is larger than currentConfigNum", sm.currentConfigNum)
		Num = sm.currentConfigNum
	}

	return copyConfig(&sm.configs[Num])
}

func (sm *ShardMaster) MoveHandler (op *Op) (interface{}) {

	// The Move RPC's arguments are a shard number and a GID. The shardmaster
	// should create a new configuration in which the shard is assigned to the
	// group. The purpose of Move is to allow us to test your software. A Join
	// or Leave following a Move will likely un-do the Move, since Join and
	// Leave re-balance.


	//GID := op.Args.(MoveArgs).GID
	//ShardID := op.Args.(MoveArgs).Shard
	GID := op.ArgsMove.GID
	ShardID := op.ArgsMove.Shard

	if enable_debug_lab4a {
		fmt.Println("Move GID=", GID, ", Shard=", ShardID)
	}

	newConfig := copyConfig(&sm.configs[sm.currentConfigNum])// copy, not references!!

	// config.Rebalance() // do not do rebalance
	newConfig.Shards[ShardID] = GID

	sm.currentConfigNum++

	newConfig.Num = sm.currentConfigNum

	sm.configs = append(sm.configs, newConfig)

	return ""
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

	/*
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
	*/

	switch op.Type {
	case OP_TYPE_JOIN:
		clientID = op.ArgsJoin.ClientID
		requestID = op.ArgsJoin.RequestID
	case OP_TYPE_LEAVE:
		clientID = op.ArgsLeave.ClientID
		requestID = op.ArgsLeave.RequestID
	case OP_TYPE_MOVE:
		clientID = op.ArgsMove.ClientID
		requestID = op.ArgsMove.RequestID
	case OP_TYPE_QUERY:
		clientID = op.ArgsQuery.ClientID
		requestID = op.ArgsQuery.RequestID
	}

	return
}

func (sm *ShardMaster) StartOpRaft(op Op, opHandler fn) (wrongLeader bool, err Err, reply interface{}) {

	wrongLeader = false
	err = OK

	// fmt.Println("Start() called:", op, "at server:", sm.me)

	index, startTerm, isLeader := sm.rf.Start(op)

	if !isLeader {
		wrongLeader = true
		return
	}


	//clientID, requestID := sm.GetOpIDs(&op)

	sm.mu.Lock()

	newChan := make(chan interface{}, 1)
	//sm.insertToPendingOps(clientID, requestID, newChan)

	sm.pendingOps[index] = newChan


	sm.mu.Unlock()

	select {
	case r := <- newChan:

		// Same as kvraft lab3.
		// TODO: actually I am not able to handle this situation:
		// to handle the case in which a leader has called Start()
		// for a Clerk's RPC, but loses its leadership before the request is committed to the log.
		// In this case you should arrange for the Clerk to re-send the request to other servers
		// until it finds the new leader.

		endTerm := sm.rf.GetCurrentTerm()
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


func (sm *ShardMaster) tryApplyOp(op *Op) (r interface{}) {


	// In the face of unreliable connections and server failures,
	// a Clerk may send an RPC multiple times until it finds a kvserver that replies positively.
	// If a leader fails just after committing an entry to the Raft log,
	// the Clerk may not receive a reply, and thus may re-send the request to another leader.
	// Each call to Clerk.Put() or Clerk.Append() should result in just a single execution,
	// so you will have to ensure that the re-send doesn't result in the servers executing
	// the request twice.

	clientID, requestID := sm.GetOpIDs(op)


	recentReqID, ok := sm.mostRecentWrite[clientID]

	if !ok || recentReqID<requestID {
		// Apply the non-duplicated Op to the database.

		switch op.Type {
		case OP_TYPE_JOIN:
			r = sm.JoinHandler(op)
		case OP_TYPE_LEAVE:
			r = sm.LeaveHandler(op)
		case OP_TYPE_MOVE:
			r = sm.MoveHandler(op)
		case OP_TYPE_QUERY:
			r = sm.QueryHandler(op)
		}
		// update the table
		sm.mostRecentWrite[clientID] = requestID


	} else {
		// the op is duplicated, but still reply ok
		//if debug_getputappend {
		//	fmt.Println("Duplicated to PutAppend", op)
		//}
	}



	return
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	//argsc := JoinArgs{copyMap(args.Servers), args.ClientID, args.RequestID}
	// TODO: is there necessary?

	//op := Op{OP_TYPE_JOIN, argsc}

	//op := Op{OP_TYPE_JOIN, *args}
	op := Op{}
	op.Type = OP_TYPE_JOIN
	op.ArgsJoin = *args

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

	//op := Op{OP_TYPE_LEAVE, *args}
	op := Op{}
	op.Type = OP_TYPE_LEAVE
	op.ArgsLeave = *args

	wrongLeader, err, _ := sm.StartOpRaft(op, sm.LeaveHandler)

	reply.WrongLeader = wrongLeader
	reply.Err = err

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	//op := Op{OP_TYPE_MOVE, *args}
	op := Op{}
	op.Type = OP_TYPE_MOVE
	op.ArgsMove = *args

	wrongLeader, err, _ := sm.StartOpRaft(op, sm.MoveHandler)

	reply.WrongLeader = wrongLeader
	reply.Err = err

}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	//op := Op{OP_TYPE_QUERY, *args}
	op := Op{}
	op.Type = OP_TYPE_QUERY
	op.ArgsQuery = *args

	wrongLeader, err, r := sm.StartOpRaft(op, sm.QueryHandler)

	if !wrongLeader && err==OK {
		//*reply = r.(QueryReply)
		config := r.(Config)
		reply.Config = copyConfig(&config)
	}

	reply.WrongLeader = wrongLeader
	reply.Err = err
}


func (sm *ShardMaster) MainLoop() {
	for {
		msg := <-sm.applyCh

		//fmt.Println("MainLoop(): ", msg)

		if msg.CommandValid {


			if msg.Command==nil {
				fmt.Println("Error: msg.Command==nil:", msg, "for server", sm.me, "i.e. raft server", sm.rf.GetServerID())
			} else {


			// Type assertion, see https://stackoverflow.com/questions/18041334/convert-interface-to-int-in-golang
			op := msg.Command.(Op)

			sm.mu.Lock()

			//clientID, requestID := sm.GetOpIDs(&op)

			reply := sm.tryApplyOp(&op)

			ch, ok := sm.pendingOps[msg.CommandIndex]

			if ok {
				go func() {
					ch <- reply
				}()
			} else {

			}

			sm.mu.Unlock() // avoid deadlock
			}
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
	//sm.pendingOps = make(map[ClientIndexType] map[RequestIndexType] chan interface{})
	sm.pendingOps = make(map[int] chan interface{})
	sm.mostRecentWrite = make(map[ClientIndexType] RequestIndexType)

	sm.currentConfigNum = 0
	// The very first configuration should be numbered zero.
	// It should contain no groups, and all shards should be assigned to GID zero (an invalid GID).
	// This is true when make Config


	go sm.MainLoop()



	return sm
}
