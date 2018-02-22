package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"


// import "bytes"
// import "labgob"


import (
	"fmt"
	"time"
	"log"
)


// wangyu: some design parameters:
// TODO: The tester requires that the leader send heartbeat RPCs no more than ten times per second.
// I found it on piazza, where does this requirement come from?
const HeartbeatSendPeriod = 100
const HeartbeatTimeout = 300
const ElectionTimeout = 300

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// wangyu:
// https://stackoverflow.com/questions/14426366/what-is-an-idiomatic-way-of-representing-enums-in-go

type ServerState int

const (  // iota is reset to 0
//	SERVER_STATE_BASE ServerState = iota
	SERVER_STATE_FOLLOWER ServerState = iota  //  == 0
	SERVER_STATE_CANDIDATE ServerState = iota  //  == 1
	SERVER_STATE_LEADER ServerState = iota  //  == 2
)

type ServerEvent int

const (

	// This design is exactly following Fig. 4.
	// If changed, should be consistent with isInTheStateFor()

	// Events for Follower
	EVENT_HEARTBEAT_TIMEOUT ServerEvent = iota

	// Events for Candidate
	EVENT_ELECTION_TIMEOUT ServerEvent = iota
	EVENT_ELECTION_WIN ServerEvent = iota
	EVENT_DISCOVER_LEADER_OR_NEW_TERM = iota

	// Events for Leader
	EVENT_DISCOVER_HIGHER_TERM_SERVER =iota
)

func (serverState *ServerState) stateShouldBe(expectedState ServerState) (bool) {

	return *serverState==expectedState
}

func (state *ServerState) isInTheStateFor(event ServerEvent) (bool) {
	switch event {
	case EVENT_HEARTBEAT_TIMEOUT:
		return state.stateShouldBe(SERVER_STATE_FOLLOWER)
	case EVENT_ELECTION_TIMEOUT:
		return state.stateShouldBe(SERVER_STATE_CANDIDATE)
	case EVENT_ELECTION_WIN:
		return state.stateShouldBe(SERVER_STATE_CANDIDATE)
	case EVENT_DISCOVER_LEADER_OR_NEW_TERM:
		return state.stateShouldBe(SERVER_STATE_CANDIDATE)
	case EVENT_DISCOVER_HIGHER_TERM_SERVER:
		return state.stateShouldBe(SERVER_STATE_LEADER)
	default:
		return false
	}
}

func assertEqual(a interface{}, b interface{}, message string) {
	// https://gist.github.com/samalba/6059502
	if a == b{
		return
	}

	if len(message) == 0 {
		message = fmt.Sprintf("Assert fails: %v != %v", a, b)
	}
	fmt.Println(message)
	log.Fatal(message)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Remember to initialize in Make()!!

	serverState ServerState

	// Persistent state on all servers:

	currentTerm int // latest term server has seen
		// (initialized to 0 on first boot, increases monotonically)

	votedFor int // candidateId that received vote in current
		// term (or null if none)

	// TODO: implement log[]
	//log[] //log entries; each entry contains command
		//for state machine, and term when entry
		//was received by leader (first index is 1)


	// Volatile state on all servers:

	commitIndex int // index of highest log entry known to be
	//committed (initialized to 0, increases
		//monotonically)

	// TODO: implemented after log[] done
	// lastApplied // index of highest log entry applied to state
		//machine (initialized to 0, increases
		// monotonically)


	// Volatile state on leaders:
	// (Reinitialized after election)
	// TODO: implemented after log[] done
	// nextIndex[] // for each server, index of the next log entry
		// to send to that server (initialized to leader
		// last log index + 1)

	// TODO: implemented after log[] done
	// matchIndex[] // for each server, index of highest log entry
		// known to be replicated on server
		// (initialized to 0, increases monotonically)


	// All timers

	// This is used by Follower to check if the leader if still alive
	timerHeartbeatMonitor time.Timer

	// This is used by Candidate
	timerElection time.Timer

	// This is used by Leader

	// All Channels

	// Channels must be initialized with make(chan type, xxx)
	// in Make() or it will not work!!
	eventsChan chan ServerEvent
	heartbeatsChan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = (rf.serverState==SERVER_STATE_LEADER)

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// All starts with *Upper* Letter!!!

	//Arguments:
	Term int //candidate’s term
	CandidateID int //candidate requesting vote
	// LastLogIndex //index of candidate’s last log entry (§5.4)
	// LastLogTerm //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// All starts with *Upper* Letter!!!

	//Results:
	Term int // currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

// wangyu

type AppendEntriesArgs struct {
	// All starts with *Upper* Letter!!!

	// Invoked by leader to replicate log entries (§5.3); also used as
	// heartbeat (§5.2).


	// Arguments:

	Term int//leader’s term

	LeaderID int//so follower can redirect clients

	//PrevLogIndex //index of log entry immediately preceding
	//new ones
	//PrevLogTerm //term of prevLogIndex entry
	//Entries[] //log entries to store (empty for heartbeat;
	//may send more than one for efficiency)
	//leaderCommit leader’s commitIndex
}

type AppendEntriesReply struct {
	// All starts with *Upper* Letter!!!

	// Results:
	Term int//currentTerm, for leader to update itself
	Success bool//true if follower contained entry matching
	// prevLogIndex and prevLogTerm

}

func (rf *Raft) AppendEntries (args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if args.Term < rf.currentTerm {
		// stale RPC
		fmt.Println("Server ", rf.me," received stale RPC from the (stale) leader", args.LeaderID,".")
		reply.Success = false
		return
	}

	// update current term from args.Term
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		// whenever set currentTerm, clear voteFor
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.mu.Unlock()

		rf.eventsChan <- EVENT_DISCOVER_HIGHER_TERM_SERVER
	}

	// fmt.Println("Server ", rf.me," received heartbeat msg from the leader.")

	// do not need to have it within goroutine
	rf.heartbeatsChan <- true

	// fmt.Println("Double check rf.heartbeatsChan is picked.")
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}


// wangyu

func (rf *Raft) stateFollowerToCandidate() {

	rf.mu.Lock()
	state := rf.serverState
	rf.mu.Unlock()

	if !state.stateShouldBe(SERVER_STATE_FOLLOWER){
		return
	}

	rf.mu.Lock()
	rf.serverState = SERVER_STATE_CANDIDATE
	rf.mu.Unlock()

	// TODO

	rf.mu.Lock()
	// whenever set currentTerm, clear voteFor
	rf.currentTerm = rf.currentTerm
	rf.votedFor = -1
	rf.mu.Unlock()

	go rf.startElection()
}

func (rf *Raft) stateCandidateToCandidate() {
	// i.e. restart election

	rf.mu.Lock()
	state := rf.serverState
	rf.mu.Unlock()

	if !state.stateShouldBe(SERVER_STATE_CANDIDATE){
		return
	}

	// This can be skipped since it does not change.
	// rf.mu.Lock()
	// rf.serverState = SERVER_STATE_CANDIDATE
	// rf.mu.Unlock()

	// TODO

	go rf.startElection()
}

func (rf *Raft) stateCandidateToLeader() {

	rf.mu.Lock()
	state := rf.serverState
	rf.mu.Unlock()

	if !state.stateShouldBe(SERVER_STATE_CANDIDATE){
		return
	}

	rf.mu.Lock()
	rf.serverState = SERVER_STATE_LEADER
	rf.mu.Unlock()

	// TODO
}

func (rf *Raft) stateCandidateToFollower() {

	rf.mu.Lock()
	state := rf.serverState
	rf.mu.Unlock()

	if !state.stateShouldBe(SERVER_STATE_CANDIDATE){
		return
	}

	rf.mu.Lock()
	rf.serverState = SERVER_STATE_FOLLOWER
	rf.mu.Unlock()

	// TODO
}

func (rf *Raft) stateLeaderToFollower() {

	rf.mu.Lock()
	state := rf.serverState
	rf.mu.Unlock()

	if !state.stateShouldBe(SERVER_STATE_LEADER){
		return
	}

	rf.mu.Lock()
	rf.serverState = SERVER_STATE_FOLLOWER
	rf.mu.Unlock()

	// TODO
}


func (rf *Raft) stateMachineLoop() {

	// All the state transitions happen in this function.
	// This implements the server state machine as shown in Fig. 4.

	fmt.Println("Raft Server #", rf.me," is online (Server State: ", rf.serverState,")!")

	for {

		// fmt.Println("stateMachineLoop(): waiting for events.", &rf.eventsChan)

		event := <- rf.eventsChan



		fmt.Println("stateMachineLoop(): event", event, "happens.")

		switch event {
		case EVENT_HEARTBEAT_TIMEOUT:
			rf.stateFollowerToCandidate()
		case EVENT_ELECTION_TIMEOUT:
			rf.stateCandidateToCandidate()
		case EVENT_ELECTION_WIN:
			rf.stateCandidateToLeader()
		case EVENT_DISCOVER_LEADER_OR_NEW_TERM:
			rf.stateCandidateToFollower()
		case EVENT_DISCOVER_HIGHER_TERM_SERVER:
			rf.stateLeaderToFollower()
		}
	}
}

func backendLoopFollower(rf *Raft) {
	if rf.serverState == SERVER_STATE_FOLLOWER {
		// This double check is necessary, due to possibility of package delay.


		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate:
		// convert to candidate.
	}
}

func mainLoopBackend(rf *Raft) {

	fmt.Printf("Raft Server Backend #%d is online!\n", rf.me)

	rf.mu.Lock()
		state := rf.serverState
	rf.mu.Unlock()

	switch state {
	case SERVER_STATE_FOLLOWER:
		backendLoopFollower(rf)
	case SERVER_STATE_CANDIDATE:
		// If election timeout elapses: start new election.
	case SERVER_STATE_LEADER:
		// (heartbeat) to each server; repeat during idle periods to
		// prevent election timeouts (§5.2)
	}

}

/*
switch state {
case SERVER_STATE_FOLLOWER:
case SERVER_STATE_CANDIDATE:
case SERVER_STATE_LEADER:
}
*/

func (rf *Raft) initHeartbeatMonitor() {

	rf.timerHeartbeatMonitor.Reset( HeartbeatTimeout*time.Millisecond)

	go func(rf *Raft) {

		//fmt.Println("This has been started!!!!!!!!!!!")

		for {
			select {
			case <- rf.heartbeatsChan:
				//fmt.Println("initHeartbeatMonitor(): Server ", rf.me, "recives heartbeat.")
				rf.timerHeartbeatMonitor.Reset( HeartbeatTimeout*time.Millisecond)
				rf.mu.Lock()
				state := rf.serverState
				rf.mu.Unlock()
				switch state {
				case SERVER_STATE_FOLLOWER:
				case SERVER_STATE_CANDIDATE:
					rf.eventsChan <- EVENT_DISCOVER_LEADER_OR_NEW_TERM
				case SERVER_STATE_LEADER:
					// do nothing
				}

			case <- rf.timerHeartbeatMonitor.C:

				//fmt.Println("Maybe I am locked here!!!!!!!!!!")
				rf.mu.Lock()
				state := rf.serverState
				rf.mu.Unlock()
				switch state {
				case SERVER_STATE_FOLLOWER:
					// Do not need to have it in a goroutine
					//go func() {
						//fmt.Println("See if I ever got here.", &rf.eventsChan)
						rf.eventsChan <- EVENT_HEARTBEAT_TIMEOUT
						fmt.Println("HeartbeatMonitor times out for server ",rf.me,".")
					//}()
				case SERVER_STATE_CANDIDATE:
					// do nothing, since already became candidate.
				case SERVER_STATE_LEADER:
					// do nothing
				}

			}
		}

	}(rf)
}

func (rf *Raft) initHeartbeatSender(){

	for i,p := range rf.peers {
		if i!= rf.me {
			go func(peer *labrpc.ClientEnd, index int) {

				for {
					rf.mu.Lock()
					term := rf.currentTerm
					state := rf.serverState
					rf.mu.Unlock()

					if state == SERVER_STATE_LEADER {
						// Only leader sends heartbeat signal

						args := AppendEntriesArgs{term, rf.me}

						// Put this in a go routine, which is optional.
						// So the delay in RPC is also considered as part of the network delay

						go func(args AppendEntriesArgs) {

							reply := AppendEntriesReply{-1, false}

							//fmt.Println("Server ", rf.me, "tries to send heartbeat msg to server ",index,".")

							ok := peer.Call("Raft.AppendEntries", &args, &reply)

							if ok {
								//fmt.Println("Server ", rf.me, "successfully sends heartbeat msg to server ",index,".")
								if reply.Success {
								}
							} else {
								fmt.Println("Server ", rf.me, "fails to send heartbeat msg to server ",index,".")
							}

						}(args)


					}
					time.Sleep(HeartbeatSendPeriod*time.Millisecond)
				}
			}(p,i) // i must be sent in.
		}
	}
}

func (rf *Raft) initElectionMonitor() {

	// TODO

	go func(rf *Raft) {


	}(rf)
}

func (rf *Raft) startElection() {
	assertEqual(rf.serverState, SERVER_STATE_CANDIDATE, "")

	rf.timerElection.Reset(ElectionTimeout*time.Millisecond)

	go func(me int) {
		<- rf.timerElection.C
		fmt.Println("Election times out for server ",me,".")
		rf.eventsChan <- EVENT_ELECTION_TIMEOUT
	}(rf.me)


	rf.mu.Lock()
	term := rf.currentTerm
	n := len(rf.peers)
	// state := rf.serverState
	rf.mu.Unlock()

	args := RequestVoteArgs{term, rf.me} // same for all

	onetimeVoteChan := make(chan bool, n)

	// Broadcast vote requests.
	for i,p := range rf.peers {
		if i != rf.me {
			go func(peer *labrpc.ClientEnd, index int) {

				reply := RequestVoteReply{-1, false}

				ok := peer.Call("Raft.RequestVote", &args, &reply)

				if ok {
					if reply.VoteGranted {
						onetimeVoteChan <- true
					} else {
						onetimeVoteChan <- false
					}
				} else {
					onetimeVoteChan <- false
				}
				// onetimeVoteChan is assigned last so no additional layer of goroutine is needed.
			}(p, i)
		}
	}

	// Collect results
	votesCount := 0

	for i:=0; i<n; i++ {
		voteGrant := <-onetimeVoteChan
		if voteGrant {
			votesCount++
		}
		if 2*votesCount>n { // cannot use n/2 on the right
			// wins the election.
			// TODO: should also pass an identifier of this election
			// note the term is insufficient as an identifier
			// since one server use the same term for sequential elections.
			rf.eventsChan <- EVENT_ELECTION_WIN
			break
		}

	}
	// TODO

	// Lose the election or timeout.
	rf.eventsChan <- EVENT_ELECTION_TIMEOUT

	/*
	// double check when election ends current term is still valid
	rf.mu.Lock()
	endTerm := rf.currentTerm
	rf.mu.Unlock()

	// TODO: there is an issue: what if the rf.currentTerm is modified in between.
	if endTerm == term {
		rf.eventsChan <- EVENT_ELECTION_WIN
	}
	*/
}

func (rf *Raft) initServerState(state ServerState) {
	rf.mu.Lock()

	rf.serverState = state

	rf.mu.Unlock()

	switch state {
	case SERVER_STATE_FOLLOWER:
		//rf
	case SERVER_STATE_CANDIDATE:
		// If election timeout elapses: start new election.
	case SERVER_STATE_LEADER:
		// (heartbeat) to each server; repeat during idle periods to
		// prevent election timeouts (§5.2)


	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.serverState = SERVER_STATE_FOLLOWER
	rf.currentTerm = 0
	// (initialized to 0 on first boot, increases monotonically)
	rf.votedFor = -1

	// rf.commitIndex // 2B


	rf.eventsChan = make( chan ServerEvent, 200)
	rf.heartbeatsChan = make( chan bool, 200)

	if me == 0 {
		//rf.initServerState(SERVER_STATE_LEADER)
		rf.initServerState(SERVER_STATE_FOLLOWER)
	} else {
		rf.initServerState(SERVER_STATE_FOLLOWER)
	}

	// TODO: check how to declare a timer without using it.
	rf.timerHeartbeatMonitor = *time.NewTimer(100000000000)
	rf.timerElection = *time.NewTimer(100000000000)
	rf.timerHeartbeatMonitor.Stop()
	rf.timerElection.Stop()

	rf.initHeartbeatSender()
	rf.initHeartbeatMonitor()

	rf.initElectionMonitor()

	go rf.stateMachineLoop()

	go mainLoopBackend( rf)



	// back to given code

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
