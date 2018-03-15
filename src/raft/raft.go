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
	"math/rand"
	"bytes"
	"labgob"
)



// The tester requires that the leader send heartbeat RPCs no more than ten times per second.
// I found it on piazza https://piazza.com/class/j9xqo2fm55k1cy?cid=99,
// why this requirement?
// Unit: millisecond.
const HeartbeatSendPeriod = 100//100

const HeartbeatTimeoutLower = 250//250
const ElectionTimeoutLower = 250//250
const HeartbeatTimeoutUpper = 400//700//400
const ElectionTimeoutUpper = 400//700//400
// wangyu: some design parameters:

const verbose = 0

const enable_lab_2b = true // must be true in final submission
const enable_lab_2c = true


const enable_incrementing_output = false
const enable_debug_lab_2b = false
const enable_debug_lab_2c = false

func randTimeBetween(Lower int64, Upper int64) (time.Duration) {
	r := time.Duration(Lower+rand.Int63n(Upper-Lower+1))*time.Millisecond
	// fmt.Println(r)
	return r
}

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

type ErrorTypeAppendEntries int

const (  // iota is reset to 0
	//	SERVER_STATE_BASE ServerState = iota
	ErrorType_AppendEntries_DEFAULT ErrorTypeAppendEntries = iota  //  == 0
	ErrorType_AppendEntries_NO_LOG_WITH_INDEX ErrorTypeAppendEntries = iota
	ErrorType_AppendEntries_LOG_WITH_WRONG_TERM ErrorTypeAppendEntries = iota
	ErrorTypeAppendEntries_REJECT_BY_HIGHER_TERM = iota  //  == 2
)

func (serverState *ServerState) toString() (string) {
	switch *serverState {
	case SERVER_STATE_FOLLOWER:
		return "FOLLOWER"
	case SERVER_STATE_CANDIDATE:
		return "CANDIDATE"
	case SERVER_STATE_LEADER:
		return "LEADER"
	default:
		return "No such server state"
	}
}

type ServerEvent int

const (

	// This is no longer actually used by the code,
	// but to make the concept of events clear.

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


func (event *ServerEvent) toString() (string) {
	switch *event {
	case EVENT_HEARTBEAT_TIMEOUT:
		return "Event Follow Heartbeat Timeout"
	case EVENT_ELECTION_TIMEOUT:
		return "Event Candidate Election Timeout"
	case EVENT_ELECTION_WIN:
		return "Event Candidate Election Win"
	case EVENT_DISCOVER_LEADER_OR_NEW_TERM:
		return "Event Candidate Discover Leader or New Term"
	case EVENT_DISCOVER_HIGHER_TERM_SERVER:
		return "Event Leader Discover Higher Term Server"
	default:
		return "No such event"
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

// used if enable_lab_2b:
type LogEntry struct {
	// upper case for potential used by RPC call
	// LogIndex int
	LogTerm int
	Command interface{}
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


	// used if enable_lab_2b:

	baseIndex int // the displacement of index, when snapshot it used.

	// TODO: implement log[]
	log [] LogEntry//log entries; each entry contains command
		//for state machine, and term when entry
		//was received by leader (first index is 1)

	// Volatile state on all servers:

	commitIndex int // index of highest log entry known to be
	//committed (initialized to 0, increases
		//monotonically)

	lastApplied int// index of highest log entry applied to state
		//machine (initialized to 0, increases
		// monotonically)


	// Volatile state on leaders:
	// TODO: (Reinitialized after election)
	nextIndex [] int// for each server, index of the next log entry
		// to send to that server (initialized to leader
		// last log index + 1)

	matchIndex[] int // for each server, index of highest log entry
		// known to be replicated on server
		// (initialized to 0, increases monotonically)


	// All timers

	// This is used by Follower to check if the leader if still alive
	// timerHeartbeatMonitor time.Timer


	// This is used by Leader

	// All Channels

	// Channels must be initialized with make(chan type, xxx)
	// in Make() or it will not work!!
	// this two channels are protected by locks, and does not need to be in a separate goroutine.

	heartbeatsChan chan bool

	heartbeatsSendChan [] chan bool // this is used for leader only

	// I have got rid of the use of eventsChan, and use
	// program call instead. My eventsChan was a misuse of
	// channel and it will suffer from the data race problem.
	// eventsChan chan ServerEvent

	// used if enable_lab_2b:
	applyChan chan ApplyMsg // used by every server
	commitCheckerTriggerChan chan bool // used by leader, send a signal to it whenever any matchIndex is updated.

	peerBeingAppend []*sync.Mutex
 	// cannot have more than one tryAppendEntriesRecursively running at the same time

 	applyStack []ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.serverState==SERVER_STATE_LEADER)
	rf.mu.Unlock()

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

	if enable_lab_2c {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.log)
		data := w.Bytes()
		rf.persister.SaveRaftState(data)
	}
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

	// The labgob encoder you'll use to encode persistent state only
	// saves fields whose names start with upper case letters. Using
	// small caps for field names is a common source of mysterious bugs.

	if enable_lab_2c {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)

		var currentTerm int
		var votedFor int
		var log [] LogEntry


		if d.Decode(&currentTerm) != nil ||
			d.Decode(&votedFor) != nil ||
				d.Decode(&log) != nil {
				//fmt.Println("readPersist() fails.")
		} else {

			rf.mu.Lock()

			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.log = log
			//fmt.Println("readPersist() succeeds.")

			//fmt.Println("currentTerm:", rf.currentTerm)
			//fmt.Println("votedFor:", rf.votedFor)
			//fmt.Println("log:")
			//for i := 0; i < len(rf.log); i++ {
			//	fmt.Println(rf.log[i])
			//}


			rf.mu.Unlock()
		}

	}
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

	// used if enable_lab_2b
	LastLogIndex int//index of candidate’s last log entry (§5.4)
	LastLogTerm int//term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// All starts with *Upper* Letter!!!

	//Results:
	Term int // currentTerm, for candidate to update itself, so this is the term of voteGrater, not candidate.
	VoteGranted bool //true means candidate received vote
}

func (rf *Raft) getLogDisp(index int) (int) {
	// this can be -1 since index may be
	return index-rf.baseIndex
}

// TODO: it is possible that snapshoted log is no longer in rf.log, fix this in lab 3b
func (rf *Raft) getLogTerm(index int) (int) {
	if index == -1 {
		return -1
	} else {
		return rf.log[rf.getLogDisp(index)].LogTerm
	}
}

func (rf *Raft) getLog(index int) (LogEntry) {
	return rf.log[rf.getLogDisp(index)]
}

func (rf *Raft) getLastLogTerm() (int) {
	if len(rf.log)>0 {
		return rf.log[len(rf.log)-1].LogTerm
	} else { // no log entry
		return -1
	}
}

func (rf *Raft) getLastLogIndex() (int) {
	// it can return -1 when no log entry and rf.baseIndex
	return len(rf.log)-1+rf.baseIndex
}

func (rf *Raft) existLogIndex(index int) (bool) {
	return index <= rf.getLastLogIndex()
}

func (rf *Raft) deleteLogSince(index int) () {
	// delete the log whose index is index, and the ones after it.
	rf.log = rf.log[0:(rf.getLogDisp(index))]
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// Receiver implementation:

	rf.mu.Lock()
	defer rf.mu.Unlock()

	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// 1. Reply false if term < currentTerm (§5.1)
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		if enable_incrementing_output {
			fmt.Println("Server", rf.me, "RequestVote(): Increamenting current term from", rf.currentTerm, "to", args.Term)
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if enable_lab_2c {
			needPersist = true
		}
		switch rf.serverState {
		case SERVER_STATE_FOLLOWER:
			// do nothing
		case SERVER_STATE_CANDIDATE:
			//rf.eventsChan <- EVENT_DISCOVER_LEADER_OR_NEW_TERM
			rf.stateCandidateToFollower()
		case SERVER_STATE_LEADER:
			//rf.eventsChan <- EVENT_DISCOVER_HIGHER_TERM_SERVER
			rf.stateLeaderToFollower()
		}
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor != -1 && rf.votedFor!=args.CandidateID {
		// voted already
		reply.VoteGranted = false
		return
	}
	if enable_lab_2b {
		// Raft determines which of two logs is more up-to-date
		// by comparing the index and term of the last entries in the
		// logs. If the logs have last entries with different terms, then
		// the log with the later term is more up-to-date. If the logs
		// end with the same term, then whichever log is longer is
		// more up-to-date.
		// Note: LastLogTerm instead of args.Term / rf.currentTerm should be used here!
		myLastLogTerm := rf.getLastLogTerm()
		myLastLogIndex := rf.getLastLogIndex()
		// it does not matter if any term/index are -1
		candidate_log_more_or_equal_update_to_date :=
			args.LastLogTerm > myLastLogTerm || (args.LastLogTerm==myLastLogTerm && args.LastLogIndex>=myLastLogIndex)
		if !candidate_log_more_or_equal_update_to_date {
			reply.VoteGranted = false
			return
		}
	}

	// Now it has not voted for anyone for current term.
	reply.VoteGranted = true

	rf.votedFor = args.CandidateID
	if enable_lab_2c {
		needPersist = true
	}

}

// wangyu

type AppendEntriesArgs struct {
	// All starts with *Upper* Letter!!!

	// Invoked by leader to replicate log entries (§5.3); also used as
	// heartbeat (§5.2).


	// Arguments:

	Term int//leader’s term

	LeaderID int//so follower can redirect clients

	// used if enable_lab_2b
	PrevLogIndex int//index of log entry immediately preceding
	//new ones
	PrevLogTerm int//term of prevLogIndex entry
	Entries[] LogEntry//log entries to store (empty for heartbeat;
	//may send more than one for efficiency)
	LeaderCommit int //leader’s commitIndex
}

type AppendEntriesReply struct {
	// All starts with *Upper* Letter!!!

	// Results:
	Term int//currentTerm, for leader to update itself
	Success bool//true if follower contained entry matching prevLogIndex and prevLogTerm
	//

	// If Success==false, return the additional information
	Error ErrorTypeAppendEntries
	// useful only if Success==false

	ConflictTerm int
	FirstIndexOfConflictTerm int
	// used only if Error==ErrorType_AppendEntries_LOG_WITH_WRONG_TERM

	LastLogIndex int
	// used only if Error==ErrorType_AppendEntries_NO_LOG_WITH_INDEX
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}


func (rf *Raft) AppendEntries (args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.Term = rf.currentTerm

	needPersist := false
	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		// stale RPC
		if verbose >= 1 {
			fmt.Println("Server ", rf.me," received stale RPC from the (stale) leader", args.LeaderID,".")
		}
		reply.Error = ErrorTypeAppendEntries_REJECT_BY_HIGHER_TERM
		reply.Success = false
		return
	}

	// starting from here
	// args.Term >= rf.currentTerm is true

	// TODO: consider replace it with a checkTermNoLessThan

	// update current term from args.Term
	if args.Term > rf.currentTerm {
		if enable_incrementing_output {
			fmt.Println("Server", rf.me, "AppendEntries(): Increamenting current term from", rf.currentTerm, "to", args.Term)
		}
		// whenever set currentTerm, clear voteFor
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if enable_lab_2c {
			needPersist = true
		}

		if rf.serverState == SERVER_STATE_LEADER {
			//rf.eventsChan <- EVENT_DISCOVER_HIGHER_TERM_SERVER
			rf.stateLeaderToFollower()
		}
	}


	// no need to add lock, since we only do rf.heartbeatsChan<-XXX within locks()
	//fmt.Println("initHeartbeatMonitor(): Server ", rf.me, "receives heartbeat.")
	// rf.timerHeartbeatMonitor.Reset( HeartbeatTimeout*time.Millisecond)
	state := rf.serverState
	switch state {
	case SERVER_STATE_FOLLOWER:
	case SERVER_STATE_CANDIDATE:
		//rf.eventsChan <- EVENT_DISCOVER_LEADER_OR_NEW_TERM
		rf.stateCandidateToFollower()
	case SERVER_STATE_LEADER:
		// do nothing
	}

	if enable_lab_2b {
		if true {//if len(args.Entries)>0 {
			// not a heartbeat entry

			// TODO: think about if always args.PrevLogIndex>=0
			if args.PrevLogIndex > rf.getLastLogIndex() || rf.getLogTerm(args.PrevLogIndex)!=args.PrevLogTerm {
				// 2. Reply false if log doesn’t contain an entry at prevLogIndex
				// whose term matches prevLogTerm (§5.3)
				if enable_debug_lab_2b {
					fmt.Println("Server", rf.me, "AppendEntries: Case 2 false")
				}
				reply.Success = false

				if args.PrevLogIndex <= rf.getLastLogIndex() {
					reply.Error = ErrorType_AppendEntries_LOG_WITH_WRONG_TERM
					reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
					firstIndex := args.PrevLogIndex
					for index:= firstIndex; index>=1; index-- {
						if rf.getLogTerm(index) < reply.ConflictTerm {
							break
						}
						if rf.getLogTerm(index) == reply.ConflictTerm {
							firstIndex = index
						}
					}
					reply.FirstIndexOfConflictTerm = firstIndex
				} else {
					// no such index, let alone its term
					reply.Error = ErrorType_AppendEntries_NO_LOG_WITH_INDEX
					reply.LastLogIndex = rf.getLastLogIndex()
				}


			} else {
				reply.Success = true
				// so it will not trigger a retreat to recursively appendentries at the leader side

				// 3. If an existing entry conflicts with a new one (same index
				// but different terms), delete the existing entry and all that
				// follow it (§5.3)
				// 4. Append any new entries not already in the log
				// Entries are in reverse order
				n := len(args.Entries)
				for i := 0; i < n; i++ {
					if (rf.existLogIndex(args.PrevLogIndex+1+i)) { // if there is an existing entry there
						if rf.getLogTerm(args.PrevLogIndex+1+i) != args.Entries[i].LogTerm {// if conflicts

							if enable_debug_lab_2b {
								fmt.Println("Server", rf.me, "AppendEntries(): conflicting log detected.")
							}

							rf.deleteLogSince(args.PrevLogIndex+1+i)
							rf.log = append(rf.log, args.Entries[i])
							if enable_lab_2c {
								needPersist = true
							}


							if enable_debug_lab_2b && false {
								for i := 0; i < len(args.Entries); i++ {
									fmt.Println("Entry appended", args.Entries[i])
								}
								fmt.Println("Current log:")
								for i := 0; i < len(rf.log); i++ {
									fmt.Println(rf.log[i])
								}
							}


						} else {
							// no need to append
							// TODO: double check the command are same

							// if there is an existing entry there (same index and same term), the existing entry must be
							// identical to the one in Entries, as guaranteed by the Leader Append-only Property and that
							// one term has at most one leader.

							//fmt.Println("Server",rf.me,"AppendEntries(): no need to append entries)" )
							//fmt.Println("Server",rf.me,"  existing log:",rf.getLog(args.PrevLogIndex+1+i))
							//fmt.Println("Server",rf.me,"  incoming log:",args.Entries[i])
						}
					} else {
						if enable_debug_lab_2b {
							fmt.Println("Server", rf.me, "AppendEntries(): appending log index=", args.PrevLogIndex+1+i, "command", args.Entries[i].Command, ".")
							if args.Entries[i].Command == rf.getLog(rf.getLastLogIndex()).Command {
								fmt.Println("Debug point5")
							}
						}

						rf.log = append(rf.log, args.Entries[i])
						if enable_lab_2c {
							needPersist = true
						}

						if enable_debug_lab_2b && false{
							for i := 0; i < len(args.Entries); i++ {
								fmt.Println("Entry appended", args.Entries[i])
							}
							fmt.Println("Current log:")
							for i := 0; i < len(rf.log); i++ {
								fmt.Println(rf.log[i])
							}
						}
					}
				}

				rf.deleteLogSince(args.PrevLogIndex+1+n)
				// this is important, this ensures no redundant entries behind
				// so the index_of_last_new_entry used later is correct.

				// 5. If leaderCommit > commitIndex, set commitIndex =
				// 	min(leaderCommit, index of last new entry)
				if args.LeaderCommit > rf.commitIndex {

					oldCommitIndex := rf.commitIndex

					index_of_last_new_entry := rf.getLastLogIndex()
					// this is not only true for non-heartbeat
					// but also for any appendentries that succeed to append (even empty) entries.
					// index_of_last_new_entry is pretty hard to get right

					rf.commitIndex = min(args.LeaderCommit, index_of_last_new_entry) // index of last new entry

					// Send newly committed entries to the applyCh

					msgs := make([]ApplyMsg, 0)

					for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
						msg := ApplyMsg{i>0, rf.getLog(i).Command, i}
						msgs = append(msgs, msg)


						if enable_debug_lab_2b {
							fmt.Println("Server", rf.me, "committed entry", msg)
						}

						rf.applyStack = append(rf.applyStack, msg)
					}

					/*go func(msgs []ApplyMsg) {
						//  order is perserved
						for i:=0; i<len(msgs); i++ {
								// the log with index 0 is a place holder and do not need to be applied
								fmt.Println("Server", rf.me, "apply msg",msgs[i].Command)
								rf.applyChan <- msgs[i]
						}
					}(msgs)*/
				}


			}
		} else {
			// this is a heartbeat
			reply.Success = true
		}

	}


	// Put this last seems a good choice for me.
	// do not need to have it within goroutine
	//go func() { // change this for lab2b
		rf.heartbeatsChan <- true
	//}()

	// whenever this channel is sent, the heartbeats channel will be reset.
	// fmt.Println("Double check rf.heartbeatsChan is picked.")

	// fmt.Println("Server ", rf.me," received heartbeat msg from the leader.")


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


// wangyu:
// Used only for Lab3 and later.
func (rf *Raft) GetCurrentTerm() int {
	term := -1
	rf.mu.Lock()
	term = rf.currentTerm
	rf.mu.Unlock()
	return term
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


	if enable_lab_2b {

		isLeader = false

		rf.mu.Lock()
		defer rf.mu.Unlock()

		defer rf.persist()


		term = rf.currentTerm
		isLeader = (rf.serverState == SERVER_STATE_LEADER)

		if isLeader {

			index = rf.getLastLogIndex() + 1
			entry := LogEntry{rf.currentTerm,command}
			rf.log = append(rf.log, entry)
			if enable_lab_2c {
				//always persist//rf.persist()
			}

			if enable_debug_lab_2b {
				fmt.Println("Server", rf.me, "Start(): rf.log size is:", len(rf.log))
				fmt.Println("new log entry: ", entry)
			}

			// Notify all other servers.
			//for i,_ := range rf.peers {
			//	if i!= rf.me {
			//		go rf.trySendAppendEntriesRecursively(i, rf.currentTerm)// i is sent in.
			//	}
			//}
			// do not append until the next heartbeat.
		}


	}

	return index, term, isLeader
}


func (rf *Raft) checkTermNoLessThan(term int) (bool) {
	termNoLessThan := true
	// TODO optional: consider make this a checkTermNoLessThan fun
	if rf.currentTerm < term {
		termNoLessThan = false
		if enable_incrementing_output {
		//	fmt.Println("Server", rf.me, "trySendAppendEntriesRecursively(): Increamenting current term from", rf.currentTerm, "to", args.Term)
		}
		rf.currentTerm = term
		rf.votedFor = -1
		switch rf.serverState {
		case SERVER_STATE_FOLLOWER:
		case SERVER_STATE_CANDIDATE:
			rf.stateCandidateToFollower()
		case SERVER_STATE_LEADER:
			rf.stateLeaderToFollower()
		}
		if enable_lab_2c {
			rf.persist()
		}
	}
	return termNoLessThan
}

// used if enable_lab_2b
func (rf *Raft) trySendAppendEntriesRecursively(serverIndex int, termWhenStarted int){


	//fmt.Println("trySendAppendEntriesRecursively(): server",serverIndex,"step 0.")

	rf.peerBeingAppend[serverIndex].Lock()
	// make sure at most one tryAppendEntriesRecursively running for the serverIndex-th server

	// entries := make( [] LogEntry, 0)

	// Lock is used in a rather unique way:
	// everything is protected except the RPC call.

	rf.mu.Lock()

	//if rf.serverState == SERVER_STATE_LEADER {
	//fmt.Println("trySendAppendEntriesRecursively(): server",serverIndex,"step 1.")
	//}

	for {

		//fmt.Println(".")

		if rf.currentTerm == termWhenStarted && rf.serverState == SERVER_STATE_LEADER {

			//term := rf.currentTerm
			//state := rf.serverState
			prevLogIndex := rf.nextIndex[serverIndex]-1
			prevLogTerm := rf.getLogTerm(prevLogIndex)
			leaderCommit := rf.commitIndex

			// entries are in reverse index order!

		 	// https://codingair.wordpress.com/2014/07/18/go-appendprepend-item-into-slice/
			// entries = append( []LogEntry{rf.getLog(prevLogIndex+1)}, entries...)
			entries := rf.log[(prevLogIndex+1):]

			// Remember to decode entries in reverse order
			// TODO optional: optimize to avoid sending entries before serverIndex converges

			args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, leaderCommit}

			// TODO optional: optimize this later
			rf.mu.Unlock()

			if enable_debug_lab_2b {
				//if (args.PrevLogIndex+1) == 5 && args.Entries[0].Command == 105 {
				//	fmt.Println("Debug point2")
				//}
			}

			reply := AppendEntriesReply{}
			ok := rf.peers[serverIndex].Call("Raft.AppendEntries", &args, &reply)

			go func() {
				rf.heartbeatsSendChan[serverIndex] <- true
			}()

			rf.mu.Lock()

			if rf.currentTerm == termWhenStarted && rf.serverState == SERVER_STATE_LEADER {
				// necessary to check if the RPC issuer is still the same term leader in charge.

				if ok {
					if reply.Success {

						if verbose >0 || enable_debug_lab_2b {
							fmt.Println("Leader", rf.me, "succeeded to append entries [", args.PrevLogIndex+1,
								",", args.PrevLogIndex+1+len(entries), ") to server", serverIndex, ".")
							for i := 0; i < len(entries); i++ {
								fmt.Println("Entry", entries[i])
							}
						}

						rf.nextIndex[serverIndex] += len(entries)
						rf.matchIndex[serverIndex] = rf.nextIndex[serverIndex] - 1

						if enable_debug_lab_2c {
							if rf.nextIndex[serverIndex]-1 > rf.getLastLogIndex() {
								fmt.Println("Debug point")
							}
						}
						// try to commit entries if possible
						go func() {
							rf.commitCheckerTriggerChan <- true
						}()

						break
					} else {

						if verbose > 0 {
							fmt.Println("Leader", rf.me, "failed to append entries to server", serverIndex, ".")
						}

						if reply.Error == ErrorType_AppendEntries_NO_LOG_WITH_INDEX ||
							reply.Error == ErrorType_AppendEntries_LOG_WITH_WRONG_TERM {

							// Retreat
							if enable_debug_lab_2b {
								fmt.Println("Leader", rf.me, "failed to append entries", args.PrevLogIndex+1, "to server", serverIndex, ".")
							}


							if false {
								// This is my original implementation of pass lab2b,
								// retreating by 1.
								rf.nextIndex[serverIndex] = rf.nextIndex[serverIndex] - 1
							} else {

								if reply.Error == ErrorType_AppendEntries_LOG_WITH_WRONG_TERM {
									// Retreat more than 1 as suggested in the lecture
									nI:=rf.nextIndex[serverIndex]-1
									for ; nI>=1; nI-- {
										if rf.getLogTerm(nI) == reply.ConflictTerm {
											// no need to retreat more
											rf.nextIndex[serverIndex] = nI
											break
										}
										if rf.getLogTerm(nI) < reply.ConflictTerm {
											// leader does not have entries with conflicting term
											rf.nextIndex[serverIndex] = reply.FirstIndexOfConflictTerm
											break
										}
									}
									if nI==0 {
										// it also means leader does not have entries with conflicting term
										// but we can be more aggresive since all rf.nextIndex > reply.ConflictTerm
										rf.nextIndex[serverIndex] = 1 //reply.FirstIndexOfConflictTerm
									}
									if enable_debug_lab_2c {
										if rf.nextIndex[serverIndex] == 0 {
											fmt.Println("Error: this should never happen")
										}
									}
								} else { // reply.Error == ErrorType_AppendEntries_NO_LOG_WITH_INDEX
									rf.nextIndex[serverIndex] = reply.LastLogIndex + 1
								}
								if enable_debug_lab_2c {
									if rf.nextIndex[serverIndex]-1 > rf.getLastLogIndex() {
										fmt.Println("Debug point")
									}
								}
							}
						} else if reply.Error == ErrorTypeAppendEntries_REJECT_BY_HIGHER_TERM {

							// This is actual *not* optional and missing this part leads to deadlock.
							// Especially, for TestFailAgree2B, an offline server becoming back online needs to
							// use this to force the leader quit, and re-elect later.

							if !rf.checkTermNoLessThan(reply.Term) {
								break
							}
						} else {
							if enable_debug_lab_2b {
								fmt.Println("ERROR: not implemented!!!!!!!!!!!!!!!!")
							}
							break
						}


					}
				} else {
					if verbose >0 || enable_debug_lab_2b {
						fmt.Println("Leader", rf.me, "failed to call Raft.AppendEntries", args.PrevLogIndex+1, "to server", serverIndex, ".")
					}
					break// important to break here.
				}
			} else {
				break
			}

		} else{
			// This goroutine is already out-of-date.
			break
		}



		// time.Sleep(10*time.Millisecond) // limit the speed to send recursively. TODO: make the number better.
		// somehow the limit makes  TestFigure8Unreliable2C fails for me.

	}


	rf.mu.Unlock()
	rf.peerBeingAppend[serverIndex].Unlock()



	//fmt.Println("trySendAppendEntriesRecursively(): server",serverIndex,"step 2.")

}


// peers is copied by values (hopefully that is what happens)
func broadcastAppendEntries(peers []labrpc.ClientEnd, me int, args AppendEntriesArgs){

	for i,p := range peers {
		if i!= me {
			go func(peer *labrpc.ClientEnd, index int) {

				reply := AppendEntriesReply{}
				// non default values like {-1, false} leads to
				// labgob warning: Decoding into a non-default variable/field Term may not work
				// https://piazza.com/class/j9xqo2fm55k1cy?cid=75

				//fmt.Println("Server ", rf.me, "tries to send heartbeat msg to server ",index,".")

				ok := peer.Call("Raft.AppendEntries", &args, &reply)

				if ok {
					if verbose >= 2 {
						fmt.Println("Server ", me, "successfully sends heartbeat msg to server ", index, ".")
					}
					if reply.Success {
						// not really need to use the reply value
					}
				} else {
					if verbose >= 2 {
						fmt.Println("Server ", me, "fails to send heartbeat msg to server ", index, ".")
					}
				}

			}(&p,i) // i must be sent in.
		}
	}
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

	if verbose >= 1 {
		fmt.Println("stateFollowerToCandidate(): lock status", rf.mu)
	}

	state := rf.serverState

	if !state.stateShouldBe(SERVER_STATE_FOLLOWER){
		return
	}

	rf.serverState = SERVER_STATE_CANDIDATE

	if enable_incrementing_output {
		fmt.Println("Server", rf.me, "stateFollowerToCandidate(): Increamenting current term from", rf.currentTerm, "by 1")
	}
	// whenever set currentTerm, clear voteFor
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = -1
	if enable_lab_2c {
		// persist later
	}

	termWhenInit := rf.currentTerm
	rf.votedFor = rf.me
	if enable_lab_2c {
		rf.persist()
	}
	go rf.startElection(termWhenInit)
}

func (rf *Raft) stateCandidateToCandidate() {
	// i.e. restart election
	if verbose >= 1 {
		fmt.Println("stateCandidateToCandidate(): lock status", rf.mu)
	}

	state := rf.serverState

	if !state.stateShouldBe(SERVER_STATE_CANDIDATE){
		return
	}

	// It seems everything below should be same as
	// stateFollowerToCandidate(), including incrementing
	// the currentTerm

	rf.serverState = SERVER_STATE_CANDIDATE

	if enable_incrementing_output {
		fmt.Println("Server", rf.me, "stateCandidateToCandidate(): Increamenting current term from", rf.currentTerm, "by 1")
	}
	// whenever set currentTerm, clear voteFor
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = -1
	if enable_lab_2c {
		// persist later //rf.persist()
	}

	termWhenInit := rf.currentTerm
	rf.votedFor = rf.me
	if enable_lab_2c {
		rf.persist()
	}
	go rf.startElection(termWhenInit) // this is necessary to avoid cycle of competing
}

func (rf *Raft) stateCandidateToLeader() {
	if verbose >= 1 {
		fmt.Println("stateCandidateToLeader(): lock status", rf.mu)
	}

	state := rf.serverState

	if !state.stateShouldBe(SERVER_STATE_CANDIDATE){
		return
	}

	rf.serverState = SERVER_STATE_LEADER

	// No need to increment currentTerm, see e.g. Fig. 5.

	if enable_lab_2b {
		// (Re-)Initialize the indices

		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		for i,_ := range rf.peers {
			// Initialized to the leader's last log index + 1 = len(rf.log)-1 + 1 = len(rf.log)
			rf.nextIndex[i] = rf.getLastLogIndex() + 1

			rf.matchIndex[i] = 0
		}

	}

	if verbose>0 || enable_debug_lab_2b {
		fmt.Println("New leader elected:", rf.me, "for term", rf.currentTerm, "with log")
		for i := 0; i < len(rf.log); i++ {
			fmt.Println("  log:", rf.log[i])
		}
	}

	term := rf.currentTerm
	// Notify all other servers.
	for i,_ := range rf.peers {
		if i!= rf.me {
			go func(index int){
				rf.trySendAppendEntriesRecursively(index, term)// i is sent in.
			} (i)
		}
	}
}

func (rf *Raft) stateCandidateToFollower() {
	if verbose >= 1 {
		fmt.Println("stateCandidateToFollower(): lock status", rf.mu)
	}

	state := rf.serverState

	if !state.stateShouldBe(SERVER_STATE_CANDIDATE){
		return
	}

	rf.serverState = SERVER_STATE_FOLLOWER

	// where to put this line should not really matter, since the competitor timer
	// of rf.heartbeatschan  case time.After(xxx) is followed by lock(), so it has to
	// wait till the lock is released. This just makes the timeout period slight longer.

	//go func() { // move it in a goroutine from lab2b
		rf.heartbeatsChan <- true
	//}()

}

func (rf *Raft) stateLeaderToFollower() {
	if verbose >= 1 {
		fmt.Println("stateLeaderToFollower(): lock status", rf.mu)
	}

	state := rf.serverState

	if !state.stateShouldBe(SERVER_STATE_LEADER){
		return
	}

	rf.serverState = SERVER_STATE_FOLLOWER

	//go func() { // move it in a goroutine from lab2b
		rf.heartbeatsChan <- true
	//}()
}


func (rf *Raft) stateMachineLoop() {


	if verbose >= 1 {
		fmt.Println("Raft Server #", rf.me, " is online (Server State: ", rf.serverState, ")!")
	}

	/* The following is no longer used, delete at some point.
	// All the state transitions happen in this function.
	// This implements the server state machine as shown in Fig. 4.
	for {

		// fmt.Println("stateMachineLoop(): waiting for events.", &rf.eventsChan)

		event := <- rf.eventsChan

		//  assumed that when event is put in the eventsChan,
		// rf.mu has been locked already. //

		fmt.Println("stateMachineLoop(): lock status", rf.mu)
		//rf.mu.Unlock()
		//rf.mu.Lock()


		fmt.Println("stateMachineLoop(): Server",rf.me,rf.serverState.toString(), event.toString(), "happens.")

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
*/
}

/*
switch state {
case SERVER_STATE_FOLLOWER:
case SERVER_STATE_CANDIDATE:
case SERVER_STATE_LEADER:
}
*/



func (rf *Raft) initHeartbeatMonitor() {

	// rf.timerHeartbeatMonitor.Reset( HeartbeatTimeout*time.Millisecond)

	go func(rf *Raft) {

		//fmt.Println("This has been started!!!!!!!!!!!")

		for {
			select {
			case <- rf.heartbeatsChan:
				// do nothing
				// Using channel in this way provides an elegant way for resetting the timer
				// without using a lock for it.
				// Usage like case <- rf.timerHeartbeatMonitor.C without locker protection
				// will incur data race issue.
				// See also comments in stateCandidateToFollower() regarding
				// the use of rf.heartbeatsChan.
			case <- time.After( randTimeBetween(HeartbeatTimeoutLower, HeartbeatTimeoutUpper)):
				rf.mu.Lock()
				state := rf.serverState

				switch state {
				case SERVER_STATE_FOLLOWER:
					// Cannot have it in a goroutine
					//fmt.Println("HeartbeatMonitor times out for server ",rf.me,".")
					// rf.eventsChan <- EVENT_HEARTBEAT_TIMEOUT
					rf.stateFollowerToCandidate()
					//fmt.Println("HeartbeatMonitor times out for server double check ",rf.me,".")
				case SERVER_STATE_CANDIDATE:
					// do nothing, since already became candidate.
				case SERVER_STATE_LEADER:
					// do nothing
				}

				rf.mu.Unlock()

			}
		}

	}(rf)
}

func (rf *Raft) initHeartbeatSender(){ //exitChan chan bool

/*
	for i,_ := range rf.peers {
		if i != rf.me {
			rf.mu.Lock()
			term := rf.currentTerm
			rf.mu.Unlock()

			go rf.trySendAppendEntriesRecursively(i, term) // i is sent in.

			// time.Sleep(HeartbeatSendPeriod * time.Millisecond)
			time.Sleep(randTimeBetween(HeartbeatSendPeriod/2, HeartbeatSendPeriod*2))
		}
	}
*/

	for i,_ := range rf.peers {
		if i != rf.me {
		//if true {
			go func(rf *Raft, index int) {

				//fmt.Println("Server",rf.me,"initHeartbeatSender(): for", index)
				for {
					select {
					//case <- exitChan:
					//	break
					case <- rf.heartbeatsSendChan[index]:
					case <- time.After( (HeartbeatSendPeriod * time.Millisecond) ):
						if false { // this does not work to pass 2A ReElect, which I do not understand.
							rf.mu.Lock()
							term := rf.currentTerm
							rf.mu.Unlock()
							go func(serverIndex int) {
								if enable_debug_lab_2c {
									fmt.Println("Server", rf.me, "initHeartbeatSender(): for", serverIndex)
								}
								rf.trySendAppendEntriesRecursively(serverIndex, term)
								// must pass serverIndex in this way, I have been stuck on this bug.
								}(index)
						} else {
						go func(index int) {

							rf.mu.Lock()
							term := rf.currentTerm
							state := rf.serverState

							// no need to lock the following, since goroutines and RPCs incur delay anyway.

							if state == SERVER_STATE_LEADER {

								// TODO: for heartbeat try not to use these on the receiver side.
								prevLogIndex := rf.getLastLogIndex() //I have to use this, this is something I do not understand.
								prevLogTerm := rf.getLastLogTerm()
								//fmt.Println("index:", index)
								//prevLogIndex := rf.nextIndex[index]-1  this does not pass TestFigure8Unreliable2C
								//prevLogTerm := rf.getLogTerm(prevLogIndex)// for the reason I do not understand
								if enable_debug_lab_2c {
									if rf.nextIndex[index]-1 > rf.getLastLogIndex() {
										fmt.Println("Debug point")
										// happened when the server is not a leader, so we put it under state == SERVER_STATE_LEADER
									}
								}
								leaderCommit := rf.commitIndex
								rf.mu.Unlock()

								// Only leader sends heartbeat signal

								// args := AppendEntriesArgs{term, rf.me}
								args := AppendEntriesArgs{term, rf.me, prevLogIndex, prevLogTerm, make([]LogEntry, 0), leaderCommit}

								// Put this in a go routine, which is optional.
								// So the delay in RPC is also considered as part of the network delay

								go func(args AppendEntriesArgs) {

									reply := AppendEntriesReply{}
									// non default values like {-1, false} leads to
									// labgob warning: Decoding into a non-default variable/field Term may not work
									// https://piazza.com/class/j9xqo2fm55k1cy?cid=75

									//fmt.Println("Server ", rf.me, "tries to send heartbeat msg to server ",index,".")

									ok := rf.peers[index].Call("Raft.AppendEntries", &args, &reply)

									if ok {
										if verbose >= 2 {
											fmt.Println("Server ", rf.me, "successfully sends heartbeat msg to server ", index, ".")
										}
										if reply.Success {
											// not really need to use the reply value
										} else {


											if reply.Error == ErrorType_AppendEntries_NO_LOG_WITH_INDEX ||
												reply.Error == ErrorType_AppendEntries_LOG_WITH_WRONG_TERM {
												rf.mu.Lock()
												term := rf.currentTerm
												rf.mu.Unlock()
												go rf.trySendAppendEntriesRecursively(index, term)
											} else if reply.Error == ErrorTypeAppendEntries_REJECT_BY_HIGHER_TERM {
												rf.mu.Lock()
												rf.checkTermNoLessThan(reply.Term)
												rf.mu.Unlock()
											} else {
												if enable_debug_lab_2b {
													fmt.Println("ERROR: not implemented!!!!!!!!!!!!!!!!")
												}
											}

										}
									} else {
										if verbose >= 2 {
											fmt.Println("Server ", rf.me, "fails to send heartbeat msg to server ", index, ".")
										}
									}

								}(args)

							} else {
								rf.mu.Unlock()
							}

						}(index) // i must be sent in.
						}
					}
				}
			}(rf, i)
		}
	}

/*
	for i,_ := range rf.peers {
		//if i != rf.me {
		if true {
			go func(rf *Raft, index int) {
				for {
					select {
					case <- rf.heartbeatsSendChan[index]:
					case <- time.After( (HeartbeatSendPeriod * time.Millisecond) ):
						go func(index int) {

								rf.mu.Lock()
								term := rf.currentTerm
								state := rf.serverState
								// TODO: for heartbeat try not to use these on the receiver side.
								prevLogIndex := rf.getLastLogIndex() //TODO: this is clearly a bug
								// prevLogIndex := rf.getLastLogIndex()
								prevLogTerm := rf.getLastLogTerm()
								leaderCommit := rf.commitIndex
								rf.mu.Unlock()

								// no need to lock the following, since goroutines and RPCs incur delay anyway.

								if state == SERVER_STATE_LEADER {
									// Only leader sends heartbeat signal

									// args := AppendEntriesArgs{term, rf.me}
									args := AppendEntriesArgs{term, rf.me, prevLogIndex, prevLogTerm, make([]LogEntry, 0), leaderCommit}

									// Put this in a go routine, which is optional.
									// So the delay in RPC is also considered as part of the network delay

									go func(args AppendEntriesArgs) {

										reply := AppendEntriesReply{}
										// non default values like {-1, false} leads to
										// labgob warning: Decoding into a non-default variable/field Term may not work
										// https://piazza.com/class/j9xqo2fm55k1cy?cid=75

										//fmt.Println("Server ", rf.me, "tries to send heartbeat msg to server ",index,".")

										ok := rf.peers[index].Call("Raft.AppendEntries", &args, &reply)

										if ok {
											if verbose >= 2 {
												fmt.Println("Server ", rf.me, "successfully sends heartbeat msg to server ", index, ".")
											}
											if reply.Success {
												// not really need to use the reply value
												// TODO optional: check reply.Term
											}
										} else {
											if verbose >= 2 {
												fmt.Println("Server ", rf.me, "fails to send heartbeat msg to server ", index, ".")
											}
										}

									}(args)

								}

						}(index) // i must be sent in.
					}
				}
			}(rf, i)
		}
	}
*/

/*
	for i,p := range rf.peers {

		go func(peer *labrpc.ClientEnd, index int) {

			for {

				rf.mu.Lock()
				term := rf.currentTerm
				state := rf.serverState
				// TODO: for heartbeat try not to use these on the receiver side.
				prevLogIndex := rf.getLastLogIndex() //TODO: this is clearly a bug
				// prevLogIndex := rf.getLastLogIndex()
				prevLogTerm := rf.getLastLogTerm()
				leaderCommit := rf.commitIndex
				rf.mu.Unlock()

				// no need to lock the following, since goroutines and RPCs incur delay anyway.

				if state == SERVER_STATE_LEADER {
					// Only leader sends heartbeat signal

					// args := AppendEntriesArgs{term, rf.me}
					args := AppendEntriesArgs{term, rf.me, prevLogIndex, prevLogTerm, make([]LogEntry, 0), leaderCommit}

					// Put this in a go routine, which is optional.
					// So the delay in RPC is also considered as part of the network delay

					go func(args AppendEntriesArgs) {

						reply := AppendEntriesReply{}
						// non default values like {-1, false} leads to
						// labgob warning: Decoding into a non-default variable/field Term may not work
						// https://piazza.com/class/j9xqo2fm55k1cy?cid=75

						//fmt.Println("Server ", rf.me, "tries to send heartbeat msg to server ",index,".")

						ok := peer.Call("Raft.AppendEntries", &args, &reply)

						if ok {
							if verbose >= 2 {
								fmt.Println("Server ", rf.me, "successfully sends heartbeat msg to server ", index, ".")
							}
							if reply.Success {
								// not really need to use the reply value
								// TODO optional: check reply.Term
							}
						} else {
							if verbose >= 2 {
								fmt.Println("Server ", rf.me, "fails to send heartbeat msg to server ", index, ".")
							}
						}

					}(args)

				}
				time.Sleep(HeartbeatSendPeriod * time.Millisecond)
			}
		}(p, i) // i must be sent in.

	}
*/

}

func (rf *Raft) startElection(termWhenInit int) {

	//time.Sleep(waitPeriod)

	// should not use lock in this goroutine except protecting the state transition
	// in the end. Though it seems ok to use lock, but there is no need.

	// TODO: think about when disabled lab 2b
	lastLogIndex := 0
	lastLogTerm := 0

	if enable_lab_2b {
		rf.mu.Lock()

		lastLogIndex = rf.getLastLogIndex()
		lastLogTerm = rf.getLastLogTerm()

		rf.mu.Unlock()
	}


	n := len(rf.peers)

	args := RequestVoteArgs{termWhenInit, rf.me, lastLogIndex, lastLogTerm} // same for all

	onetimeVoteChan := make(chan bool, n)

	// Broadcast vote requests.
	for i,p := range rf.peers {
		if i != rf.me {
			go func(peer *labrpc.ClientEnd, index int) {

				reply := RequestVoteReply{}

				ok := peer.Call("Raft.RequestVote", &args, &reply)
				if verbose >= 1 {
					fmt.Println("Server ", rf.me, "send RequestVote() RPC to server ", index, " with result", reply.VoteGranted, ".")
				}

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

	electionTimeoutChan := make(chan bool)
	go func() {
		time.Sleep( randTimeBetween(ElectionTimeoutLower, ElectionTimeoutUpper) )
		electionTimeoutChan <- true
	}()

	// Collect results
	votesCountTrue := 1 // candidate always vote for itself.
	votesCountFalse := 0
	for {
		select {
		// When the election is stale, do nothing in all case.

		case voteGrant := <-onetimeVoteChan:
				if voteGrant {
					votesCountTrue++
				} else {
					votesCountFalse++
				}
				if 2*votesCountTrue>n { // cannot use n/2 on the right
					// It wins an election, which however may be a *stale* election.
					rf.mu.Lock()
					if rf.currentTerm == termWhenInit {
						//rf.eventsChan <- EVENT_ELECTION_WIN
						rf.stateCandidateToLeader()
					}
					rf.mu.Unlock()
					return
				}
				if 2*votesCountFalse>=n { //use >= here
					// stateCandidateToCandidate is subject to two step verification
					// to avoid multiple completing Candidate
					preCandidate := false
					rf.mu.Lock()
					if rf.currentTerm == termWhenInit {
						preCandidate = true
					}
					rf.mu.Unlock()
					if preCandidate {
						time.Sleep( randTimeBetween(ElectionTimeoutLower, ElectionTimeoutUpper) )
						// Maybe consider make it a seperate parameter.
						rf.mu.Lock()
						if rf.currentTerm == termWhenInit {
							// Currently treating losing an election same as timeout
							//rf.eventsChan <- EVENT_ELECTION_TIMEOUT
							rf.stateCandidateToCandidate()
						}
						rf.mu.Unlock()
					}
					return
				}
		case <- electionTimeoutChan:
			// copy pasted from above, merge the two case.
				preCandidate := false
				rf.mu.Lock()
				if rf.currentTerm == termWhenInit {
					preCandidate = true
				}
				rf.mu.Unlock()
				if preCandidate {
					time.Sleep( randTimeBetween(ElectionTimeoutLower, ElectionTimeoutUpper) )
					// Maybe consider make it a seperate parameter.
					rf.mu.Lock()
					if rf.currentTerm == termWhenInit {
						// Currently treating losing an election same as timeout
						//rf.eventsChan <- EVENT_ELECTION_TIMEOUT
						rf.stateCandidateToCandidate()
					}
					rf.mu.Unlock()
				}
				return
		}

	}

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

func (rf *Raft) startCommitChecker() {

	for {
		<- rf.commitCheckerTriggerChan


		// fmt.Println("startCommitChecker() step 0")

		for i:= 0; i<len(rf.peers); i++ {
			//rf.peerBeingAppend[i].Lock()
		}

		// fmt.Println("startCommitChecker() step 1")

		// lock between rf.peerBeingAppend[i].Lock()/Unlock()
		// otherwise it may deadlock
		rf.mu.Lock()

		// fmt.Println("startCommitChecker() step 2")

		if rf.serverState==SERVER_STATE_LEADER {

			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).
			N := rf.commitIndex
			for cN := rf.commitIndex + 1; cN <= rf.getLastLogIndex(); cN++ {
				if rf.getLogTerm(cN) != rf.currentTerm {
					continue
				}
				count := 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if cN <= rf.matchIndex[i] {
						count++
					}
				}
				if count*2 > len(rf.peers) {
					N = cN
				}
			}
			if N > rf.commitIndex {

				if enable_debug_lab_2b {
					fmt.Println("Leader", rf.me, "committed index: ", N, "")
				}

				// other server will figure out this later from RPC calls


				msgs := make([]ApplyMsg, 0)

				for i := rf.commitIndex + 1; i <= N; i++ {
					msg := ApplyMsg{i>0, rf.getLog(i).Command, i}
					msgs = append(msgs, msg)
					//fmt.Println("Leader", rf.me, "apply msg", msg.Command)
					rf.applyStack = append(rf.applyStack, msg)
				}

				//  do this in a goroutine
				/*go func(msgs []ApplyMsg) {
					//  order is perserved
					for i:=0; i<len(msgs); i++ {
							// the log with index 0 is a place holder and do not need to be applied
							fmt.Println("Leader", rf.me, "apply msg",msgs[i].Command)
							rf.applyChan <- msgs[i]
					}
				}(msgs)*/

				rf.commitIndex = N
			}

		}

		rf.mu.Unlock()

		for i:= len(rf.peers)-1; i>=0; i-- {
			//rf.peerBeingAppend[i].Unlock()
		}
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

	rand.Seed(int64(rf.me))

	// rf.commitIndex // 2B


	// rf.eventsChan = make( chan ServerEvent, 200)
	rf.heartbeatsChan = make( chan bool, 200)

	if enable_lab_2b {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.log = make([] LogEntry, 0)
		rf.log = append(rf.log, LogEntry{}) // Place holder
		rf.applyStack = make([] ApplyMsg, 0)
		rf.applyChan = applyCh
		rf.commitCheckerTriggerChan = make( chan bool, 200)
		rf.peerBeingAppend = make( []*sync.Mutex, len(rf.peers))
		for i:=0; i<len(rf.peerBeingAppend); i++ {
			rf.peerBeingAppend[i] = &sync.Mutex{}
		}
		rf.baseIndex = 0
		rf.heartbeatsSendChan = make( []chan bool, len(rf.peers))
		for i:=0; i<len(rf.heartbeatsChan); i++ {
			rf.heartbeatsSendChan[i] = make( chan bool, 200)
		}
	}


	if me == 0 {
		//rf.initServerState(SERVER_STATE_LEADER)
		rf.initServerState(SERVER_STATE_FOLLOWER)
	} else {
		rf.initServerState(SERVER_STATE_FOLLOWER)
	}


	rf.initHeartbeatSender()
	rf.initHeartbeatMonitor()

	go rf.startCommitChecker()

	go func() {
		for {
			rf.mu.Lock()
			// clear the stack
			copyStack := rf.applyStack
			rf.applyStack = nil
			rf.applyStack = make([] ApplyMsg, 0)
			rf.mu.Unlock()

			for i:=0; i<len(copyStack); i++ {
				rf.applyChan <- copyStack[i]
			}

			time.Sleep(50*time.Millisecond) // TODO: why need this?
		}
	}()

	// go rf.stateMachineLoop()

	// back to given code

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
