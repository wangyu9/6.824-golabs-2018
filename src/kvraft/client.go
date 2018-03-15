package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	//"sync"
	"fmt"
	"time"
)

type ClientIndexType int
type RequestIndexType int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// mu	sync.Mutex // do not need to lock for lab3a

	clientID	ClientIndexType
	cachedLeader int // the server ID that the client believe is the current leader.

	requestID	RequestIndexType // the number of requests have been made.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.clientID = ClientIndexType(int(nrand())) // assigned a random number and *hope* there is no conflict... ideally this should be assigned by the kvserver...

	//fmt.Println("Client", ck.clientID, "initialized")

	ck.cachedLeader = 0 // set a random initial cacheLeader

	ck.requestID = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	//ck.mu.Lock()

	args := GetArgs{key, ck.clientID, ck.requestID}
	ck.requestID++

	//ck.mu.Unlock()

	for {
		// keep retrying until it succeed to reach to the leader
		for i :=0; i<len(ck.servers); i++ {

			reply := GetReply{}

			index := (ck.cachedLeader+i) % len(ck.servers) // start from cachedLeader.

			ok := ck.servers[index].Call("KVServer.Get", &args, &reply)

			if ok {
				// able to get in contact with the server.
				if !reply.WrongLeader {
					// get to the right leader

					if (reply.Err==""||reply.Err==OK) {

						if false {
							fmt.Println("Succeed to Get", args, reply)
						}

						if index!= ck.cachedLeader {
							//ck.mu.Lock()
							ck.cachedLeader = index
							//ck.mu.Unlock()
						}

						return reply.Value
					} else {

						if reply.Err==ErrNoKey {
							return ""
							// returns "" if the key does not exist.
							// This is necessary otherwise the Get() will never terminate.
							// do nothing.
							// It is safe to return because it really comes from the true leader
							// since it comes from an applied (committed) message, which means a
							// majority of servers agree on the result of this get()
						}
					}
				}
			}
		}

		// Sleep some time, since probably need to wait for network healing or leader re-election.
		// Should not give up after certain number of trails since the lab3 tester ask for a result.
		time.Sleep(200*time.Millisecond)
	}



	return "" // returns "" if the key does not exist.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// You'll need to add RPC-sending code to the Clerk Put/Append/Get methods in client.go,
	// and implement PutAppend() and Get() RPC handlers in server.go.

	//ck.mu.Lock()

	args := PutAppendArgs{key, value, op, ck.clientID, ck.requestID}
	ck.requestID++

	//ck.mu.Unlock()

	for {
		// keep retrying until it succeed to reach to the leader
		for i :=0; i<len(ck.servers); i++ {

			reply := PutAppendReply{}

			index := (ck.cachedLeader+i) % len(ck.servers) // start from cachedLeader.

			// fmt.Println("PutAppend() try to leader", index)

			ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)

			if ok {
				// able to get in contact with the server.
				if !reply.WrongLeader {
					// get to the right leader

					//fmt.Println("PutAppend() to leader", index)


					if (reply.Err==""||reply.Err=="OK") {


						if index!= ck.cachedLeader {
							//ck.mu.Lock()
							ck.cachedLeader = index
							//ck.mu.Unlock()
						}

						return
					} else {
					}
				} else {
					// fmt.Println("PutAppend() to non-leader")
				}
			}
		}

		// Sleep some time, since probably need to wait for network healing or leader re-election.
		// Should not give up after certain number of trails since the lab3 tester ask for a result.
		time.Sleep(200*time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
