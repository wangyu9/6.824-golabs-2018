package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
	//"time"
	"fmt"
	"time"
)

type ClientIndexType int
type RequestIndexType int

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu	sync.Mutex

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

	ck.mu.Lock()

	args := GetArgs{key, ck.clientID, ck.requestID}
	ck.requestID++

	ck.mu.Unlock()

	// TODO

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

					if index!= ck.cachedLeader {
						ck.mu.Lock()
						ck.cachedLeader = index
						ck.mu.Unlock()
					}

					if (reply.Err==""||reply.Err==OK) {

						if false {
							fmt.Println("Succeed to Get", args, reply)
						}
						// TODO: do some thing

						return reply.Value
					} else {
						// TODO: handling error

						if reply.Err==ErrNoKey {
							// return "" this is wrong // returns "" if the key does not exist.
							// do nothing.
							// Since servers[i] *thinks* it is the leader, but it may no longer be
							// the leader and the key may exist at current leader, so ErrNoKey
						}
					}
				}
			}
		}

		// TODO: sleep some time.
		// TODO: give up after certain number of trails.
		time.Sleep(100*time.Millisecond)
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

	ck.mu.Lock()

	args := PutAppendArgs{key, value, op, ck.clientID, ck.requestID}
	ck.requestID++

	ck.mu.Unlock()

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

					if index!= ck.cachedLeader {
						ck.mu.Lock()
						ck.cachedLeader = index
						ck.mu.Unlock()
					}

					if (reply.Err==""||reply.Err=="OK") {



						// TODO: do some thing

						return
					} else {
						// TODO: handle error
					}
				} else {
					// fmt.Println("PutAppend() to non-leader")
				}
			}
		}

		// TODO: sleep some time.
		// TODO: give up after certain number of trails.
		time.Sleep(100*time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
