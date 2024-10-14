package kvsrv

import (
	"labrpc"
	"slices"
	"time"
)
import "crypto/rand"
import "math/big"

var clientCount uint = 0

type Clerk struct {
	server     *labrpc.ClientEnd
	id         uint
	sequence   uint
	incomplete []uint
	// You will have to modify this struct.
}

func nRand() int64 {
	maxInt := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, maxInt)
	x := bigX.Int64()
	return x
}

func (ck *Clerk) firstIncomplete() uint {
	return slices.Min(ck.incomplete)
}

func (ck *Clerk) markSequenceComplete() {
	for i, other := range ck.incomplete {
		if other == ck.sequence {
			ck.incomplete = append(ck.incomplete[:i], ck.incomplete[i+1:]...)
		}
	}
}

var time0 = time.Now()

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	clientCount++
	ck.id = clientCount
	ck.incomplete = make([]uint, 0)
	ck.sequence = 1
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	timestamp := int64(time.Since(time0))
	args := GetArgs{key, ck.id, timestamp}
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}

	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	timestamp := int64(time.Since(time0))
	args := PutAppendArgs{}
	if op == "Append" {
		ck.incomplete = append(ck.incomplete, ck.sequence)
		args = PutAppendArgs{
			key, value,
			ck.id, ck.sequence,
			timestamp, ck.firstIncomplete(),
		}
		DPrintf("-> FI: %v, %v\n", ck.firstIncomplete(), ck.sequence)
	} else {
		// we don't check these for put because we don't care about duplicates
		args = PutAppendArgs{
			key, value,
			0, 0,
			timestamp, 0,
		}
	}
	reply := PutAppendReply{}
	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			if op == "Append" {
				ck.markSequenceComplete()
				ck.sequence++
				DPrintf("%v+%v", ck.sequence)
			}
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
