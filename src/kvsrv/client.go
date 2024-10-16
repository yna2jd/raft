package kvsrv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

var clientCount uint = 0

type Clerk struct {
	server *labrpc.ClientEnd
	id     uint
	seq    uint
	// You will have to modify this struct.
}

func nRand() int64 {
	maxInt := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, maxInt)
	x := bigX.Int64()
	return x
}

var time0 = time.Now()

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	clientCount++
	ck.id = clientCount
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
	args := GetArgs{key, ck.id, ck.seq, timestamp}
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			ck.seq++
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
	args := PutAppendArgs{
		key, value, ck.id, ck.seq, timestamp,
	}
	reply := PutAppendReply{}
	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			ck.seq++
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
