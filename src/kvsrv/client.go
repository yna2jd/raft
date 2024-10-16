package kvsrv

import (
	"labrpc"
	"time"
)

var clientCount uint = 0

type Clerk struct {
	server *labrpc.ClientEnd
	id     uint
	seq    uint
}

var time0 = time.Now()

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	clientCount++
	ck.id = clientCount
	return ck
}

// Get
// Fetch the current value for a key.
// Returns "" if the key does not exist.
// Keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	timestamp := int64(time.Since(time0))
	args := GetArgs{key, ck.id, ck.seq, timestamp}
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			ck.seq++
			// technically, we can use a bool here instead of an uint
			// but that just feels wrong, don't you think?
			return reply.Value
		}

	}
}

// PutAppend
// shared by Put and Append.
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

// Put value into key and return the put value
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return original value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
