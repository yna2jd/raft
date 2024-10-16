package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = true
const DebugEvents = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Command int

const (
	GET    Command = iota
	PUT    Command = iota
	APPEND Command = iota
)

type Operation struct {
	cmd   Command
	args  interface{}
	reply interface{}
}

func (o *Operation) String() string {
	castStruct := o.args
	str := []string{"G", "P", "A"}[o.cmd] + ": "
	if o.cmd == GET {
		getStruct := castStruct.(*GetArgs)
		str += fmt.Sprintf("[xid: %v, k: %v,  ts: %v]", getStruct.Id, getStruct.Key, getStruct.Timestamp)
	} else {
		appendStruct := castStruct.(*PutAppendArgs)
		str += fmt.Sprintf(
			"[xid: %v, k: %v, v: %v, ts: %v]",
			appendStruct.Id, appendStruct.Key, appendStruct.Value, appendStruct.Timestamp,
		)
	}
	return str
}

type EventType bool

const (
	PREPARE EventType = false
	COMMIT  EventType = true
)

type Event struct {
	Type EventType
	Time int64
	Data Operation
}

func (e *Event) String() string {
	eventType := "Prepare"
	if e.Type == COMMIT {
		eventType = "Commit"
	}
	return fmt.Sprintf("[%v, %v]\n", eventType, e.Data.String())
}

func String(events []Event) string {
	str := ""
	for _, event := range events {
		str += event.String()
	}
	return str
}

type SeenData struct {
	seq   uint
	value string
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]string
	seen map[uint]SeenData

	//for debugging
	mutexEvents sync.Mutex
	events      []Event
}

func (kv *KVServer) PrintServer() {
	if DebugEvents {
		fmt.Printf("KVServer events:\n%v", String(kv.events))
	}
}

// Get (key) fetches the current value for the key.
// A Get for a non-existent key should return an empty string.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if DebugEvents {
		event := Event{
			PREPARE,
			args.Timestamp,
			Operation{GET, args, reply},
		}
		kv.mutexEvents.Lock()
		kv.events = append(kv.events, event)
		kv.mutexEvents.Unlock()
	}
	value, ok := kv.data[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

// Put (key, value) installs or replaces the value for a particular key in the map
// Duplicate puts have no effect
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if DebugEvents {
		event := Event{
			PREPARE,
			args.Timestamp,
			Operation{PUT, args, reply},
		}
		kv.mutexEvents.Lock()
		kv.events = append(kv.events, event)
		kv.mutexEvents.Unlock()
	}
	kv.data[args.Key] = args.Value
	reply.Value = kv.data[args.Key]
}

// Append (key, arg) appends arg to key’s value and returns the old value.
// An Append to a non-existent key should act as if the existing value were a zero-length string.
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// As each clerk only sends one value at a time, we can use this system
	cachedValue, wasSeen := kv.seen[args.Id]
	if wasSeen && cachedValue.seq == args.Sequence {
		reply.Value = cachedValue.value
		return
	}
	if DebugEvents {
		event := Event{
			PREPARE,
			args.Timestamp,
			Operation{APPEND, args, reply},
		}
		kv.mutexEvents.Lock()
		kv.events = append(kv.events, event)
		kv.mutexEvents.Unlock()
	}
	value, ok := kv.data[args.Key]
	if !ok {
		value = ""
	}
	reply.Value = value
	kv.seen[args.Id] = SeenData{args.Sequence, value} // last append
	kv.data[args.Key] = value + args.Value
	return
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.seen = make(map[uint]SeenData)
	kv.events = make([]Event, 0)
	return kv
}
