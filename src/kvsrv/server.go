package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = true
const DebugEvents = false

const TIMEOUT = 200

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
		str += fmt.Sprintf("[id: %v, k: %v,  ts: %v]", getStruct.Id, getStruct.Key, getStruct.Timestamp)
	} else {
		appendStruct := castStruct.(*PutAppendArgs)
		str += fmt.Sprintf(
			"[id: %v, k: %v, v: %v, ts: %v]",
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

type SeenVal struct {
	ts   int64
	data string
}

type KVServer struct {
	mu        sync.Mutex
	data      map[string]string
	seenIndex int
	//seen        map[uint]map[uint]SeenVal
	seen        map[uint]map[uint]string
	pending     map[string]struct{} // in-progress xids and their args
	mutexEvents sync.Mutex
	events      []Event
	//time.After(100 * time.Millisecond)
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
	return
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
	value, wasSeen := kv.seen[args.Id][args.Sequence]
	if wasSeen {
		reply.Value = value
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
	kv.data[args.Key] = value + args.Value
	reply.Value = value

	if kv.seen[args.Id] == nil {
		kv.seen[args.Id] = make(map[uint]string)
	}
	kv.seen[args.Id][args.Sequence] = reply.Value

	// Cleans up old seen values based on SeqLenience const
	// Higher value is needed based on unreliability
	if int(args.Sequence)-SeqLenience > 0 {
		_, exists := kv.seen[args.Id][args.Sequence-SeqLenience]
		if exists {
			delete(kv.seen[args.Id], args.Sequence-SeqLenience)
		}
	}

	return
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.seen = make(map[uint]map[uint]string)
	kv.events = make([]Event, 0)
	return kv
}
