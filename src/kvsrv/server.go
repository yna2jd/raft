package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = true

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
	str := []string{"Get", "Put", "Append"}[o.cmd] + ": "
	if o.cmd == GET {
		getStruct := castStruct.(*GetArgs)
		str += fmt.Sprintf("[k: %v, id: %v, ts: %v]", getStruct.Key, getStruct.Id, getStruct.Timestamp)
	} else if o.cmd == PUT {
		putStruct := castStruct.(*PutAppendArgs)
		str += fmt.Sprintf("[k: %v, v: %v, ts: %v]", putStruct.Key, putStruct.Value, putStruct.Timestamp)
	} else {
		appendStruct := castStruct.(*PutAppendArgs)
		str += fmt.Sprintf(
			"[k: %v, v: %v, i: %v, s: %v, ts: %v, fi: %v]",
			appendStruct.Key, appendStruct.Value, appendStruct.Id,
			appendStruct.Sequence, appendStruct.Timestamp, appendStruct.Incomplete,
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
	return fmt.Sprintf("[%v, %v, %v]\n", eventType, e.Time, e.Data.String())
}

func String(events []Event) string {
	str := ""
	for _, event := range events {
		str += event.String()
	}
	return str
}

type KVServer struct {
	mutexData sync.Mutex
	data      map[string]string
	mutexSeen sync.Mutex
	seen      map[uint]map[uint]string
	pending   map[string]struct{} // in-progress xids and their args
	// Your definitions here.
	mutexEvents sync.Mutex
	events      []Event
}

func PrintServer(kv *KVServer) {
	if false {
		kv.mutexEvents.Lock()
		DPrintf("KVServer events: %v", String(kv.events))
		kv.mutexEvents.Unlock()
	}
}

// Get (key) fetches the current value for the key.
// A Get for a non-existent key should return an empty string.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	event := Event{
		PREPARE,
		args.Timestamp,
		Operation{GET, args, reply},
	}
	kv.mutexEvents.Lock()
	kv.events = append(kv.events, event)
	kv.mutexEvents.Unlock()
	PrintServer(kv)
	kv.GetCommit(args, reply)
	return
}

func (kv *KVServer) GetCommit(args *GetArgs, reply *GetReply) {
	kv.mutexData.TryLock()
	value, ok := kv.data[args.Key]
	kv.mutexData.Unlock()
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
	event := Event{
		PREPARE,
		args.Timestamp,
		Operation{PUT, args, reply},
	}
	kv.mutexEvents.Lock()
	kv.events = append(kv.events, event)
	kv.mutexEvents.Unlock()
	kv.mutexData.Lock()
	kv.data[args.Key] = args.Value
	reply.Value = kv.data[args.Key]
	kv.mutexData.Unlock()
	PrintServer(kv)
}

// returns a true if the value has already been calculated
func isXidSeen(kv *KVServer, id uint, sequence uint, replyValue *string) bool {
	//_, isPending := kv.data[xid]
	//if isPending {
	//	for {
	//		time.Sleep(2 * time.Second)
	//		_, ok := kv.seen[xid]
	//		if ok{
	//			break
	//		}
	//	}
	//	return true
	//}
	kv.mutexSeen.Lock()
	value, beenSeen := kv.seen[id][sequence]
	kv.mutexSeen.Unlock()
	if beenSeen {
		*replyValue = value
		return true
	}
	return false
}

func markXidSeen(kv *KVServer, id uint, sequence uint, replyValue string) {
	kv.mutexSeen.Lock()
	if kv.seen[id] == nil {
		kv.seen[id] = make(map[uint]string)
	}
	kv.seen[id][sequence] = replyValue
	kv.mutexSeen.Unlock()
}

// Append (key, arg) appends arg to key’s value and returns the old value.
// An Append to a non-existent key should act as if the existing value were a zero-length string.
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	replyString := ""
	seen := isXidSeen(kv, args.Id, args.Sequence, &replyString)
	if seen {
		reply.Value = replyString
		return
	}

	kv.mutexSeen.Lock()
	oldSeen := kv.seen[args.Id]
	newSeen := make(map[uint]string)
	kv.mutexSeen.Unlock()
	for seq := range oldSeen {
		if seq >= args.Incomplete {
			newSeen[seq] = oldSeen[seq]
		}
	}
	kv.mutexSeen.Lock()
	kv.seen[args.Id] = newSeen
	kv.mutexSeen.Unlock()

	event := Event{
		PREPARE,
		args.Timestamp,
		Operation{APPEND, args, reply},
	}
	kv.mutexEvents.Lock()
	kv.events = append(kv.events, event)
	kv.mutexEvents.Unlock()

	kv.mutexData.Lock()
	value, ok := kv.data[args.Key]
	if !ok {
		value = ""
	}
	kv.data[args.Key] = value + args.Value
	reply.Value = kv.data[args.Key]
	kv.mutexData.Unlock()
	markXidSeen(kv, args.Id, args.Sequence, reply.Value)
	PrintServer(kv)
	return
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.seen = make(map[uint]map[uint]string)
	kv.pending = make(map[string]struct{})
	kv.events = make([]Event, 0)
	DPrintf("Created server.")
	return kv
}
