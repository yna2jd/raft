package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Id         uint
	Sequence   uint
	Timestamp  int64
	Incomplete uint
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key       string
	Id        uint
	Timestamp int64
}

type GetReply struct {
	Value string
}
