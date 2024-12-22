package kvsrv

import "github.com/google/uuid"

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID uuid.UUID
	Seq      uint64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID uuid.UUID
	Seq      uint64
}

type GetReply struct {
	Value string
}
