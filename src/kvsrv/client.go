package kvsrv

import (
	"crypto/rand"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"github.com/google/uuid"
)

const _retries = 10

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	id  uuid.UUID
	seq atomic.Uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server

	// You'll have to add code here.
	ck.id = uuid.New()

	return ck
}

func (ck *Clerk) getSeq() uint64 {
	return ck.seq.Add(1)
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
	args := GetArgs{
		ClientID: ck.id,
		Seq:      ck.getSeq(),
		Key:      key,
	}
	reply := GetReply{}

	var ok bool
	for i := 0; i < _retries; i++ {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			break
		}
		<-time.After(20 * time.Millisecond)
	}
	if !ok {
		panic("can't get value from server")
	}

	return reply.Value
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
	args := PutAppendArgs{
		ClientID: ck.id,
		Seq:      ck.getSeq(),
		Key:      key,
		Value:    value,
	}
	reply := PutAppendReply{}

	var ok bool
	for i := 0; i < _retries; i++ {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			break
		}
		<-time.After(20 * time.Millisecond)
	}
	if !ok {
		panic("can't " + strings.ToLower(op) + " value to server")
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
