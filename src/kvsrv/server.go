package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	data map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	result, ok := kv.data[args.Key]
	kv.mu.Unlock()
	if !ok {
		reply.Value = ""
		return
	}

	reply.Value = result
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	prev := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	kv.mu.Unlock()

	reply.Value = prev
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	prev := kv.data[args.Key]
	kv.data[args.Key] += args.Value
	kv.mu.Unlock()

	reply.Value = prev
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.data = make(map[string]string)

	return kv
}
