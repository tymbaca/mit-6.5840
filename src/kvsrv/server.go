package kvsrv

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

const Debug = false

const (
	reqLogTTL           = 500 * time.Millisecond
	reqLogCleanInterval = 500 * time.Millisecond
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	data map[string]string
	logs map[uuid.UUID]clientLog
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]string),
		logs: make(map[uuid.UUID]clientLog),
	}

	go kv.cleanLogs()

	return kv
}

type clientLog struct {
	Seq      uint64
	Response string
}

func (kv *KVServer) cleanLogs() {
	for range time.Tick(reqLogCleanInterval) {
		kv.mu.Lock()

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	log, ok := kv.logs[args.ClientID]
	if ok {
		if log.Seq > args.Seq {
			return
		}

		if log.Seq == args.Seq {
			// we got retry, need to resend the result
			reply.Value = log.Response
			return
		}
	}

	result := kv.data[args.Key]
	reply.Value = result

	kv.logs[args.ClientID] = clientLog{
		Seq:      args.Seq,
		Response: result,
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if log, ok := kv.logs[args.ClientID]; ok {
		if log.Seq > args.Seq {
			return
		}

		if log.Seq == args.Seq {
			// we got retry, need to resend the result
			reply.Value = log.Response
			return
		}
	}

	prev := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	reply.Value = prev

	kv.logs[args.ClientID] = clientLog{
		Seq:      args.Seq,
		Response: prev,
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if log, ok := kv.logs[args.ClientID]; ok {
		if log.Seq > args.Seq {
			return
		}

		if log.Seq == args.Seq {
			// we got retry, need to resend the result
			reply.Value = log.Response
			return
		}
	}

	prev := kv.data[args.Key]
	kv.data[args.Key] += args.Value
	reply.Value = prev

	kv.logs[args.ClientID] = clientLog{
		Seq:      args.Seq,
		Response: prev,
	}
}
