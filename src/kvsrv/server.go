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
	Seq       uint64
	AppendPos int
	Time      time.Time
}

func (kv *KVServer) cleanLogs() {
	for range time.Tick(reqLogCleanInterval) {
		kv.mu.Lock()

		ttlTime := time.Now().Add(-reqLogTTL)
		for id, log := range kv.logs {
			if log.Time.Before(ttlTime) {
				delete(kv.logs, id)
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// log, ok := kv.logs[args.ClientID]
	// if ok {
	// 	if log.Seq > args.Seq {
	// 		return
	// 	}
	//
	// 	if log.Seq == args.Seq {
	// 		// we got retry, need to resend the result
	// 		reply.Value = log.Response
	// 		return
	// 	}
	// }

	result := kv.data[args.Key]
	reply.Value = result

	// kv.logs[args.ClientID] = clientLog{
	// 	Seq:      args.Seq,
	// 	Response: result,
	// }
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
			// reply.Value = log.Response
			return
		}
	}

	prev := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	reply.Value = prev

	kv.logs[args.ClientID] = clientLog{
		Seq: args.Seq,
		// Response: prev,
		Time: time.Now(),
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
			reply.Value = kv.data[args.Key][:log.AppendPos] // WARN: what if we got PUT after APPEND
			return
		}
	}

	prev := kv.data[args.Key]
	kv.data[args.Key] += args.Value
	reply.Value = prev

	kv.logs[args.ClientID] = clientLog{
		Seq:       args.Seq,
		AppendPos: len(prev),
		Time:      time.Now(),
	}
}
