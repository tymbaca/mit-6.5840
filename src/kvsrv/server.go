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
	logs map[uuid.UUID]reqLog
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]string),
		logs: make(map[uuid.UUID]reqLog),
	}

	go kv.cleanLogs()

	return kv
}

type reqLog struct {
	// maybe args
	Response string
	Time     time.Time
}

func (kv *KVServer) cleanLogs() {
	for range time.Tick(reqLogCleanInterval) {
		kv.mu.Lock()

		ttlTime := time.Now().Add(-reqLogTTL)

		for id, lg := range kv.logs {
			if lg.Time.Before(ttlTime) {
				delete(kv.logs, id)
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reqLog, ok := kv.logs[args.ID]; ok {
		reply.Value = reqLog.Response
		return
	}

	result := kv.data[args.Key]
	reply.Value = result

	kv.logs[args.ID] = reqLog{
		Response: result,
		Time:     time.Now(),
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reqLog, ok := kv.logs[args.ID]; ok {
		reply.Value = reqLog.Response
		return
	}

	prev := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	reply.Value = prev

	kv.logs[args.ID] = reqLog{
		Response: prev,
		Time:     time.Now(),
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reqLog, ok := kv.logs[args.ID]; ok {
		reply.Value = reqLog.Response
		return
	}

	prev := kv.data[args.Key]
	kv.data[args.Key] += args.Value
	reply.Value = prev

	kv.logs[args.ID] = reqLog{
		Response: prev,
		Time:     time.Now(),
	}
}
