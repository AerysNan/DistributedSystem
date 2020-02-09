package raftkv

import (
	"distributed/labgob"
	"distributed/labrpc"
	"distributed/raft"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const Debug = 0

const ExecuteTimeout = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		logrus.Printf(format, a...)
	}
	return
}

type Op struct {
	Key       string
	Value     string
	Type      string
	ClientId  int64
	CommandId int64
}

func (op *Op) equals(other Op) bool {
	return op.ClientId == other.ClientId && op.CommandId == other.CommandId
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	entries      map[string]string
	notifyChs    map[int]chan Op
	commandIds   map[int64]int64
}

func (kv *KVServer) execute(op Op, timeout time.Duration) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return true
	}
	kv.mu.Lock()
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan Op, 1)
	}
	ch := kv.notifyChs[index]
	kv.mu.Unlock()
	select {
	case result := <-ch:
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		if !result.equals(op) {
			return true
		}
		return false
	case <-time.After(timeout):
		return true
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:       args.Key,
		Type:      "Get",
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	reply.WrongLeader = kv.execute(op, ExecuteTimeout)
	if reply.WrongLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.entries[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Type:      args.Op,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	reply.WrongLeader = kv.execute(op, ExecuteTimeout)
}

func (kv *KVServer) dispatch() {
	for message := range kv.applyCh {
		if !message.CommandValid {
			continue
		}
		op := message.Command.(Op)
		kv.mu.Lock()
		commandId, ok := kv.commandIds[op.ClientId]
		if !ok || commandId < op.CommandId {
			switch op.Type {
			case "Put":
				kv.entries[op.Key] = op.Value
			case "Append":
				kv.entries[op.Key] += op.Value
			}
			kv.commandIds[op.ClientId] = op.CommandId
		}
		ch, ok := kv.notifyChs[message.CommandIndex]
		kv.mu.Unlock()
		if !ok {
			continue
		}
		ch <- op
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.entries = make(map[string]string)
	kv.notifyChs = make(map[int]chan Op)
	kv.commandIds = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.dispatch()
	return kv
}
