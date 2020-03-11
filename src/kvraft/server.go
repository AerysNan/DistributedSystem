package raftkv

import (
	"bytes"
	"distributed/labgob"
	"distributed/labrpc"
	"distributed/raft"
	"distributed/util"
	"sync"
	"time"
)

const ExecuteTimeout = 500 * time.Millisecond

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	entries             map[string]string
	notifyChs           map[int]chan util.Op
	commandIds          map[int64]int64
	appliedRaftLogIndex int
}

func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int64) bool {
	appliedRequestId, ok := kv.commandIds[clientId]
	return ok && requestId <= appliedRequestId
}

func (kv *KVServer) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.entries)
	e.Encode(kv.commandIds)
	appliedRaftLogIndex := kv.appliedRaftLogIndex
	kv.mu.Unlock()
	kv.rf.ReplaceLogWithSnapshot(appliedRaftLogIndex, w.Bytes())
}

func (kv *KVServer) execute(op util.Op, timeout time.Duration) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return true
	}
	if kv.maxraftstate >= 0 && kv.rf.RaftStateSize() >= kv.maxraftstate {
		kv.snapshot()
	}
	kv.mu.Lock()
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan util.Op, 1)
	}
	ch := kv.notifyChs[index]
	kv.mu.Unlock()
	var wrongLeader bool
	select {
	case result := <-ch:
		wrongLeader = !result.Equals(op)
	case <-time.After(timeout):
		kv.mu.Lock()
		wrongLeader = !kv.isDuplicateRequest(op.ClientId, op.CommandId)
		kv.mu.Unlock()
	}
	kv.mu.Lock()
	delete(kv.notifyChs, index)
	kv.mu.Unlock()
	return wrongLeader
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.entries) != nil ||
		d.Decode(&kv.commandIds) != nil {
		DPrintf("[%vT%vS%v] install snapshot failed", kv.me, kv.rf.CurrentTerm(), kv.rf.SnapshotIndex())
	}
}

func (kv *KVServer) Get(args *util.GetArgs, reply *util.GetReply) {
	op := util.Op{
		Type:      util.OpGet,
		Args:      *args,
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
		reply.Err = util.ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *util.PutAppendArgs, reply *util.PutAppendReply) {

	op := util.Op{
		Type:      args.Op,
		Args:      *args,
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
		DPrintf("KV[%v] got message %v", kv.me, message)
		op := message.Command.(util.Op)
		if op.Type == util.OpSnapshot {
			kv.installSnapshot(op.Args.(util.SnapshotArgs).Data)
			continue
		}
		kv.mu.Lock()
		if kv.isDuplicateRequest(op.ClientId, op.CommandId) {
			kv.mu.Unlock()
			continue
		}
		switch op.Type {
		case util.OpPut:
			args := op.Args.(util.PutAppendArgs)
			kv.entries[args.Key] = args.Value
		case util.OpAppend:
			args := op.Args.(util.PutAppendArgs)
			kv.entries[args.Key] += args.Value
		}
		kv.commandIds[op.ClientId] = op.CommandId
		kv.appliedRaftLogIndex = message.CommandIndex
		if ch, ok := kv.notifyChs[message.CommandIndex]; ok {
			ch <- op
		}
		kv.mu.Unlock()
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
	labgob.Register(util.Op{})
	labgob.Register(util.GetArgs{})
	labgob.Register(util.PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.entries = make(map[string]string)
	kv.notifyChs = make(map[int]chan util.Op)
	kv.commandIds = make(map[int64]int64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.installSnapshot(persister.ReadSnapshot())
	go kv.dispatch()
	return kv
}
