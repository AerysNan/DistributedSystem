package shardkv

// import "shardmaster"
import (
	"bytes"
	"distributed/labgob"
	"distributed/labrpc"
	"distributed/raft"
	"distributed/shardmaster"
	"distributed/util"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	ExecuteTimeout    = 500 * time.Millisecond
	PollConfigTimeout = 100 * time.Millisecond
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	logger       *logrus.Logger

	dbs                 [util.NShards]map[string]string
	notifyChs           map[int]chan util.Response
	commandIds          map[int64]int64
	appliedRaftLogIndex int

	sm     *shardmaster.Clerk
	config util.Config
}

func (kv *ShardKV) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.config)
	e.Encode(kv.dbs)
	e.Encode(kv.commandIds)
	appliedRaftLogIndex := kv.appliedRaftLogIndex
	kv.mu.Unlock()
	kv.rf.ReplaceLogWithSnapshot(appliedRaftLogIndex, w.Bytes())
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, requestId int64) bool {
	appliedRequestId, ok := kv.commandIds[clientId]
	return ok && requestId <= appliedRequestId
}

func (kv *ShardKV) execute(op util.Op, timeout time.Duration) (bool, bool) {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return true, false
	}
	kv.logger.Infof("ShardKV[%v-%vC%v] execute operation %v", kv.gid, kv.me, kv.config.Num, op)
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan util.Response, 1)
	}
	ch := kv.notifyChs[index]
	kv.mu.Unlock()
	if kv.maxraftstate >= 0 && kv.rf.RaftStateSize() >= kv.maxraftstate {
		kv.logger.Infof("ShardKV[%v-%vC%v] take snapshot %v %v", kv.gid, kv.me, kv.config.Num, kv.maxraftstate, kv.rf.RaftStateSize())
		kv.snapshot()
	}
	wrongLeader := false
	wrongGroup := false
	select {
	case result := <-ch:
		kv.logger.Debugf("ShardKV[%v-%vC%v] read channel %v", kv.gid, kv.me, kv.config.Num, index)
		wrongLeader = !result.Op.Equals(op)
		if wrongLeader {
			kv.logger.Warnf("ShardKV[%v-%vC%v] operation mismatch expected %v actual %v", kv.gid, kv.me, kv.config.Num, op, result)
		}
		wrongGroup = result.WrongGroup
	case <-time.After(timeout):
		kv.mu.Lock()
		kv.logger.Warnf("ShardKV[%v-%vC%v] operation timeout %v", kv.gid, kv.me, kv.config.Num, op)
		wrongLeader = !kv.isDuplicateRequest(op.ClientId, op.CommandId)
		kv.mu.Unlock()
	}
	kv.mu.Lock()
	delete(kv.notifyChs, index)
	kv.logger.Debugf("ShardKV[%v-%vC%v] delete channel %v", kv.gid, kv.me, kv.config.Num, index)
	kv.mu.Unlock()
	return wrongLeader, wrongGroup
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.logger.Debugf("ShardKV[%v-%vC%v] try install snapshot", kv.gid, kv.me, kv.config.Num)
	if snapshot == nil {
		kv.logger.Warnf("ShardKV[%v-%vC%v] found empty snapshot", kv.gid, kv.me, kv.config.Num)
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.config) != nil ||
		d.Decode(&kv.dbs) != nil ||
		d.Decode(&kv.commandIds) != nil {
		kv.logger.Warnf("ShardKV[%v-%vC%v] install snapshot failed", kv.gid, kv.me, kv.config.Num)
	} else {
		kv.logger.Infof("ShardKV[%v-%vC%v] install snapshot", kv.gid, kv.me, kv.config.Num)
	}
}

func (kv *ShardKV) Get(args *util.GetArgs, reply *util.GetReply) {
	op := util.Op{
		Type:      util.OpGet,
		Args:      *args,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	wrongGroup := false
	reply.WrongLeader, wrongGroup = kv.execute(op, ExecuteTimeout)
	if reply.WrongLeader {
		return
	}
	if wrongGroup {
		reply.Err = util.ErrWrongGroup
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.dbs[key2shard(args.Key)][args.Key]; ok {
		reply.Value = value
		kv.logger.Infof("ShardKV[%v-%vC%v] get entries key %v value %v", kv.gid, kv.me, kv.config.Num, args.Key, value)
		reply.Err = util.OK
	} else {
		reply.Err = util.ErrNoKey
	}
}

func (kv *ShardKV) PutAppend(args *util.PutAppendArgs, reply *util.PutAppendReply) {
	op := util.Op{
		Type:      args.Op,
		Args:      *args,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	wrongGroup := false
	reply.WrongLeader, wrongGroup = kv.execute(op, ExecuteTimeout)
	if reply.WrongLeader {
		return
	}
	if wrongGroup {
		reply.Err = util.ErrWrongGroup
		return
	}
	reply.Err = util.OK
}

func (kv *ShardKV) dispatch() {
	for message := range kv.applyCh {
		if !message.CommandValid {
			kv.logger.Debugf("ShardKV[%v-%vC%v] found invalid command %v", kv.gid, kv.me, kv.config.Num, message)
			continue
		}
		op := message.Command.(util.Op)
		if op.Type == util.OpSnapshot {
			kv.installSnapshot(op.Args.(util.SnapshotArgs).Data)
			continue
		}
		kv.mu.Lock()
		if op.Type != util.OpReconfigure && kv.isDuplicateRequest(op.ClientId, op.CommandId) {
			kv.logger.Warnf("ShardKV[%v-%vC%v] duplicate request client %v given %v actual %v", kv.gid, kv.me, kv.config.Num, op.ClientId, op.CommandId, kv.commandIds[op.ClientId])
			kv.mu.Unlock()
			continue
		}
		kv.logger.Infof("ShardKV[%v-%vC%v] dispatch operation %v", kv.gid, kv.me, kv.config.Num, op)
		wrongGroup := false
		if op.Type == util.OpReconfigure {
			args := op.Args.(util.ReconfigureArgs)
			if args.Config.Num > kv.config.Num {
				for sid, db := range args.DBs {
					for k, v := range db {
						kv.dbs[sid][k] = v
					}
				}
				for clientId, commandId := range args.CommandIds {
					if _, ok := kv.commandIds[clientId]; !ok || kv.commandIds[clientId] < commandId {
						kv.logger.Infof("ShardKV[%v-%vC%v] reconfigure client %v commandID %v", kv.gid, kv.me, kv.config.Num, clientId, commandId)
						kv.commandIds[clientId] = commandId
					}
				}
				kv.logger.Infof("ShardKV[%v-%vC%v] update config to %v", kv.gid, kv.me, kv.config.Num, args.Config.Num)
				kv.config.Num = args.Config.Num
				for i := 0; i < util.NShards; i++ {
					kv.config.Shards[i] = args.Config.Shards[i]
				}
				newGroups := make(map[int][]string)
				for k, v := range args.Config.Groups {
					newServers := make([]string, len(v))
					copy(newServers, v)
					newGroups[k] = newServers
				}
				kv.config.Groups = newGroups
			}
			goto exit
		}
		switch op.Type {
		case util.OpPut:
			args := op.Args.(util.PutAppendArgs)
			if !kv.responsible(args.Key) {
				wrongGroup = true
				kv.logger.Debugf("ShardKV[%v-%vC%v] irresponsible for %v", kv.gid, kv.me, kv.config.Num, args.Key)
				goto exit
			}
			kv.dbs[key2shard(args.Key)][args.Key] = args.Value
			kv.logger.Infof("ShardKV[%v-%vC%v] write entries key %v value %v", kv.gid, kv.me, kv.config.Num, args.Key, args.Value)
		case util.OpAppend:
			args := op.Args.(util.PutAppendArgs)
			if !kv.responsible(args.Key) {
				wrongGroup = true
				kv.logger.Debugf("ShardKV[%v-%vC%v] irresponsible for %v", kv.gid, kv.me, kv.config.Num, args.Key)
				goto exit
			}
			kv.dbs[key2shard(args.Key)][args.Key] += args.Value
			kv.logger.Infof("ShardKV[%v-%vC%v] append entries key %v value %v", kv.gid, kv.me, kv.config.Num, args.Key, kv.dbs[key2shard(args.Key)][args.Key])
		case util.OpGet:
			args := op.Args.(util.GetArgs)
			if !kv.responsible(args.Key) {
				wrongGroup = true
				kv.logger.Debugf("ShardKV[%v-%vC%v] irresponsible for %v", kv.gid, kv.me, kv.config.Num, args.Key)
				goto exit
			}
		}
		kv.commandIds[op.ClientId] = op.CommandId
		kv.logger.Infof("ShardKV[%v-%vC%v] update client %v commandID %v", kv.gid, kv.me, kv.config.Num, op.ClientId, op.CommandId)
	exit:
		kv.appliedRaftLogIndex = message.CommandIndex
		ch, ok := kv.notifyChs[message.CommandIndex]
		kv.mu.Unlock()
		if ok {
			kv.logger.Debugf("ShardKV[%v-%vC%v] before write channel %v", kv.gid, kv.me, kv.config.Num, message.CommandIndex)
			ch <- util.Response{
				Op:         op,
				WrongGroup: wrongGroup,
			}
			kv.logger.Debugf("ShardKV[%v-%vC%v] after write channel %v", kv.gid, kv.me, kv.config.Num, message.CommandIndex)
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) poll() {
	ticker := time.NewTicker(PollConfigTimeout)
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			latestConfig := kv.sm.Query(-1)
			kv.mu.Lock()
			kv.logger.Debugf("ShardKV[%v-%vC%v] latest config %v", kv.gid, kv.me, kv.config.Num, latestConfig.Num)
			num := kv.config.Num
			kv.mu.Unlock()
			for i := num + 1; i <= latestConfig.Num; i++ {
				nextConfig := kv.sm.Query(i)
				kv.logger.Debugf("ShardKV[%v-%vC%v] get next config %v", kv.gid, kv.me, kv.config.Num, nextConfig)
				args, ok := kv.broadcastPullShards(nextConfig)
				if !ok {
					kv.logger.Warnf("ShardKV[%v-%vC%v] pull config %v shards failed", kv.gid, kv.me, kv.config.Num, nextConfig.Num)
					break
				}
				if !kv.SyncConfig(args) {
					kv.logger.Warnf("ShardKV[%v-%vC%v] sync config %v failed", kv.gid, kv.me, nextConfig.Num, kv.config.Num)
					break
				}
			}
		}
		<-ticker.C
	}
}

func (kv *ShardKV) sendPullShards(gid int, args *util.PullShardsArgs, reply *util.PullShardsReply) bool {
	for _, server := range kv.config.Groups[gid] {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.PullShards", args, reply)
		if ok && reply.OK {
			return true
		}
	}
	return false
}

func (kv *ShardKV) broadcastPullShards(config util.Config) (util.ReconfigureArgs, bool) {
	args := util.ReconfigureArgs{
		Config:     config,
		CommandIds: make(map[int64]int64),
	}
	for i := 0; i < util.NShards; i++ {
		args.DBs[i] = make(map[string]string)
	}
	shardsToPull := make(map[int][]int)
	kv.mu.Lock()
	for i := 0; i < util.NShards; i++ {
		if kv.config.Shards[i] != kv.gid && config.Shards[i] == kv.gid && kv.config.Shards[i] != 0 {
			shardsToPull[kv.config.Shards[i]] = append(shardsToPull[kv.config.Shards[i]], i)
		}
	}
	kv.mu.Unlock()
	waitGroup := sync.WaitGroup{}
	mu := sync.Mutex{}
	success := true
	for gid, shards := range shardsToPull {
		waitGroup.Add(1)
		go func(gid int, shards []int) {
			defer waitGroup.Done()
			defer kv.logger.Debugf("ShardKV[%v-%vC%v] request to %v for %v finished", kv.gid, kv.me, kv.config.Num, gid, shards)
			var reply util.PullShardsReply
			kv.logger.Infof("ShardKV[%v-%vC%v] request shards %v from %v", kv.gid, kv.me, kv.config.Num, shards, gid)
			if kv.sendPullShards(gid, &util.PullShardsArgs{
				Num:    config.Num,
				Shards: shards,
			}, &reply) {
				mu.Lock()
				for sid, db := range reply.DBs {
					for k, v := range db {
						args.DBs[sid][k] = v
					}
				}
				for clientId, commandId := range reply.CommandIds {
					if _, ok := args.CommandIds[clientId]; !ok || args.CommandIds[clientId] < commandId {
						args.CommandIds[clientId] = commandId
					}
				}
				mu.Unlock()
			} else {
				mu.Lock()
				success = false
				mu.Unlock()
			}
		}(gid, shards)
	}
	waitGroup.Wait()
	kv.logger.Infof("ShardKV[%v-%vC%v] pull shards all finished", kv.gid, kv.me, kv.config.Num)
	return args, success
}

func (kv *ShardKV) PullShards(args *util.PullShardsArgs, reply *util.PullShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.logger.Debugf("ShardKV[%v-%vC%v] requested pull shards %v for config %v", kv.gid, kv.me, kv.config.Num, args.Shards, args.Num)
	if kv.config.Num < args.Num {
		kv.logger.Infof("ShardKV[%v-%vC%v] reject pull shards wanted %v got %v", kv.gid, kv.me, kv.config.Num, args.Num, kv.config.Num)
		reply.OK = false
		return
	}
	kv.logger.Debugf("ShardKV[%v-%vC%v] granted pull shards %v", kv.gid, kv.me, kv.config.Num, args.Shards)
	reply.OK = true
	reply.CommandIds = make(map[int64]int64)
	for i := 0; i < util.NShards; i++ {
		reply.DBs[i] = make(map[string]string)
	}
	for _, sid := range args.Shards {
		for k, v := range kv.dbs[sid] {
			reply.DBs[sid][k] = v
		}
	}
	for clientId := range kv.commandIds {
		reply.CommandIds[clientId] = kv.commandIds[clientId]
	}
}

func (kv *ShardKV) SyncConfig(args util.ReconfigureArgs) bool {
	op := util.Op{
		Type: util.OpReconfigure,
		Args: args,
	}
	wrongLeader, wrongGroup := kv.execute(op, ExecuteTimeout)
	return !wrongLeader && !wrongGroup
}

func (kv *ShardKV) responsible(key string) bool {
	shardId := key2shard(key)
	return kv.gid == kv.config.Shards[shardId]
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(util.Op{})
	labgob.Register(util.Config{})
	labgob.Register(util.GetArgs{})
	labgob.Register(util.GetReply{})
	labgob.Register(util.PutAppendArgs{})
	labgob.Register(util.PutAppendReply{})
	labgob.Register(util.PullShardsArgs{})
	labgob.Register(util.PullShardsReply{})
	labgob.Register(util.ReconfigureArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.logger = logrus.New()
	kv.logger.SetLevel(logrus.ErrorLevel)

	for i := 0; i < util.NShards; i++ {
		kv.dbs[i] = make(map[string]string)
	}
	kv.notifyChs = make(map[int]chan util.Response)
	kv.commandIds = make(map[int64]int64)

	// Use something like this to talk to the shardmaster:

	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.installSnapshot(persister.ReadSnapshot())
	go kv.dispatch()
	go kv.poll()

	return kv
}
