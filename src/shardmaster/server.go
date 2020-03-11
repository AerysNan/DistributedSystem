package shardmaster

import (
	"distributed/labgob"
	"distributed/labrpc"
	"distributed/raft"
	"distributed/util"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const ExecuteTimeout = 500 * time.Millisecond

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	logger     *logrus.Logger
	configs    []util.Config
	commandIds map[int64]int64
	notifyChs  map[int]chan util.Op
}

func (sm *ShardMaster) isDuplicateRequest(clientId int64, requestId int64) bool {
	appliedRequestId, ok := sm.commandIds[clientId]
	return ok && requestId <= appliedRequestId
}

func (sm *ShardMaster) execute(op util.Op, timeout time.Duration) bool {
	sm.logger.Infof("ShardMaster[%v] execute operation %v", sm.me, op)
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return true
	}
	sm.mu.Lock()
	if _, ok := sm.notifyChs[index]; !ok {
		sm.notifyChs[index] = make(chan util.Op, 1)
	}
	ch := sm.notifyChs[index]
	sm.mu.Unlock()
	var wrongLeader bool
	select {
	case result := <-ch:
		wrongLeader = !result.Equals(op)
	case <-time.After(timeout):
		sm.mu.Lock()
		wrongLeader = !sm.isDuplicateRequest(op.ClientId, op.CommandId)
		sm.mu.Unlock()
	}
	sm.mu.Lock()
	delete(sm.notifyChs, index)
	sm.mu.Unlock()
	return wrongLeader
}

func (sm *ShardMaster) Join(args *util.JoinArgs, reply *util.JoinReply) {
	op := util.Op{
		Args:      *args,
		Type:      util.OpJoin,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	reply.WrongLeader = sm.execute(op, ExecuteTimeout)
	if reply.WrongLeader {
		return
	}
	reply.Err = util.OK
}

func (sm *ShardMaster) Leave(args *util.LeaveArgs, reply *util.LeaveReply) {
	op := util.Op{
		Args:      *args,
		Type:      util.OpLeave,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	reply.WrongLeader = sm.execute(op, ExecuteTimeout)
	if reply.WrongLeader {
		return
	}
	reply.Err = util.OK
}

func (sm *ShardMaster) Move(args *util.MoveArgs, reply *util.MoveReply) {
	op := util.Op{
		Args:      *args,
		Type:      util.OpMove,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	reply.WrongLeader = sm.execute(op, ExecuteTimeout)
	if reply.WrongLeader {
		return
	}
	reply.Err = util.OK
}

func (sm *ShardMaster) Query(args *util.QueryArgs, reply *util.QueryReply) {
	op := util.Op{
		Args:      *args,
		Type:      util.OpQuery,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	reply.WrongLeader = sm.execute(op, ExecuteTimeout)
	if reply.WrongLeader {
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if args.Num == -1 {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else if args.Num < len(sm.configs) {
		reply.Config = sm.configs[args.Num]
	} else {
		reply.Config = sm.configs[len(sm.configs)-1]
	}
	reply.Err = util.OK
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) dispatch() {
	for message := range sm.applyCh {
		if !message.CommandValid {
			continue
		}
		op := message.Command.(util.Op)
		sm.mu.Lock()
		if sm.isDuplicateRequest(op.ClientId, op.CommandId) {
			sm.mu.Unlock()
			continue
		}
		switch op.Type {
		case util.OpJoin:
			sm.PerformJoin(op.Args.(util.JoinArgs))
		case util.OpLeave:
			sm.PerformLeave(op.Args.(util.LeaveArgs))
		case util.OpMove:
			sm.PerformMove(op.Args.(util.MoveArgs))
		}
		sm.commandIds[op.ClientId] = op.CommandId
		if ch, ok := sm.notifyChs[message.CommandIndex]; ok {
			ch <- op
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) PerformJoin(args util.JoinArgs) {
	config := sm.configs[len(sm.configs)-1]
	newGroups := make(map[int][]string)
	for gid, servers := range config.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	m := sm.Group2Shards(config)
	for gid, servers := range args.Servers {
		if _, ok := m[gid]; !ok {
			m[gid] = make([]int, 0)
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newGroups[gid] = newServers
		}
	}
	for {
		source := sm.GetSource(m)
		target := sm.GetTarget(m)
		if source != 0 && len(m[source])-len(m[target]) <= 1 {
			break
		}
		m[target] = append(m[target], m[source][0])
		m[source] = m[source][1:]
	}
	var newShards [util.NShards]int
	for gid, shards := range m {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	sm.configs = append(sm.configs, util.Config{
		Num:    len(sm.configs),
		Shards: newShards,
		Groups: newGroups,
	})
}

func (sm *ShardMaster) PerformLeave(args util.LeaveArgs) {
	config := sm.configs[len(sm.configs)-1]
	m := sm.Group2Shards(config)
	shards := make([]int, 0)
	newGroups := make(map[int][]string)
	for gid, servers := range config.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	for _, gid := range args.GIDs {
		delete(newGroups, gid)
		if s, ok := m[gid]; ok {
			shards = append(shards, s...)
			delete(m, gid)
		}
	}
	var newShards [util.NShards]int
	if len(newGroups) == 0 {
		for i := 0; i < util.NShards; i++ {
			newShards[i] = 0
		}
	} else {
		for _, shard := range shards {
			target := sm.GetTarget(m)
			m[target] = append(m[target], shard)
		}
		for gid, shards := range m {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	sm.configs = append(sm.configs, util.Config{
		Num:    len(sm.configs),
		Shards: newShards,
		Groups: newGroups,
	})
}

func (sm *ShardMaster) PerformMove(args util.MoveArgs) {
	config := sm.configs[len(sm.configs)-1]
	newGroups := make(map[int][]string)
	for gid, servers := range config.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	var newShards [util.NShards]int
	for i, gid := range config.Shards {
		newShards[i] = gid
	}
	newShards[args.Shard] = args.GID
	newConfig := util.Config{
		Num:    len(sm.configs),
		Groups: newGroups,
		Shards: newShards,
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) GetSource(m map[int][]int) int {
	if shards, ok := m[0]; ok && len(shards) > 0 {
		return 0
	}
	index := -1
	max := -1
	for gid, shards := range m {
		if len(shards) > max {
			max = len(shards)
			index = gid
		}
	}
	return index
}

func (sm *ShardMaster) GetTarget(m map[int][]int) int {
	index := -1
	min := util.NShards + 1
	for gid, shards := range m {
		if gid != 0 && len(shards) < min {
			min = len(shards)
			index = gid
		}
	}
	return index
}

func (sm *ShardMaster) Group2Shards(config util.Config) map[int][]int {
	m := make(map[int][]int)
	for gid := range config.Groups {
		m[gid] = make([]int, 0)
	}
	for i, gid := range config.Shards {
		m[gid] = append(m[gid], i)
	}
	return m
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(util.Op{})
	labgob.Register(util.JoinArgs{})
	labgob.Register(util.LeaveArgs{})
	labgob.Register(util.MoveArgs{})
	labgob.Register(util.QueryArgs{})

	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]util.Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.commandIds = make(map[int64]int64)
	sm.notifyChs = make(map[int]chan util.Op)
	sm.logger = logrus.New()
	sm.logger.SetLevel(logrus.ErrorLevel)
	go sm.dispatch()
	return sm
}
