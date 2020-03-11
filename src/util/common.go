package util

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// shardkv
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        int
	ClientId  int64
	CommandId int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	CommandId int64
	ConfigId  int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type PullShardsArgs struct {
	Num    int
	Shards []int
}

type PullShardsReply struct {
	DBs        [NShards]map[string]string
	CommandIds map[int64]int64
	OK         bool
}

type ReconfigureArgs struct {
	Config     Config
	DBs        [NShards]map[string]string
	CommandIds map[int64]int64
}

// shardmaster
const NShards = 10

type Config struct {
	Num    int
	Shards [NShards]int
	Groups map[int][]string
}

type JoinArgs struct {
	Servers   map[int][]string
	ClientId  int64
	CommandId int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClientId  int64
	CommandId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientId  int64
	CommandId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int
	ClientId  int64
	CommandId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
