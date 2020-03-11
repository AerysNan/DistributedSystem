package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"distributed/labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:   servers,
		clientId:  nrand(),
		commandId: 0,
	}
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:       num,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && !reply.WrongLeader {
				ck.commandId++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:   servers,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && !reply.WrongLeader {
				ck.commandId++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:      gids,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				ck.commandId++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && !reply.WrongLeader {
				ck.commandId++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
