package raftkv

import (
	"crypto/rand"
	"distributed/labrpc"
	"distributed/util"
	"math/big"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int64
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
		leaderId:  0,
	}
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	for {
		args := util.GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			CommandId: ck.commandId,
		}
		var reply util.GetReply
		if ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply); !ok || reply.WrongLeader {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId += 1
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var opType int
	if op == "Put" {
		opType = util.OpPut
	} else {
		opType = util.OpAppend
	}
	for {
		args := util.PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        opType,
			ClientId:  ck.clientId,
			CommandId: ck.commandId,
		}
		var reply util.PutAppendReply
		if ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply); !ok || reply.WrongLeader {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId += 1
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
