package util

const (
	OpJoin = iota
	OpMove
	OpLeave
	OpQuery

	OpPut
	OpAppend
	OpGet

	OpSnapshot
	OpReconfigure
)

type Op struct {
	Type      int
	ClientId  int64
	CommandId int64
	Args      interface{}
}

type Response struct {
	Op         Op
	WrongGroup bool
}

func (op *Op) Equals(other Op) bool {
	if op.Type == OpReconfigure {
		return op.Args.(ReconfigureArgs).Config.Num == other.Args.(ReconfigureArgs).Config.Num
	}
	return op.ClientId == other.ClientId && op.CommandId == other.CommandId
}

type SnapshotArgs struct {
	Data []byte
}
