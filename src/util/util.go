package util

import "github.com/sirupsen/logrus"

type Op struct {
	Key       string
	Value     string
	Type      string
	ClientId  int64
	CommandId int64
}

func (op *Op) Equals(other Op) bool {
	return op.ClientId == other.ClientId && op.CommandId == other.CommandId
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		logrus.Printf(format, a...)
	}
	return
}
