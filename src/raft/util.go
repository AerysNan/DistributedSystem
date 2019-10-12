package raft

import "github.com/sirupsen/logrus"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		logrus.Printf(format, a...)
	}
	return
}
