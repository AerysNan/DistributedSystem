package raft

import (
	"github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		logrus.Printf(format, a...)
	}
	return
}

func StateToString(state int) string {
	switch state {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	}
	return ""
}

func RandomRange(min int, max int) time.Duration {
	return time.Duration(rand.Intn(max-min) + min)
}
