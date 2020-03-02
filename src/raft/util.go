package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// Debugging

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

func EntriesToString(entries []LogEntry) string {
	s := ""
	for _, entry := range entries {
		s += fmt.Sprintf("%v ", entry.Term)
	}
	return fmt.Sprintf("{%v}", s)
}

func RandomRange(min int, max int) time.Duration {
	return time.Duration(rand.Intn(max-min) + min)
}
