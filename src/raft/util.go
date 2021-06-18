package raft

import "log"

// Debugging
const (
	VotingDebug    = false
	AppendingDebug = false
	ElectionDebug  = false
)

func DPrintf(flag bool, format string, a ...interface{}) (n int, err error) {
	if flag {
		log.Printf(format, a...)
	}
	return
}
