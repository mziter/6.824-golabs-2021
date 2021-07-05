package raft

import (
	"log"
	"os"
	"strconv"
)

// Debugging
const (
	VotingDebug    = false
	AppendingDebug = false
	ElectionDebug  = false
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
