package raft

import (
	"log"
	"slices"
)

// Debugging

type Log int

const (
	Test      Log = iota
	Election  Log = iota
	State     Log = iota
	StateFine Log = iota
	Accept    Log = iota
	Commit    Log = iota
)

var Logs = []Log{Accept, Commit, State}

func DPrintf(logType Log, format string, a ...interface{}) {
	if slices.Contains(Logs, logType) || logType == Test {
		format = []string{"Test  ", "Electn", "State ", "StateF", "Accept", "Commit"}[logType] + ": " + format
		log.Printf(format, a...)
	}
	return
}
