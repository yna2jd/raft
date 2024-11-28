package raft

import (
	"log"
	"slices"
)

// Debugging

type Log int

const Debugging = true

const (
	Test      Log = iota
	Election  Log = iota
	State     Log = iota
	StateFine Log = iota
	Accept    Log = iota
	Commit    Log = iota
	Count     Log = iota
)

var Logs = []Log{Accept, Commit, Election, State, StateFine, Count}

func DPrintf(logType Log, format string, a ...interface{}) {
	if Debugging && (slices.Contains(Logs, logType) || logType == Test) {
		format = []string{"Test  ", "Electn", "State ", "StateF", "Accept", "Commit", "Count "}[logType] + ": " + format
		log.Printf(format, a...)
	}
	return
}
