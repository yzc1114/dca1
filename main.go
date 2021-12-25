package main

import (
	"log"
	"time"
)

func main() {
	log.Printf("Start routines...")
	SetConf(&SnapshotAppConf{
		AppMsgs:                           []AppMsg{"AppMsg1"},                                          // 给出了程序传送哪些消息，是一个切片，程序间可以传输多个消息。按照题意，这里应该是1个消息。
		AppProcessMainLoopIntervalBetween: []time.Duration{5 * time.Millisecond, 10 * time.Millisecond}, // 决定了程序运行的快慢
	})
	StartRoutines([]ProcessID{1, 2, 3, 4, 5, 6, 7}, func(pid ProcessID, status ProcessStatus) bool {
		if pid == 1 && (status == 101 || status == 201) {
			return true
		}
		return false
	}, 1)
}
