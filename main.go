package main

import "log"

func main() {
	// startThreeRoutines()
	log.Printf("start routines...")
	StartRoutines([]ProcessID{1, 2, 3, 4}, func(pid ProcessID, status ProcessStatus) bool {
		if pid == 1 && status == 10 {
			return true
		}
		return false
	}, 1)
}
