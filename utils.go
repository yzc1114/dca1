package main

import "sync"

var genNewSnapshotEpoch = func() func()Epoch {
	mu := &sync.Mutex{}
	epoch := 0
	return func() Epoch{
		mu.Lock()
		defer mu.Lock()
		epoch += 1
		return Epoch(epoch)
	}
}()