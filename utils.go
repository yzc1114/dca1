package main

import "sync"

var genNewSnapshotEpoch = func() func()Epoch {
	mu := &sync.Mutex{}
	epoch := 1
	return func() Epoch{
		defer func() {
			mu.Lock()
			defer mu.Unlock()
			epoch += 1
		}()
		return Epoch(epoch)
	}
}()