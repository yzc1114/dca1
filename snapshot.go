package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"
)

func NewChanMsgWithName(name string) *ChanMsgWithName {
	return &ChanMsgWithName{
		C:    make(chan ChanMsg),
		Name: name,
	}
}

func doSnapshotRecorderProcess(ctx context.Context, processCount int, recorderChan chan *RecorderMsg) {
	type ProcessInfo struct {
		ProcessID   ProcessID
		ProcessStatus ProcessStatus
		Msgs          map[string][]Msg // chan name to msgs
	}
	type snapshotInfo map[ProcessID]*ProcessInfo
	type epoch2SnapshotInfo map[Epoch]snapshotInfo
	epoch2Snapshots := make(epoch2SnapshotInfo)

	extractTargetProcessInfo := func(recorderMsg *RecorderMsg) *ProcessInfo {
		if _, ok := epoch2Snapshots[recorderMsg.Epoch]; !ok {
			epoch2Snapshots[recorderMsg.Epoch] = make(snapshotInfo)
		}
		snapshot := epoch2Snapshots[recorderMsg.Epoch]
		processID := recorderMsg.ProcessID
		if _, ok := snapshot[processID]; !ok {
			snapshot[processID] = &ProcessInfo{
				ProcessID:   processID,
				ProcessStatus: nil,
				Msgs:          make(map[string][]Msg),
			}
		}
		return snapshot[recorderMsg.ProcessID]
	}
	getFinishedEpochs := func() []Epoch {
		finishedEpochs := make([]Epoch, 0)
		nextEpoch:
		for epoch, snapshot := range epoch2Snapshots {
			if len(snapshot) < processCount {
				continue
			}
			for _, processInfo := range snapshot {
				if processInfo.ProcessStatus == ProcessStatus(nil) {
					continue nextEpoch
				}
				if len(processInfo.Msgs) < processCount - 1 {
					continue nextEpoch
				}
			}
			finishedEpochs = append(finishedEpochs, epoch)
		}
		return finishedEpochs
	}
	printSnapshot := func(epoch Epoch) {
		snapshot := epoch2Snapshots[epoch]
		if snapshot == nil {
			log.Fatalf("printSnapshot epoch not exists, epoch = [%v]", epoch)
		}
		log.Printf("start print epoch %v snapshot...", epoch)
		for pid, processInfo := range snapshot {
			log.Printf("processID = [%v]", pid)
			log.Printf("processStatus = [%v]", processInfo.ProcessStatus)
			log.Printf("process's recordMsgs from each chan...")
			for fromChanName, msgs := range processInfo.Msgs {
				log.Printf("process's record from chan [%s]: [%+v]", fromChanName, msgs)
			}
		}
		log.Printf("finished print epoch %v snapshot...", epoch)
	}
	// start main routine cycle
	for {
		select {
		case m := <-recorderChan:
			targetProcessInfo := extractTargetProcessInfo(m)
			source := m.Source
			switch source.(type) {
			case ProcessSource:
				targetProcessInfo.ProcessStatus = m.ProcessStatus
			case ChanSource:
				targetProcessInfo.Msgs[m.ChanName] = m.RecordedValues
			default:
				log.Fatalf("source not specified or violated, m = {%v}", m)
			}
			case <-ctx.Done():
				log.Printf("snapshot recorder shutdown.")
				// 优雅关停
				return
		}
		finishedEpochs := getFinishedEpochs()
		for _, epoch := range finishedEpochs {
			printSnapshot(epoch)
		}
	}
}

func doProcess(ctx context.Context, selfProcessID ProcessID, chanPairs map[ProcessID]*ChanPair, recorderChan chan *RecorderMsg, shouldStartSnapshot func(processID ProcessID, processStatus ProcessStatus) bool) {
	processStatus := 0
	if shouldStartSnapshot == nil {
		shouldStartSnapshot = func(pid ProcessID, processStatus ProcessStatus) bool {
			return false
		}
	}

	getNextProcessID := func(selfProcessID, targetProcessID ProcessID) ProcessID {
		keys := make([]int, 0, len(chanPairs))
		for k := range chanPairs {
			keys = append(keys, int(k))
		}
		sort.Ints(keys)
		idx := sort.SearchInts(keys, int(targetProcessID))
		idx = (idx + 1) % len(keys)
		if keys[idx] != int(selfProcessID) {
			return ProcessID(keys[idx])
		}
		idx = (idx + 1) % len(keys)
		return ProcessID(keys[idx])
	}

	// 使用闭包定义在主循环中使用的各类函数。
	dealWithMarkerMsgIncoming, addRecordMsgIfNecessary := func() (func(markerMsg MarkerMsg, fromProcessID ProcessID, pair *ChanPair), func(msg Msg)) {
		type isRecording struct {
			ProcessID      ProcessID
			Recording      bool
			RecordedValues []Msg
		}
		type recordStatus map[ProcessID]*isRecording // processID2IsRecording
		type epoch2recordStatus map[Epoch]recordStatus

		recordStatusForAllEpochs := make(epoch2recordStatus)

		isNewMarkerMsg := func(markerMsg MarkerMsg) bool {
			_, ok := recordStatusForAllEpochs[Epoch(markerMsg)]
			return ok
		}
		initRecordStatus := func(markerMsg MarkerMsg) {
			s := make(recordStatus)
			recordStatusForAllEpochs[Epoch(markerMsg)] = s
			for i := range chanPairs {
				s[i] = &isRecording{
					ProcessID:      i,
					Recording:      true,
					RecordedValues: make([]Msg, 0),
				}
			}
		}
		getCurrRecordStatus := func(markerMsg MarkerMsg) recordStatus {
			return recordStatusForAllEpochs[Epoch(markerMsg)]
		}

		dealWithMarkerMsgIncoming := func(markerMsg MarkerMsg, fromProcessID ProcessID, pair *ChanPair) {
			// start snapshot
			stopRecordingSendValues := func(pid ProcessID) {
				rs := getCurrRecordStatus(markerMsg)
				s := rs[pid]
				s.Recording = false
				recorderChan <- &RecorderMsg{
					Source:    chanSource,
					ProcessID: selfProcessID,
					RecordedValues: s.RecordedValues,
				}
			}
			if isNewMarkerMsg(markerMsg) {
				initRecordStatus(markerMsg)
				recorderChan <- &RecorderMsg{
					Source:    processSource,
					ProcessID: selfProcessID,
					ProcessStatus:   ProcessStatus(processStatus),
				}
				if fromProcessID != selfProcessID {
					stopRecordingSendValues(fromProcessID)
				}
			} else {
				// 不是第一次收到marker msg，则将此次的发信者记录停止，并将内容发送至记录者手中。
				stopRecordingSendValues(fromProcessID)
			}
		}

		addRecordMsgIfNecessary := func(msg Msg) {
			for _, rs := range recordStatusForAllEpochs {
				for _, ir := range rs {
					if ir.Recording {
						ir.RecordedValues = append(ir.RecordedValues, msg)
					}
				}
			}
		}

		return dealWithMarkerMsgIncoming, addRecordMsgIfNecessary
	} ()



	for {
		for fromProcessID, chanPair := range chanPairs {
			select {
			case m := <-chanPair.fromChan.C:
				switch m.(type) {
				case MarkerMsg:
					dealWithMarkerMsgIncoming(m.(MarkerMsg), fromProcessID, chanPair)
				case Msg:
					addRecordMsgIfNecessary(m.(Msg))
					processStatus++
					nextProcessID := getNextProcessID(selfProcessID, fromProcessID)
					nextChanPair := chanPairs[nextProcessID]
					nextChanPair.toChan.C <- m
					if shouldStartSnapshot(selfProcessID, ProcessStatus(processStatus)) {
						// 算法启动时，认为从一个不存在的通道上接收到了一个标记
						epoch := genNewSnapshotEpoch()
						markerMsg := MarkerMsg(epoch)
						dealWithMarkerMsgIncoming(markerMsg, selfProcessID, nil)
					}
				case ProcessStatus:
					log.Fatalf("process %d received ProcessStatus", selfProcessID)
				}
			case <-ctx.Done():
				// 优雅关停
				log.Printf("process %d shutdown.", selfProcessID)
				return
			default:
				// 没收到消息直接访问下一个fromChan
				continue
			}
		}
	}
}

func startThreeRoutines() {
	pid1 := ProcessID(1)
	pid2 := ProcessID(2)
	pid3 := ProcessID(3)

	recorderChan := NewRecorderMsgChan()

	c12 := NewChanMsgWithName("C12")
	c13 := NewChanMsgWithName("C13")
	c21 := NewChanMsgWithName("C21")
	c23 := NewChanMsgWithName("C23")
	c31 := NewChanMsgWithName("C31")
	c32 := NewChanMsgWithName("C32")

	ctx, cancel := context.WithCancel(context.Background())
	go doSnapshotRecorderProcess(ctx, 3, recorderChan)
	// start p1
	p1ChanPairs := map[ProcessID]*ChanPair {
		pid2: NewChanPair(pid2, c21, c12),
		pid3: NewChanPair(pid3, c31, c13),
	}
	p2ChanPairs := map[ProcessID]*ChanPair {
		pid1: NewChanPair(pid1, c12, c21),
		pid3: NewChanPair(pid3, c32, c23),
	}
	p3ChanPairs := map[ProcessID]*ChanPair {
		pid1: NewChanPair(pid1, c13, c31),
		pid2: NewChanPair(pid2, c23, c32),
	}
	go doProcess(ctx, pid1, p1ChanPairs, recorderChan, func(processID ProcessID, processStatus ProcessStatus) bool {
		return processStatus == 101
	})
	go doProcess(ctx, pid2, p2ChanPairs, recorderChan, nil)
	go doProcess(ctx, pid3, p3ChanPairs, recorderChan, nil)
	time.Sleep(10*time.Second)
	cancel()
}

func startRoutines(count int, shouldStartSnapshot func(pid ProcessID, status ProcessStatus) bool) {
	pids := make([]ProcessID, 0, count)
	for i := 0; i < count; i++ {
		pids = append(pids, ProcessID(i))
	}

	recorderChan := NewRecorderMsgChan()
	chans := make([][]*ChanMsgWithName, count)
	for i := 0; i < count; i++ {
		chans[i] = make([]*ChanMsgWithName, count)
		for j := 0; j < count; i++ {
			if i != j {
				chans[i][j] = NewChanMsgWithName(fmt.Sprintf("C%d%d", i, j))
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go doSnapshotRecorderProcess(ctx, 3, recorderChan)
	pid2ChanPairs := make(map[ProcessID]map[ProcessID]*ChanPair)
	for _, pid := range pids {
		cps := make(map[ProcessID]*ChanPair)
		for _, otherPid := range pids {
			if otherPid != pid {
				pid := int(pid)
				otherPid := int(otherPid)
				cps[ProcessID(otherPid)] = NewChanPair(ProcessID(otherPid), chans[otherPid][pid], chans[pid][otherPid])
			}
		}
		pid2ChanPairs[pid] = cps
	}
	for pid, cps := range pid2ChanPairs {
		go doProcess(ctx, pid, cps, recorderChan, shouldStartSnapshot)
	}
	time.Sleep(10*time.Second)
	cancel()
}

func main() {

}
