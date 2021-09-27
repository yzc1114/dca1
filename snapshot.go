package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"
)

const (
	recorderCheckInterval = 50 * time.Millisecond
)

var processCheckIntervalRange = []time.Duration{50 * time.Millisecond, 60 * time.Millisecond}

func doSnapshotRecorderProcess(ctx context.Context, processCount int, recorderChan chan *RecorderMsg) {
	type ProcessInfo struct {
		ProcessID     ProcessID
		ProcessStatus ProcessStatus
		Msgs          map[string][]Msg // chan name to msgs
	}
	type snapshotInfo map[ProcessID]*ProcessInfo
	type epoch2SnapshotInfo map[Epoch]snapshotInfo
	epoch2Snapshots := make(epoch2SnapshotInfo)
	// util func 1 将该记录信息对应的快照进程信息提取出来。
	extractTargetProcessInfo := func(recorderMsg *RecorderMsg) *ProcessInfo {
		if _, ok := epoch2Snapshots[recorderMsg.Epoch]; !ok {
			epoch2Snapshots[recorderMsg.Epoch] = make(snapshotInfo)
		}
		snapshot := epoch2Snapshots[recorderMsg.Epoch]
		processID := recorderMsg.ProcessID
		if _, ok := snapshot[processID]; !ok {
			snapshot[processID] = &ProcessInfo{
				ProcessID:     processID,
				ProcessStatus: ProcessStatus(-1),
				Msgs:          make(map[string][]Msg),
			}
		}
		return snapshot[recorderMsg.ProcessID]
	}

	// util func 2 获取已经结束记录的快照ID
	getFinishedEpochs := func() []Epoch {
		finishedEpochs := make([]Epoch, 0)
	nextEpoch:
		for epoch, snapshot := range epoch2Snapshots {
			if len(snapshot) < processCount {
				continue
			}
			for _, processInfo := range snapshot {
				if processInfo.ProcessStatus == ProcessStatus(-1) {
					continue nextEpoch
				}
				if len(processInfo.Msgs) < processCount-1 {
					continue nextEpoch
				}
			}
			finishedEpochs = append(finishedEpochs, epoch)
		}
		return finishedEpochs
	}

	// util func 3 获取已经输出过得快照以及，判断快照是否输出过。
	addPrintedEpoch, isEpochPrinted := func() (func(epoch Epoch), func(epoch Epoch) bool) {
		printedEpochs := make(map[Epoch]bool)
		addPrintedEpoch := func(epoch Epoch) {
			printedEpochs[epoch] = true
		}
		isEpochPrinted := func(epoch Epoch) bool {
			_, ok := printedEpochs[epoch]
			return ok
		}
		return addPrintedEpoch, isEpochPrinted
	}()

	// util func 4 打印快照到日志
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
	// start main routine loop
	log.Printf("start recorder main routine loop")
	for {
		select {
		case m := <-recorderChan:
			targetProcessInfo := extractTargetProcessInfo(m)
			source := m.Source
			switch source {
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
		default:
			time.Sleep(recorderCheckInterval)
			// check finished epochs
		}
		finishedEpochs := getFinishedEpochs()
		for _, epoch := range finishedEpochs {
			if isEpochPrinted(epoch) {
				continue
			}
			printSnapshot(epoch)
			addPrintedEpoch(epoch)
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

	getNextProcessID := func(selfProcessID ProcessID) ProcessID {
		keys := make([]int, 0, len(chanPairs))
		for k := range chanPairs {
			if k != adminPid {
				keys = append(keys, int(k))
			}
		}
		sort.Ints(keys)
		idx := sort.SearchInts(keys, int(selfProcessID))
		idx = idx % len(keys)
		if keys[idx] != int(selfProcessID) {
			return ProcessID(keys[idx])
		}
		idx = (idx + 1) % len(keys)
		return ProcessID(keys[idx])
	}

	// 使用闭包定义在主循环中使用的各类函数。
	dealWithMarkerMsgIncoming, addRecordMsgIfNecessary := func() (func(markerMsg MarkerMsg, fromProcessID ProcessID), func(fromChanName string, msg Msg)) {
		type isRecording struct {
			ProcessID      ProcessID
			Recording      bool
			FromChanName   string
			RecordedValues []Msg
		}
		type recordStatus map[ProcessID]*isRecording // processID2IsRecording
		type epoch2recordStatus map[Epoch]recordStatus

		recordStatusForAllEpochs := make(epoch2recordStatus)

		isNewMarkerMsg := func(markerMsg MarkerMsg) bool {
			_, ok := recordStatusForAllEpochs[Epoch(markerMsg)]
			return !ok
		}
		initRecordStatus := func(markerMsg MarkerMsg) {
			s := make(recordStatus)
			recordStatusForAllEpochs[Epoch(markerMsg)] = s
			for _, chanPair := range chanPairs {
				pid := chanPair.ProcessID
				s[pid] = &isRecording{
					ProcessID:      pid,
					FromChanName:   chanPair.fromChan.Name,
					Recording:      true,
					RecordedValues: make([]Msg, 0),
				}
			}
		}
		getCurrRecordStatus := func(markerMsg MarkerMsg) recordStatus {
			return recordStatusForAllEpochs[Epoch(markerMsg)]
		}

		dealWithMarkerMsgIncoming := func(markerMsg MarkerMsg, fromProcessID ProcessID) {
			// start snapshot
			stopRecordingSendValues := func(pid ProcessID) {
				rs := getCurrRecordStatus(markerMsg)
				s := rs[pid]
				s.Recording = false
				log.Printf("Process %d stop recording from Process %d, RecordedValues = [%+v]", selfProcessID, pid, s.RecordedValues)
				recorderChan <- &RecorderMsg{
					Source:         ChanSource,
					ProcessID:      selfProcessID,
					ChanName:       s.FromChanName,
					RecordedValues: s.RecordedValues,
				}
			}
			if isNewMarkerMsg(markerMsg) {
				// 第一次收到MarkerMsg，首先将自己的进程信息记录，随后向所有其他进程发送MarkerMsg
				log.Printf("Process %d received first MarkerMsg %v", selfProcessID, markerMsg)
				initRecordStatus(markerMsg)
				log.Printf("Process %d sending ProcessStatus %d", selfProcessID, processStatus)
				recorderChan <- &RecorderMsg{
					Source:        ProcessSource,
					ProcessID:     selfProcessID,
					ProcessStatus: ProcessStatus(processStatus),
				}
				if fromProcessID != selfProcessID {
					stopRecordingSendValues(fromProcessID)
				}
				// 向其他全部的toChan发送MarkerMsg
				for _, chanPair := range chanPairs {
					if chanPair.ProcessID == adminPid {
						continue
					}
					chanPair.toChan.C <- markerMsg
				}
			} else {
				// 不是第一次收到marker msg，则将此次的发信者记录停止，并将内容发送至记录者手中。
				stopRecordingSendValues(fromProcessID)
			}
		}

		addRecordMsgIfNecessary := func(fromChanName string, msg Msg) {
			for _, rs := range recordStatusForAllEpochs {
				for _, ir := range rs {
					if ir.FromChanName == fromChanName && ir.Recording {
						ir.RecordedValues = append(ir.RecordedValues, msg)
					}
				}
			}
		}

		return dealWithMarkerMsgIncoming, addRecordMsgIfNecessary
	}()

	log.Printf("start process %d main routine loop", selfProcessID)
	for {
		for fromProcessID, chanPair := range chanPairs {
			select {
			case m := <-chanPair.fromChan.C:
				switch m.(type) {
				case MarkerMsg:
					log.Printf("Process %d received MarkerMsg %+v from Process %d", selfProcessID, m.(MarkerMsg), fromProcessID)
					dealWithMarkerMsgIncoming(m.(MarkerMsg), fromProcessID)
				case Msg:
					addRecordMsgIfNecessary(chanPair.fromChan.Name, m.(Msg))
					processStatus++
					log.Printf("Process %d received Msg from Process %d, msg content = [%+v], curr ProcessStatus = %d", selfProcessID, fromProcessID, m.(Msg), processStatus)
					nextProcessID := getNextProcessID(selfProcessID)
					log.Printf("Process %d sending Msg to Process %d", selfProcessID, nextProcessID)
					nextChanPair := chanPairs[nextProcessID]
					nextChanPair.toChan.C <- m
					if shouldStartSnapshot(selfProcessID, ProcessStatus(processStatus)) {
						log.Printf("Process %d start snapshot, current processStatus = %d", selfProcessID, processStatus)
						// 算法启动时，认为从一个不存在的通道上接收到了一个标记
						epoch := genNewSnapshotEpoch()
						markerMsg := MarkerMsg(epoch)
						dealWithMarkerMsgIncoming(markerMsg, selfProcessID)
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
				lower := processCheckIntervalRange[0]
				upper := processCheckIntervalRange[1]
				s := rand.Intn(int(upper-lower)) + int(processCheckIntervalRange[0])
				time.Sleep(time.Duration(s))
				continue
			}
		}
	}
}

func StartRoutines(pids []ProcessID, shouldStartSnapshot func(pid ProcessID, status ProcessStatus) bool, kickerPID ProcessID) {
	recorderChan := NewRecorderMsgChan()
	fromChans := make(map[ProcessID]map[ProcessID]*FromChanMsgWithName)
	toChans := make(map[ProcessID]map[ProcessID]*ToChanMsgWithName)
	adminChans := make(map[ProcessID]*DualChanMsgWithName)
	for _, pid := range pids {
		fromChans[pid] = make(map[ProcessID]*FromChanMsgWithName)
		toChans[pid] = make(map[ProcessID]*ToChanMsgWithName)
	}
	for i := 0; i < len(pids); i++ {
		pid := pids[i]
		for j := i; j < len(pids); j++ {
			otherPID := pids[j]
			if pid != otherPID {
				cij := NewMsgChan()
				cji := NewMsgChan()

				cjiName := fmt.Sprintf("C%d%d", otherPID, pid)
				fromChans[pid][otherPID] = NewFromChanMsgWithName(cjiName, cji)
				toChans[otherPID][pid] = NewToChanMsgWithName(cjiName, cji)

				cijName := fmt.Sprintf("C%d%d", pid, otherPID)
				fromChans[otherPID][pid] = NewFromChanMsgWithName(cijName, cij)
				toChans[pid][otherPID] = NewToChanMsgWithName(cijName, cij)
			} else {
				c := NewMsgChan()
				adminDualChan := NewDualChanMsgWithName(fmt.Sprintf("admin_%d", pid), c)
				adminChans[pid] = adminDualChan
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go doSnapshotRecorderProcess(ctx, len(pids), recorderChan)
	pid2ChanPairs := make(map[ProcessID]map[ProcessID]*ChanPair)
	for _, pid := range pids {
		cps := make(map[ProcessID]*ChanPair)
		for _, otherPid := range pids {
			if otherPid != pid {
				cps[otherPid] = NewChanPair(otherPid, fromChans[pid][otherPid], toChans[pid][otherPid])
			} else {
				// admin process
				cps[adminPid] = NewChanPair(adminPid, adminChans[pid].FromChanMsgWithName, adminChans[pid].ToChanMsgWithName)
			}
		}
		pid2ChanPairs[pid] = cps
	}
	for pid, cps := range pid2ChanPairs {
		go doProcess(ctx, pid, cps, recorderChan, shouldStartSnapshot)
	}

	adminChans[kickerPID].ToChanMsgWithName.C <- Msg("Msg")

	time.Sleep(60 * time.Second)
	cancel()
}
