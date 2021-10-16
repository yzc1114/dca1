package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"
)

var Conf = &SnapshotAppConf{
	AppMsgs:                           []AppMsg{"AppMsg1"},
	AppProcessMainLoopIntervalBetween: []time.Duration{50 * time.Millisecond, 60 * time.Millisecond},
	RecorderMainLoopInterval:          50 * time.Millisecond,
}

func SetConf(conf *SnapshotAppConf) {
	confRange := conf.AppProcessMainLoopIntervalBetween
	if confRange != nil && len(confRange) != 2 && confRange[0] <= confRange[1] {
		panic("AppProcessMainLoopIntervalBetween must be a range")
	}
	if conf.AppMsgs != nil {
		Conf.AppMsgs = conf.AppMsgs
	}
	if conf.RecorderMainLoopInterval != 0 {
		Conf.RecorderMainLoopInterval = conf.RecorderMainLoopInterval
	}
	if confRange != nil {
		Conf.AppProcessMainLoopIntervalBetween = confRange
	}
}

type SnapshotRecorderProcess struct {
	ctx             context.Context
	processCount    int
	recorderChan    chan *RecorderMsg
	epoch2Snapshots Epoch2SnapshotInfo
}

func NewSnapshotRecorderProcess(ctx context.Context, processCount int, recorderChan chan *RecorderMsg) *SnapshotRecorderProcess {
	return &SnapshotRecorderProcess{
		ctx:             ctx,
		processCount:    processCount,
		recorderChan:    recorderChan,
		epoch2Snapshots: make(Epoch2SnapshotInfo),
	}
}

func (rp *SnapshotRecorderProcess) extractTargetProcessInfo(recorderMsg *RecorderMsg) *ProcessInfo {
	if _, ok := rp.epoch2Snapshots[recorderMsg.Epoch]; !ok {
		rp.epoch2Snapshots[recorderMsg.Epoch] = make(SnapshotInfo)
	}
	snapshot := rp.epoch2Snapshots[recorderMsg.Epoch]
	processID := recorderMsg.ProcessID
	if _, ok := snapshot[processID]; !ok {
		snapshot[processID] = &ProcessInfo{
			ProcessID:     processID,
			ProcessStatus: ProcessStatus(-1),
			Msgs:          make(map[string][]AppMsg),
		}
	}
	return snapshot[recorderMsg.ProcessID]
}

func (rp *SnapshotRecorderProcess) getFinishedEpochs() []Epoch {
	finishedEpochs := make([]Epoch, 0)
nextEpoch:
	for epoch, snapshot := range rp.epoch2Snapshots {
		if len(snapshot) < rp.processCount {
			continue
		}
		for _, processInfo := range snapshot {
			if processInfo.ProcessStatus == ProcessStatus(-1) {
				continue nextEpoch
			}
			if len(processInfo.Msgs) < rp.processCount-1 {
				continue nextEpoch
			}
		}
		finishedEpochs = append(finishedEpochs, epoch)
	}
	return finishedEpochs
}

func (rp *SnapshotRecorderProcess) getPrintUtil() (func(epoch Epoch), func(epoch Epoch) bool, func(epoch Epoch)) {
	printedEpochs := make(map[Epoch]bool)
	addPrintedEpoch := func(epoch Epoch) {
		printedEpochs[epoch] = true
	}
	isEpochPrinted := func(epoch Epoch) bool {
		_, ok := printedEpochs[epoch]
		return ok
	}
	printer := func(epoch Epoch) {
		snapshot := rp.epoch2Snapshots[epoch]
		if snapshot == nil {
			log.Fatalf("printSnapshot epoch not exists, epoch = [%v]", epoch)
		}
		log.Printf("Start print epoch %v snapshot...", epoch)
		processIDs := make([]int, 0, len(snapshot))
		for pid := range snapshot {
			processIDs = append(processIDs, int(pid))
		}
		sort.Ints(processIDs)
		for _, pid := range processIDs {
			processInfo := snapshot[ProcessID(pid)]
			log.Printf("processID = [%v]", pid)
			log.Printf("processStatus = [%v]", processInfo.ProcessStatus)
			log.Printf("process's recordMsgs from each chan...")
			for fromChanName, msgs := range processInfo.Msgs {
				log.Printf("process's record from chan [%s]: %+v", fromChanName, msgs)
			}
		}
		log.Printf("finished print epoch %v snapshot...", epoch)
	}
	return addPrintedEpoch, isEpochPrinted, printer
}

func (rp *SnapshotRecorderProcess) Start() {
	log.Printf("Start recorder main routine loop")
	addPrintedEpoch, isEpochPrinted, printer := rp.getPrintUtil()
	for {
		select {
		case m := <-rp.recorderChan:
			targetProcessInfo := rp.extractTargetProcessInfo(m)
			source := m.Source
			switch source {
			case SourceProcess:
				targetProcessInfo.ProcessStatus = m.ProcessStatus
			case SourceChan:
				targetProcessInfo.Msgs[m.ChanName] = m.RecordedValues
			default:
				log.Fatalf("source not specified or violated, m = {%v}", m)
			}
		case <-rp.ctx.Done():
			log.Printf("snapshot recorder shutdown.")
			// 优雅关停
			return
		default:
			time.Sleep(Conf.RecorderMainLoopInterval)
			// check finished epochs
		}
		finishedEpochs := rp.getFinishedEpochs()
		for _, epoch := range finishedEpochs {
			if isEpochPrinted(epoch) {
				continue
			}
			printer(epoch)
			addPrintedEpoch(epoch)
		}
	}
}

type AppProcess struct {
	ctx                 context.Context
	processStatus       ProcessStatus
	selfProcessID       ProcessID
	chanPairs           map[ProcessID]*ChanPair
	recorderChan        chan *RecorderMsg
	shouldStartSnapshot func(processID ProcessID, processStatus ProcessStatus) bool
}

func NewAppProcess(ctx context.Context, selfProcessID ProcessID, chanPairs map[ProcessID]*ChanPair, recorderChan chan *RecorderMsg, shouldStartSnapshot func(processID ProcessID, processStatus ProcessStatus) bool) *AppProcess {
	return &AppProcess{
		ctx:                 ctx,
		processStatus:       0,
		selfProcessID:       selfProcessID,
		chanPairs:           chanPairs,
		recorderChan:        recorderChan,
		shouldStartSnapshot: shouldStartSnapshot,
	}
}

func (a *AppProcess) getNextProcessID() ProcessID {
	keys := make([]int, 0, len(a.chanPairs))
	for k := range a.chanPairs {
		if k != adminPid {
			keys = append(keys, int(k))
		}
	}
	sort.Ints(keys)
	idx := sort.SearchInts(keys, int(a.selfProcessID))
	idx = idx % len(keys)
	if keys[idx] != int(a.selfProcessID) {
		return ProcessID(keys[idx])
	}
	idx = (idx + 1) % len(keys)
	return ProcessID(keys[idx])
}

func (a *AppProcess) genMarkerMsgUtil() (func(markerMsg MarkerMsg, fromProcessID ProcessID), func(fromChanName string, msg AppMsg)) {
	type recordStatus struct {
		ProcessID      ProcessID
		Recording      bool
		FromChanName   string
		RecordedValues []AppMsg
	}
	type pid2recordStatus map[ProcessID]*recordStatus // processID2IsRecording
	type epoch2recordStatus map[Epoch]pid2recordStatus

	recordStatusForAllEpochs := make(epoch2recordStatus)

	isNewMarkerMsg := func(markerMsg MarkerMsg) bool {
		_, ok := recordStatusForAllEpochs[getEpochByMarker(markerMsg)]
		return !ok
	}
	initRecordStatus := func(markerMsg MarkerMsg) {
		s := make(pid2recordStatus)
		recordStatusForAllEpochs[getEpochByMarker(markerMsg)] = s
		for _, chanPair := range a.chanPairs {
			pid := chanPair.ProcessID
			s[pid] = &recordStatus{
				ProcessID:      pid,
				FromChanName:   chanPair.FromChan.Name,
				Recording:      true,
				RecordedValues: make([]AppMsg, 0),
			}
		}
	}

	dealWithMarkerMsgIncoming := func(markerMsg MarkerMsg, fromProcessID ProcessID) {
		stopRecordingSendValues := func(pid ProcessID) {
			rs := recordStatusForAllEpochs[getEpochByMarker(markerMsg)]
			s := rs[pid]
			s.Recording = false
			log.Printf("Process %d stop recording from Process %d, RecordedValues = %+v", a.selfProcessID, pid, s.RecordedValues)
			a.recorderChan <- &RecorderMsg{
				Epoch: getEpochByMarker(markerMsg),
				Source:         SourceChan,
				ProcessID:      a.selfProcessID,
				ChanName:       s.FromChanName,
				RecordedValues: s.RecordedValues,
			}
		}
		if isNewMarkerMsg(markerMsg) {
			// 第一次收到MarkerMsg，首先将自己的进程信息记录，随后向所有其他进程发送MarkerMsg
			log.Printf("Process %d received first MarkerMsg %v", a.selfProcessID, markerMsg)
			initRecordStatus(markerMsg)
			log.Printf("Process %d sending ProcessStatus %d", a.selfProcessID, a.processStatus)
			a.recorderChan <- &RecorderMsg{
				Epoch: getEpochByMarker(markerMsg),
				Source:        SourceProcess,
				ProcessID:     a.selfProcessID,
				ProcessStatus: a.processStatus,
			}
			if fromProcessID != adminPid {
				stopRecordingSendValues(fromProcessID)
			}
			// 向其他全部的toChan发送MarkerMsg
			for _, chanPair := range a.chanPairs {
				if chanPair.ProcessID == adminPid {
					continue
				}
				chanPair.ToChan.C <- markerMsg
			}
		} else {
			// 不是第一次收到marker msg，则将此次的发信者记录停止，并将内容发送至记录者手中。
			stopRecordingSendValues(fromProcessID)
		}
	}

	addRecordMsgIfNecessary := func(fromChanName string, msg AppMsg) {
		for _, rs := range recordStatusForAllEpochs {
			for _, ir := range rs {
				if ir.FromChanName == fromChanName && ir.Recording {
					ir.RecordedValues = append(ir.RecordedValues, msg)
				}
			}
		}
	}

	return dealWithMarkerMsgIncoming, addRecordMsgIfNecessary
}

func (a *AppProcess) start() {
	log.Printf("Start process %d main routine loop", a.selfProcessID)
	dealWithMarkerMsgIncoming, addRecordMsgIfNecessary := a.genMarkerMsgUtil()
	for {
		for fromProcessID, chanPair := range a.chanPairs {
			select {
			case m := <-chanPair.FromChan.C:
				switch m.(type) {
				case MarkerMsg:
					log.Printf("Process %d received MarkerMsg %+v from Process %d", a.selfProcessID, m.(MarkerMsg), fromProcessID)
					dealWithMarkerMsgIncoming(m.(MarkerMsg), fromProcessID)
				case AppMsg:
					addRecordMsgIfNecessary(chanPair.FromChan.Name, m.(AppMsg))
					a.processStatus++
					log.Printf("Process %d received AppMsg from Process %d, msg content = [%+v], curr ProcessStatus = %d", a.selfProcessID, fromProcessID, m.(AppMsg), a.processStatus)
					nextProcessID := a.getNextProcessID()
					log.Printf("Process %d sending AppMsg to Process %d", a.selfProcessID, nextProcessID)
					nextChanPair := a.chanPairs[nextProcessID]
					nextChanPair.ToChan.C <- m
					if a.shouldStartSnapshot(a.selfProcessID, ProcessStatus(a.processStatus)) {
						log.Printf("Process %d Start snapshot, current processStatus = %d", a.selfProcessID, a.processStatus)
						// 算法启动时，认为从一个不存在的通道上接收到了一个标记
						epoch := genNewSnapshotEpoch()
						markerMsg := getMarkerMsgByEpoch(epoch)
						dealWithMarkerMsgIncoming(markerMsg, adminPid)
					}
				default:
					log.Fatalf("process %d received invalid msg = [%+v]", a.selfProcessID, m)
				}
			case <-a.ctx.Done():
				// 优雅关停
				log.Printf("process %d shutdown.", a.selfProcessID)
				return
			default:
				// 没收到消息直接访问下一个fromChan
				lower := Conf.AppProcessMainLoopIntervalBetween[0]
				upper := Conf.AppProcessMainLoopIntervalBetween[1]
				s := rand.Intn(int(upper-lower)) + int(lower)
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
	go NewSnapshotRecorderProcess(ctx, len(pids), recorderChan).Start()
	pid2ChanPairs := make(map[ProcessID]map[ProcessID]*ChanPair)
	for _, pid := range pids {
		cps := make(map[ProcessID]*ChanPair)
		for _, otherPid := range pids {
			if otherPid != pid {
				cps[otherPid] = NewChanPair(otherPid, fromChans[pid][otherPid], toChans[pid][otherPid])
			} else {
				// admin process
				cps[adminPid] = NewChanPair(adminPid, adminChans[pid].from, adminChans[pid].to)
			}
		}
		pid2ChanPairs[pid] = cps
	}
	for pid, cps := range pid2ChanPairs {
		go NewAppProcess(ctx, pid, cps, recorderChan, shouldStartSnapshot).start()
	}

	for _, msg := range Conf.AppMsgs {
		adminChans[kickerPID].to.C <- msg
	}

	time.Sleep(60 * time.Second)
	cancel()
}
