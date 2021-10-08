package main

import (
	"strconv"
	"sync"
)

const (
	adminPid              = -1
	chanMsgBuffer         = 5
	recorderMsgChanBuffer = 5
)

// ChanMsg 定义程序间通用的消息传递对象。
type ChanMsg interface {
	String() string
}

// MarkerMsg 定义MarkerMsg为int，与Epoch存在一一对应的关系。
type MarkerMsg int

func (m MarkerMsg) String() string {
	return strconv.Itoa(int(m))
}

// Epoch int型，表示全局唯一的一次快照ID，与MarkerMsg内含数值一一对应。
type Epoch int

// 生成全局唯一的快照ID
var genNewSnapshotEpoch = func() func() Epoch {
	mu := &sync.Mutex{}
	epoch := 0
	return func() Epoch {
		mu.Lock()
		defer mu.Unlock()
		epoch += 1
		return Epoch(epoch)
	}
}()

// ProcessStatus 程序状态值。
type ProcessStatus int

func (m ProcessStatus) String() string {
	return strconv.Itoa(int(m))
}

// AppMsg 程序之间传递的应用级消息，可以是任意值（interface{}），这里用string代替。
type AppMsg string // Can be anything
func (m AppMsg) String() string {
	return string(m)
}

// FromChanMsgWithName 单向命名通道的实现。
type FromChanMsgWithName struct {
	C    <-chan ChanMsg
	Name string
}

func NewMsgChan() chan ChanMsg {
	return make(chan ChanMsg, chanMsgBuffer)
}

func NewFromChanMsgWithName(name string, chanMsg chan ChanMsg) *FromChanMsgWithName {
	return &FromChanMsgWithName{
		C:    chanMsg,
		Name: name,
	}
}

// DualChanMsgWithName 双向命名通道的实现。
type DualChanMsgWithName struct {
	to   *ToChanMsgWithName
	from *FromChanMsgWithName
}

func NewDualChanMsgWithName(name string, chanMsg chan ChanMsg) *DualChanMsgWithName {
	return &DualChanMsgWithName{
		&ToChanMsgWithName{
			C:    chanMsg,
			Name: name,
		},
		&FromChanMsgWithName{
			C:    chanMsg,
			Name: name,
		},
	}
}

// ToChanMsgWithName 单向命名通道的实现。
type ToChanMsgWithName struct {
	C    chan<- ChanMsg
	Name string
}

func NewToChanMsgWithName(name string, chanMsg chan ChanMsg) *ToChanMsgWithName {
	return &ToChanMsgWithName{
		C:    chanMsg,
		Name: name,
	}
}

// ProcessID 表示进程ID
type ProcessID int

// RecorderMsgSource 表示快照记录进程，收到快照消息的来源。可以是进程自身状态，也可以是进程对每个接入通道的数据。
type RecorderMsgSource int

const (
	SourceProcess RecorderMsgSource = 1
	SourceChan    RecorderMsgSource = 2
)

// RecorderMsg 传输给快照记录进程的数据。包括这次快照的ID，数据源，进程号，通道名，进程状态以及针对接入通道记录的数据。
type RecorderMsg struct {
	Epoch     Epoch
	Source    RecorderMsgSource
	ProcessID ProcessID

	// For Source == SourceProcess
	ProcessStatus ProcessStatus

	// For Source == SourceChan
	ChanName       string
	RecordedValues []AppMsg
}

func NewRecorderMsgChan() chan *RecorderMsg {
	return make(chan *RecorderMsg, recorderMsgChanBuffer)
}

// ChanPair 针对每个进程提供的一组进出通道。
type ChanPair struct {
	ProcessID ProcessID            // 目标进程号
	fromChan  *FromChanMsgWithName // 从该目标进程到本进程的通道
	toChan    *ToChanMsgWithName   // 从本进程到目标进程的通道
}

func NewChanPair(pid ProcessID, fromChan *FromChanMsgWithName, toChan *ToChanMsgWithName) *ChanPair {
	return &ChanPair{
		ProcessID: pid,
		fromChan:  fromChan,
		toChan:    toChan,
	}
}

type ProcessInfo struct {
	ProcessID     ProcessID
	ProcessStatus ProcessStatus
	Msgs          map[string][]AppMsg // chan name to msgs
}
type SnapshotInfo map[ProcessID]*ProcessInfo
type Epoch2SnapshotInfo map[Epoch]SnapshotInfo
