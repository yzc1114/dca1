package main

import (
	"strconv"
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

// ProcessStatus 程序状态值。
type ProcessStatus int

func (m ProcessStatus) String() string {
	return strconv.Itoa(int(m))
}

// Msg 程序之间传递的消息，可以是任意值（interface{}），这里用string代替。
type Msg string // Can be anything
func (m Msg) String() string {
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

// DualChanMsgWithName 单向命名通道的实现。
type DualChanMsgWithName struct {
	*ToChanMsgWithName
	*FromChanMsgWithName
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
	ProcessSource RecorderMsgSource = 1
	ChanSource    RecorderMsgSource = 2
)

// RecorderMsg 传输给快照记录进程的数据。包括这次快照的ID，数据源，进程号，通道名，进程状态以及针对接入通道记录的数据。
type RecorderMsg struct {
	Epoch     Epoch
	Source    RecorderMsgSource
	ProcessID ProcessID

	// For Source == ProcessSource
	ProcessStatus ProcessStatus

	// For Source == ChanSource
	ChanName       string
	RecordedValues []Msg
}

func NewRecorderMsgChan() chan *RecorderMsg {
	return make(chan *RecorderMsg, recorderMsgChanBuffer)
}

// ChanPair 针对每个进程提供的一组进出通道。
type ChanPair struct {
	ProcessID ProcessID        // 目标进程号
	fromChan  *FromChanMsgWithName // 从该目标进程到本进程的通道
	toChan    *ToChanMsgWithName // 从本进程到目标进程的通道
}

func NewChanPair(pid ProcessID, fromChan *FromChanMsgWithName, toChan *ToChanMsgWithName) *ChanPair {
	return &ChanPair{
		ProcessID: pid,
		fromChan:  fromChan,
		toChan:    toChan,
	}
}
