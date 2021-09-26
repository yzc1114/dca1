package main

import (
	"strconv"
)

type ChanMsg interface {
	String() string
}
type MarkerMsg int
func (m MarkerMsg) String() string {
	return strconv.Itoa(int(m))
}
type Epoch int
type ProcessStatus int
func (m ProcessStatus) String() string {
	return strconv.Itoa(int(m))
}
type Msg string // Can be anything
func (m Msg) String() string {
	return string(m)
}
type ChanMsgWithName struct {
	C    chan ChanMsg
	Name string
}
type ProcessID int

type RecorderMsgSource interface{}
type ProcessSource RecorderMsgSource
type ChanSource RecorderMsgSource

var processSource ProcessSource
var chanSource ChanSource

type RecorderMsg struct {
	Epoch Epoch
	Source      RecorderMsgSource
	ProcessID ProcessID
	ChanName    string
	ProcessStatus ProcessStatus
	RecordedValues     []Msg
}

func NewRecorderMsgChan() chan *RecorderMsg {
	return make(chan *RecorderMsg)
}

type ChanPair struct {
	ProcessID ProcessID
	fromChan *ChanMsgWithName
	toChan *ChanMsgWithName
}

func NewChanPair(pid ProcessID, fromChan, toChan *ChanMsgWithName) *ChanPair {
	return &ChanPair{
		ProcessID: pid,
		fromChan:  fromChan,
		toChan:    toChan,
	}
}
