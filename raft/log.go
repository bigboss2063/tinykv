// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Panicf(err.Error())
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Panicf(err.Error())
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	raftLog := &RaftLog{
		storage:         storage,
		committed:       firstIndex - 1,
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		dummyIndex:      firstIndex - 1,
	}
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	after := ents[0].Index
	if after <= l.committed {
		log.Panicf("Cannot overwrite logs that have been submitted!")
	}
	li := l.LastIndex()
	switch {
	case after == li+uint64(len(l.entries)):
		// 新日志跟在后面则直接接上
		for i := range ents {
			l.entries = append(l.entries, *ents[i])
		}
	default:
		// 否则需要拼接一下
		l.entries = l.entries[:after-l.dummyIndex-1]
		for i := range ents {
			l.entries = append(l.entries, *ents[i])
		}
		if l.stabled >= after {
			l.stabled = after - 1
		}
	}
	return l.LastIndex()
}

func (l *RaftLog) Entries(lo, hi uint64) ([]*pb.Entry, error) {
	if len(l.entries) == 0 {
		return nil, nil
	}
	entries, err := l.slice(lo, hi)
	if err != nil {
		return nil, err
	}
	result := make([]*pb.Entry, 0)
	for i := range entries {
		result = append(result, &entries[i])
	}
	return result, nil
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.stabled-l.dummyIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied == l.committed {
		return nil
	}
	// 如果 applied + 1 的那条日志已经被压缩了，那就没办法拿到了会直接 panic，所有要比较一下跟 firstIndex 谁比较大
	offset := max(l.applied+1, l.FirstIndex())
	if l.committed+1 > offset {
		ents, err := l.slice(offset, l.committed+1)
		if err != nil {
			log.Panicf(err.Error())
		}
		return ents
	}
	return nil
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) != 0 {
		return l.entries[0].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return firstIndex
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if length := len(l.entries); length != 0 {
		return l.dummyIndex + uint64(length)
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	li, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return li
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.dummyIndex || i > l.LastIndex() {
		return 0, nil
	}
	if l.pendingSnapshot != nil {
		if i == l.pendingSnapshot.Metadata.Index {
			return l.pendingSnapshot.Metadata.Term, nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			return 0, ErrCompacted
		}
	}
	if i == l.dummyIndex {
		return l.storage.Term(i)
	}
	if len(l.entries) == 0 {
		return 0, nil
	}
	return l.entries[i-l.dummyIndex-1].Term, nil
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	mt, err := l.Term(index)
	if err != nil {
		return false
	}
	return mt == term
}

func (l *RaftLog) applyTo(appliedTo uint64) {
	if appliedTo == 0 {
		return
	}
	if appliedTo < l.applied || appliedTo > l.committed {
		log.Panicf("applyTo a invalid index %v, apply range (%v, %v]", appliedTo, l.applied, l.committed)
	}
	l.applied = appliedTo
}

func (l *RaftLog) commitTo(commitTo uint64) bool {
	if l.committed < commitTo {
		if l.LastIndex() < commitTo {
			log.Panicf("commitTo %v can't bigger than lastIndex %v", commitTo, l.LastIndex())
		}
		l.committed = commitTo
		return true
	}
	return false
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	return l.entries[lo-l.dummyIndex-1 : hi-l.dummyIndex-1], nil
}

func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.FirstIndex()
	if lo < fi {
		return ErrCompacted
	}
	length := l.LastIndex() + 1 - fi
	if hi > fi+length {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}

func (l *RaftLog) stableTo(i uint64) {
	if i > l.stabled {
		l.stabled = i
	}
}

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index != 0
}

func (l *RaftLog) Snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *RaftLog) Restore(s *pb.Snapshot) {
	l.committed = s.Metadata.Index
	l.dummyIndex = s.Metadata.Index
	l.stabled = s.Metadata.Index
	l.entries = nil
	l.pendingSnapshot = s
}
