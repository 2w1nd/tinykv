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
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:    storage,
		entries:    entries,
		committed:  hardState.Commit,
		applied:    firstIndex - 1,
		stabled:    lastIndex,
		firstIndex: firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex()
	if first > l.firstIndex {
		if len(l.entries) > 0 {
			entries := l.entries[first-l.firstIndex:]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.firstIndex = first
	}
}

// for follower append entries, commit log
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...*pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			if ci-offset > uint64(len(ents)) {
				log.Panicf("index, %d, is out of range [%d]", ci-offset, len(ents))
			}
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		log.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	// todo 应该还有其他情况
	for _, ent := range ents {
		if ent.Index < l.firstIndex {
			continue
		}
		if ent.Index <= l.LastIndex() {
			logTerm, err := l.Term(ent.Index)
			if err != nil {
				panic(err)
			}
			if logTerm != ent.Term {
				idx := ent.Index - l.firstIndex
				l.entries[idx] = *ent
				l.entries = l.entries[:idx+1]
				l.stabled = min(l.stabled, ent.Index-1)
			}
		} else {
			l.entries = append(l.entries, *ent)
		}
	}
	return l.LastIndex()
}

func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]", ne.Index, l.zeroTermOnErrCompacted(l.Term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); index > li {
		log.Warningf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
			index, li)
		return index
	}
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	//off := max(l.applied+1, l.firstIndex)
	//if l.committed+1 > off {
	//	ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
	//	if err != nil {
	//		l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
	//	}
	//	return ents
	//}
	//return nil

	if len(l.entries) > 0 {
		return l.entries[l.applied-l.firstIndex+1 : l.committed-l.firstIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) stableTo(index uint64, term uint64) {
	// todo ?
	l.stabled = index
}

func (l *RaftLog) LastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	dummyIndex := l.firstIndex - 1
	if i < dummyIndex || i > l.LastIndex() {
		return 0, nil
	}
	if len(l.entries) > 0 && i >= l.firstIndex {
		return l.entries[i-l.firstIndex].Term, nil
	}
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

func (l *RaftLog) Entries(lo, hi uint64) ([]pb.Entry, error) {
	if lo >= l.firstIndex && hi-l.firstIndex <= uint64(len(l.entries)) {
		return l.entries[lo-l.firstIndex : hi-l.firstIndex], nil
	}
	return l.storage.Entries(lo, hi)
}

func (l *RaftLog) isUpToDate(index, term uint64) bool {
	// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) marchLog(term, index uint64) bool {
	logTerm, _ := l.Term(index)
	return index <= l.LastIndex() && logTerm == term
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}
