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
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
	"sort"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)

var vtmap = [...]string{
	"VotePending",
	"VoteLost",
	"VoteWon",
}

func (vr VoteResult) String() string {
	return vtmap[vr]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	// 每个节点的ID
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// 保存集群所有节点ID的数组（包括自身）
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// 选举超时tick
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// 心跳超时tick
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

type Raft struct {
	id uint64

	Term uint64 // 任期号
	Vote uint64 // 投票给哪个节点ID

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool // 该map存放了哪些节点投票给本节点

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// tick advances the internal logical clock by a single tick.
	tick func()
	step stepFunc
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftLog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	if c.peers == nil {
		c.peers = cs.Nodes
	}
	r := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		votes:            map[uint64]bool{},
	}
	r.becomeFollower(r.Term, None)
	lastIndex := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			r.Prs[peer] = &Progress{Next: lastIndex + 1}
		}
	}
	r.resetRandomizedElectionTimeout()
	r.loadState(hs)
	if c.Applied > 0 {
		raftLog.appliedTo(c.Applied)
	}
	return r
}

func (r *Raft) softState() *SoftState { return &SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	// todo 一些判断，暂时没弄明白
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	m := pb.Message{}
	m.To, m.From = to, r.id

	preLogIndex := pr.Next - 1
	preLogTerm, errt := r.RaftLog.Term(preLogIndex)
	ents, erre := r.RaftLog.Entries(preLogIndex + 1)
	if errt != nil || erre != nil {
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		log.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x",
			r.id, r.RaftLog.FirstIndex(), r.RaftLog.committed, sindex, sterm, to)
	} else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = preLogIndex
		m.LogTerm = preLogTerm
		m.Commit = r.RaftLog.committed
		m.Entries = ents
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		Commit:  commit,
	}
	r.send(m)
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgTimeoutNow})
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// bcastHeartbeat 向所有节点发送没有entries的RPC
func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) advance(rd Ready) {
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		oldApplied := r.RaftLog.applied
		applyEntries := r.RaftLog.nextEnts()
		r.RaftLog.appliedTo(newApplied)
		log.Infof("%s %d apply entries: %v, now applyIdx: %d", r.State, r.id, applyEntries, r.RaftLog.applied)

		//  todo project3会用
		if oldApplied <= r.PendingConfIndex && newApplied >= r.PendingConfIndex && r.State == StateLeader {
			log.Infof("initiating automatic transition out of joint configuration")
		}
	}
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.RaftLog.stableTo(e.Index)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
		r.RaftLog.pendingSnapshot = nil
	}
	r.RaftLog.maybeCompact()
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()
	r.resetVotes()
	for peer := range r.Prs {
		r.Prs[peer].Match = 0
		r.Prs[peer].Next = r.RaftLog.LastIndex() + 1
		if peer == r.id {
			r.Prs[peer].Match = r.RaftLog.LastIndex()
		}
	}
	r.PendingConfIndex = 0
}

// for leader commit
func (r *Raft) maybeCommit() bool {
	i, n := 0, len(r.Prs)
	srt := make(uint64Slice, n)
	for _, prs := range r.Prs {
		srt[i] = prs.Match
		i++
	}
	sort.Sort(srt)
	newCommitIndex := srt[n-(n/2+1)]
	if newCommitIndex > r.RaftLog.committed { // 如果有半数复制的最高日志条目大于当前提交日志条目
		if r.RaftLog.marchLog(r.Term, newCommitIndex) { // 保证安全性
			return r.RaftLog.maybeCommit(newCommitIndex, r.Term)
		}
	}
	return false
}

func (r *Raft) appendEntry(es ...*pb.Entry) (accepted bool) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	li = r.RaftLog.append(es...)
	// update progress
	r.Prs[r.id].MaybeUpdate(li)
	// commit
	r.maybeCommit()
	return true
}

func (r *Raft) tickElection() {
	r.electionElapsed++

	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
			log.Debugf("error occurred during election: %v", err)
		}
	}
}

func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		// 如果当前的领导者在选举超时时间之前不能转移领导权，那么会再次成为领导者
		if r.State == StateLeader && r.leadTransferee != None {

		}
	}
	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Debugf("error occurred during checking sending heartbeat: %v", err)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.step = r.stepFollower
	r.State = StateFollower
	r.reset(term)
	r.Lead = lead
	r.tick = r.tickElection
	log.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = r.stepCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.State = StateCandidate
	log.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = r.stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartBeat
	r.Lead = r.id
	r.State = StateLeader

	// todo 为之后的配置更改做准备
	r.PendingConfIndex = r.RaftLog.LastIndex()

	emptyEnt := &pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		log.Panicf("empty entry was dropped")
	}
	log.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *Raft) hup() {
	if r.State == StateLeader {
		log.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}
	if !r.promotable() {
		log.Warningf("%x is unPromotable and can not campaign", r.id)
		return
	}
	log.Infof("%x is starting a new election at term %d", r.id, r.Term)
	r.campaign()
}

func (r *Raft) promotable() bool {
	pr := r.Prs[r.id]
	if pr == nil {
		log.Infof("%x is unPromotable because not in progress list", r.id)
		return false
	}
	if r.RaftLog.hasPendingSnapshot() {
		log.Infof("%x is unPromotable because not has pending snapshot", r.id)
		return false
	}
	return true
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	if _, _, res := r.poll(r.id, pb.MessageType_MsgRequestVote, true); res == VoteWon {
		r.becomeLeader()
		return
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		log.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), pb.MessageType_MsgRequestVote, peer, r.Term)
		r.send(pb.Message{Term: r.Term, To: peer, MsgType: pb.MessageType_MsgRequestVote, Index: lastIndex, LogTerm: lastLogTerm})
	}
}

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result VoteResult) {
	if v {
		log.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		log.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.RecordVote(id, v)
	return r.TallyVotes()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// raft状态机
func (r *Raft) Step(m pb.Message) error {
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		log.Infof("%x [term: %d] received a %s message with higher term from %v [term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None) || r.Term < m.Term
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			log.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			log.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	default:
		err := r.step(m)
		if err != nil {
			return err
		}
	}
	return nil
}

type stepFunc func(m pb.Message) error

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgTimeoutNow:
		log.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
		r.hup()
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, m.MsgType, !m.Reject)
		log.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.MsgType, rj)
		switch res {
		case VoteWon:
			r.becomeLeader()
			r.bcastAppend()
		case VoteLost:
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:
		log.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.State, m.From)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Panicf("%x stepped empty MsgProp", r.id)
		}
		if r.Prs[r.id] == nil {
			return ErrProposalDropped
		}
		if r.leadTransferee != None {
			log.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return ErrProposalDropped
		}
		//for i := range m.Entries {
		// todo 一些对于配置更新的处理，后续完成
		//}
		if !r.appendEntry(m.Entries...) {
			return ErrProposalDropped
		}
		r.bcastAppend()
		return nil
	}
	pr := r.Prs[m.From]
	if pr == nil {
		log.Debugf("%x no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			log.Debugf("%x received MsgAppResp(rejected, hint: (term %d)) from %x for index %d",
				r.id, m.LogTerm, m.From, m.Index)
			nextProbeIdx := r.RaftLog.findConflictByIndex(m.Index)
			if pr.MaybeDecrTo(m.Index, nextProbeIdx) {
				log.Debugf("%x decreased next index progress of %x to [%d]", r.id, m.From, pr.Next)
				r.sendAppend(m.From)
			}
		} else {
			if pr.MaybeUpdate(m.Index) {
				if r.maybeCommit() {
					r.bcastAppend()
				} else if pr.Match < r.RaftLog.LastIndex() {
					r.sendAppend(m.From)
				}
			}
			if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
				log.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
				r.sendTimeoutNow(m.From)
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:

	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex})
	} else {
		log.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Reject: true})
	}
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(m.Snapshot) {
		log.Infof("%x [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()})
	} else {
		log.Infof("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	}
}

// restore 从快照中恢复状态机。它恢复日志和状态机的配置。如果此方法返回 false，则忽略快照，因为它已过时或因为错误。
func (r *Raft) restore(s *pb.Snapshot) bool {
	if s.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	if r.State != StateFollower {
		log.Warningf("%x attempted to restore snapshot as leader; should never happen", r.id)
		r.becomeFollower(r.Term+1, None)
		return false
	}
	if r.RaftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		log.Infof("%x [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, "+
			"term: %d]",
			r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.LastTerm(), s.Metadata.Index, s.Metadata.Term)
		r.RaftLog.commitTo(s.Metadata.Index)
		return false
	}
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range s.Metadata.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.RaftLog.restore(s)
	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout 如果超时时间大于或等于
// [electionTimeout, 2 * electionTimeout - 1] 中的随机选举超时，
// 则返回 true
func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) resetVotes() {
	r.votes = map[uint64]bool{}
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

// RecordVote 记录在v==true时投票给id对应的节点(否则拒绝)
func (r *Raft) RecordVote(id uint64, v bool) {
	_, ok := r.votes[id]
	if !ok {
		r.votes[id] = v
	}
}

// TallyVotes 返回同意和拒绝的数量，以及结果是否已知
func (r *Raft) TallyVotes() (granted int, rejected int, result VoteResult) {
	for _, v := range r.votes {
		if v {
			granted++
		}
	}
	votes := len(r.votes)
	threshold := len(r.Prs)/2 + 1
	if granted >= threshold {
		result = VoteWon
	} else if votes-granted >= threshold {
		result = VoteLost
	} else {
		result = VotePending
	}
	rejected = votes - granted
	return granted, rejected, result
}
