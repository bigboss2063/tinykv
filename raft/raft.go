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
	"github.com/pingcap-incubator/tinykv/log"
	"math/rand"
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

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
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

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
	// RecentActive 用于记录这个节点是否还活跃
	RecentActive bool
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

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
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	randomizedElectionTimeout int

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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		log.Panicf(err.Error())
	}
	peers := c.peers
	if len(confState.Nodes) > 0 {
		if len(peers) > 0 {
			panic("cannot specify both newRaft(peers) and ConfState.Nodes)")
		}
		peers = confState.Nodes
	}
	raftLog := newLog(c.Storage)
	raft := &Raft{
		id:               c.ID,
		Term:             None,
		Vote:             None,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
	}
	for _, p := range peers {
		raft.Prs[p] = &Progress{Match: 0, Next: 1}
	}
	if !IsEmptyHardState(hardState) {
		raft.Term = hardState.Term
		raft.Vote = hardState.Vote
		raft.RaftLog.committed = hardState.Commit
	}
	if c.Applied > 0 {
		raftLog.applyTo(c.Applied)
	}
	raft.resetRandomizedElectionTimeout()
	return raft
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) advance(rd Ready) {
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		r.RaftLog.applyTo(newApplied)
	}
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		if r.id == r.Lead {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, Index: e.Index})
		}
		r.RaftLog.stableTo(e.Index)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

func (r *Raft) sendRequestVote(to uint64) {
	li := r.RaftLog.LastIndex()
	term, err := r.RaftLog.Term(li)
	if err != nil {
		log.Panicf(err.Error())
	}
	message := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: term,
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) broadcastAppend() {
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendAppend(to)
	}
}

func (r *Raft) broadcastHeartbeat() {
	for to := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendHeartbeat(to)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}
	li := r.RaftLog.LastIndex()
	var prevLogIndex uint64
	if pr.Next-1 > li {
		prevLogIndex = li
	} else {
		prevLogIndex = pr.Next - 1
	}
	prevLogTerm, tErr := r.RaftLog.Term(prevLogIndex)
	entries, eErr := r.RaftLog.Entries(prevLogIndex+1, r.RaftLog.LastIndex()+1)
	// Term 和 Entries 方法只会返回 ErrCompacted，表示要发送的日志已经被压缩成快照了，所以要发送快照
	if tErr != nil || eErr != nil {
		message := pb.Message{MsgType: pb.MessageType_MsgSnapshot, To: to, From: r.id, Term: r.Term}
		snapshot, err := r.RaftLog.Snapshot()
		if err != nil {
			// 快照还没准备好，等待下一次发送
			if err == ErrSnapshotTemporarilyUnavailable {
				DPrintf("%v failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err)
		}
		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}
		message.Snapshot = &snapshot
		DPrintf("%v send snapshot to %v", r.id, to)
		r.msgs = append(r.msgs, message)
		return true
	}
	message := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, message)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id, To: to,
		Commit: min(r.RaftLog.committed, r.Prs[to].Match), // 如果它没有这么长则不能提交到 leader 的 commit 的位置
		Term:   r.Term,
	}
	r.msgs = append(r.msgs, message)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatTicker()
	default:
		r.electionTicker()
	}
}

func (r *Raft) electionTicker() {
	r.electionElapsed++
	if r.promotable() && r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id})
	}
}

func (r *Raft) heartbeatTicker() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}
	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, To: r.id})
	}
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
	r.votes = make(map[uint64]bool)
	r.PendingConfIndex = None
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		log.Panicf("%v can not become candidate from leader", r.id)
	}
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		log.Panicf("%v can not become candidate from leader", r.id)
	}
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id
	for i := range r.Prs {
		r.Prs[i].Match = 0
		r.Prs[i].Next = r.RaftLog.LastIndex() + 1
	}
	ents, err := r.RaftLog.Entries(r.RaftLog.committed+1, r.RaftLog.LastIndex()+1)
	if err != nil {
		log.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}
	pendingConfIndex := indexOfPendingConf(ents)
	if pendingConfIndex != None {
		r.PendingConfIndex = pendingConfIndex
	}
	r.RaftLog.append(&pb.Entry{Index: r.RaftLog.LastIndex() + 1, Term: r.Term})
	// 为什么要更新自己的 progress ？不更新过不了测试
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	// 也有可能只有单独一个节点了，跟 propose 的时候一样直接提交
	if r.isSingleNode() {
		r.RaftLog.commitTo(r.RaftLog.LastIndex())
		return
	}
	r.broadcastAppend()
}

func (r *Raft) isSingleNode() bool {
	if _, ok := r.Prs[r.id]; ok && len(r.Prs) == 1 {
		return true
	}
	return false
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
		// 本地消息
	case m.Term > r.Term:
		var lead uint64
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		} else {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Reject: true, Term: r.Term})
		case pb.MessageType_MsgAppend:
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term})
		case pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From, Term: r.Term})
		}
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 收到 Hup 消息，发起一轮选举
		DPrintf("hup message: %v", m)
		if r.State == StateLeader {
			DPrintf("%v already became a leader", r.id)
			return nil
		}
		// 找出还没有应用但已经提交的 conf change
		ents := r.RaftLog.nextEnts()
		pendingConfIndex := None
		for i := range ents {
			if ents[i].EntryType == pb.EntryType_EntryConfChange {
				pendingConfIndex = ents[i].Index
				break
			}
		}
		// 存在已经提交但没有应用的 conf change 则不允许发起选举
		if pendingConfIndex != None && r.RaftLog.committed > r.RaftLog.applied {
			DPrintf("%v can't begin a election, because there is a conf change entry wait to apply ,index %v", r.id, pendingConfIndex)
			return nil
		}
		r.becomeCandidate()
		if r.isSingleNode() {
			r.becomeLeader()
			return nil
		}
		for to := range r.Prs {
			if to == r.id {
				continue
			}
			r.sendRequestVote(to)
		}
	case pb.MessageType_MsgRequestVote:
		DPrintf("request vote message: %v", m)
		reply := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Reject: true, Term: r.Term}
		if r.Vote == None || r.Vote == m.From {
			lastLogIndex := r.RaftLog.LastIndex()
			lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
			if err != nil {
				r.msgs = append(r.msgs, reply)
				return err
			}
			if lastLogIndex == 0 || lastLogTerm < m.LogTerm || (lastLogTerm == m.LogTerm && lastLogIndex <= m.Index) {
				reply.Reject = false
				r.Vote = m.From
				r.votes[m.From] = true
				r.electionElapsed = 0
				DPrintf("%v vote for %v at term %v", r.id, m.From, r.Term)
			}
		}
		r.msgs = append(r.msgs, reply)
	default:
		switch r.State {
		case StateLeader:
			tickLeader(r, m)
		case StateCandidate:
			tickCandidate(r, m)
		case StateFollower:
			tickFollower(r, m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	if m.Index < r.RaftLog.committed {
		resp.Index = r.RaftLog.committed
		r.msgs = append(r.msgs, resp)
		return
	}
	li := r.RaftLog.LastIndex()
	if li < m.Index {
		resp.Reject = true
		resp.Index = r.RaftLog.LastIndex() + 1
		r.msgs = append(r.msgs, resp)
		DPrintf("%v's logs are short than leader, last log index %v", r.id, r.RaftLog.LastIndex())
		return
	}
	if r.RaftLog.matchTerm(m.Index, m.LogTerm) {
		resp.Reject = false
		resp.Index = m.Index + uint64(len(m.Entries))
		logInsertIndex := m.Index + 1
		newEntriesIndex := 0
		for {
			if logInsertIndex >= li || newEntriesIndex >= len(m.Entries) {
				break
			}
			term, err := r.RaftLog.Term(logInsertIndex)
			if err != nil {
				log.Panicf("index %v ErrUnavailable", logInsertIndex)
			}
			if term != m.Entries[newEntriesIndex].Term {
				break
			}
			logInsertIndex++
			newEntriesIndex++
		}
		if newEntriesIndex < len(m.Entries) {
			m.Entries = m.Entries[newEntriesIndex:]
			li = r.RaftLog.append(m.Entries...)
		}
		if r.RaftLog.committed < m.Commit {
			r.RaftLog.commitTo(min(m.Commit, m.Index+uint64(len(m.Entries))))
			DPrintf("%v change committed to %v", r.id, r.RaftLog.committed)
		}
	} else {
		resp.Reject = true
		term, _ := r.RaftLog.Term(m.Index)
		resp.LogTerm = term
		resp.Index = m.Index
		// 找出冲突日志所在任期的第一条日志的索引
		for idx := m.Index; idx >= r.RaftLog.FirstIndex(); idx-- {
			if !r.RaftLog.matchTerm(idx, term) {
				resp.Index = idx + 1
				break
			}
		}
		DPrintf("%v conflict with leader at %v", r.id, resp.Index)
	}
	r.msgs = append(r.msgs, resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	resp := pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From, From: r.id, Term: r.Term, Reject: false}
	committed := r.RaftLog.commitTo(m.Commit)
	if committed {
		DPrintf("%v change committed to %v", r.id, r.RaftLog.committed)
	}
	r.msgs = append(r.msgs, resp)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	sIndex, sTerm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	message := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	if r.restore(m.Snapshot) {
		DPrintf("%v [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sIndex, sTerm)
		message.Index = r.RaftLog.LastIndex()
	} else {
		DPrintf("%x [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sIndex, sTerm)
		message.Index = r.RaftLog.committed
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) restore(s *pb.Snapshot) bool {
	if s.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	if r.RaftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		// 通过 matchTerm 方法，说明当前节点中已经有快照中的日志，不需要进行 restore，直接同步 committed 即可
		r.RaftLog.commitTo(s.Metadata.Index)
		return false
	}
	r.RaftLog.Restore(s)
	r.Prs = make(map[uint64]*Progress)
	for _, n := range s.Metadata.ConfState.Nodes {
		r.Prs[n] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = None
	// 不能添加重复的节点
	if _, ok := r.Prs[id]; ok {
		return
	}
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	r.PendingConfIndex = None
	if len(r.Prs) == 0 {
		return
	}
	// 现在节点变少了，那么可能达到提交的要求（一半的节点已经复制日志）了，尝试一下提交
	r.maybeCommit()
	// 如果被移除的节点是 leadTransferee，且当前节点是 leader，终止 leader transfer
	if r.State == StateLeader && id == r.leadTransferee {
		r.abortLeaderTransfer()
	}
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func (r *Raft) promotable() bool {
	_, ok := r.Prs[r.id]
	return ok
}

func (r *Raft) maybeCommit() {
	savedCommitted := r.RaftLog.committed
	for N := r.RaftLog.LastIndex(); N >= r.RaftLog.FirstIndex(); N-- {
		matchCount := 1
		for i := range r.Prs {
			if r.Prs[i].Match >= N && i != r.id {
				matchCount++
			}
		}
		term, _ := r.RaftLog.Term(N)
		if matchCount > len(r.Prs)/2 && term == r.Term && N > savedCommitted {
			r.RaftLog.commitTo(N)
			DPrintf("%v commit to %v at term %v, logs %v %v", r.id, N, r.Term, r.RaftLog.entries, r.RaftLog.committed)
			r.broadcastAppend()
			break
		}
	}
}

func tickLeader(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgPropose:
		DPrintf("%v receive propose message: %v", r.id, m)
		if len(m.Entries) == 0 {
			log.Panicf("%v can't append nil entries", r.id)
		}
		// 如果当前正在进行 leadTransfer，就不再接收新的日志，直接返回
		if r.leadTransferee != None {
			DPrintf("%v leadTransferee is not None %v, stopping propose logs", r.id, r.leadTransferee)
			return
		}
		// 先设置日志的 Index 再设置 PendingConfIndex
		li := r.RaftLog.LastIndex()
		for i := range m.Entries {
			m.Entries[i].Index = li + 1 + uint64(i)
			m.Entries[i].Term = r.Term
		}
		for i, e := range m.Entries {
			if e.EntryType == pb.EntryType_EntryConfChange {
				// 如果还有未 apply 的 conf change，就无视新的 conf change
				if r.PendingConfIndex != None {
					DPrintf("%v ignore conf change entry", r.id)
					m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
				}
				r.PendingConfIndex = m.Entries[i].Index
			}
		}
		li = r.RaftLog.append(m.Entries...)
		r.Prs[r.id].Match = li
		r.Prs[r.id].Next = li + 1
		if r.isSingleNode() {
			r.RaftLog.commitTo(li)
		}
		// 给 follower 发送 appendEntries message
		r.broadcastAppend()
	case pb.MessageType_MsgHeartbeatResponse:
		pr, ok := r.Prs[m.From]
		if !ok {
			return
		}
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppendResponse:
		DPrintf("%v receive append response msg %v", r.id, m)
		pr, ok := r.Prs[m.From]
		if !ok {
			return
		}
		switch m.Reject {
		case true:
			// 如果 logTerm 等于 0 表示 follower 在 pervLogIndex 位置没有日志，将 next 设置为 follower 的 lastIndex + 1
			if m.LogTerm == 0 {
				pr.Next = m.Index
			} else {
				lastIndexOfTerm := uint64(0)
				for i := r.RaftLog.LastIndex(); i >= r.RaftLog.FirstIndex(); i-- {
					if term, _ := r.RaftLog.Term(i); term == m.LogTerm {
						lastIndexOfTerm = i
						break
					}
				}
				// leader 有冲突任期的日志，那么把冲突任期后面任期的日志都发过去
				if lastIndexOfTerm > 0 {
					pr.Next = lastIndexOfTerm + 1
				} else {
					// 否则将包括 follower 冲突任期的第一条日志所在的索引位置和之后的日志全部发过去
					pr.Next = m.Index
				}
			}
			r.sendAppend(m.From)
		case false:
			pr.Match = m.Index
			pr.Next = r.Prs[m.From].Match + 1
			r.maybeCommit()
			// 如果这条消息是由 leadTransferee 发送过来的，并且日志已经同步到最新，那么就可以通知 leadTransferee 开始转移
			if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: m.From, From: r.id, Term: r.Term})
			}
		}
	case pb.MessageType_MsgTransferLeader:
		leaderTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leaderTransferee {
				DPrintf("leaderTransfer already begin, leaderTransferee %v", leaderTransferee)
				return
			}
			// 出现了新的 leaderTransferee，就把老的结束
			r.abortLeaderTransfer()
		}
		if leaderTransferee == r.id {
			DPrintf("leaderTransferee %v already been a leader", leaderTransferee)
			return
		}
		// 转移要在一个选举间隔内完成，重置选举时间
		r.electionElapsed = 0
		r.leadTransferee = leaderTransferee
		// leaderTransferee 的日志已经是最新的了，直接通知它转移
		if pr, ok := r.Prs[leaderTransferee]; ok && pr.Match == r.RaftLog.LastIndex() {
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: leaderTransferee, From: r.id, Term: r.Term})
		} else {
			// 否则帮助它更新到最新
			r.sendAppend(leaderTransferee)
		}
	}
}

func tickCandidate(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		DPrintf("request vote response reply: %v", m)
		r.votes[m.From] = !m.Reject
		reject, agree := 0, 0
		for _, vote := range r.votes {
			if vote {
				agree++
				continue
			}
			reject++
		}
		if agree > len(r.Prs)/2 {
			DPrintf("%v become leader at term %v", r.id, r.Term)
			r.becomeLeader()
			return
		}
		if reject > len(r.Prs)/2 {
			DPrintf("%v become follower at term %v, since election fail", r.id, r.Term)
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgAppend:
		DPrintf("%v receive append entries message: %v", r.id, m)
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		DPrintf("%v receive snapshot message: %v", r.id, m)
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		DPrintf("%v is a candidate and ignored time message from %v", r.id, m.From)
	}
}

func tickFollower(r *Raft, m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			return
		}
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
		DPrintf("%v redirect propose msg to lead %v", r.id, r.Lead)
	case pb.MessageType_MsgAppend:
		DPrintf("%v receive append entries message: %v", r.id, m)
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		DPrintf("%v receive snapshot message: %v", r.id, m)
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		DPrintf("%v receive timeout message: %v", r.id, m)
		if r.promotable() {
			// 还在集群中才能发起选举
			DPrintf("%v can begin a new election", r.id)
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, To: r.id, From: r.id})
			return
		}
		DPrintf("%v already been removed", r.id)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			DPrintf("%v don't have a leader", r.id)
			return
		}
		// 给 leader 转发消息
		m.To = r.Lead
		m.Term = r.Term
		r.msgs = append(r.msgs, m)
		DPrintf("%v redirect transfer leader msg to leader %v", r.id, r.Lead)
	}
}

func indexOfPendingConf(ents []*pb.Entry) uint64 {
	pendingConfIndex := None
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			pendingConfIndex = ents[i].Index
			break
		}
	}
	return pendingConfIndex
}
