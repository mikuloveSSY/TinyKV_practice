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
	"math/rand"
	"sort"

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

	electionTimeoutBase int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	//增加Tick字段用于切换时钟
	tickFn func()

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
	hardstate, _, _ := c.Storage.InitialState()
	raftlog := newLog(c.Storage)
	raftlog.committed = hardstate.Commit
	r := &Raft{
		id:                  c.ID,
		Term:                hardstate.Term,
		RaftLog:             raftlog,
		electionTimeoutBase: c.ElectionTick,
		heartbeatTimeout:    c.HeartbeatTick,
	}
	//Prs——用于当Leader时追踪每个 Follower的日志到哪了
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{Match: 0, Next: 1}
	}
	r.becomeFollower(r.Term, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	prevLogIndex := pr.Next - 1
	// 用于Follower确认已经收到的最后一条的term是否一致
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		// prevLogIndex 已被 compact，需要发 snapshot（2C 再处理）
		return false
	}

	// 创建要发送的日志切片，从Next到lastIndex
	lastIndex := r.RaftLog.LastIndex()
	entries := make([]*pb.Entry, 0)
	for i := pr.Next; i <= lastIndex; i++ {
		idx := i - r.RaftLog.entries[0].Index
		entries = append(entries, &pb.Entry{
			Index: r.RaftLog.entries[idx].Index,
			Term:  r.RaftLog.entries[idx].Term,
			Data:  r.RaftLog.entries[idx].Data,
		})
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.tickFn()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	if r.Term < term {
		r.Term = term
	}
	r.Lead = lead
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.tickFn = r.tickElection
	// 重新随机化选举超时，避免节点间同时超时引发分裂投票
	r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase)

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	//投票给自己
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.tickFn = r.tickElection
	// 重新随机化选举超时，避免反复同时超时引发分裂投票
	r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase)
	// 全部只剩1个结点时，自然都不用选举了，直接成为领导者
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	// Term不用管，因为成为Leader前必然经过candidate，已经加过了，是新一轮
	// 选举结束了，与投票相关的都清空
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.tickFn = r.tickHeartbeat
	// 成为新Leader后，要重置一下Prs
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Match: 0, // 默认设置为0表示还未知
			// 下次从自己最后一条之后开始发
			Next: r.RaftLog.LastIndex() + 1,
		}
	}
	// NOTE: 2AA 阶段不追加 noop 日志，避免破坏 Leader 轮换
	// （日志复制在 2AB 实现后再加上）
	// 增加一条空日志，用于刷新其他结点状态，并推动旧日志的提交
	noop := pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, noop)
	// 因为增加了新日志，所以要更新一下prs里的自己的progress
	r.Prs[r.id].Match = noop.Index
	r.Prs[r.id].Next = noop.Index + 1
	// 分发空日志更新Follower结点状态
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 本地消息跳过对term的判断，直接处理
	if m.MsgType != pb.MessageType_MsgHup &&
		m.MsgType != pb.MessageType_MsgBeat &&
		m.MsgType != pb.MessageType_MsgPropose {
		// term 比我高说明进入了新任期，重置状态
		// term 比我低说明过时了，忽略
		if m.Term > r.Term {
			// Leader由后面的更新
			r.becomeFollower(m.Term, None)
		} else if m.Term < r.Term {
			return nil
		}
	}
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		// 单节点时直接变 Leader 了
		if r.State != StateCandidate {
			return nil
		}
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      id,
				From:    r.id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			})
		}

	case pb.MessageType_MsgRequestVote:
		reject := false
		// 1. 已经投给别人了，拒绝
		if r.Vote != None && r.Vote != m.From {
			reject = true
		}
		// 2. 日志比较：对方的日志至少和自己一样新
		if !reject {
			lastIndex := r.RaftLog.LastIndex()
			lastTerm, _ := r.RaftLog.Term(lastIndex)
			if m.LogTerm < lastTerm {
				reject = true
			} else if m.LogTerm == lastTerm && m.Index < lastIndex {
				reject = true
			}
		}
		// 通过所有检查 → 投票
		if !reject {
			r.Vote = m.From
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  reject,
		})

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)

	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		// 单节点直接变 Leader 了，不用再发
		if r.State != StateCandidate {
			return nil
		}
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      id,
				From:    r.id,
				Term:    r.Term,
				Index:   lastIndex,
				LogTerm: lastTerm,
			})
		}

	case pb.MessageType_MsgRequestVote:
		// 比较日志新旧，再决定是否退位
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		// 对方日志只有比我新时 → 投票给它，退位；否则继续竞争
		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index > lastIndex) {
			r.becomeFollower(r.Term, None)
			r.Vote = m.From
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  r.Vote != m.From,
		})

	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		// 统计票数
		granted := 0
		rejected := 0
		for _, v := range r.votes {
			if v {
				granted++
			} else {
				rejected++
			}
		}
		// 注意不能直接if-else，因为其他结点的投票结果不是同时到达的
		if granted > len(r.Prs)/2 {
			r.becomeLeader()
		} else if rejected > len(r.Prs)/2 {
			r.becomeFollower(r.Term, None)
		}

	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)

	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		// 这里的测试不考虑
	}
	return nil
}
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for id := range r.Prs {
			if id != r.id {
				r.sendHeartbeat(id)
			}
		}

	case pb.MessageType_MsgRequestVote:
		// 有个相同新的term的结点在拉票
		// 回复拒绝
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})

	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)

	case pb.MessageType_MsgHeartbeatResponse:
		// 收到心跳回复，确认 Follower 还活着
		// 如果这个Follower落后，则补发日志
		if r.Prs[m.From].Next <= r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}

	case pb.MessageType_MsgPropose:
		// 把提案的 entries 追加到自己的日志
		lastIndex := r.RaftLog.LastIndex()
		for _, e := range m.Entries {
			e.Term = r.Term
			e.Index = lastIndex + 1
			lastIndex = e.Index
			r.RaftLog.entries = append(r.RaftLog.entries, *e)
		}
		// 更新自己的 Progress
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
		// 广播给所有 Follower
		for id := range r.Prs {
			if id != r.id {
				r.sendAppend(id)
			}
		}

	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			// Follower拒绝 → Prs里对应结点记录的Next减1往回找一致点
			r.Prs[m.From].Next--
			r.sendAppend(m.From)
		} else {
			// Follower 接受 → 更新 Match 和 Next
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1
			// 尝试推进 commit
			// 收集Prs里所有Match，取中位数
			// 因为中位数说明已经有半数结点认同了的
			matches := make([]uint64, 0, len(r.Prs))
			for _, pr := range r.Prs {
				matches = append(matches, pr.Match)
			}
			sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
			newCommit := matches[len(matches)/2]
			// 大于记录的commit时准备提交
			if newCommit > r.RaftLog.committed {
				term, _ := r.RaftLog.Term(newCommit)
				// 只有当前 term 的 entry 才能通过计数提交
				// 这个规则必须遵守
				if term == r.Term {
					r.RaftLog.committed = newCommit
					// 广播新的 commit 给所有人
					for id := range r.Prs {
						if id != r.id {
							r.sendAppend(id)
						}
					}
				}
			}
		}

	case pb.MessageType_MsgAppend:
		// 既然能收到相同的term的日志复制消息，选择退位
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// 1、一致性检查：prevLogIndex 处 term 是否匹配
	if m.Index > r.RaftLog.LastIndex() {
		// 说明 Follower 日志更短，告诉 Leader 自己最后一条的位置
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
			Index:   r.RaftLog.LastIndex() + 1, // 告诉 Leader 下一条日志从这里发
		})
		return
	}
	term, _ := r.RaftLog.Term(m.Index)
	if term != m.LogTerm {
		// prevLogIndex 处 term 不匹配，拒绝
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	// 2、冲突解决 & 追加
	for _, e := range m.Entries {
		if e.Index <= r.RaftLog.LastIndex() {
			// 若这个 index 本地已有，就比较 term
			localTerm, _ := r.RaftLog.Term(e.Index)
			if localTerm != e.Term {
				// 冲突！从这里截断
				firstIndex := r.RaftLog.entries[0].Index
				r.RaftLog.entries = r.RaftLog.entries[:e.Index-firstIndex]
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
			// term 一样说明已经存在，这里不是冲突位置，直接跳过
		} else {
			// 多出来的新日志，直接追加
			r.RaftLog.entries = append(r.RaftLog.entries, *e)
		}
	}

	// 3、更新 commit（Leader若commit更大，那就要更新）
	lastNewIndex := r.RaftLog.LastIndex()
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}

	// 4、回复日志成功
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
		Index:   lastNewIndex,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// 增加​tickElection()​与tickHeartbeat()​，用于tick()方法调用
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		// 把“触发”包装成消息，将具体处理逻辑丢给step处理
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			To:      r.id,
		})
	}
}
