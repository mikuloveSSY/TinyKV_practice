# Project 2 PartA 测试 Bug 总结

## Project 2AA

`make project2aa` 共 24 个测试，初次运行 5 个失败，根因分为两类。

### 一、选举超时随机化

**失败测试**：`TestCandidateStartNewElection2AA`、`TestFollowerElectionTimeoutRandomized2AA`、`TestCandidateElectionTimeoutRandomized2AA`、`TestFollowersElectionTimeoutNonconflict2AA`

问题在于一开始只在newRaft函数创建新结点时进行随机化`electionTimeout`，但在后续结点反复运行的过程中，依然可能存在同时超时的问题。因此为了保证结点的超时时间错开，在becomeFollowe与becomeCandidate的最后，都将`electionTimeout`随机化处理。

**修复**：在`Raft`结合体里加上`electionTimeoutBase`记录config给出的初始timeout，然后在两处函数末尾统一增加基于  `electionTimeoutBase` 的稳定公式：

```go
r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase)
```

### 二、noop 日志破坏 Leader 轮换

**失败测试**：`TestLeaderCycle2AA`

`becomeLeader()` 追加 noop 日志，但发送日志相关的代码还未实现，该日志并未发送出去，且 2AA 阶段日志处理代码也还未完成， `stepFollower` 收到 `MsgAppend` 没有调用 `handleAppendEntries`，日志未落盘。结果当过 Leader 的节点日志"更新"，拒绝未当过 Leader 的节点的投票。3 节点依次竞选到 node 3 时被两边拒绝，退回 Follower。

**修复**：注释掉 noop 追加，等 2AB `handleAppendEntries` 实现后、链路完整时再加回。

修复后 24 个测试全部通过。

## Project 2AB

### 一、单节点集群 commit 不推进

**失败测试**：`TestLeaderAcknowledgeCommit2AB`（#0）、`TestSingleNodeCommit2AB`

**问题**：单节点集群没有 Follower，不会收到 `MsgAppendResponse`，因此 `MsgAppendResponse` 里的 commit 推进逻辑永远不会触发。Leader 的 committed 一直停留在旧值，无法前进。

**修复**：在两处新增 commit 显式推进——`becomeLeader` 的 noop 追加后、`stepLeader` 的 `MsgPropose` 追加后，判断单节点则直接提交：

```go
if len(r.Prs) == 1 {
    r.RaftLog.committed = r.RaftLog.LastIndex()
}
```

### 二、handleAppendEntries 截断日志时 stabled 未同步更新

**失败测试**：`TestFollowerAppendEntries2AB`（#1、#3）

**问题**：Follower 收到冲突的 AppendEntries 时，会截断本地日志并覆盖。但被覆盖的 entry 在截断前可能已标记为 `stabled`（已持久化），覆盖后没有把 `stabled` 回退。导致 `unstableEntries()` 漏掉了这些实际已变更的 entry。

```
截断前: [idx1] [idx2] [idx3], stabled=3
截断覆盖 idx2、idx3 → [idx1] [idx2新] [idx3新]
但 stabled 还是 3 → unstableEntries() 认为 idx2、idx3 仍是旧的已持久化数据
```

**修复**：在 `handleAppendEntries` 的冲突截断分支中，若截断位置 `<= stabled`，将 `stabled` 回退到截断点之前：

```go
if e.Index <= r.RaftLog.stabled {
    r.RaftLog.stabled = e.Index - 1
}
```

### 三、节点重启恢复时丢失 Vote 记录

**失败测试**：`TestLeaderElectionOverwriteNewerLogs2AB`

**问题**：节点从 `HardState` 恢复时读取了 `Vote`，但 `newRaft` 末尾调用 `becomeFollower` 时**无条件**执行 `r.Vote = None`，导致刚恢复的投票记录被清空。重启后的节点忘了自己在上一个任期投过票，不该投票时却投了，导致日志过时的候选人不该当选却当选。

**修复**：将 `r.Vote = None` 移入 `if r.Term < term` 分支内，只在 term 升级时才清空投票记录——任期没变就不该丢。同时补充 `newRaft` 初始化时从 `HardState` 读取 `Vote`：

```go
// newRaft 初始化
r := &Raft{
    ...
    Vote: hardstate.Vote,
}

// becomeFollower
if r.Term < term {
    r.Term = term
    r.Vote = None   // 只在换任时才清
}
```

### 四、Candidate 收到投票请求时忽略了自己的 Vote

**失败测试**：`TestRecvMessageType_MsgRequestVote2AB`（#19）

**问题**：`stepCandidate` 处理 `MsgRequestVote` 时没有检查 `r.Vote`。Candidate 在 `becomeCandidate` 时已经投了自己（`r.Vote = r.id`），但收到另一个 Candidate 的投票请求时，看到对方日志更优就退位改投，忘了自己已经投过了。Raft 规定同一任期只能投一次票（first-come-first-served）。

**修复**：在 `stepCandidate` 的 `MsgRequestVote` 分支，如果请求者不是自己，直接返回拒绝——Candidate 只投自己。

```go
case pb.MessageType_MsgRequestVote:
    if m.From != r.id {
        r.msgs = append(r.msgs, pb.Message{
            MsgType: pb.MessageType_MsgRequestVoteResponse,
            To:      m.From, From: r.id,
            Term: r.Term, Reject: true,
        })
        return nil
    }
```

### 五、`handleAppendEntries` 的 commit 更新用错范围

**失败测试**：`TestHandleMessageType_MsgAppend2AB`（#7）

**问题**：更新 commit 时用了 `r.RaftLog.LastIndex()` 作为上限。但**当 `m.Entries` 为空时**，由于不经过for语句循环的覆盖，后面还有旧日志的残留，Follower 的 `LastIndex()` 可能大于 Leader 这条 RPC 实际覆盖的 index——Follower 多出来的日志 Leader 根本没有验证过，不能因为 Leader 说 commit=3 就把 Follower 自己未验证的本地日志一起提交了。

```
Follower: [idx1,T=1] [idx2,T=2]     ← idx2 是本地存货，Leader 没验证
Leader 发: prevLogIndex=1, Entries=[], Commit=3
           "确认 idx1 是对的，commit=3"

LastIndex() = 2  → commit = min(3,2) = 2  ← 错，把未验证的 idx2 提交了
m.Index       = 1  → commit = min(3,1) = 1  ← 对，只提交 RPC 实际覆盖的
```

**修复**：将 `lastNewIndex` 从 `r.RaftLog.LastIndex()` 改为 `m.Index + uint64(len(m.Entries))`——只提交 Leader 这条 RPC 真正涉及到的 index 范围。
