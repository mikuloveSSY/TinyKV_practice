# Project2 Part A 详解

## 概述

Part A 要求在 `raft/` 目录下实现 Raft 共识算法的核心，包括 Leader 选举、日志复制和 RawNode 接口。总共约 **15 个存根函数**，分布在 3 个文件中，分 3 个测试阶段渐进推进。

**三个文件的关系**：

```
log.go (RaftLog)    ← 最底层，管理日志条目的增删改查
    ↑
raft.go (Raft)      ← 核心层，持有 *RaftLog，实现选举和复制逻辑
    ↑
rawnode.go (RawNode) ← 封装层，包装 *Raft，提供 Ready 接口给上层
```

**核心数据结构**：

```
snapshot/first.....applied....committed....stabled.....last
--------|------------------------------------------------|
                         log entries
```

- `stabled`：已持久化的最高 index
- `committed`：已提交的最高 index（被多数节点确认）
- `applied`：已应用到状态机的最高 index
- 不变式：`applied <= committed <= stabled <= last`

---

## 阶段 1：Leader 选举（`make project2aa`）

**目标**：通过 24 个选举相关测试。

这个阶段 Raft 节点能正确地在 Follower / Candidate / Leader 三种角色之间转换，完成选举流程。不涉及日志复制。

### 1.1 先在 `log.go` 打好基础

RaftLog 是日志管理器，即使选举阶段不涉及日志复制，也需要基本的初始化和查询功能。

#### `newLog(storage Storage) *RaftLog`

从 `Storage` 恢复日志状态：

1. 调用 `storage.FirstIndex()` 获取第一个 entry 的 index
2. 调用 `storage.LastIndex()` 获取最后一个 entry 的 index
3. 从 `storage` 读出 `[firstIndex, lastIndex+1)` 范围的 entries 存入 `l.entries`（使用 `storage.Entries()`）
4. 初始化 `committed` 和 `applied` 为 `firstIndex - 1`（即 snapshot 的 index）
5. 初始化 `stabled` 为 `lastIndex`（刚从 Storage 读出的都算已持久化）

> **提示**：`MemoryStorage` 初始化时有一个 dummy entry（`Index=0, Term=0`），所以 `FirstIndex()` 返回 1，`LastIndex()` 返回 0。此时 `firstIndex > lastIndex`，entries 应为空切片。

#### `LastIndex() uint64`

返回最后一条日志条目的 index：
- 如果有 entries，返回 `entries[len(entries)-1].Index`
- 如果 entries 为空但有 `pendingSnapshot`，返回 snapshot 的 index
- 如果都没有，返回 `l.stabled`（从 storage 恢复的值）

#### `Term(i uint64) (uint64, error)`

返回指定 index 的 term：
1. 如果 index 在内存 entries 范围内，直接返回对应 entry 的 term
2. 如果 index 小于第一个 entry 的 index（已被 compact），尝试从 `storage.Term(i)` 查询
3. 如果 index 超出范围，返回错误

> **选举需要 Term()**：投票时要比较 candidate 的 last log term 是否 >= 自己的 last log term，所以即使阶段 1 也要正确实现 `Term()`。

### 1.2 在 `raft.go` 实现选举核心

#### 第一步：`newRaft(c *Config) *Raft`

初始化 Raft 节点：

1. 调用 `c.Storage.InitialState()` 获取持久化的 `HardState`（Term、Vote、Commit）
2. 创建 `RaftLog`：`newLog(c.Storage)`
3. 用 `HardState` 设置 `r.Term`、`r.Vote`
4. 设置 `r.RaftLog.committed = hardState.Commit`
5. 初始化 `Prs`：遍历 `c.peers`，为每个 peer 创建 `Progress{Match: 0, Next: 1}`
6. 将自己也加入 `Prs`（`Match: 0, Next: 1`）
7. 初始化 `votes` map
8. 初始角色为 Follower：调用 `becomeFollower(term, None)`
9. 设置 `heartbeatTimeout` 和 `electionTimeout`
10. 随机化 `electionElapsed`（见下文）

> **关键**：`Config` 中的 `peers` 包含了集群中所有节点的 ID（包括自己），需要遍历它来初始化 `Prs`。

#### 第二步：状态转换函数 `becomeXXX`

这三个函数负责在角色切换时重置内部状态。

**`becomeFollower(term uint64, lead uint64)`**：
1. 重置 `r.State = StateFollower`
2. 如果传入的 term 比当前 term 大，更新 `r.Term = term`
3. 清空 `r.Vote = None`
4. 设置 `r.Lead = lead`
5. 重置 `r.electionElapsed = 0`（收到有效消息后重置选举计时）
6. 重置 `r.heartbeatElapsed = 0`
7. 清空 `r.votes`
8. 将 `r.tick` 函数设为 `tickElection`

**`becomeCandidate()`**：
1. 重置 `r.State = StateCandidate`
2. `r.Term++`（开始新一轮选举）
3. `r.Vote = r.id`（投票给自己）
4. `r.Lead = None`
5. 重置 `r.electionElapsed = 0`
6. 重置 `r.heartbeatElapsed = 0`
7. 初始化 `r.votes = map[uint64]bool{r.id: true}`（自己先投一票）
8. 将 `r.tick` 函数设为 `tickElection`
9. 如果只剩一个节点（`len(r.Prs) == 1`），直接调用 `becomeLeader()`

**`becomeLeader()`**：
1. 重置 `r.State = StateLeader`
2. `r.Lead = r.id`
3. 重置 `r.electionElapsed = 0`
4. 重置 `r.heartbeatElapsed = 0`
5. 清空 `r.votes`
6. 将 `r.tick` 函数设为 `tickHeartbeat`
7. 重置所有 peer 的 `Progress`：`Match = 0`，`Next = r.RaftLog.LastIndex() + 1`
8. **追加 noop entry**：构造一条 `Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1}`，追加到 `r.RaftLog.entries`。更新 `Prs[r.id].Match` 和 `Prs[r.id].Next`

> **noop entry 是测试强制要求的**。`TestLeaderElection2AA` 等测试会检查新 leader 是否追加了 noop entry。这是 Raft 论文第 8 节的规则。

#### 第三步：`tick()`

推进逻辑时钟。需要根据当前角色选择不同的 tick 函数：

**策略**：将 `tick` 设为一个函数变量，在 `becomeXXX` 中切换：
```go
// 在 Raft 结构体中添加字段
type Raft struct {
    // ... 其他字段 ...
    tick func()  // 函数指针，指向 tickElection 或 tickHeartbeat
}

func (r *Raft) tick() {
    r.tick()  // 调用实际的 tick 函数
}
```

或者直接在 `tick()` 里根据 `r.State` 分发。

**`tickElection()`**（Follower / Candidate 使用）：
1. `r.electionElapsed++`
2. 如果 `r.electionElapsed >= r.electionTimeout`：
   - 重置 `r.electionElapsed = 0`
   - 给自己发 `MsgHup`：`r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})`

**`tickHeartbeat()`**（Leader 使用）：
1. `r.heartbeatElapsed++`
2. 如果 `r.heartbeatElapsed >= r.heartbeatTimeout`：
   - 重置 `r.heartbeatElapsed = 0`
   - 给自己发 `MsgBeat`：`r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})`

> **选举超时随机化**：每个节点的 `electionTimeout` 应该在 `[c.ElectionTick, 2*c.ElectionTick)` 内随机。在 `newRaft` 中设置 `electionTimeout` 时加入随机偏移。`TestFollowerElectionTimeoutRandomized2AA` 会统计 1000 轮验证这一点。

#### 第四步：`Step(m pb.Message)` —— 消息分发入口

`Step()` 是消息处理的入口，按当前角色将消息路由到不同的处理方法：

```go
func (r *Raft) Step(m pb.Message) error {
    // 如果消息的 term 比当前 term 高，先更新
    if m.Term > r.Term {
        // 除了本地消息（MsgHup/MsgBeat/MsgPropose 测试不设 term）
        // 收到更高 term 的消息时，变为 Follower
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
```

> **重要**：`MsgHup`、`MsgBeat`、`MsgPropose` 是本地消息，测试不会为它们设置 Term。在 `Step()` 中检查 `m.Term > r.Term` 时要排除这三种本地消息。

#### 第五步：处理选举相关消息

以下是在阶段 1 需要处理的消息类型：

**收到 `MsgHup`**（在任何角色下）：
- 如果在 `Prs` 中没有自己（不在集群中），忽略
- 否则调用 `r.campaign()`：先 `becomeCandidate()`，然后向所有其他 peer 发送 `MsgRequestVote`
- 如果已经是 Leader，忽略

**Follower / Candidate 收到 `MsgRequestVote`**：
- 如果 `m.Term < r.Term`：回复 `MsgRequestVoteResponse{Reject: true}`
- 如果已经投过票（`r.Vote != None && r.Vote != m.From`）：回复 `MsgRequestVoteResponse{Reject: true}`
- 比较 candidate 的日志是否至少和自己一样新：获取 candidate 的 `LastLogIndex`（从 `m.Index`、`m.LogTerm`），与自己的 `RaftLog.LastIndex()` 和 `RaftLog.Term(lastIndex)` 比较。如果 candidate 的日志不如自己新，回复 `Reject: true`
- 否则：设置 `r.Vote = m.From`，回复 `MsgRequestVoteResponse{Reject: false}`

**Candidate 收到 `MsgRequestVoteResponse`**：
- 如果 `m.Reject == false`：记录 `r.votes[m.From] = true`，检查是否获得多数票（`len(votes) > len(r.Prs)/2`）。如果是，调用 `becomeLeader()`
- 如果 `m.Reject == true`：记录 `r.votes[m.From] = false`，检查是否多数拒绝。如果是，退回到 Follower

**收到 `MsgHeartbeat`**（Follower / Candidate）：
- 如果 `m.Term >= r.Term`：调用 `becomeFollower(m.Term, m.From)`
- 回复 `MsgHeartbeatResponse`

**Follower / Candidate 收到 `MsgAppend`**（阶段 1 只需做基本的 term 检查）：
- 如果 `m.Term >= r.Term`：调用 `becomeFollower(m.Term, m.From)`

**Leader 收到 `MsgBeat`**：
- 向所有其他 peer 发送 `MsgHeartbeat`

> **本地消息处理技巧**：`MsgHup` → 触发选举；`MsgBeat` → 触发心跳广播；这些本地消息可以不经过 `Step()` 的 term 检查，直接在 `Step()` 中单独判断 `MsgType`。

### 1.3 阶段 1 测试验证

```bash
make project2aa
```

通过了说明：
- 节点能从 Follower 超时变 Candidate
- Candidate 正确请求投票和计票
- 获得多数票后成为 Leader
- Leader 正确发送心跳
- 更高 term 的消息能让节点退回到 Follower
- 选举超时已随机化

---

## 阶段 2：日志复制（`make project2ab`）

**目标**：通过 24 个日志复制相关测试。

在选举通过的基础上，实现日志的追加、复制、冲突解决和提交。

### 2.1 补全 `log.go`

#### `allEntries() []pb.Entry`

返回所有未被 compact 的 entries（不包括 dummy entry）。

如果 entries 的第一个 entry 的 Index 为 0（dummy），排除它。

#### `unstableEntries() []pb.Entry`

返回所有未持久化的 entries（index > stabled 的部分）。

如果 len > 0，返回 `entries[stabled - entries[0].Index + 1:]`。

#### `nextEnts() []pb.Entry`

返回所有已提交但未应用的 entries（applied < index <= committed）。

### 2.2 在 `raft.go` 实现日志复制

#### `sendAppend(to uint64) bool`

Leader 向指定 peer 发送 `MsgAppend`：

1. 获取该 peer 的 `Progress`，`prevLogIndex = pr.Next - 1`
2. 获取 `prevLogTerm`：使用 `r.RaftLog.Term(prevLogIndex)`
3. 收集从 `pr.Next` 开始到 `r.RaftLog.LastIndex()` 的所有 entries
4. 构造 `MsgAppend{To: to, Term: r.Term, Index: prevLogIndex, LogTerm: prevLogTerm, Entries: ..., Commit: r.RaftLog.committed}`
5. 将消息加入 `r.msgs`

> **如果 `prevLogIndex` 已被 compact**（`Term()` 返回 `ErrCompacted`）：不要发送 `MsgAppend`，改为发送 `MsgSnapshot`（阶段 2 先跳过，阶段 3/Part C 再处理）。

#### `sendHeartbeat(to uint64)`

向指定 peer 发送 `MsgHeartbeat`：
- 构造 `MsgHeartbeat{To: to, Term: r.Term, Commit: r.RaftLog.committed}`，加入 `r.msgs`

#### `handleAppendEntries(m pb.Message)`

处理收到的 `MsgAppend`（Follower / Candidate 端）：

1. **一致性检查**：用 `m.Index`（prevLogIndex）和 `m.LogTerm`（prevLogTerm）验证日志连续性
   - 如果 `m.Index > r.RaftLog.LastIndex()`：返回 `MsgAppendResponse{Reject: true, Index: r.RaftLog.LastIndex() + 1}`
   - 调用 `r.RaftLog.Term(m.Index)` 与 `m.LogTerm` 比较，如果不匹配：返回 `MsgAppendResponse{Reject: true}`
2. **冲突解决**：遍历 `m.Entries`，找到第一个与本地日志冲突的位置（index 相同但 term 不同的 entry），从该位置截断，追加新的 entries
3. **更新 commit index**：`r.RaftLog.committed = min(m.Commit, 最后一条新 entry 的 Index)`
4. 返回 `MsgAppendResponse{Reject: false, Index: 最后一条 entry 的 Index}`

> **关键**：AppendEntries 的一致性检查是"找到 prevLogIndex 处 term 匹配"的日志，而不是简单的 index 比较。如果 leader 的 `prevLogIndex` 处 term 与本地不同，说明之前有不一致的日志，必须拒绝。

#### 扩展 `Step()` 处理

**Leader 收到 `MsgPropose`**：
1. 将 `m.Entries` 中的 entry 追加到 `r.RaftLog.entries`（设置正确的 `Index` 和 `Term`）
2. 更新自己的 `Prs[r.id].Match = lastIndex`，`Prs[r.id].Next = lastIndex + 1`
3. 如果是单节点集群：直接 `r.RaftLog.committed = lastIndex`
4. 调用 `bcastAppend()`：遍历所有 peer，给每个其他 peer 调用 `sendAppend()`

**Leader 收到 `MsgAppendResponse`**：
1. 如果 `m.Reject == false`：更新该 peer 的 `Progress.Match = m.Index`，`Progress.Next = m.Index + 1`
2. 如果 `m.Reject == true`：将 `Progress.Next` 减 1，重新 `sendAppend()`（回溯找到一致点）
3. **尝试推进 commit index**：
   - 将所有 peer 的 `Match` 值排序
   - 取中位数的 `Match` 作为 `newCommit`
   - **仅当 `r.RaftLog.Term(newCommit) == r.Term` 时**才更新 `r.RaftLog.committed`（只有当前 term 的 entry 才能通过计数来提交）
   - 更新后调用 `bcastAppend()` 广播新的 commit index

> **这是最容易出错的地方**：`TestLeaderOnlyCommitsLogFromCurrentTerm2AB` 专门测试这一点。Leader 不能通过计数副本来提交之前 term 的 entry——那些只能通过提交一条当前 term 的 entry 来间接提交。

#### `Step()` 中 Leader 收到 `MsgBeat`

向所有其他 peer 调用 `sendHeartbeat()`。

### 2.3 阶段 2 测试验证

```bash
make project2ab
```

通过了说明：
- Leader 能正确追加日志
- AppendEntries 的 prevLogIndex/prevLogTerm 校验正确
- 日志冲突解决正确（Raft 论文 Figure 7 的全部 6 种情况）
- 提交规则正确（只有当前 term 的 entry 能通过计数提交）
- Leader 能正确更新 peer 的 Match/Next
- 分区场景下的日志一致性

---

## 阶段 3：RawNode 接口（`make project2ac`）

**目标**：通过 2 个 RawNode 测试。

在 Raft 核心算法跑通后，实现 `RawNode` 封装层。`RawNode` 是上层应用（Part B 的 raftstore）与 Raft 模块交互的接口。

### 3.1 `rawnode.go` 需要添加的状态

`RawNode` 需要追踪"上次 Ready 以来的变更"：

```go
type RawNode struct {
    Raft *Raft
    // 需要添加的字段：
    prevSoftSt *SoftState   // 上一次的 SoftState，用于检测变化
    prevHardSt pb.HardState // 上一次的 HardState，用于检测变化
}
```

### 3.2 实现函数

#### `NewRawNode(config *Config) (*RawNode, error)`

1. 调用 `newRaft(config)` 创建 Raft 实例
2. 从 `config.Storage.InitialState()` 获取初始 HardState 和 ConfState
3. 初始化 `prevSoftSt` 和 `prevHardSt`
4. 将 `prevHardSt` 设置为从 Storage 读取的值（这样第一次 Ready 不会重复返回已持久化的 HardState）

#### `HasReady() bool`

判断是否有待处理的 Ready：

返回 true 的条件（任意一项满足即可）：
1. **SoftState 有变化**：`r.Lead != prevSoftSt.Lead` 或 `r.State != prevSoftSt.RaftState`
2. **HardState 有变化**：`r.Term != prevHardSt.Term` 或 `r.Vote != prevHardSt.Vote` 或 `r.RaftLog.committed != prevHardSt.Commit`
3. **有未持久化的 entries**：`len(r.RaftLog.unstableEntries()) > 0`
4. **有 snapshot**：`r.RaftLog.pendingSnapshot != nil` 且 `!IsEmptySnap(pendingSnapshot)`
5. **有已提交未应用的 entries**：`len(r.RaftLog.nextEnts()) > 0`
6. **有待发送的消息**：`len(r.msgs) > 0`

#### `Ready() Ready`

收集所有待处理的变更：

1. **SoftState**：比较当前 `Lead` 和 `State` 与 `prevSoftSt`，如果有变化则包含在 Ready 中；更新 `prevSoftSt`
2. **HardState**：构造 `pb.HardState{Term: r.Term, Vote: r.Vote, Commit: r.RaftLog.committed}`，与 `prevHardSt` 比较，有变化则包含；更新 `prevHardSt`
3. **Entries**：`r.RaftLog.unstableEntries()`
4. **Snapshot**：从 `r.RaftLog.pendingSnapshot` 取，取后置为 nil
5. **CommittedEntries**：`r.RaftLog.nextEnts()`
6. **Messages**：取出 `r.msgs` 中的所有消息，然后清空 `r.msgs`

> **SoftState vs HardState**：SoftState 是易失的（不持久化），HardState 需要持久化。`Ready` 中 SoftState 为 nil 表示无变化时可以不处理，而空 HardState（Term=0, Vote=0, Commit=0）也表示无变化。

#### `Advance(rd Ready)`

上层处理完 Ready 后调用，更新 Raft 内部状态：

1. 如果有 Entries（已持久化），更新 `r.RaftLog.stabled = 最后一条 entry 的 Index`
2. 如果有 CommittedEntries（已应用），更新 `r.RaftLog.applied = 最后一条 committed entry 的 Index`
3. 从 `r.RaftLog.entries` 中删除已持久化和已应用的旧 entries（可选的内存优化）
4. 如果有 Snapshot，更新 `pendingSnapshot = nil`

> **Advance 是 Ready 的"确认"**：Ready 说"这些数据需要处理"，Advance 说"已经处理完了"。两者必须配对。如果 Ready 中有 Entries 但 Advance 没更新 stabled，下次 `HasReady()` 会误判。

### 3.3 阶段 3 测试验证

```bash
make project2ac
```

通过后运行：
```bash
make project2a   # 一次性跑全部 Part A
```

`TestRawNodeStart2AC` 测试从头创建 RawNode 的完整流程。
`TestRawNodeRestart2AC` 测试从已有持久化状态重启 RawNode——HardState 和 Entries 不重复出现在 Ready 中。

---

## 完整实现检查清单

### 阶段 1（2AA）：Leader 选举

- [ ] `log.go`：`newLog()` 从 Storage 恢复状态
- [ ] `log.go`：`LastIndex()` 返回最后 entry 的 index
- [ ] `log.go`：`Term(i)` 查指定 index 的 term
- [ ] `raft.go`：`newRaft()` 初始化所有字段，包括 peers、Prs、角色
- [ ] `raft.go`：`becomeFollower()` 重置状态
- [ ] `raft.go`：`becomeCandidate()` term++，投票给自己
- [ ] `raft.go`：`becomeLeader()` 初始化 Prs，追加 noop entry
- [ ] `raft.go`：`tick()` 驱动 `tickElection` / `tickHeartbeat`
- [ ] `raft.go`：`Step()` term 检查 + 角色路由
- [ ] `raft.go`：处理 `MsgHup` → 发起选举
- [ ] `raft.go`：处理 `MsgRequestVote` → 投票逻辑
- [ ] `raft.go`：处理 `MsgRequestVoteResponse` → 计票
- [ ] `raft.go`：处理 `MsgHeartbeat` → 更新 leader
- [ ] `raft.go`：处理 `MsgBeat` → 广播心跳

**跑通**：`make project2aa`

### 阶段 2（2AB）：日志复制

- [ ] `log.go`：`allEntries()` 返回所有未 compact 的 entries
- [ ] `log.go`：`unstableEntries()` 返回未持久化的 entries
- [ ] `log.go`：`nextEnts()` 返回已提交未应用的 entries
- [ ] `raft.go`：`sendAppend()` 构造 MsgAppend
- [ ] `raft.go`：`sendHeartbeat()` 构造 MsgHeartbeat
- [ ] `raft.go`：`handleAppendEntries()` 一致性检查 + 冲突解决 + 更新 commit
- [ ] `raft.go`：`Step()` 处理 `MsgPropose`（leader 追加日志）
- [ ] `raft.go`：`Step()` 处理 `MsgAppendResponse`（leader 更新 Progress + 推进 commit）
- [ ] `raft.go`：日志冲突时 Next 回溯

**跑通**：`make project2ab`

### 阶段 3（2AC）：RawNode 接口

- [ ] `rawnode.go`：添加 `prevSoftSt`、`prevHardSt` 字段
- [ ] `rawnode.go`：`NewRawNode()` 初始化 RawNode + 记录初始 HardState
- [ ] `rawnode.go`：`HasReady()` 判断是否有待处理变更
- [ ] `rawnode.go`：`Ready()` 收集变更并返回
- [ ] `rawnode.go`：`Advance()` 更新 stabled、applied、清理 entries

**跑通**：`make project2ac` → `make project2a`

---

## 调试技巧

1. **设置 debug 日志**：`LOG_LEVEL=debug make project2aa` 可以看到每个 tick、每条消息的内部分配
2. **从简单到复杂**：`TestSingleNodeCandidate2AA` 是最简单的选举测试（单节点），先确保它过
3. **表驱动测试的定位**：`TestLeaderElection2AA` 等包含多个子用例，看错误信息中的 `#i` 可以定位具体哪个子用例失败
4. **利用 `raft/doc.go`**：每条 `MessageType` 的处理规则都有详细描述，不确定行为时回去翻
5. **`raft_paper_test.go` 的测试更接近论文**：遇到不确定的行为，可以参考论文原文
6. **常见低级错误**：
   - entry 的 `Index` 和 `Term` 忘了正确设置
   - entries 是 `[]*pb.Entry`（指针切片），append 时要 `&pb.Entry{...}`
   - `Prs` 的 key 是 peer ID，遍历时不要漏了自己
7. **选举超时随机化**：在 `newRaft` 中 `electionTimeout = c.ElectionTick + rand.Intn(c.ElectionTick)`。`TestFollowerElectionTimeoutRandomized2AA` 会做统计检验

---

## Part A 时间预估

| 阶段 | 预计时间 | 说明 |
|---|---|---|
| 阶段 1（2AA） | 1.5-2 天 | Leader 选举，代码量最大，概念需要消化 |
| 阶段 2（2AB） | 1.5-2 天 | 日志复制，冲突解决和提交规则容易出错 |
| 阶段 3（2AC） | 0.5-1 天 | RawNode，代码量最少，依赖前两阶段正确 |
| **合计** | **3.5-5 天** | 建议每阶段都跑通后再进入下一阶段 |
