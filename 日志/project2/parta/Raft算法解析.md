# Raft 算法解析

## Raft 解决什么问题

假设有 3 台服务器组成一个集群，客户端发来 `Put("x", 1)`、`Put("x", 2)` 两个请求。由于网络延迟不同，节点 A 先收到请求 1，节点 B 先收到请求 2。如果没有共识协议，节点 A 会认为 x=1，节点 B 认为 x=2——这就是不一致。

共识协议的目标是：**所有节点最终以相同的顺序执行相同的命令**，使得每个节点上的状态机到达相同的最终状态。

Raft 把共识分解为两个相对独立的子问题：

1. **Leader 选举**——任何时候集群中最多只有一个 Leader，只有 Leader 能决定日志顺序
2. **日志复制**——Leader 把日志条目复制到多数节点后，该条目就被"提交"，可以安全地应用到状态机

---

## 核心概念

### Term（任期）

Term 是一个单调递增的整数，是 Raft 的"逻辑时间"。每发生一次新的 Leader 选举，term 就 +1。每个节点在每个 term 最多投一票。

```text
Term 1: 节点 A 当选 Leader，正常工作
Term 2: 节点 A 宕机，节点 B 当选 Leader
Term 3: 网络分区恢复，节点 C 发起新选举
...
```

任何 Raft 消息都携带 term。节点收到一条 term 比自己高的消息时，说明集群已经进入了新时代，自己落伍了——立即退位为 Follower。

### 日志条目（Log Entry）

每个节点维护一份有序的 entry 数组。每个 entry 包含 `{Index, Term, Command}`：

```text
节点 1 (Leader):  [1: t1 Put x=1] [2: t1 Put y=2] [3: t1 Put z=3]
节点 2:           [1: t1 Put x=1] [2: t1 Put y=2]                     ← 落后 1 条
节点 3:           [1: t1 Put x=1] [2: t1 Put y=2] [3: t1 Put z=3]
```

`Index` 在日志中的位置（1, 2, 3...），`Term` 是该 entry 被创建时的 term。**如果两个节点在同一个 index 上的 entry 的 term 相同，那么该 index 之前的所有 entry 都相同**（Log Matching Property）。这个性质是日志一致性的基石。

---

## 日志复制

日志复制是 Raft 正常运行时的主要活动。Leader 通过 **AppendEntries** RPC 把日志推给 Follower。这个 RPC 身兼两职：既是心跳（告诉 Follower "我还活着"），也是日志复制命令。TinyKV 将它们拆分成了 `MsgHeartbeat` 和 `MsgAppend`，但逻辑本质相同。

### 一次 AppendEntries 的完整流程

Leader 向 Follower 2 发送 AppendEntries：

```text
{
  Term:         5,              // Leader 的当前 term
  PrevLogIndex: 2,              // "新 entry 之前那条日志的 index"
  PrevLogTerm:  5,              // "那条日志的 term，你核对一下"
  Entries:      [3: t5 Put z=3], // 要追加的新条目
  LeaderCommit: 2               // Leader 已经提交到哪个 index 了
}
```

### Follower 端：一致性检查

Follower 收到 AppendEntries 后，用自己的日志验证：

> "我 index=2 的 entry，它的 term 是 5 吗？"

- **是** → 检查通过。把 `Entries` 追加到日志末尾。如果 index=3 处已有旧条目（可能来自上一个 Leader 的未提交日志），**直接覆盖**——Leader 的日志就是真相。
- **不是** → 拒绝（`Reject: true`）。Leader 收到拒绝后把 `PrevLogIndex` 减 1，重新发送。逐步回溯，直到找到双方日志一致的位置，然后从这个位置开始覆盖。

```text
Leader 回溯:
  发 PrevLogIndex=6 → Follower: "index 6 的 term 不匹配, 拒绝"
  发 PrevLogIndex=5 → Follower: "index 5 的 term 不匹配, 拒绝"
  发 PrevLogIndex=4 → Follower: "index 4 的 term 匹配!"
  → 发送 [5: ..., 6: ...] 覆盖 Follower 的 index 5 和 6
```

这个冲突解决机制简单但有效：Leader 不需要协商，直接用自己的日志覆盖 Follower 的冲突部分。为什么不担心覆盖了已提交的数据？因为只有 Leader 的日志能被提交，而 Leader 当选时必须拥有所有已提交的 entry（选举时的日志比较规则保证这一点）。

### Leader 端：提交

Leader 不能无限等地确认。提交规则：

1. 一条 entry 被复制到**多数**节点（> N/2）后 → 可以提交
2. **但有一个关键安全约束**：只有 Leader **当前 term** 的 entry 能通过"数副本"来提交。之前 term 的 entry 只能通过提交一条当前 term 的 entry 来**间接提交**

为什么需要第 2 条？看图：

```text
Term 1: S1 是 Leader, 把 index=2 复制到 S2, 但 S1 在提交前宕机
         S1: [1:t1] [2:t1]
         S2: [1:t1] [2:t1]
         S3: [1:t1]

Term 2: S3 当选 Leader (S1 宕机, S2 投了 S3 因为日志够新)
         S3: [1:t1] [2:t2 Put y=2]  ← S3 在 index=2 写了新 entry
         
         如果此时 S3 宕机, S1 恢复, S1 当选 Term 3 的 Leader:
         S1: [1:t1] [2:t1]  ← S1 会用自己的 [2:t1] 覆盖 S3 的 [2:t2]
```

如果 Term 1 时 S1 能用"多数"来提交 [2:t1]（S1+S2 已超半数），那么 [2:t1] 就是已提交的。但 Term 2 时 S3 当选后用 [2:t2] 覆盖了 S2 上的 [2:t1]——已提交的 entry 被覆盖了！这就是 Raft 论文 Figure 8 描述的经典问题。

**解决方案**：S1 在 Term 1 时不能通过数副本提交 [2:t1]（因为它不是当前 term 的 entry）。只有当 S3 在 Term 2 提交了 [2:t2]（当前 term 的 entry）后，[2:t1] 才被间接提交。

> 在代码中这意味着：推进 commit index 之前，必须检查 `r.RaftLog.Term(newCommit) == r.Term`。这是 Part A 最容易犯的错误之一，`TestLeaderOnlyCommitsLogFromCurrentTerm2AB` 专门测试这一点。

---

## Leader 选举

### 三种角色

```text
Follower ──(超时)──→ Candidate ──(获得多数票)──→ Leader
    ↑                                                  │
    └─────────── (收到更高 term 的消息) ←───────────────┘
```

| 角色 | 行为 |
| --- | --- |
| **Follower** | 被动响应 Leader 和 Candidate 的 RPC。如果 election timeout 内没收到 Leader 的消息，发起选举 |
| **Candidate** | 正在竞选。term++，投票给自己，向所有节点发 `RequestVote`。获得多数票 → Leader；收到更高 term 的消息 → 退位 |
| **Leader** | 接收客户端请求，向 Follower 复制日志。周期性发送心跳（空 AppendEntries）防止 Follower 超时 |

### 选举流程

1. Follower 的 `electionElapsed` 达到 `electionTimeout`（在 `[ElectionTick, 2*ElectionTick)` 内随机）
2. 超时的节点变成 Candidate：
   - `term++`
   - **投票给自己**
   - 重置 election timer
   - 向所有其他节点发送 `RequestVote{term, lastLogIndex, lastLogTerm}`
3. 其他节点收到 `RequestVote` 后决定是否投票：
   - **term 检查**：如果 `msg.Term < 自己的 term` → 拒绝
   - **投票记录**：如果这个 term 已经投给其他人了 → 拒绝
   - **日志比较**：比较 candidate 的日志是否"至少和自己一样新"——先比 `lastLogTerm`，再比 `lastLogIndex`。如果 candidate 的日志更旧 → 拒绝
   - 通过所有检查 → 投票，**重置自己的 election timeout**（这很重要：防止自己在同一 term 又发起选举）
4. Candidate 获得**多数票**（> N/2）→ 变成 Leader
5. 新 Leader 立即向所有节点发心跳（空 AppendEntries），宣告"我上台了"
6. 新 Leader 追加一条 **noop entry**（空日志），用当前 term 提交。这样会间接提交之前 term 的所有未提交 entry

> 在代码中：`TestDisruptiveFollower2AA` 测试了一个场景——被隔离的 Follower 超时变 Candidate（term 更高），旧 Leader 的心跳到达时会被拒绝（因为心跳的 term 更低），旧 Leader 被迫退位。

### 为什么选举超时需要随机化

如果所有节点的 `electionTimeout` 相同，它们会同时超时、同时变成 Candidate、各自投票给自己、谁也得不到多数票（split vote）→ 所有人再次同时超时 → 无限循环。

随机超时让**有一个节点通常先超时**，先发起选举，先获得多数票。

### 为什么投票要比较日志新旧

假设节点 A 有 10 条日志、节点 B 有 5 条。如果 B 当选 Leader，它会用自己的日志覆盖 A 的，导致 A 上可能已提交的 5 条 entry 丢失。

"至少和自己一样新"的比较规则：先比最后一条 entry 的 term，term 相同再比 index。这确保当选的 Leader **拥有所有可能已提交的 entry**。

---

## 安全机制汇总

| 机制 | 保证什么 |
| --- | --- |
| 每个 term 每个节点最多投一票 | 每个 term 最多一个 Leader |
| 多数票才能当选 | 两个 candidate 不可能同时获得多数（任意两个多数必然有交集） |
| 投票时比较日志新旧 | 当选的 Leader 一定拥有所有已提交的 entry |
| 只有当前 term 的 entry 能通过计数提交 | 旧 Leader 的未提交 entry 不会被错误提交（Raft 论文 Figure 8） |
| AppendEntries 的 prevLogIndex/prevLogTerm 一致性检查 | Follower 日志不会出现"洞"，冲突条目被 Leader 日志覆盖 |
| Leader 的日志是真相 | Follower 在冲突时无条件接受 Leader 的日志，保证最终一致性 |

---

## 对应到 Part A 代码

有了以上理解，`raft.go` 里的每个函数在算法中的角色就很清晰了：

| 函数 | 在 Raft 算法中的作用 |
| --- | --- |
| `tick()` | 推进逻辑时钟。Follower/Candidate 超时 → 发起选举；Leader 超时 → 发心跳 |
| `becomeFollower()` | 退位：收到更高 term 的消息时调用 |
| `becomeCandidate()` | 发起选举：term++，投票给自己，准备发 RequestVote |
| `becomeLeader()` | 当选 Leader：重置 Progress 追踪器，追加 noop entry |
| `Step()` + `MsgHup` | 收到选举触发信号 → `becomeCandidate()` → 广播 `RequestVote` |
| `Step()` + `MsgRequestVote` | 收到投票请求 → term 检查 + 已投票检查 + 日志比较 → 决定是否投票 |
| `Step()` + `MsgRequestVoteResponse` | 收到投票结果 → 计票 → 获得多数票就 `becomeLeader()` |
| `Step()` + `MsgBeat` | Leader 定期收到心跳触发 → 广播 `MsgHeartbeat` |
| `Step()` + `MsgHeartbeat` | Follower 收到心跳 → 确认 Leader 存活，重置 election timer |
| `Step()` + `MsgPropose` | Leader 收到客户端请求 → 追加到自己的日志 → 广播 `MsgAppend` |
| `sendAppend()` | Leader 构造 AppendEntries RPC：计算 prevLogIndex、prevLogTerm，收集新 entries |
| `handleAppendEntries()` | Follower 做一致性检查 → 冲突解决（截断 + 追加） → 更新 commit index |
| `Step()` + `MsgAppendResponse` | Leader 收到 Follower 的响应 → 更新 Match/Next → 尝试推进 commit（**必须检查当前 term**） |
| `NewRawNode()` | 从持久化存储恢复 Raft 状态，创建 RawNode |
| `HasReady()` | 检查是否有待处理的变更（新 HardState、未持久化 entries、待发送消息等） |
| `Ready()` | 收集自上次 Advance 以来的所有增量变更 |
| `Advance()` | 确认 Ready 已处理完毕，更新 stabled、applied 指针 |

当你在 Part A 详解中看每个步骤的实现指导时，可以对照这张表确认自己理解了每个函数**为什么需要这样做**。