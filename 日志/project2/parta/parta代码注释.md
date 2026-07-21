# Part A 代码注释

## log.go

### （1）aa：Raftlog结构 — `stabled`、`committed`、`applied`

```
snapshot/first.....applied....committed....stabled.....last
--------|------------------------------------------------|
                         log entries
```

| 指针          | 含义                                                       |
| ------------- | ---------------------------------------------------------- |
| `stabled`   | 已写硬盘的最后一条（自己说了算）                           |
| `committed` | 多数节点确认的最后一条（**共识过的，不会再被覆盖**） |
| `applied`   | 已执行到状态机的最后一条                                   |

不变式：`applied <= committed <= stabled`

**与传统单机数据库的区别**：单机的 committed = 写盘成功 ≈ Raft 的 `stabled`。Raft 的 `committed` 多一个条件——必须多数节点写盘，单自己写盘不够，新 Leader 可能覆盖。

**为什么 committed 之后还要 applied？** Raft 是异步的——committed 只是"可以执行"，不是"已执行"。上层通过 Ready 拿 committed entries，执行完调 Advance()，applied 才追上 committed。

**`newLog()` 里 committed/applied 为什么填 `firstIndex - 1`？** snapshot 覆盖的 `[0, firstIndex)` 已经是共识+执行完的，所以初始化时这么填是符合逻辑的。填别的不行：填 0 则节点以为没提交，填大了则逻辑错乱。后续 newRaft() 用 HardState.Commit 覆盖它。

### （2）aa：log.go — `LastIndex()` 的三种场景

`LastIndex()` 返回最后一条日志的 index，但"最后一条"可能来自三个不同的地方：

| 优先级 | 来源                               | 场景                                      |
| ------ | ---------------------------------- | ----------------------------------------- |
| 1      | `entries` 最后一条的 Index       | 正常运行，内存里有日志                    |
| 2      | `pendingSnapshot.Metadata.Index` | 刚收到 Leader 发来的 snapshot，还没处理   |
| 3      | `stabled`                        | 兜底：全新启动（entries 空，无 snapshot） |

**entries 为什么会为空？** 两种原因：(1) 全新启动，硬盘里没日志；(2) Part C 做了 compaction，日志被压缩成 snapshot 清掉了。数据还在，只是从"逐条日志"变成了"一个 snapshot 文件"。

**pendingSnapshot 是什么？** Follower 落后太多，Leader 已经把旧日志 compaction 删掉了，没法逐条补发，于是直接发一个 snapshot（当前状态机的完整快照）。这个 snapshot 到达后先存在 `pendingSnapshot`，等 Ready 时交给上层应用。在应用之前 entries 为空，但 `pendingSnapshot.Index` 告诉节点"日志到哪了"。

**entries 空、pendingSnapshot nil、stabled 不为 0 可能吗？** 可能。本地 compaction 之后，entries 被清空但没有收到外来 snapshot，stabled 保留了 compaction 前最后一条的 index。返回 stabled 兜底刚好正确。

## Raft.go

### （1）aa：`Raft` 结构体字段 & `newRaft()` 初始化

Raft 结构体 13 个字段按初始化方式分成两拨：

**`newRaft()` 直接设的**（有明确来源——Config 或硬盘）：

| 字段                    | 含义                                                                                                                 | 来源                               |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| `id`                  | 节点编号                                                                                                             | `Config.ID`                      |
| `Term`                | 当前任期，区分新旧 Leader                                                                                            | `HardState` 从硬盘恢复           |
| `RaftLog`             | entries + 三个指针                                                                                                   | `newLog(storage)` 从硬盘恢复日志 |
| `Prs`                 | **作为Leader时**，记录每一个结点：已经收到的最新日志的编号和下一个需要接收的日志编号                           | `Config.peers` 遍历              |
| `electionTimeoutBase` | 用于超时时限随机化的**基准值**                                                                                 | `Config.ElectionTick`            |
| `electionTimeout`     | **超时时限**。距离上次接收到心跳的时间超过了它就发起选举（*随机是为了防止所有节点同时超时从而整个Raft卡死*） | `Config.ElectionTick + 随机`     |
| `heartbeatTimeout`    | **心跳时限**。作为Leader时隔多久发一次心跳                                                                     | `Config.HeartbeatTick`           |

**注意**：`electionTimeout`需要在每次becomeXXX的时候都随机化

**`becomeFollower()` 在 `newRaft()` 末尾统一调用来设置字段**（附加好处：后续角色切换时复用同一套逻辑）：

| 字段                 | 含义                                                                                                                               | becomeFollower的设置                             |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `State`            | 角色：Follower / Candidate / Leader                                                                                                | Follower                                         |
| `Lead`             | **作为Follower时**，追随的领导者是谁                                                                                         | 传入参数lead                                     |
| `electionElapsed`  | **作为Follower时**，距上次收到 Leader 消息过了几个 tick，超时就选举                                                          | 0                                                |
| `heartbeatElapsed` | **作为Leader时启用**，距上次发心跳过了几个 tick                                                                              | 0                                                |
| `tickFn`           | 函数指针：**作为follower或者candidate的计时**，指向`tickElection`；<br />**作为leader的计时**，指向`tickHeartbeat` | `tickElection`                                 |
| `votes`            | **仅在投票阶段启用**，map形式，记录了其他结点的对自己的投票结果（True or False）                                             | 空map（不设置成nil是为了方便后面的函数统一调用） |
| `vote`             | **仅在投票阶段启用**，记录投票给了谁                                                                                         | None                                             |

`msgs` 两边都不管——nil 切片可直接 `append`。

**HardState** 是Raft论文里指出的 Raft结点 必须持久化的三条信息，主要用于中途宕机恢复和关闭后的重启。`newRaft()` 首先调 `c.Storage.InitialState()` 从硬盘读出 `{Term, Vote, Commit}`：

- 丢了 Term → 用旧 term 发消息，被其他节点无视
- 丢了 Vote（主要与宕机恢复有关） → 同一 term 投两次票，可能选出两个 Leader
- 丢了 Commit → 拒绝执行已共识的日志，节点卡住，导致请求超时

`newLog()`初始化时把 committed 填了保守值 `firstIndex-1`（snapshot 兜底）。但在中途启动时，实际的共识可能已经远超 snapshot 覆盖的范围了，所以`newRaft()` 必须立刻用 `HardState.Commit` 覆盖为真正的共识位置。全新启动时三项都是 0，代码不需要区分。

### （2）ab：`tick()` 为什么拆成 `tickElection()` 和 `tickHeartbeat()`

`tick()` 每次被上层调用时，需要根据当前角色执行不同逻辑：Follower/Candidate 超时发起选举，Leader 超时发送心跳。

两种实现可选：

| 方式        | 做法                                                                       | 每次 tick 开销 |
| ----------- | -------------------------------------------------------------------------- | -------------- |
| switch 分支 | `tick()` 里 `switch r.State`                                           | 一次分支判断   |
| 函数指针    | 加`tickFn func()` 字段，角色切换时换指针，`tick()` 只调 `r.tickFn()` | 一次函数调用   |

效果一样，但 etcd/TinyKV 选择了函数指针，每次角色设置时会设置tick指向的函数——避免了每次 tick 都走 switch的消耗。两个方法绑定在 `*Raft` 上（`func (r *Raft) tickElection()` / `func (r *Raft) tickHeartbeat()`），这样不用每次传参数了。

### （3）"只有当前 term 的 entry 能通过数副本提交"——为什么需要这条核心规则

Raft 论文第 5.4.2 节的核心规则：**Leader 不能通过数副本的方式提交之前 term 的 entry，只有当前 term 的 entry 可以。**

> 新 Leader 当选时不知道之前 term 的哪些日志已经提交了。只有**当前 term**的 entry 可以通过数副本方式提交——旧 term 的 entry 即使被多数节点持有，也不能直接提交。因此 Leader 必须在任期开始时提交一条当前 term 的 **noop entry** （空日志），通过它间接提交之前 term 的积压日志。

**为什么需要这条规则**：没有它，已提交的数据可能被覆盖。

场景推演（5 节点 S1~S5，多数=3）：

1. Term 1：S1 是 Leader，写 `x=1` 到 index=2（term=1），只发给了 S3。2 份不到多数，没提交。
2. S1 宕机。
3. Term 2：S3 当选 Leader（因为获得了S3、4、5的票），写 `y=2` 到自己的 index=2（term=2），只发给了 S4。
4. S2 也宕机。
5. Term 3：S1 恢复，当选 Leader。把 `t1，x=1` 推给 S5，现在 S1+S2+S5 有 `x=1`——3 份过半数。**但 index=2 的 term=1，不是当前 term=3，不能提交。**
6. 如果没有这条规则，S1 直接提交 `x=1` 然后宕机。到了 Term 4，S2 恢复——它的最后一条是 index=2 **term=2**。多数节点的最后 term 只有 1（因为 S1 在 term=3 没产出新日志，所以此时S2的term=1，S3、4的term=2），term=2 的 S3 可能当选，用自己的 `y=2` 覆盖所有人的 index=2。**已提交的 `x=1` 永久丢失。**

但是遵守这条规则会导致一个问题：这些已经被大多数结点认可的日志积压在了一起，始终无法被提交，进而导致后面的也无法进行下去。

**那noop是怎么解决这个问题的：** 有了noop后，就可以保证如果在最新的term里到达了提交条件，那么说明当前这个日志已经超过半数都有了，那么如果还带有此前积压着的旧日志，此时这些旧日志已经不可能会产生被覆盖的问题了（*因为noop的标签是最新的term，所以不存在其它的旧结点成为新Leader覆盖数据*），就也可以顺带着一起提交了。

### （4）aa：为什么把触发事件也包装成消息（`MsgHup`/`MsgBeat`/`MsgPropose`）

`tickElection` 超时后不直接调 `becomeCandidate()`，而是发一条 `MsgHup` 给 `Step()`。好处是**解耦**——`Step()` 是所有消息的唯一入口，里面统一做了 term 检查、角色路由。`tickElection`（超时触发）和 `RawNode.Campaign()`（上层手动触发）都发同一条 `MsgHup`，多个触发源共用一个处理逻辑，不用把"发起选举"的代码复制粘贴。

### （5）aa：`Step()` 里的 term 检查

* **收到更高 term 的消息时，立即退位重置状态成为 Follower。** term 比我高 → 说明已经进入了新任期，消息来自于新任期的结点，我落伍了，退位追随。
* **收到更低 term 的消息时则跳过不处理。** term 比我低 → 说明对方是旧任期的残留结点，消息过时了。

整个过程通过消息里自带的 term 值自然收敛——谁 term 高谁的"真相"更新，没有"Leader 变更事件"、没有"请切换到新 Leader"的专门消息，不需要额外通知机制。

### （6）aa：`pb.Message` 字段解析

```go
pb.Message {
    MsgType  // 消息类型（MsgHup / MsgRequestVote / MsgAppend 等）
    To       // 发给谁（节点 ID）
    From     // 谁发的（节点 ID）
    Term     // 发送者的当前任期

    // MsgAppend 时=prevLogTerm；即Follower已经确认收到的最后一条index，用于接收时确认一致；
	// MsgRequestVote 时=candidate 最后一条日志的 term
	LogTerm  

    // MsgAppend 时=prevLogIndex；
    // MsgRequestVote 时=candidate最后一条日志的index
    Index   

    Entries  // 携带的日志条目（MsgAppend 用）
    Commit   // 发送者的 committed index
    Snapshot // 快照（MsgSnapshot 用）
    Reject   // 拒绝标志（响应消息用：true=拒绝，false=同意）
}
```

同一个结构体，不同 `MsgType` 下字段含义不同——发 `MsgRequestVote` 时 `Index`/`LogTerm` 是候选者的最后一条日志信息，接收方用它们比较"谁的日志更新"。

**各消息类型含义：**

| 发送方    | MsgType                    | 用途                                   |
| --------- | -------------------------- | -------------------------------------- |
| 本地      | `MsgHup`                 | 触发选举（超时或手动调用 Campaign）    |
| 本地      | `MsgBeat`                | 触发 Leader 广播心跳                   |
| 本地      | `MsgPropose`             | 提议新日志（上层调用 RawNode.Propose） |
| Follower  | `MsgRequestVoteResponse` | 回复投票结果（同意/拒绝）              |
| Follower  | `MsgHeartbeatResponse`   | 回复心跳                               |
| Follower  | `MsgAppendResponse`      | 回复日志复制结果                       |
| Candidate | `MsgRequestVote`         | 请求其他节点投票                       |
| Leader    | `MsgHeartbeat`           | 心跳，维持 Leader 地位                 |
| Leader    | `MsgAppend`              | 日志复制                               |
| Leader    | `MsgSnapshot`            | 安装快照（Part C）                     |

### （7）aa：Candidate的`MsgRequestVoteResponse` 计票为什么 `granted` 和 `rejected` 分别判断

投票结果不是同时到达的——每个节点的回复有先有后。每到一个回复就要重新算一次：同意票过半 → 立刻当选 Leader，拒绝票过半 → 没希望了退回 Follower，两边的票都还不够多数 → 继续等下一票。不能等所有人投完再判断，因为网络和 tick 控制下，投票是有先后顺序的。

### （8）aa：当接收到Term相同的消息时的响应

当**接收到`Term`更大**的消息时Candidate/Leader都选择退位，因为此时自己太落后了。但：

* Candidate 收到`相同term`的`心跳、MsgAppend` → 说明Leader 已存在，退位成为 Follower 再处理；若收到的是`相同term`的`MsgRequestVote`，则根据日志比较新旧，再决定是否退位（**注意：**若日志一样新，则继续竞争，防止一直退位导致无Leader）。
* Leader 收到`相同term`的`心跳、MsgAppend` → 另一个 Leader 在发日志和心跳，直接退位成为Follower再处理。

### （9）ab：`handleAppendEntries` —— Follower 端日志复制处理

`handleAppendEntries` 是 Follower 收到 Leader 的 `MsgAppend` 后的核心处理函数，主要三部分：

#### 一致性检查（prevLogIndex 处 term 是否匹配）

Leader 在消息中带了 `Index`（prevLogIndex）和 `LogTerm`（prevLogTerm），表示"我日志里紧挨着要发的这一批日志之前的那一条在哪个位置、term 是什么"。Follower 检查自己这个位置的 term 是否一致：

- **Index 超出 Follower 日志末尾**：Follower 日志比 Leader 短，无法做一致性检查。拒绝，回复 `Reject: true, Index: LastIndex()+1`，告诉 Leader"从这里发"。
- **Index 处 term 不匹配**：同一个 index 但 term 不同，说明此前某处已经分叉。拒绝，Leader 会把 `Next` 减 1 往前回溯。

两种情况本质上都是告诉 Leader"你说的位置对不上，往前找"。

#### 冲突解决 & 追加

一致性检查通过后，逐条扫描 Leader 发来的 entries：

- **index 已存在 + term 相同** → 跳过（已存在，不需要重复）
- **index 已存在 + term 不同** → 冲突。从这里一刀切掉本地该 index 及之后所有日志，用 Leader 的版本覆盖。截断时用 `entries[0].Index` 作为基准换算切片下标（因为 entries 开头可能已被 compact，不能直接用 `e.Index - 1`）
- **index 不存在** → 新日志，直接追加

#### 更新 commit

Leader 在 `m.Commit` 里告诉 Follower"集群已共识到 index=X"。Follower 不能直接用这个值，因为 Leader 的 commit 可能超过 Follower 自己日志的末尾：

```go
r.RaftLog.committed = min(m.Commit, lastNewIndex)
```

- `m.Commit > lastNewIndex` → 取 `lastNewIndex`，不能提交还不存在的日志
- `m.Commit <= r.RaftLog.committed` → 不动，commit 只能前进不能后退。这种情况只可能发生在旧消息乱序到达时，直接忽略即可
