# Project2 详细说明

## 概述

Project2 要求基于 Raft 共识算法实现一个高可用的 KV 服务器。与 Project1 的独立单机 KV 不同，Project2 是一个分布式系统——多个节点通过 Raft 协议保持数据一致性，即使部分节点故障或网络分区，只要多数节点存活，服务就能继续处理请求。

### 整体架构

Project2 的代码分为两层：

**下层：Raft 算法层（`raft/`）**——对应 Part A。这是 Raft 共识协议的纯算法实现，不涉及网络传输和磁盘 I/O。核心是 `Raft` 结构体（状态机），通过 `Step()` 接收消息、通过 `tick()` 推进逻辑时钟、通过 `msgs` 产出待发送消息。`RawNode` 将其封装为 `Ready` 接口供上层消费。

**上层：raftstore 服务层（`kv/raftstore/`）**——对应 Part B / Part C。这是真正让 Raft"跑起来"的框架层，负责网络消息收发、日志持久化到 badger、将已提交的日志应用到状态机（kvdb）。核心是 `raftWorker` 事件循环：

```text
raftCh (channel)
  │
  ├── MsgTypeTick      → RawNode.Tick()         // 推进 Raft 逻辑时钟
  ├── MsgTypeRaftCmd   → proposeRaftCommand()   // 客户端请求 → 序列化为 Raft 日志
  └── MsgTypeRaftMessage → RawNode.Step()       // 来自其他 peer 的 Raft 消息
  │
  ▼
HandleRaftReady()
  ├── SaveReadyState()   → badger (raftdb)      // 持久化 HardState + 日志条目
  ├── Transport.Send()   → 网络                  // 发送 Raft 消息给其他 peer
  ├── Apply committed entries → badger (kvdb)   // 应用到状态机
  └── Advance()          → Raft                 // 确认处理完成
```

两个 badger 实例的分工：**raftdb** 存 Raft 日志和 `RaftLocalState`；**kvdb** 存真实 KV 数据、`RaftApplyState` 和 `RegionLocalState`。

客户端请求的完整链路：`RPC → RaftStorage → raftCh → raftWorker → proposeRaftCommand → RawNode.Propose → Raft 复制/提交 → HandleRaftReady → 应用到 kvdb → 回调返回响应`。

该项目分为三个部分：

- **Part A**：实现基本的 Raft 算法（Leader 选举、日志复制、RawNode 接口）
- **Part B**：在 Raft 之上构建容错的 KV 服务器
- **Part C**：添加 Raft 日志 GC 和快照支持

**总体难度排序：Part B >> Part A > Part C**

---

## Part A —— Raft 算法核心

### 你需要实现什么

Part A 涉及 `raft/` 目录下的三个文件，约 **15 个存根函数**：

| 文件 | 需实现的函数 |
| --- | --- |
| `raft/raft.go` | `newRaft()`, `tick()`, `Step()`, `becomeFollower()`, `becomeCandidate()`, `becomeLeader()`, `sendAppend()`, `sendHeartbeat()`, `handleAppendEntries()`, `handleHeartbeat()` |
| `raft/log.go` | `newLog()`, `allEntries()`, `unstableEntries()`, `nextEnts()`, `LastIndex()`, `Term()` |
| `raft/rawnode.go` | `NewRawNode()`, `Ready()`, `HasReady()`, `Advance()` |

### 难点详解

#### 1. 逻辑时钟而非物理时钟

TinyKV 不使用物理定时器，而是通过 tick 计数来驱动超时：

- 上层应用调用 `RawNode.Tick()` 推进一个 tick
- `Raft` 内部维护 `electionElapsed` 和 `heartbeatElapsed` 两个计数器
- 当 `electionElapsed` 达到 `electionTimeout` 时触发选举
- 当 `heartbeatElapsed` 达到 `heartbeatTimeout` 时发送心跳

你需要在 `tick()` 中根据当前角色（Follower/Candidate/Leader）选择不同的 tick 逻辑（`tickElection` 或 `tickHeartbeat`），并在角色切换时正确切换。

#### 2. 10 种消息类型

`eraftpb.proto` 定义了 10 种 `MessageType`，比 Raft 论文更细粒度（Heartbeat 和 AppendEntries 被拆分）：

| 消息类型 | 用途 |
| --- | --- |
| `MsgHup` | 本地消息，触发选举 |
| `MsgBeat` | 本地消息，触发心跳广播 |
| `MsgPropose` | 本地消息，提议新日志 |
| `MsgAppend` | 日志复制 RPC |
| `MsgAppendResponse` | 日志复制响应 |
| `MsgRequestVote` | 请求投票 RPC |
| `MsgRequestVoteResponse` | 投票响应 |
| `MsgHeartbeat` | 心跳 RPC |
| `MsgHeartbeatResponse` | 心跳响应 |
| `MsgSnapshot` | 快照安装（Part C） |

`Step()` 是消息分发的入口，按当前角色（Follower/Candidate/Leader）分别处理每种消息，形成 3×N 的分支逻辑。

#### 3. 需要自己添加状态字段

`Raft` 结构体和 `RaftLog` 结构体都有 `// Your Data Here (2A)` 的占位标记。框架不会告诉你要加什么字段，你需要自行判断：

- 哪些状态需要持久化（term、vote、log）
- 哪些是易失的（`electionElapsed`、`votes` 票数、`Progress` 等）
- 在 `becomeXXX` 状态转换时各重置哪些字段

#### 4. RaftLog 的区间管理

`RaftLog` 维护了一段区间，你需要正确区分：

```text
snapshot/first.....applied....committed....stabled.....last
--------|------------------------------------------------|
                         log entries
```

- `unstableEntries()`：已追加但未持久化的 entries（index > stabled）
- `nextEnts()`：已提交但未应用（applied < index <= committed）
- `allEntries()`：所有未被 compact 的 entries
- `Term(i)`：需要先从内存 entries 查，再从 Storage 查
- 首次启动时从 `Storage` 恢复状态（`stabled`、`applied`、`committed` 的初始值要正确读出）

#### 5. Leader 选举的关键规则

- 新当选的 leader 必须立即追加一条 **noop entry**（测试强制要求）
- 选举超时必须在 [et, 2et) 范围内**随机化**，否则多个节点同时超时会导致反复 split vote
- `MsgHup`、`MsgBeat`、`MsgPropose` 是本地消息，测试不会为其设置 term
- 投票时需比较 candidate 的 last log term/index 是否"至少和自己一样新"

#### 6. 日志复制的关键规则

- 只有 leader 当前 term 的 entry 可以通过计数副本数来提交（Raft 论文 5.4.2 节）
- 之前 term 的 entry 只能通过提交一条当前 term 的 entry 来间接提交
- leader 推进 commit index 后，通过 `MsgAppend` 广播 commit index
- leader 和 follower 的 AppendEntries 处理逻辑差异很大——来源不同、检查不同、处理方式不同

#### 7. Ready 机制（RawNode 接口）

`Ready` 不是"当前状态"，而是**从上次 `Advance()` 以来累积的变更**：

- `HasReady()` 需要检查：是否有新的 HardState、未持久化 entries、待发送消息、待应用 committed entries、snapshot
- `Ready()` 收集这些变更并封装返回
- `Advance()` 在上层处理完后更新内部指针（移动 `stabled`、`applied`）
- 处理顺序有严格要求：先持久化 → 再发消息 → 再应用到状态机 → 最后 Advance

#### 8. 需要留意的测试场景

- **`TestDisruptiveFollower2AA`**：被隔离的 follower 超时变 candidate（term 更高），旧 leader 的心跳到达时会因 term 更低而被拒绝，被迫下台
- **`TestLeaderSyncFollowerLog2AB`**：复现 Raft 论文 Figure 7 的全部 6 种 follower 日志不一致情况（缺失、多余、冲突），leader 必须通过回溯 `Next` 找到一致点
- **`TestCommitWithoutNewTermEntry2AB`**：被分区的 leader 的 proposal 无法提交，新 leader 当选后通过 noop entry 间接提交了旧 leader 的日志
- **`TestDuelingCandidates2AB`**：两个节点同时竞选，败者在分区恢复后再次尝试但被拒绝（日志不够新）

### Part A 测试

| 命令 | 内容 | 测试数量 |
| --- | --- | --- |
| `make project2aa` | Leader 选举 | 24 个测试函数（含大量表驱动子用例） |
| `make project2ab` | 日志复制 | 24 个测试函数（含大量表驱动子用例） |
| `make project2ac` | RawNode 接口 | 2 个测试函数 |
| `make project2a` | 全部 Part A | 上述全部 |

### Part A 准备工作（动手前必做）

1. **精读 Raft 论文第 5、6 节**。第 5 节覆盖 Leader Election，第 6 节覆盖 Log Replication。TinyKV 将 Heartbeat 和 AppendEntries 拆成了独立消息，需要理解它们各自的职责。`raft/doc.go` 对每种 `MessageType` 的语义和处理规则有逐条说明——务必精读完毕再写代码。

2. **在纸上画出 Raft 内部状态图**。把 `Raft` 结构体的每个字段在三种角色（Follower/Candidate/Leader）下如何变化推导清楚：
   - `becomeFollower` / `becomeCandidate` / `becomeLeader` 各重置哪些字段？
   - `tick()` 如何根据角色切换 `tickElection` / `tickHeartbeat`？
   - 选举超时为什么需要随机化？在哪里加随机？
   - `Prs`（Progress）中 `Match` 和 `Next` 的含义，在发送 append 时如何更新？

3. **理解 Ready 的语义**。`Ready` 不是"当前状态"，而是从上次 `Advance()` 以来的累积变更：
   - `HasReady()` 返回 true 的条件：有新的 HardState、或有未持久化 entries、或有待发送 messages、或有待应用 committed entries、或有 snapshot
   - `Advance()` 要正确移动 `stabled`、`applied` 指针
   - 处理顺序：先持久化 → 再发消息 → 再应用到状态机 → 最后 Advance

4. **阅读参考资源**：[Raft 官网](https://raft.github.io/) 的交互式可视化可以直观理解 Raft 运行过程；[扩展 Raft 论文](https://raft.github.io/raft.pdf) 作为权威参考随时查阅。

5. **善用 debug 日志**。设置 `LOG_LEVEL=debug` 运行测试。Part A 是纯单元测试，跑一次几秒钟，调试效率高。务必让 Part A 全部通过再进入 Part B——如果 Raft 层有 bug，在 Part B 的分布式测试中排查将是灾难级的。

### Part A 常见踩坑点

1. **选举超时忘记随机化** → `TestFollowerElectionTimeoutRandomized2AA` 直接挂
2. **Leader 当选后忘记追加 noop entry** → `TestLeaderElection2AA` 挂
3. **`becomeXXX` 状态转换不完整** → 漏重置 `electionElapsed`、`votes`、`Prs` 等会导致后续逻辑错乱
4. **`RaftLog.Term()` 未正确处理 compacted 区间** → 返回错误而非 `ErrCompacted`
5. **`Ready()` 中 SoftState 和 HardState 判断不准确** → 不必要的持久化或遗漏持久化
6. **提交规则：用之前 term 的 entry 来计数提交** → `TestLeaderOnlyCommitsLogFromCurrentTerm2AB` 挂。只有 leader 当前 term 的 entry 才能通过计数副本数来提交
7. **AppendEntries 的 prevLogIndex/prevLogTerm 校验不正确** → 日志冲突解决失败，`TestFollowerAppendEntries2AB` 挂
8. **`Advance()` 指针更新与 `Ready()` 不对应** → 下次 `HasReady()` 返回错误结果

### Part A 时间预估

**3-5 天**。算法正确性为主，单元测试跑得快，调试反馈快。

---

## Part B —— 容错 KV 服务器

Part B 是整个 Project2 **最难的部分**。原因不在于算法复杂度，而在于**工程复杂度**——你需要理解多层架构、消息流转、异步回调、错误传播，以及两种持久化存储的协调。

### 你需要理解的概念

#### Store、Peer、Region

| 术语 | 含义 |
| --- | --- |
| Store | tinykv-server 的一个实例（一个进程） |
| Peer | 运行在某个 Store 上的 Raft 节点 |
| Region | 一组 Peer 的集合，即 Raft group |

在 Project2 中，一个 Store 只有一个 Peer，一个集群只有一个 Region。

#### 请求处理全链路

```text
客户端 RPC 调用
  → RaftStorage.RawGet/RawPut/RawDelete/RawScan
    → 构造 RaftCmdRequest，通过 raftCh channel 发送给 raftstore
      → raftWorker 轮询 raftCh，调用 HandleMsg()
        → proposeRaftCommand() 将命令序列化为 Raft 日志条目，RawNode.Propose()
          → Raft 模块复制日志，提交
            → HandleRaftReady() 获取 Ready
              → SaveReadyState() 持久化 HardState + Entries
              → Transport 发送 Raft 消息
              → 应用 CommittedEntries 到状态机（kvdb）
              → 调用回调返回响应给客户端
```

### 你需要实现的 4 个关键函数

#### 1. `PeerStorage.SaveReadyState()`（核心函数）

将 `raft.Ready` 中的数据持久化到 badger：

- 调用 `Append()` 追加日志条目到 raftdb
- 如果有 HardState 变更，更新 `RaftLocalState` 并写入 raftdb
- 如果有 Snapshot，调用 `ApplySnapshot()` 处理
- 必须使用 `WriteBatch` 保证原子性

#### 2. `PeerStorage.Append()`

- 将 entries 写入 raftdb（Key 格式：`0x01 0x02 region_id 0x01 log_idx`）
- 更新 `raftState.LastIndex` 和 `raftState.LastTerm`
- 删除之前已追加但永远不会被提交的 entries

#### 3. `proposeRaftCommand()`

- 调用 `preProposeRaftCommand()` 做前置校验（Store ID、Peer ID、Term、Region Epoch）
- 将请求序列化为 Raft 日志条目
- 调用 `RawNode.Propose()`
- **记录回调函数**（`*message.Callback`）——这是关键，每个请求的回调必须在命令被应用后调用 `cb.Done()` 返回响应

#### 4. `HandleRaftReady()`

Part B 最核心的函数，流程如下：

1. 检查 `HasReady()`，如果有则调用 `Ready()`
2. 调用 `SaveReadyState()` 持久化
3. 通过 `Transport` 发送 `ready.Messages`
4. 逐个应用 `ready.CommittedEntries`：
   - 普通命令（Get/Put/Delete/Snap）：执行实际的 KV 操作
   - 通过 proposal 的回调返回响应
5. 调用 `Advance()` 通知 Raft 模块

### 错误处理

Part B 的测试会主动触发错误场景，你必须正确处理：

| 错误 | 触发条件 | 处理方式 |
| --- | --- | --- |
| `ErrNotLeader` | 请求发到了 follower | 返回此错误，附带 leader 信息，让客户端重试 |
| `ErrStaleCommand` | leader 变更导致已 propose 的日志被覆盖 | 通知等待中的客户端重试 |

使用 `BindRespError()` 把内部错误转成 `errorpb.proto` 定义的 protobuf 格式。

### 两个 badger 实例 + 4 种元数据

| DB | 存储内容 | Key 格式 |
| --- | --- | --- |
| raftdb | Raft 日志条目 | `0x01 0x02 region_id 0x01 log_idx` |
| raftdb | `RaftLocalState`（HardState + LastIndex） | `0x01 0x02 region_id 0x02` |
| kvdb | KV 数据（状态机） | 由 `engine_util` 管理 |
| kvdb | `RaftApplyState`（AppliedIndex + TruncatedState） | `0x01 0x02 region_id 0x03` |
| kvdb | `RegionLocalState`（Region 信息 + Peer 状态） | `0x01 0x03 region_id 0x01` |

使用 `meta` 包提供的 Key 生成函数，用 `WriteBatch.SetMeta()` 写入。

### Part B 测试

11 个集成测试，每个跑 3 轮迭代，覆盖从基础到极端的各种场景：

| 测试 | 场景 | 并发 | 网络 | 重启 | 分区 |
| --- | --- | --- | --- | --- | --- |
| `TestBasic2B` | 基础读写 | 1 | 可靠 | 否 | 否 |
| `TestConcurrent2B` | 并发客户端 | 5 | 可靠 | 否 | 否 |
| `TestUnreliable2B` | 不可靠网络 | 5 | 丢包/重排 | 否 | 否 |
| `TestOnePartition2B` | 单次分区 | 自定义 | 可靠 | 否 | 是 |
| `TestManyPartitionsOneClient2B` | 多次分区 | 1 | 可靠 | 否 | 是 |
| `TestManyPartitionsManyClients2B` | 多次分区 + 并发 | 5 | 可靠 | 否 | 是 |
| `TestPersistOneClient2B` | 持久化重启 | 1 | 可靠 | 是 | 否 |
| `TestPersistConcurrent2B` | 持久化 + 并发 | 5 | 可靠 | 是 | 否 |
| `TestPersistConcurrentUnreliable2B` | 持久化 + 并发 + 不可靠网络 | 5 | 丢包/重排 | 是 | 否 |
| `TestPersistPartition2B` | 持久化 + 分区 | 5 | 可靠 | 是 | 是 |
| `TestPersistPartitionUnreliable2B` | 持久化 + 分区 + 不可靠网络 | 5 | 丢包/重排 | 是 | 是 |

> 原文档提示："After 2A, some tests you may need to run them multiple times to find bugs"——说明部分 bug 是偶发的，在极端组合下才会暴露。

### Part B 准备工作（动手前必做）

1. **在纸上默画请求流转图**。从客户端 `RawGet` 调用开始，追踪请求完整路径：
   ```
   RaftStorage → raftCh → raftWorker → HandleMsg → proposeRaftCommand
   → RawNode.Propose → Raft 日志 → 提交 → HandleRaftReady → 应用到 kvdb
   → 回调 → 响应返回客户端
   ```
   理解完整链路后，你才能知道缺失的代码应该放在哪个环节。把每个步骤对应的文件和方法标注出来。

2. **阅读 TiKV 设计文档的 raftstore 部分**：[中文版](https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore) 或 [英文版](https://pingcap.com/blog/design-and-implementation-of-multi-raft/#raftstore)。TinyKV 的 raftstore 设计直接参考了 TiKV，理解原始设计意图会大大降低阅读代码的难度。

3. **精读已有框架代码**。以下文件的代码大部分已经实现，先读懂它们再动手写自己的部分：
   - `kv/raftstore/peer_msg_handler.go`：`HandleMsg()` 的消息路由逻辑、`preProposeRaftCommand()` 的前置校验、`onRaftMsg()` 的消息验证
   - `kv/raftstore/peer_storage.go`：`InitialState()`、`Entries()`、`Term()` 等方法如何读写 badger——你的 `Append()` 和 `SaveReadyState()` 需要与之协调
   - `kv/raftstore/raft_worker.go`：主事件循环如何调用 `HandleMsg()` 和 `HandleRaftReady()`
   - `kv/raftstore/meta/`：Key 生成和读写辅助函数

4. **搞清两个 badger 实例的分工**。raftdb 存 Raft 日志和 RaftLocalState，kvdb 存 KV 数据、RegionLocalState 和 RaftApplyState。画一张表写出每种数据存在哪个 DB、用什么 Key 格式。读写元数据时使用 `meta` 包的函数，用 `WriteBatch.SetMeta()` 写入。

5. **理解 WriteBatch 的原子性要求**。应用 committed entry 时必须在一个 batch 里同时完成：(a) 执行 KV 操作；(b) 更新 `RaftApplyState.AppliedIndex`。如果分开写，崩溃后 KV 数据已写入但 appliedIndex 未更新，重启后会重复应用已应用过的 entry。

### Part B 常见踩坑点

1. **忘记在 propose 时记录回调** → 客户端永久阻塞，测试超时
2. **应用 committed entries 时忘记更新 appliedIndex** → 重启后重复应用（基础测试可能不暴露，到 persist 测试才挂）
3. **原子写入不完整** → KV 操作和元数据更新未在同一 `WriteBatch`，崩溃后状态不一致
4. **`ErrNotLeader` 未正确返回** → follower 收到请求后没有返回 leader 信息，客户端无法重试到正确的节点
5. **`ErrStaleCommand` 未正确返回** → leader 变更后旧 proposal 的 callback 未收到错误响应，客户端永久阻塞
6. **`Transport` 发送 raft 消息位置不对** → 必须在持久化 HardState 之后再发送，否则崩溃后可能丢数据

### Part B 时间预估

**5-7 天**。工程复杂度高，需要阅读理解大量已有代码。集成测试每次运行耗时较长，偶发 bug 需要反复跑几十次来复现。

---

## Part C —— 快照与日志压缩

Part C 工作量比 Part B 小，但概念密度高。快照的生成和应用是异步的，理解其流水线是关键。

### Raft 层面的快照

在 `raft.go` 中实现 `handleSnapshot()`：

- 快照消息携带 `SnapshotMetadata`（term、commit index、成员信息等）
- 收到快照的 follower 直接用这些信息覆盖自己的 Raft 内部状态
- 与普通 AppendEntries 不同，快照包含的是整个状态机的"切面"而非增量日志

### raftstore 层面的快照流水线

#### 生成快照

```text
Storage.Snapshot() 被调用
  → 通过 regionSched 向 region worker 发 RegionTaskGen 任务
  → 立即返回 ErrSnapshotTemporarilyUnavailable
  → region worker 异步扫描引擎生成快照
  → 下次 Raft 调用 Snapshot() 时检查是否完成
  → 完成后，leader 向 follower 发送 MsgSnapshot
```

#### 应用快照

在 `HandleRaftReady()` 中检测到 snapshot 后：

1. 调用 `PeerStorage.ApplySnapshot()`
2. 更新内存中的 `raftState`、`applyState`、`region`
3. 调用 `clearMeta()` 清理 kvdb 和 raftdb 中的旧元数据
4. 调用 `clearExtraData()` 清理不在新 region 范围内的 KV 数据
5. 设置 `snapState` 为 `SnapState_Applying`
6. 通过 `regionSched` 向 region worker 发 `RegionTaskApply` 任务
7. 等待 region worker 完成后返回

### 日志 GC

- Raftstore 根据 `RaftLogGcCountLimit` 配置定期检查是否需要 GC
- 需要 GC 时，propose 一条 admin 命令 `CompactLogRequest`（像普通命令一样走 Raft 流程）
- 该命令提交后，更新 `RaftApplyState.TruncatedState`
- 通过 `ScheduleCompactLog()` 向 raftlog-gc worker 发送任务
- raftlog-gc worker 异步删除 raftdb 中的旧日志条目

### 状态一致性要求

应用快照时，`RaftLocalState`、`RaftApplyState`、`RegionLocalState` 三者必须保持一致并持久化。如果有任何一个状态没正确更新，重启后会出现不一致——Part C 的测试专门测重启恢复场景。

### Part C 测试

| 测试 | 场景 |
| --- | --- |
| `TestOneSnapshot2C` | 3 节点集群，隔离一个节点，写入触发快照，恢复分区后验证快照应用和日志截断 |
| `TestSnapshotRecover2C` | 快照 + 重启恢复 |
| `TestSnapshotRecoverManyClients2C` | 快照恢复 + 20 并发客户端 |
| `TestSnapshotUnreliable2C` | 不可靠网络 + 快照 |
| `TestSnapshotUnreliableRecover2C` | 不可靠网络 + 快照 + 重启 |
| `TestSnapshotUnreliableRecoverConcurrentPartition2C` | 最极端场景：不可靠网络 + 重启 + 并发 + 分区 + 快照 |

加上 6 个 Raft 层的快照单元测试：`TestRestoreSnapshot2C`、`TestRestoreIgnoreSnapshot2C`、`TestProvideSnap2C`、`TestRestoreFromSnapMsg2C`、`TestRestoreFromSnapWithOverlapingPeersMsg2C`、`TestSlowNodeRestore2C`、`TestRawNodeRestartFromSnapshot2C`。

### Part C 准备工作（动手前必做）

1. **回顾 Part B 的 HandleRaftReady 流程**。Part C 是在 Part B 的基础上增加快照和 GC 处理分支。确保你对 `HandleRaftReady()` 的现有流程已经烂熟于心，然后只在其中增加 snapshot 的判断和处理分支。

2. **理解快照的异步模型**。快照生成和应用都不是同步的：`Snapshot()` 发一个任务给 region worker 然后立即返回 `ErrSnapshotTemporarilyUnavailable`，下次再调才检查结果。这个模式在后续 TiKV 源码中也广泛使用，理解它对长远有帮助。

3. **区分普通命令和 admin 命令**。`CompactLogRequest` 是一种 admin 命令，和 Get/Put/Delete/Snap 一样走 Raft 流程，但提交后修改的是元数据（`RaftTruncatedState`）而非 KV 数据。在应用 committed entries 时需要根据 `EntryType` 或 `CmdType` 区分处理。

4. **搞清楚 snapshot 涉及的三个 worker**：
   - raftlog-gc worker：异步删除 raftdb 中的旧日志条目
   - region worker：异步生成快照（`RegionTaskGen`）和应用快照（`RegionTaskApply`）
   - snapRunner：处理快照文件在网络上的实际发送和接收
   你的代码主要负责触发这些 worker 的任务，以及在正确的时机等待其完成。

5. **回顾 `PeerStorage` 中已有的快照相关方法**：`Snapshot()`、`ApplySnapshot()`、`clearMeta()`、`clearExtraData()`、`ClearMeta()`。很多辅助逻辑已经实现，你需要做的是在正确的时机调用它们，并用 `WriteBatch` 保证原子性。

### Part C 常见踩坑点

1. **快照应用后忘记清理旧数据** → 调用 `clearMeta()` 和 `clearExtraData()` 清理旧元数据和不在新 region 范围内的 KV 数据，漏掉会导致 `TestSnapshotRecover2C` 挂
2. **`RaftLocalState`、`RaftApplyState`、`RegionLocalState` 三者更新不同步** → 应用快照后这三个状态必须一致，否则重启后状态机不一致
3. **`snapState` 状态机流转错误** → 生成中（`Generating`）、应用中（`Applying`）、空闲（`Relax`）三个状态之间切换不正确会导致快照生成/应用卡死
4. **`CompactLogRequest` 提交后未调度 GC 任务** → 日志永远不会被删除，磁盘持续增长
5. **原子写入不完整** → 应用快照时，清理元数据和写入新状态必须在同一批 `WriteBatch` 中完成
6. **`handleSnapshot()` 中未正确处理成员信息** → follower 收到快照后，`Prs` 和 `ConfState` 未更新，导致后续成员变更出问题

### Part C 时间预估

**2-3 天**。代码量比 Part B 少，但概念密度高。理解快照的异步流水线需要花时间，一旦理解清楚，实现相对直接。

---

## 参考资源汇总

| 资源 | 用途 |
| --- | --- |
| [Raft 官网](https://raft.github.io/) | 交互式可视化，直观理解 Raft 运行过程 |
| [扩展 Raft 论文](https://raft.github.io/raft.pdf) | 权威算法参考 |
| `raft/doc.go` | 每条 `MessageType` 的语义和处理规则，必读 |
| [TiKV raftstore 设计](https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore) | 理解 raftstore 的设计意图（中文） |
| `kv/raftstore/peer_msg_handler.go` | 已实现的消息处理框架，参考代码 |
| `kv/raftstore/peer_storage.go` | 已实现的存储接口方法，参考代码 |
| `kv/raftstore/meta/` | 元数据 Key 生成和读写辅助函数 |
