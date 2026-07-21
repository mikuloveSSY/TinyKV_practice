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

`becomeLeader()` 追加 noop 日志，但该日志并未发送出去，且 2AA 阶段 `stepFollower` 收到 `MsgAppend` 没有调用 `handleAppendEntries`，日志未落盘。结果当过 Leader 的节点日志"更新"，拒绝未当过 Leader 的节点的投票。3 节点依次竞选到 node 3 时被两边拒绝，退回 Follower。

**修复**：注释掉 noop 追加，等 2AB `handleAppendEntries` 实现后、链路完整时再加回。

### 修复汇总

| 位置                | 修改               |
| ------------------- | ------------------ |
| `becomeFollower`  | 随机化公式修改     |
| `becomeCandidate` | 新增相同随机化逻辑 |
| `becomeLeader`    | 注释 noop 的追加   |

修复后 24 个测试全部通过。
