# Part A 代码注释

## log.go

## （1）Raftlog结构 — `stabled`、`committed`、`applied`

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

## （2）log.go — `LastIndex()` 的三种场景

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

## （1）
