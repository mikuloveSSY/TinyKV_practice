# TinyKV 架构与运行逻辑

## 一、整体架构

TinyKV 是分层架构的分布式 KV 存储系统，从上到下分为五层：

```
客户端 (TinySQL)
  │
  │ gRPC (protobuf + HTTP/2)
  │
┌─▼──────────────────────────────────────────────┐
│ 第一层  服务层  kv/server/                       │
│         实现 TinyKvServer gRPC 接口              │
│         · Raw  API   原始键值操作                 │
│         · Txn  API   事务操作                    │
├────────────────────────────────────────────────┤
│ 第二层  存储抽象层  kv/storage/                   │
│         Storage {Start, Stop, Write, Reader}     │
│         ├── StandAloneStorage  单机直接写         │
│         └── RaftStorage        经 Raft 共识后写   │
├────────────────────────────────────────────────┤
│ 第三层  Raft 共识层                              │
│         ├── raftstore/  状态机引擎               │
│         └── raft/       核心算法（选举/日志/快照） │
├────────────────────────────────────────────────┤
│ 第四层  引擎封装层  kv/util/engine_util/          │
│         通过 key 前缀模拟列族                      │
├────────────────────────────────────────────────┤
│ 第五层  存储引擎层                               │
│         ├── Kv Badger    用户数据                │
│         └── Raft Badger  Raft 元数据              │
└──────────────────────────────────────────────┘
```

一个客户端请求进入 TinyKV 后，沿着这五层**纵向穿透**：服务层解析 gRPC 请求 → 存储抽象层决定走单机还是分布式路径 →（分布式模式下）Raft 层完成多节点共识 → 引擎封装层处理列族前缀 → 存储引擎层最终写入磁盘。

这种分层设计的核心价值在于**每一层只对自己的职责负责**。服务层不关心数据如何持久化，存储抽象层不关心底层是单机还是集群，引擎封装层不关心上层的业务逻辑。因此从单机版切换到分布式版只需要替换第二层的实现（StandAloneStorage → RaftStorage），上层 Server 的代码一行不变。

---

## 二、核心组件与职责

| 组件                        | 路径                               | 一句话角色                                              |
| --------------------------- | ---------------------------------- | ------------------------------------------------------- |
| **Server**            | `kv/server/`                     | gRPC 入口，解析请求 → 调 Storage 接口 → 返回响应      |
| **Storage**           | `kv/storage/storage.go`          | 存储抽象接口：`Write`(批量写) + `Reader`(快照读)    |
| **StandAloneStorage** | `kv/storage/standalone_storage/` | P1：直接操作 Badger，无网络无共识                       |
| **RaftStorage**       | `kv/storage/raft_storage/`       | P2-4：对上层提供相同的 Storage 接口，对下层走 Raft 共识 |
| **Raftstore**         | `kv/raftstore/`                  | 胶水层：消息路由 → 命令提议 → 日志 apply → 回调响应  |
| **Raft**              | `raft/`                          | 纯 Raft 算法：选举 Leader、复制日志、压缩快照           |
| **engine_util**       | `kv/util/engine_util/`           | Badger 封装，通过 key 前缀模拟列族                      |
| **Scheduler**         | `scheduler/`                     | 独立的集群控制中心，管理 Region 分布和负载均衡          |

这些组件的关系可以按两条数据通道来理解：

```
通道一：用户数据路径
  Server → Storage.Write/Reader → engine_util → Kv Badger
  单机：直接贯穿。分布式：Storage 层先转到 Raft 共识，再走这条路径。

通道二：Raft 元数据路径（仅分布式）
  Raft 状态机 → PeerStorage → Raft Badger
  存放 Term、VoteFor、日志条目等 Raft 内部状态，用户完全不感知。
```

两条通道分离的原因是访问模式不同：用户数据有列族语义、有事务 MVCC；Raft 元数据是简单的 key-value，仅 Raft 模块内部使用。分存两个 Badger 实例，互不干扰。

---

## 三、系统调用链

### 3.1 读请求（单机模式）

```
Client gRPC 请求
  │
  ▼
Server.RawGet(req)
  │
  ├─ ① storage.Reader(ctx)
  │     → StandAloneStorage.Reader()
  │       → db.NewTransaction(false)    ← 创建 Badger 只读事务，获取一致性快照
  │       → 返回 StandaloneReader{txn}
  │
  ├─ ② reader.GetCF(req.Cf, req.Key)
  │     → engine_util.GetCFFromTxn(txn, cf, key)
  │       → 拼接 key = cf + "_" + key   ← 例如 "default_foo"
  │       → txn.Get("default_foo")      ← Badger 先查内存，再查磁盘
  │       → 返回 value 或 nil（不存在）
  │
  ├─ ③ 构造响应 → 返回客户端
  │
  └─ ④ reader.Close() → txn.Discard()  ← 释放快照，允许 GC
```

读请求链路最短。核心机制是 **Badger 快照读**：`db.NewTransaction(false)` 创建的只读事务，看到的是调用瞬间数据库的一致性快照。之后即使有新的写入，这个事务读到的数据也不变。这是后面 P4 事务 MVCC 的基础——快照隔离依赖的就是这个特性。

### 3.2 写请求（单机模式）

```
Client gRPC 请求
  │
  ▼
Server.RawPut(req)
  │
  ├─ ① 构造 Modify{Data: Put{Key, Value, Cf}}
  │
  ├─ ② storage.Write(ctx, []Modify{modify})
  │     → StandAloneStorage.Write()
  │       → 遍历 batch，类型断言判断 Put/Delete
  │       → engine_util.PutCF(db, cf, key, value)
  │         → db.Update(func(txn) {
  │              txn.Set(cf + "_" + key, value)   ← 加前缀 + 原子写入
  │            })
  │         → Badger 写内存表（MemTable）→ 返回
  │         → 后续异步刷盘到 .sst 文件
  │
  └─ ③ 返回 RawPutResponse
```

写请求链路同样短。`db.Update` 创建一个**读-写事务**，内部的 `txn.Set` 是原子操作——要么完整写入，要么回滚。写入目标是 Badger 的内存表，写完即返回（不等待刷盘），所以延迟极低。后台 compaction 负责把内存表刷到磁盘文件。

### 3.3 读请求（分布式模式）

```
Client gRPC 请求
  │
  ▼
Server.RawGet(req)
  │
  ├─ ① storage.Reader(ctx)
  │     → RaftStorage.Reader()
  │       → 向 raftWorker 发送 Snap 命令
  │       → 获取当前 Leader 的 Region 快照
  │       → 返回 RegionReader{txn, region}    ← 限定 Region 范围的 badger.Txn
  │
  ├─ ② reader.GetCF(req.Cf, req.Key)
  │     → 检查 key 是否在 Region 范围内
  │     → engine_util.GetCFFromTxn(txn, cf, key) → Badger 查找
  │
  └─ ③ 返回响应
```

分布式读相比单机读多了两步：获取 Leader 快照（保证读的是最新已提交的数据），以及检查 key 是否在当前 Region 范围内（多 Region 模式下 key 被分片）。读不需要走 Raft 共识——只从 Leader 本地 Badger 快照读即可，所以读延迟比写低得多。

### 3.4 写请求（分布式模式）—— 核心链路

```
Client gRPC 请求
  │
  ▼
Server.RawPut(req)
  │
  ├─ ① 构造 Modify → storage.Write(ctx, batch)
  │
  ├─ ② RaftStorage.Write(batch)
  │     → 编码 Modify 为 raft_cmdpb.RaftCmdRequest（protobuf）
  │     → 创建 callback
  │     → router.SendRaftCommand(regionID, request, callback)
  │       → 按 RegionID 路由到对应 Peer
  │       → 消息投入 raftWorker 的 channel
  │       → ★ 当前 goroutine 阻塞，等待 callback
  │
  │  ┌── raftWorker 异步处理（另一条 goroutine）────────┐
  │  │                                                  │
  │  │  HandleMsg(msg)                                   │
  │  │    ├─ proposeRaftCommand()                       │
  │  │    │     → RawNode.Step(MsgPropose)              │
  │  │    │     → Raft.Step()：追加到 Leader Raft 日志    │
  │  │    │     → Leader 并行广播 AppendEntries          │
  │  │    │     → 注册 callback 到 proposal map         │
  │  │    │                                             │
  │  │    ├─ Follower 收到 AppendEntries                 │
  │  │    │     → 本地写 Raft Badger                     │
  │  │    │     → 回复 ACK                              │
  │  │    │                                             │
  │  │    └─ HandleRaftReady()（收到多数 ACK 后触发）      │
  │  │          ├─ RawNode.Ready() → 取出已提交 entries   │
  │  │          ├─ 逐条 apply：反序列化 → engine_util 写 Kv Badger│
  │  │          ├─ HardState + Entries 持久化到 Raft Badger│
  │  │          ├─ 发送 Raft 消息给其他 Peer               │
  │  │          ├─ callback.Done(response) ★ 唤醒等待者    │
  │  │          └─ RawNode.Advance()                    │
  │  │                                                  │
  │  └──────────────────────────────────────────────────┘
  │
  └─ ③ callback 被唤醒 → 返回响应给客户端
```

这是 TinyKV 最核心的调用链。它清楚地展示了分布式模式与单机模式的本质区别：**请求 goroutine 和 raftWorker goroutine 通过 channel + callback 解耦**。请求 goroutine 在投递命令后立即挂起，raftWorker 在后台完成 Raft 共识、日志持久化、状态机 apply 后通过 callback 唤醒它。整条链路涉及两次磁盘写入（Raft Badger 存日志 + Kv Badger 存数据）和一次网络 RPC（AppendEntries 广播），但客户端只感知到一次阻塞等待。

### 3.5 两条链路的对比

```
单机写：  Client → Server → StandAloneStorage → engine_util → Kv Badger
          └────────────── 一次同步函数调用 ──────────────────┘

分布式写：Client → Server → RaftStorage → router → raftWorker
                                                  │
                                            Raft 共识（网络 + Raft Badger）
                                                  │
                                              engine_util → Kv Badger
          └── 编码投递 ──┘└──── 阻塞等待 ────┘└─── 异步处理 ──┘└─ 回调 ─┘
```

单机模式下，整条链路是一个函数调用栈从头走到尾。分布式模式下，链路被 **channel** 切成了两段：前半段负责编码和投递，后半段在 raftWorker 中处理共识和持久化。分段的好处是请求 goroutine 不阻塞 raftWorker，raftWorker 可以同时处理多个请求的共识流程。

---

## 四、单机模式架构（P1）

```
Server
  │
  ▼
StandAloneStorage
  │
  ├── Write(batch) → engine_util.PutCF / DeleteCF → Kv Badger
  └── Reader()     → 创建 Badger 只读事务快照
                     → 返回 reader（支持 GetCF 点查询 + IterCF 范围扫描）
  │
  ▼
Kv Badger（磁盘）
```

单机模式下，整个系统只有一个 `Kv Badger` 实例。Server 收到请求后，调用 `StandAloneStorage.Write` 或 `Reader`，直接穿透到 Badger 完成磁盘读写。没有 goroutine 间通信，没有网络，没有共识——本质就是一个带列族外壳的本地 Badger 封装。

---

## 五、分布式模式架构（P2-4）

### 5.1 总体架构

```
Server
  │
  ▼
RaftStorage
  │
  ├── Write(batch) → 编码为 RaftCmdRequest
  │                  → router.SendRaftCommand(request, callback)
  │                  → 阻塞等待 callback
  │
  └── Reader()     → 从 Leader 快照获取 Region 范围的只读事务

       ┌──────────────────────────────┐
       │ raftstore（胶水层）            │
       │                              │
       │  router ──► raftWorker       │
       │               │              │
       │               ├─ HandleMsg()    接收消息 → 提议到 Raft
       │               └─ HandleRaftReady()  取出已提交 → apply → 回调
       └──────────────┬───────────────┘
                      │
       ┌──────────────▼───────────────┐
       │ raft（纯算法层）               │
       │  状态机：选举 → 日志复制 → 提交  │
       └──────────────┬───────────────┘
                      │
       ┌──────────────▼───────────────┐
       │ engine_util + Badger          │
       │  Kv Badger   用户数据          │
       │  Raft Badger Raft 日志         │
       └──────────────────────────────┘
```

分布式模式相比单机多了三件东西：

**一是 RaftStorage 层。** 它和 StandAloneStorage 实现了同样的 `Storage` 接口，但内部逻辑完全不同：`Write` 不再直接写 Badger，而是把命令编码为 protobuf 格式，通过 Router 投递给 raftWorker，然后**阻塞等待** Raft 共识完成。从"同步调用"变成了"异步投递 + 回调等待"。

**二是 raftstore 胶水层。** 这是整个分布式模式的心脏。核心是一条名叫 `raftWorker` 的 goroutine，它在一个无限循环中做两件事：`HandleMsg`（接收外部消息——客户端命令、其他节点的 Raft 消息、定时 tick——然后提议到 Raft 算法），`HandleRaftReady`（从 Raft 算法取出已共识的结果，apply 到 Kv Badger，通知等待的客户端）。

**三是 Raft Badger。** Raft 算法本身需要持久化状态（Term、VoteFor、日志条目），这些存在独立的 Raft Badger 中。为什么不用同一个 Badger？因为 Raft 日志是频繁写、短期存（很快被 GC），和用户数据的访问模式完全不同，分开管理更高效。

### 5.2 一次写入的完整历程

从客户端发送一个 Put 请求到收到响应，在分布式模式下经历三个阶段：

```
阶段一：编码与投递
  RaftStorage.Write()
    → 把 Put/Delete 编码为 protobuf 格式的 RaftCmdRequest
    → 通过 router 投递到 raftWorker 的 channel
    → 当前 goroutine 进入阻塞等待

阶段二：Raft 共识
  raftWorker.HandleMsg()
    → Leader 把命令追加到自己的 Raft 日志
    → Leader 通过 gRPC 并行发送 AppendEntries 给所有 Follower
    → Follower 各自把日志写入本地 Raft Badger，回复 ACK
    → Leader 收到多数 ACK（包括自己）→ 日志标记为"已提交"

阶段三：Apply 与响应
  raftWorker.HandleRaftReady()
    → 取出已提交的日志条目
    → 逐条执行：engine_util.PutCF / DeleteCF → Kv Badger
    → callback.Done() 唤醒等待的 goroutine
    → 响应返回客户端
```

关键点是"多数确认即提交"：在 3 节点集群中 Leader 只需要自己 + 1 个 Follower 确认即可提交，不等第三个。这就是为什么 Raft 能容忍少数节点故障——3 节点容忍 1 个宕机，5 节点容忍 2 个。

### 5.3 运行时并发模型

```
gRPC Server goroutine
  └─► 请求 goroutine × N
        │ 通过 channel 投递命令 ──► raftWorker goroutine（唯一）
        │                              for {
        │                                msg := <-raftCh
        │                                HandleMsg(msg)
        │                                HandleRaftReady()
        │                              }
        │
        ├─► tickDriver goroutine
        │     选举超时 / 心跳 / GC / Split Check
        │
        └─► 后台 runner goroutine
              日志 GC / 快照收发 / Scheduler 心跳
```

整个节点的核心设计原则是：**所有 Raft 操作串行化在一条 goroutine 里**。为什么？因为 Raft 状态机（任期、日志位置、commit 指针）是复杂的相互依赖数据，如果多线程并发修改就需要大量的锁，极易出错。单 goroutine + channel 投递的方式天然避免了所有竞态问题。外部请求 goroutine 通过 channel 把命令"投进去"，然后自己阻塞等结果——通道保证了消息的顺序，单线程保证了状态的安全。

### 5.4 多节点交互

```
Leader                    Follower A              Follower B
  │                                                │
  │══ AppendEntries RPC ═══════════════════════════►│
  │══ AppendEntries RPC ═══►│                       │
  │                          │                       │
  │◄═ 收到多数 ACK ════════►│◄══════════════════════┤
  │                          │                       │
  │ 提交 → apply KvDB         │ 等待提交 → apply       │ 等待提交 → apply
  │ 响应客户端                 │                       │
```

节点间通信基于 gRPC Streaming RPC，在 proto 中定义为 `Raft` 和 `Snapshot` 两个双向流。流式连接的好处是可以复用 TCP 连接双向持续传输，适合 Raft 这种高频消息交换场景。

---

## 六、关键机制

### 5.1 列族是怎么实现的

Badger 只认 Key → Value，不认识列族。TinyKV 不需要修改 Badger 源码，而是在 engine_util 层做了极简处理：**给每个 key 拼上 `cf_` 前缀**。

```
PutCF(db, "default", "foo", "hello")
  → 实际存入 Badger 的 key = "default_foo"

PutCF(db, "lock",    "foo", "locked")
  → 实际存入 Badger 的 key = "lock_foo"
```

读取时反向操作：`GetCF` 自动拼前缀去查 Badger，查到后去掉前缀返回。三个列族的同名 key 在 Badger 里是三个不同的物理 key，互不冲突。这种设计简单、没有性能损耗、也不需要改动 Badger 本身。

| 列族        | 用途                                    |
| ----------- | --------------------------------------- |
| `default` | 用户数据 + MVCC 多版本值                |
| `lock`    | 分布式锁（P4 事务的 Prewrite 阶段写入） |
| `write`   | 提交记录（P4 事务的 Commit 阶段写入）   |

### 5.2 Raft 的工作原理

Raft 对外暴露 **Step → Ready → Advance** 三步循环：

```
Step(msg)   → 把外部事件注入 Raft 状态机（客户端命令、网络消息、tick）
Ready()     → 取出本轮产出：待持久化的状态、已提交待 apply 的日志、待发送的消息
Advance()   → 确认本轮处理完毕，Raft 更新内部指针
```

Raft 状态机在三种角色间切换：

- **Follower**：默认角色，被动接收 Leader 的日志复制和心跳
- **Candidate**：Follower 超时没收到 Leader 心跳 → 变成 Candidate → 发起选举，向其他节点请求投票
- **Leader**：Candidate 获得多数票 → 变成 Leader → 接收客户端请求，复制日志到 Follower

每次角色切换伴随着 Term（任期）递增。Term 是 Raft 中的"逻辑时钟"，所有的消息都带 Term，如果消息中的 Term 比自己新就更新自己，如果旧就拒绝——这个简单的规则保证了整个集群最终只有一个 Leader。

### 5.3 事务是怎么工作的（P4）

事务层在 Server 和 Storage 之间插入，不改变 Storage 接口：

```
Server 事务 RPC
  → 获取 Latches（键级内存锁，防并发冲突）
  → 创建 MvccTxn（持有时间戳 + 快照）
     操作三个列族：
       default CF  — 存带时间戳的 value（多版本）
       lock CF     — 存分布式锁（标记"这个 key 正在被事务修改"）
       write CF    — 存提交记录（标记"这个 key 已被事务提交"）
  → storage.Write() 批量持久化（经 Raft 共识）
  → 释放 Latches
```

协议遵循 **Percolator 两阶段提交**：

```
第一阶段 Prewrite：把要改的 key 全部加上锁（写到 lock CF）
                  任何一个 key 已经被人锁了 → 事务冲突，回滚

第二阶段 Commit：  所有 key 都加锁成功 → 写提交记录到 write CF
                  删掉锁 → 事务完成
```

这套方案保证了**快照隔离**：一个事务读到的数据，是事务开始时那个时间点的一致性快照，不会读到其他事务中间态的数据。

---

## 七、两种模式对比

| 维度        | P1 单机                  | P2-4 分布式                                      |
| ----------- | ------------------------ | ------------------------------------------------ |
| Badger 实例 | 1 个（Kv）               | 2 个（Kv + Raft）                                |
| 写入方式    | 同步调用直达 Badger      | 异步投递 → Raft 共识 → callback 等待           |
| 读取方式    | 本地 Badger 快照         | Leader 快照（Region 限定范围）                   |
| 并发模型    | 单 goroutine 请求处理    | raftWorker 主循环 + 请求 goroutine + tick/runner |
| 容灾        | 无（磁盘坏了数据就没了） | 有（多数节点存活即可服务）                       |
| 列族        | 只用 default CF          | 所有 CF 都用（default + lock + write）           |

---

## 八、目录结构

```
tinykv/
├── kv/
│   ├── main.go                    ← 入口，组装组件、启动服务
│   ├── server/                    ← gRPC 服务层
│   │   └── raw_api.go             ← Raw Get/Put/Delete/Scan 处理函数
│   ├── storage/                   ← 存储抽象层
│   │   ├── storage.go             ← Storage 接口定义
│   │   ├── modify.go              ← Modify / Put / Delete 数据结构
│   │   ├── standalone_storage/    ← P1 单机实现
│   │   └── raft_storage/          ← P2-4 分布式实现
│   ├── raftstore/                 ← Raft 状态机胶水层
│   │   ├── peer_msg_handler.go    ← 消息分发 + Ready 处理
│   │   ├── peer_storage.go        ← raft.Storage 接口实现
│   │   ├── router.go              ← 按 Region 路由消息
│   │   ├── raft_worker.go         ← Raft 主事件循环
│   │   └── ticker.go              ← 定时驱动
│   ├── transaction/               ← 事务层
│   │   ├── mvcc/                  ← MvccTxn / Lock / Write
│   │   └── latches/               ← 键级并发锁
│   └── util/engine_util/          ← Badger 列族封装
├── raft/                          ← Raft 纯算法
│   ├── raft.go                    ← Raft 状态机
│   ├── rawnode.go                 ← Step/Ready/Advance 接口
│   └── log.go                     ← Raft 日志管理
├── proto/                         ← gRPC 协议定义
└── scheduler/                     ← PD 调度器
```
