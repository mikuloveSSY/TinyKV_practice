# Project1 StandaloneKV 详细说明

> 面向初学者的逐步指南

---

## 一、这个项目要我做什么？

用一句话说：**基于 badger 这个现成的嵌入式数据库，包一层外壳，对外提供 Put/Get/Delete/Scan 四个键值操作接口。**

整个 Project 1 就是一个**单机**的键值存储服务。单机（Standalone）意味着只有一台机器、一个进程，不涉及网络通信、不涉及分布式共识——这些放到后面 Project 2/3/4 才做。

---

## 二、整体架构

你可能不熟悉这些术语，先看这张图，对整体有个印象：

```
客户端（例如 TinySQL）
    │
    │ gRPC 调用（网络请求，但 P1 里客户端和服务端可以在同一台机器上）
    ▼
┌────────────────────────────────────────────┐
│  raw_api.go  ← 你要写的第 2 个文件            │
│  ┌──────────────────────────────────────┐  │
│  │ RawGet()   — 收到 Get 请求，调存储层     │  │
│  │ RawPut()   — 收到 Put 请求，调存储层     │  │
│  │ RawDelete()— 收到 Delete 请求，调存储层   │  │
│  │ RawScan()  — 收到 Scan 请求，调存储层     │  │
│  └──────────────────────────────────────┘  │
│  服务层：负责"接客"，解析请求，调用存储层，返回结果    │
└──────────────┬─────────────────────────────┘
               │ 调用 Storage 接口
               ▼
┌────────────────────────────────────────────┐
│  standalone_storage.go ← 你要写的第 1 个文件   │
│  ┌──────────────────────────────────────┐  │
│  │ Write()   — 把数据写入 badger          │  │
│  │ Reader()  — 从 badger 读数据（快照读）    │  │
│  │ Start()   — 打开 badger 数据库         │  │
│  │ Stop()    — 关闭 badger 数据库         │  │
│  └──────────────────────────────────────┘  │
│  存储层：封装 badger，实现 Storage 接口           │
└──────────────┬─────────────────────────────┘
               │ 调用 badger API
               ▼
┌────────────────────────────────────────────┐
│  badger ← 第三方的嵌入式 KV 数据库             │
│  （你不需要修改它，直接调用它的 API 即可）          │
│  真正负责把数据存到磁盘 / 从磁盘读出来              │
└────────────────────────────────────────────┘
```

---

## 三、关键概念解释

### 3.1 什么是 badger？

badger 是一个用纯 Go 语言写的**嵌入式键值数据库**。拆开理解：

**"嵌入式"是什么意思？**

它不是一个需要单独安装、单独启动、通过网络连接的数据库（比如 MySQL 你得先 `mysql.server start` 再 `mysql -u root` 连接）。badger 就是一个普通的 Go 库，你的程序 `import` 它，然后直接调用它的函数就行。运行的时候，badger 就在你的进程里面，不需要额外启动任何东西。

类比：MySQL 像一个独立运营的仓库公司，你要存东西得开车过去、填单子、交给仓管。badger 像一个工具箱，打开就能用，所有东西都在手边。

**"键值数据库"是什么意思？**

它只做一件事：你给我一个 Key（键），我存一个 Value（值）；你给我一个 Key，我把对应的 Value 还给你。没有 SQL、没有表结构、没有 JOIN。就像一个持久化到磁盘的 `map[string][]byte`——程序关了、电脑重启了，数据还在。

**"底层原理"简单说：**

```
写操作： key="foo" value="bar"
         → 先写入内存中的"热数据区"（MemTable），速度极快
         → 内存区满了，整块刷入磁盘（.sst 文件）
         → 这种结构叫 LSM-Tree，写操作几乎总是纯内存操作

读操作： key="foo"
         → 先查内存的 MemTable，找到就返回
         → 找不到再查磁盘上的 .sst 文件
         → 大部分热数据都在内存中，读也很快
```

**在 P1 里，你不需要深入理解它的内部实现**。你只需要知道：(1) 它是一个本地 KV 数据库，(2) 提供了 `Open`/`Close`/`Update`/`View`/`NewTransaction` 等 API，(3) 你通过 `engine_util` 这个中间层去调用它。

### 3.2 什么是列族（Column Family，CF）？

列族 = **键的命名空间**。同一个键名，在不同列族下存的是不同的值。

**通俗类比**：

把 badger 数据库想象成一个文件柜。一个文件柜里有多个**文件夹**（列族），每个文件夹里可以有同名的**文件**（键），但内容是独立的。

```
┌─ 文件柜（badger 数据库）─────────────┐
│                                      │
│  📂 文件夹 "default"                  │
│    ├─ "foo" → "hello"                │
│    └─ "bar" → "world"                │
│                                      │
│  📂 文件夹 "lock"                     │
│    ├─ "foo" → "locked_by_txn_5"     │  ← 和上面的 "foo" 是不同的！
│    └─ "baz" → "unlocked"            │
│                                      │
│  📂 文件夹 "write"                    │
│    └─ "foo" → "version_3=hello"     │  ← 还是不同的 "foo"
│                                      │
└──────────────────────────────────────┘
```

**为什么 TinyKV 需要列族？**

这是为 Project 4（事务）做准备的。事务需要追踪三件事：

- **"default"**：实际的用户数据（当前值是什么）
- **"write"**：每次写入的历史记录（哪个事务、什么时间写的，用于 MVCC 多版本控制）
- **"lock"**：哪些键正在被某个事务修改（防止两个事务同时改同一个键）

如果不分列族，这三种数据混在一起，根本分辨不出"这个键存的到底是数据本身、还是锁信息、还是历史记录"。列族就是为了把它们隔离开。

### 3.3 为什么需要 engine_util？

badger 只认 `Key → Value`，**不知道什么是列族**。那怎么在一个 badger 实例里同时存三个列族呢？

engine_util 的思路就是**给 key 加前缀**：

```
你的代码：  engine_util.PutCF(db, "default", "foo", "hello")

engine_util 内部做的事：
  1. 把 key 变成 "default_foo"    ← 拼上 "default_"
  2. 再调用 badger 原始 API：db.Update(func(txn){ txn.Set("default_foo", "hello") })
  3. badger 存的 key 就是 "default_foo"
```

这样一来：

```
列族 "default" 的 key="foo"    → badger 存的 key="default_foo"
列族 "lock"    的 key="foo"    → badger 存的 key="lock_foo"
列族 "write"   的 key="foo"    → badger 存的 key="write_foo"
```

三个 `foo` 在 badger 里是三个不同的物理 key，不会冲突。读取时 `GetCF` 同样自动加上前缀 `default_`，然后去 badger 读 `default_foo`，读出来后把前缀去掉，返回原始值给你。

**所以 P1 的核心规则就是**：你所有的读写操作都必须走 `engine_util` 提供的方法。不要直接用 badger 的 `txn.Set`、`txn.Get`——因为 badger 不认识列族前缀，直接调会绕过前缀机制，导致数据错乱。

### 3.4 什么是 gRPC 和 Protocol Buffer？

这两个是 C/S（客户端/服务端）通信的基石，P1 里你只需要理解概念，不需要深入。

**Protocol Buffer（protobuf）—— 数据的"合同"**

- 在 `proto/proto/kvrpcpb.proto` 中定义了一个请求长什么样，响应长什么样
- 比如定义一个 RawGet 请求："我需要 Cf 字段（string 类型）和 Key 字段（bytes 类型）"
- 定义好之后，protobuf 工具自动生成 Go 代码（`proto/pkg/kvrpcpb/` 下的 `.pb.go` 文件）
- 网络传输时，protobuf 把数据压成紧凑的二进制格式，比 JSON 体积小、速度快

**gRPC —— 远程调用的"管道"**

- gRPC 是一个远程过程调用框架，基于 HTTP/2 + protobuf
- 你的服务端实现了 `RawGet()` 方法，客户端就可以通过网络调用它，**就像调用本地函数一样**
- gRPC 帮你处理了网络连接、序列化/反序列化、超时、错误返回这些琐事

**在 P1 中，数据流是这样的：**

```
客户端（TinySQL）
  │ 构造 RawGetRequest{Cf: "default", Key: "foo"}
  │ 序列化为二进制 → 通过 HTTP/2 发送
  ▼
gRPC 框架（自动处理）
  │ 接收二进制 → 反序列化为 RawGetRequest 结构体
  ▼
你的 raw_api.go 里的 RawGet(req ...)
  │ req.Cf = "default", req.Key = "foo"
  │ 你调 Reader.GetCF() 拿到值
  │ 返回 RawGetResponse{Value: "hello"}
  ▼
gRPC 框架（自动处理）
  │ 序列化为二进制 → 通过 HTTP/2 返回
  ▼
客户端收到 RawGetResponse{Value: "hello"}
```

**你需要做的事**：只写 `RawGet/Put/Delete/Scan` 函数的内容逻辑（从 request 取参数，调 storage，填 response）。proto 文件、序列化、网络传输这些都不用管。

---

## 四、你需要完成的 10 处代码

### 文件一：`kv/storage/standalone_storage/standalone_storage.go`（6 处）

这个文件实现存储引擎，是 badger 的"外壳"。

| 位置                             | 做什么                                                                                                                          | 通俗解释                                           |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| **struct 字段**            | 添加 badger 数据库实例和配置等成员变量                                                                                          | 给结构体加上"大脑"，让它能记住 badger 实例         |
| **NewStandAloneStorage()** | 打开（或创建）badger 数据库，返回一个 StandAloneStorage 实例                                                                    | 构造函数，相当于"开机"前的准备工作                 |
| **Start()**                | 可能需要做一些初始化（P1 中可能不需要额外操作）                                                                                 | 启动服务                                           |
| **Stop()**                 | 调用 badger 的 Close 方法，关闭数据库                                                                                           | 关闭服务，释放资源                                 |
| **Write()**                | 遍历 batch 里的每个 Modify，判断是 Put 还是 Delete，调用 engine_util 的`PutCF` / `DeleteCF` 写入 badger                     | 把一批写操作落地到磁盘                             |
| **Reader()**               | 用`badger.Txn` 创建一个事务，基于这个事务返回一个 reader 对象。这个 reader 需要实现 `GetCF`、`IterCF`、`Close` 三个方法 | 创建一个"快照读"，后面的 Get/Scan 都从这个快照里读 |

> **关于 Reader 的实现方式**：你需要自己定义一个新的 struct（比如叫 `StandaloneStorageReader`），给它实现 `GetCF`、`IterCF`、`Close` 三个方法。`Reader()` 方法的职责就是创建并返回这个 struct 的实例。

### 文件二：`kv/server/raw_api.go`（4 处）

这个文件实现服务接口，是"接待客户端请求"的地方。

| 函数                  | 要做的事                                                                                                                                                                                                            |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **RawGet()**    | 1. 调`server.storage.Reader()` 拿到 reader2. 调 `reader.GetCF(req.Cf, req.Key)` 拿到 value3. 如果 key 不存在 value 为 nil，不要报错4. 把 value 填入 response，返回                                              |
| **RawPut()**    | 1. 从请求中取出 Cf、Key、Value2. 构造一个 `storage.Modify{Data: storage.Put{...}}`3. 调 `server.storage.Write(ctx, []storage.Modify{modify})` 写入4. 返回空 response（或 error）                                |
| **RawDelete()** | 同 RawPut，只是把`storage.Put` 换成 `storage.Delete`                                                                                                                                                            |
| **RawScan()**   | 1. 调`server.storage.Reader()` 拿到 reader2. 调 `reader.IterCF(req.Cf)` 拿到迭代器3. 用 `iter.Seek(startKey)` 定位到起始位置4. 循环遍历，收集 kv 对，注意 `GetLimit()` 限制数量5. 把结果填入 response，返回 |
|                       | **注意**：Scan 时别忘了在函数结束时调用 `reader.Close()`（用 `defer`）                                                                                                                                    |

---

## 五、新手实用提示

### 5.1 如何找到 engine_util 有哪些方法？

打开 `kv/util/engine_util/` 目录，看 `.go` 文件即可。或者按住 Ctrl 点击代码里的 `engine_util` 跳转。常用的：

```go
engine_util.PutCF(db, cf, key, value)     // 写入
engine_util.GetCF(txn, cf, key)            // 读取
engine_util.DeleteCF(txn, cf, key)         // 删除
engine_util.NewCFIterator(cf, txn)         // 创建迭代器（用于 Scan）
```

### 5.2 Go 语法须知

- **defer**：`defer reader.Close()` 表示"当前函数返回之前，自动调用 `reader.Close()`"，用于资源清理
- **if err != nil**：Go 里几乎所有可能出错的操作都返回 error，需要检查
- **interface**：`Storage` 是一个 interface，你的 `StandAloneStorage` 只要实现了 interface 里所有方法，就自动"实现了"这个接口，不需要用类似 `implements` 的关键字显式声明
- **:= 与 var**：短声明 `x := 42` 只能在函数内使用，struct 里的字段需要用 `var` 或直接声明类型

### 5.3 常见报错处理

| 报错                          | 可能原因                                                      |
| ----------------------------- | ------------------------------------------------------------- |
| `undefined: engine_util`    | 忘记 import 对应的包                                          |
| `cannot use ... as Storage` | 漏实现了接口中的某个方法（比如 Close）                        |
| `nil pointer dereference`   | badger 实例是 nil，检查 NewStandAloneStorage 有没有正确初始化 |

### 5.4 迭代器的正确用法

```go
iter := reader.IterCF(cf)
defer iter.Close()     // 一定要关闭！
for iter.Seek(startKey); iter.Valid(); iter.Next() {
    key := iter.Item().Key()
    value, _ := iter.Item().Value()
    // 收集 key 和 value
}
```

---

## 六、完成后怎么验证？

```bash
cd tinykv
make project1
```

看到所有测试都是 `PASS` 就说明 Project 1 通过了。
