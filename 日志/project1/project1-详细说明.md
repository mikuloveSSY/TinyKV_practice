# Project1 StandaloneKV 详细说明

> 面向初学者的逐步指南

---

## 一、这个项目要我做什么？

用一句话说：**基于 badger 这个现成的嵌入式数据库，包一层外壳，对外提供 Put/Get/Delete/Scan 四个键值操作接口。**

整个 Project 1 就是一个**单机**的键值存储服务。单机（Standalone）意味着只有一台机器、一个进程，不涉及网络通信、不涉及分布式共识——这些放到后面 Project 2/3/4 才做。

---

## 二、整体架构（新手版）

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

badger 是一个用 Go 写的**嵌入式键值数据库**。"嵌入式"意味着它不是一个独立的数据库服务器（不像 MySQL 需要单独启动），而是一个**库**，你的程序直接 import 它，然后调用它的 API 来存取数据。数据存在本地文件里。

你可以把它理解为 Go 版的 LevelDB 或 RocksDB。

### 3.2 什么是列族（Column Family，CF）？

列族 = **键的命名空间**。同一个键名，在不同列族下存的是不同的值。

你可以这样理解：每个列族就像一个独立的 Excel 表格，不同表格里可以有相同的行号，但内容是独立的。

```
列族 "default" 里：  key="foo" → value="hello"
列族 "write"  里：  key="foo" → value="world"   ← 同一个 key，不同列族，不同值
```

### 3.3 为什么需要 engine_util？

badger 本身**不支持列族**这个概念。TinyKV 通过 `engine_util` 包用了一个巧妙的方法来模拟：在 key 前面加前缀。

```
原始写入：  cf="default", key="foo", value="hello"
实际存储：  key="default_foo", value="hello"    ← key 前面自动加了 cf 前缀
```

所以你需要用 `engine_util` 提供的方法来做所有读写操作（`PutCF`、`GetCF`、`DeleteCF` 等），而不是直接用 badger 的原始 API。`engine_util` 帮你自动处理了前缀的拼接和解析。

### 3.4 什么是 gRPC 和 Protocol Buffer？

- **Protocol Buffer（protobuf）**：一种数据序列化格式，用于定义"请求长什么样、响应长什么样"。项目里 `.proto` 文件就是干这个的。
- **gRPC**：基于 protobuf 的远程调用框架，让客户端可以像调用本地函数一样调用远程服务。项目里 `raw_api.go` 里的四个函数，就是 gRPC 服务端处理请求的地方。

在 P1 里你不需要深入理解这两个，只要知道：客户端发送的请求会经过 gRPC 框架，最终到达 `raw_api.go` 里你写的函数。

---

## 四、你需要完成的 10 处代码

### 文件一：`kv/storage/standalone_storage/standalone_storage.go`（6 处）

这个文件实现存储引擎，是 badger 的"外壳"。

| 位置 | 做什么 | 通俗解释 |
|------|------|------|
| **struct 字段** | 添加 badger 数据库实例和配置等成员变量 | 给结构体加上"大脑"，让它能记住 badger 实例 |
| **NewStandAloneStorage()** | 打开（或创建）badger 数据库，返回一个 StandAloneStorage 实例 | 构造函数，相当于"开机"前的准备工作 |
| **Start()** | 可能需要做一些初始化（P1 中可能不需要额外操作） | 启动服务 |
| **Stop()** | 调用 badger 的 Close 方法，关闭数据库 | 关闭服务，释放资源 |
| **Write()** | 遍历 batch 里的每个 Modify，判断是 Put 还是 Delete，调用 engine_util 的 `PutCF` / `DeleteCF` 写入 badger | 把一批写操作落地到磁盘 |
| **Reader()** | 用 `badger.Txn` 创建一个事务，基于这个事务返回一个 reader 对象。这个 reader 需要实现 `GetCF`、`IterCF`、`Close` 三个方法 | 创建一个"快照读"，后面的 Get/Scan 都从这个快照里读 |

> **关于 Reader 的实现方式**：你需要自己定义一个新的 struct（比如叫 `StandaloneStorageReader`），给它实现 `GetCF`、`IterCF`、`Close` 三个方法。`Reader()` 方法的职责就是创建并返回这个 struct 的实例。

### 文件二：`kv/server/raw_api.go`（4 处）

这个文件实现服务接口，是"接待客户端请求"的地方。

| 函数 | 要做的事 |
|------|------|
| **RawGet()** | 1. 调 `server.storage.Reader()` 拿到 reader<br>2. 调 `reader.GetCF(req.Cf, req.Key)` 拿到 value<br>3. 如果 key 不存在 value 为 nil，不要报错<br>4. 把 value 填入 response，返回 |
| **RawPut()** | 1. 从请求中取出 Cf、Key、Value<br>2. 构造一个 `storage.Modify{Data: storage.Put{...}}`<br>3. 调 `server.storage.Write(ctx, []storage.Modify{modify})` 写入<br>4. 返回空 response（或 error） |
| **RawDelete()** | 同 RawPut，只是把 `storage.Put` 换成 `storage.Delete` |
| **RawScan()** | 1. 调 `server.storage.Reader()` 拿到 reader<br>2. 调 `reader.IterCF(req.Cf)` 拿到迭代器<br>3. 用 `iter.Seek(startKey)` 定位到起始位置<br>4. 循环遍历，收集 kv 对，注意 `GetLimit()` 限制数量<br>5. 把结果填入 response，返回 |
| | **注意**：Scan 时别忘了在函数结束时调用 `reader.Close()`（用 `defer`） |

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

| 报错 | 可能原因 |
|------|------|
| `undefined: engine_util` | 忘记 import 对应的包 |
| `cannot use ... as Storage` | 漏实现了接口中的某个方法（比如 Close） |
| `nil pointer dereference` | badger 实例是 nil，检查 NewStandAloneStorage 有没有正确初始化 |

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
