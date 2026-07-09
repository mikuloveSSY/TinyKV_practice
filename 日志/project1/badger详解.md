# badger 详解 —— 面向 P1 任务

---

## 一、什么是 badger？

badger 是一个用纯 Go 语言编写的**嵌入式键值存储引擎**（Embedded Key-Value Database），由 Dgraph 团队开发。

把它拆开理解：

| 概念 | 说明 | 类比 |
|------|------|------|
| **嵌入式** | 不是一个独立运行的数据库服务器（不像 MySQL 需要先启动服务再连接），而是一个**库**，直接 import 到程序里就能用 | 像一个"数据库工具箱"，打开就能用，不需要配服务器 |
| **键值存储** | 数据只按 Key → Value 的方式存取，没有 SQL、没有表、没有 schema | 一个超级大的、持久化的 `map[string][]byte` |
| **本地持久化** | 数据存在本地磁盘文件里，程序关了数据不丢 | 类似往文件里写内容，但读写效率高得多 |
| **事务支持** | 支持 ACID 事务，批量操作要么全成功、要么全失败 | 银行转账：扣钱和加钱必须同时完成 |

---

## 二、badger 核心 API

badger 最基础的三个操作，P1 都会间接用到：

### 2.1 打开数据库

```go
import "github.com/Connor1996/badger"

opts := badger.DefaultOptions
opts.Dir = "/tmp/badger_data"
opts.ValueDir = opts.Dir
db, err := badger.Open(opts)
defer db.Close()
```

### 2.2 写入（读写事务）

```go
err := db.Update(func(txn *badger.Txn) error {
    // 写入键值对
    err := txn.Set([]byte("name"), []byte("张三"))
    // 删除键
    err = txn.Delete([]byte("old_key"))
    return err  // 返回 nil 提交，返回 error 回滚
})
```

`db.Update` 创建一个**读写事务**。回调函数里的所有操作是原子的——要么全部保存到磁盘，要么全部撤销。

### 2.3 读取（只读事务 + 快照）

```go
err := db.View(func(txn *badger.Txn) error {
    item, err := txn.Get([]byte("name"))
    if err == badger.ErrKeyNotFound {
        // key 不存在
        return nil
    }
    if err != nil {
        return err
    }
    val, err := item.ValueCopy(nil)
    // val 就是读取到的值
    return nil
})
```

`db.View` 创建一个**只读事务**。它基于 badger 在调用瞬间的**快照**（snapshot），读到的数据是一致的，不会受同时进行的写操作干扰。

### 2.4 创建手动事务（用于 Reader）

```go
txn := db.NewTransaction(false)   // false = 只读事务
defer txn.Discard()               // 用完后必须 Discard

item, err := txn.Get([]byte("key"))
// ...
```

这是 `db.View` 的"手动版"，P1 的 `Reader()` 方法里需要用这个。

---

## 三、一张图理解 badger 的读写模型

```
┌─────────────────────────────────────────┐
│              你的代码                      │
│                                          │
│  Put(key, value)    Get(key) → value    │
│        │                  ▲              │
│        ▼                  │              │
│  ┌──────────┐      ┌──────────┐         │
│  │ db.Update│      │ db.View  │         │
│  │(读写事务) │      │(只读事务) │         │
│  └────┬─────┘      └────┬─────┘         │
│       │                  │               │
│       ▼                  ▼               │
│  ┌──────────────────────────────┐       │
│  │         badger 引擎           │       │
│  │  ┌────────┐   ┌──────────┐  │       │
│  │  │ 内存表  │ → │ 磁盘 SST │  │       │
│  │  │(MemTable)│   │ (LSM-Tree)│  │       │
│  │  └────────┘   └──────────┘  │       │
│  └──────────────────────────────┘       │
└─────────────────────────────────────────┘
```

- 写入先到内存表，满了再刷到磁盘（所以写入极快）
- 读取先从内存表查，找不到再去磁盘（大部分情况命中内存）

---

## 四、TinyKV 为什么选 badger？

| 原因 | 说明 |
|------|------|
| **纯 Go** | 不需要 CGO，编译部署都简单 |
| **写入快** | LSM-Tree 结构，顺序写磁盘，写性能极高——适合 KV 场景 |
| **天生支持快照** | `db.View` / `db.NewTransaction(false)` 天然提供快照隔离读，P4 事务会用到 |
| **概念与 RocksDB 接近** | TiKV 本身用 RocksDB（C++），badger 可以理解为 Go 版 RocksDB，学习迁移成本低 |

---

## 五、但 badger 有一个问题：不支持列族

badger 只管 `Key → Value`，不知道什么叫"列族"。而 TinyKV 需要三个列族：

| 列族 | 常量 | 作用 |
|------|------|------|
| default | `"default"` | 普通用户数据 |
| write | `"write"` | P4 事务的写记录（MVCC） |
| lock | `"lock"` | P4 事务的锁信息 |

所以就有了 `engine_util`——它的核心思路很简单：

```
给每个 key 前面拼上 "列族名_"
```

举例：
```
你调用： engine_util.PutCF(db, "default", "foo", "hello")
badger 实际存的 key = "default_foo"

你调用： engine_util.PutCF(db, "lock", "foo", "world")
badger 实际存的 key = "lock_foo"
```

同一个逻辑键名 `foo`，在不同列族下，badger 里存的是不同的物理 key，自然就不会冲突。**这就是 engine_util 的全部魔法。**

---

## 六、engine_util API 速查（P1 直接用的）

```go
// ─── 写入（内部调 db.Update）───

engine_util.PutCF(db, cf, key, value)       // 写入键值对
engine_util.DeleteCF(db, cf, key)           // 删除键


// ─── 读取（内部调 db.View）───

engine_util.GetCF(db, cf, key)              // 读取键的值
// ↑ 返回 ([]byte, error)，key不存在返回 nil, nil


// ─── 基于已有事务的读取 ───

engine_util.GetCFFromTxn(txn, cf, key)      // 从已有事务读（Reader 里用这个）


// ─── 创建迭代器（用于 Scan）───

engine_util.NewCFIterator(cf, txn)          // 创建列族迭代器


// ─── 打开数据库 ───

engine_util.CreateDB(path, false)           // 打开/创建 badger 实例
// ↑ 第二个参数 false = KV 引擎，true = Raft 引擎
```

---

## 七、回到 P1：每处代码怎么用 badger/engine_util

### standalone_storage.go

| 位置 | 怎么做 |
|------|------|
| **struct 字段** | 加字段 `db *badger.DB`（存数据库实例）和 `conf *config.Config`（存配置） |
| **NewStandAloneStorage()** | 从 `conf` 取存储路径，调 `engine_util.CreateDB(path, false)` 或 `badger.Open(opts)` 打开数据库，赋给 struct |
| **Start()** | P1 不需要额外操作，返回 nil 即可。打开数据库的动作建议放在 New 里 |
| **Stop()** | 调 `s.db.Close()` |
| **Write()** | 遍历 `batch`，类型断言判断 `Put` / `Delete`，调 `engine_util.PutCF` / `engine_util.DeleteCF`。可以用 `db.Update()` 包成一个事务 |
| **Reader()** | 1. `txn := s.db.NewTransaction(false)` 创建只读事务<br>2. 定义一个新 struct（如 `StandaloneReader`），存 `txn`<br>3. 给它实现 `GetCF`（调 `engine_util.GetCFFromTxn`）<br>4. 给它实现 `IterCF`（调 `engine_util.NewCFIterator`）<br>5. 给它实现 `Close`（调 `txn.Discard()`） |

### raw_api.go

| 函数 | 怎么做 |
|------|------|
| **RawGet()** | `reader.GetCF(req.Cf, req.Key)` 拿 value → 填 response。key 不存在时 value 为 nil，不要当错误 |
| **RawPut()** | 构造 `Modify{Put{Key, Value, Cf}}` → 调 `server.storage.Write(ctx, []Modify{...})` |
| **RawDelete()** | 同上，`Put` 换成 `Delete` |
| **RawScan()** | `reader.IterCF(cf)` 拿迭代器 → `Seek(startKey)` 定位 → 循环收集 kv → `GetLimit()` 控制数量 → `defer reader.Close()` |

---

## 八、参考代码：MemStorage

项目中 `kv/storage/mem_storage.go` 是一个用**内存红黑树**实现 `Storage` 接口的完整示例。它的结构和你要写的完全一致——把红黑树换成 badger 就是你的答案。建议对照着看。

---

## 九、常见坑

| 坑 | 说明 |
|------|------|
| 忘记 `txn.Discard()` | Reader.Close 里必须调，否则内存泄漏，badger 还会打 warning 日志 |
| 忘记关迭代器 | `defer iter.Close()`，不关的话 Reader.Close 时可能会 panic |
| 直接用 badger 原始 API | 别直接调 `txn.Set/Get`，一定要走 `engine_util`，否则列族前缀没加上 |
| key 不存在当错误 | `engine_util.GetCFFromTxn` 如果 key 不存在会返回 `badger.ErrKeyNotFound`，GetCF 需要把这个情况转为 `nil, nil` |
| 迭代器 key 已去前缀 | `NewCFIterator` 返回的 `CFItem.Key()` 返回的是**去掉前缀后的 key**，不需要手动处理 |
