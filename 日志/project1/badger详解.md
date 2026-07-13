# badger 详解

## 一、什么是 badger？

badger 是一个用纯 Go 语言编写的**嵌入式键值存储引擎**，由 Dgraph 团队开发。

| 概念 | 说明 | 类比 |
|------|------|------|
| **嵌入式** | 不是一个独立运行的数据库服务器（不像 MySQL 需要先启动再连接），而是一个库，直接 import 就能用 | 工具箱，打开就能用 |
| **键值存储** | 只按 Key → Value 存取，没有 SQL、没有表结构 | 一个持久化的 `map[string][]byte` |
| **本地持久化** | 数据存在磁盘文件里，程序关了数据还在 | 类似写文件，但效率高得多 |
| **事务支持** | 批量操作要么全成功、要么全失败（ACID） | 银行转账：扣钱加钱必须同时完成 |

## 二、badger 核心 API

P1 会间接用到的三个基础操作：

### 打开数据库

```go
import "github.com/Connor1996/badger"

opts := badger.DefaultOptions
opts.Dir = "/tmp/badger_data"
opts.ValueDir = opts.Dir
db, err := badger.Open(opts)
defer db.Close()
```

### 写入（读写事务）

```go
err := db.Update(func(txn *badger.Txn) error {
    txn.Set([]byte("key"), []byte("value"))   // 写入
    txn.Delete([]byte("old_key"))             // 删除
    return nil  // 返回 nil 提交，返回 error 回滚
})
```

### 读取（只读事务 + 快照）

```go
err := db.View(func(txn *badger.Txn) error {
    item, err := txn.Get([]byte("key"))
    if err == badger.ErrKeyNotFound {
        return nil  // key 不存在
    }
    val, err := item.ValueCopy(nil)
    // val 就是读到的值
    return nil
})
```

`db.View` 基于**快照**读取，读到的数据是一致的，不受并发写入干扰。

### 手动事务（Reader 里用）

```go
txn := db.NewTransaction(false)   // false = 只读
defer txn.Discard()               // 必须释放
item, err := txn.Get([]byte("key"))
```

这是 `db.View` 的手动版，P1 的 `Reader()` 方法里需要用。

## 三、badger 读写模型

```
你的代码
  │  Put(key, value)        Get(key) → value
  ▼                          ▲
db.Update (读写事务)     db.View (只读事务，快照读)
  │                          │
  ▼                          ▼
     ┌──────────────────────┐
     │     badger 引擎       │
     │  内存表 → 磁盘 SST    │
     │   (LSM-Tree 结构)     │
     └──────────────────────┘
```

- 写入先到内存，满了刷磁盘（所以写入极快）
- 读取先查内存，找不到再查磁盘

## 四、TinyKV 为什么选 badger？

| 原因 | 说明 |
|------|------|
| **纯 Go** | 不需要 CGO，编译部署简单 |
| **写入快** | LSM-Tree 结构，适合 KV 场景 |
| **快照读** | 天然提供一致性快照，P4 事务需要 |
| **与 RocksDB 概念接近** | TiKV 本身用 RocksDB，badger 是 Go 版等价物 |

## 五、badger 的局限：不支持列族

badger 只管 `Key → Value`，不知道什么叫列族。而 TinyKV 需要三类数据隔离：

| 列族 | 作用 |
|------|------|
| `default` | 用户数据 |
| `write` | 写记录（MVCC，P4 用） |
| `lock` | 事务锁（P4 用） |

所以就有了 `engine_util`——给 key 拼前缀来模拟列族：

```
engine_util.PutCF(db, "default", "foo", "hello")
→ badger 实际存的 key = "default_foo"

engine_util.PutCF(db, "lock",    "foo", "world")
→ badger 实际存的 key = "lock_foo"
```

**engine_util 是 badger 的列族外壳，详见 `engine_util使用说明.md`。**
