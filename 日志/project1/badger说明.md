# badger 详解

## 一、什么是 badger？

badger 是一个用纯 Go 语言编写的**嵌入式键值存储引擎**，由 Dgraph 团队开发。

| 概念 | 说明 | 类比 |
| --- | --- | --- |
| **嵌入式** | 不是一个独立运行的数据库服务器，而是一个库，直接 import 就能用 | 工具箱，打开就能用 |
| **键值存储** | 只按 Key → Value 存取，没有 SQL、没有表结构 | 一个持久化的 `map[string][]byte` |
| **本地持久化** | 数据存在磁盘文件里，程序关了数据还在 | 类似写文件，但效率高得多 |
| **事务支持** | 批量操作要么全成功、要么全失败（ACID） | 银行转账：扣钱加钱必须同时完成 |

## 二、badger 核心 API

### 打开数据库

```go
import "github.com/Connor1996/badger"

opts := badger.DefaultOptions
opts.Dir = "/tmp/badger_data"
opts.ValueDir = opts.Dir
db, err := badger.Open(opts)
defer db.Close()
```

### 读写事务（Write 用）

```go
err := db.Update(func(txn *badger.Txn) error {
    txn.Set([]byte("key"), []byte("value"))
    txn.Delete([]byte("old_key"))
    return nil  // 返回 nil 提交，返回 error 回滚
})
```

`db.Update` 内部自动创建读写事务 → 执行回调 → 提交/回滚 → 释放事务。**事务短命，一次调用即结束。**

### 只读事务（Reader 用）

```go
// 自动版：db.View 创建事务，用完自动释放
err := db.View(func(txn *badger.Txn) error {
    item, err := txn.Get([]byte("key"))
    val, _ := item.ValueCopy(nil)
    return nil
})

// 手动版：自己管事务生命周期（P1 的 Reader 用这个）
txn := db.NewTransaction(false)   // false = 只读
defer txn.Discard()               // 必须调，否则内存泄漏
item, err := txn.Get([]byte("key"))
```

`NewTransaction(false)` 创建快照，后续所有读操作基于同一个快照，不受并发写入干扰。

## 三、事务与迭代器的生命周期

### 迭代器挂在事务下面

```
txn (只读事务)
  ├── iterator → 引用 txn
  └── 规则：txn.Discard() 前，所有 iterator 必须 Close()
```

badger 在 `Txn.Discard()` 里**显式检查**是否有迭代器还引用着这个事务——有则直接 panic（不是内存泄漏慢慢来那种）：

```
panic: Unclosed iterator at time of Txn.Discard.
```

所以关闭顺序必须是：**先 Close 迭代器 → 再 Discard 事务**。和数据库里"先关游标再关连接"一个道理。

### Item 的数据拷贝

```go
item, _ := txn.Get([]byte("key"))
val, _ := item.Value()       // badger 从 value log 拷出一份新的 []byte
val, _ := item.ValueCopy(nil) // 同上但复用已分配的 buffer
```

`Value()` / `ValueCopy()` 返回的是**拷贝**，不是 badger 内部内存的引用。事务 Discard 后数据不受影响。

## 四、badger 读写模型

```
你的代码
  │  Put → db.Update(读写事务)    Get → db.View / NewTransaction(只读快照)
  ▼                                ▼
     ┌──────────────────────┐
     │     badger 引擎       │
     │  内存表 → 磁盘 SST    │
     │   (LSM-Tree 结构)     │
     └──────────────────────┘
```

- 写入先到内存，满了刷磁盘（写入极快）
- 读取先查内存，找不到再查磁盘

## 五、TinyKV 为什么选 badger？

| 原因 | 说明 |
| --- | --- |
| **纯 Go** | 不需要 CGO，编译部署简单 |
| **写入快** | LSM-Tree 结构，适合 KV 场景 |
| **快照读** | 天然提供一致性快照，P4 事务需要 |
| **与 RocksDB 概念接近** | TiKV 本身用 RocksDB，badger 是 Go 版等价物 |

## 六、badger 的局限：不支持列族

badger 只管 `Key → Value`，不知道什么叫列族。而 TinyKV 需要三类数据隔离：

| 列族 | 作用 |
| --- | --- |
| `default` | 用户数据 |
| `write` | 写记录（MVCC，P4 用） |
| `lock` | 事务锁（P4 用） |

所以就有了 `engine_util`——给 key 拼前缀来模拟列族：

```
engine_util.PutCF(db, "default", "foo", "hello")
→ badger 实际存的 key = "default_foo"
```

**engine_util 是 badger 的列族外壳，详见 `engine_util说明.md`。**
