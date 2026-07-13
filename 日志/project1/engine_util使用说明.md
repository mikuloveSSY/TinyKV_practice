# engine_util 使用说明

`engine_util` 是 TinyKV 内置的 badger 封装层（`kv/util/engine_util/`），在 badger 原始 API 之上加了一层**列族前缀**处理。

## 一、核心原理

badger 只认 `Key → Value`，engine_util 给所有 key 自动拼 `cf_` 前缀来模拟列族：

```
你调：engine_util.PutCF(db, "default", "foo", "hello")
内部：badger 存的 key = "default_foo"
```

读取时同样自动加前缀、去掉前缀，对使用者透明。

## 二、P1 用到的 API

### CreateDB —— 打开数据库

```go
db := engine_util.CreateDB(path, false)
// path: 数据存放目录（从 conf.DBPath 取）
// false: KV 引擎；true: Raft 引擎（P2+）
// 内部：创建目录 → badger.Open → 返回 *badger.DB
```

### PutCF —— 写入

```go
engine_util.PutCF(db, cf, key, value) error
// db:    *badger.DB
// cf:    列族名，P1 一般用 engine_util.CfDefault（即 "default"）
// key:   键，[]byte
// value: 值，[]byte
// 内部：db.Update → txn.Set(前缀+key, value)
```

### DeleteCF —— 删除

```go
engine_util.DeleteCF(db, cf, key) error
// 内部：db.Update → txn.Delete(前缀+key)
```

### GetCFFromTxn —— 从事务读取（Reader 里用）

```go
val, err := engine_util.GetCFFromTxn(txn, cf, key)
// txn: *badger.Txn（只读事务，由 s.db.NewTransaction(false) 创建）
// key 不存在 → err = badger.ErrKeyNotFound（注意：不是 nil！需要处理）
```

### NewCFIterator —— 创建迭代器（Scan 用）

```go
iter := engine_util.NewCFIterator(cf, txn)
defer iter.Close()

for iter.Seek(startKey); iter.Valid(); iter.Next() {
    key := iter.Item().Key()      // 返回的是去掉前缀后的原始 key
    val, _ := iter.Item().Value()
}
```

## 三、P1 不需要管的函数

| 函数                                           | 原因                                                       |
| ---------------------------------------------- | ---------------------------------------------------------- |
| `GetCF(db, cf, key)`                         | 内部自动创建事务再读。Reader 场景用`GetCFFromTxn` 更合适 |
| `GetMeta` / `PutMeta` / `GetMetaFromTxn` | Raft 元数据，P2 之后用                                     |
| `DeleteRange`                                | 内部工具函数                                               |
| `KeyWithCF`                                  | 被 PutCF / GetCF 内部调用，不需手动调                      |

## 四、列族常量

```go
engine_util.CfDefault = "default"   // P1 主要用这个
engine_util.CfWrite   = "write"     // P4 事务用
engine_util.CfLock    = "lock"      // P4 事务用
```

## 五、常见坑

| 坑                     | 说明                                                                           |
| ---------------------- | ------------------------------------------------------------------------------ |
| 直接用 badger 原始 API | 别绕开 engine_util 直接调`txn.Set/Get`，否则列族前缀没加上                   |
| 忘记`txn.Discard()`  | Reader.Close 里必须调，否则内存泄漏                                            |
| 忘记关迭代器           | `defer iter.Close()`，不关会导致 Reader.Close 时 panic                       |
| key 不存在当错误       | `GetCFFromTxn` 在 key 不存在时返回 `ErrKeyNotFound`，需要转为 `nil, nil` |
| 迭代器 key 已去前缀    | `NewCFIterator` 返回的 key 已经去掉了 `cf_` 前缀，不需要手动处理           |
