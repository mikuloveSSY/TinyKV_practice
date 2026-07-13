# engine_util 使用说明

`engine_util` 是 TinyKV 内置的 badger 封装层（`kv/util/engine_util/`），在 badger 原始 API 之上加了**列族前缀**处理。

## 一、核心原理

badger 只认 `Key → Value`，engine_util 给所有 key 自动拼 `cf_` 前缀：

```
你调：engine_util.PutCF(db, "default", "foo", "hello")
内部：badger 存的 key = "default_foo"
```

读取时自动加前缀查 badger，取到结果后去掉前缀返回上层。

## 二、P1 用到的 API

### CreateDB —— 打开数据库（engines.go）

```go
db := engine_util.CreateDB(path, false)
// path: 数据存放目录
// false: KV 引擎；true: Raft 引擎（P2+）
// 内部：创建目录 → badger.Open → 返回 *badger.DB
```

### PutCF —— 写入（util.go）

```go
engine_util.PutCF(db, cf, key, value)
// 内部：db.Update → txn.Set(KeyWithCF(cf, key), value)
// 自动创建读写事务 → 写入 → 提交 → 释放
```

### DeleteCF —— 删除（util.go）

```go
engine_util.DeleteCF(db, cf, key)
// 内部：db.Update → txn.Delete(KeyWithCF(cf, key))
```

### GetCFFromTxn —— 从已有事务读取（util.go）

```go
val, err := engine_util.GetCFFromTxn(txn, cf, key)
// 必须传入 *badger.Txn（由 NewTransaction(false) 创建）
// key 不存在 → err = badger.ErrKeyNotFound（需要转为 nil, nil）
// 内部：txn.Get(KeyWithCF(cf, key)) → item.ValueCopy()
```

### NewCFIterator —— 创建列族迭代器（cf_iterator.go）

```go
iter := engine_util.NewCFIterator(cf, txn)
defer iter.Close()   // 必须先于 txn.Discard() 关闭！
```

返回的 `*BadgerIterator` 是 badger 迭代器的列族外壳：

| 操作 | 内部行为 |
| --- | --- |
| `iter.Seek(key)` | 拼 `cf_` 前缀 → 传给 badger 定位 |
| `iter.Valid()` | 调 badger 的 `ValidForPrefix("cf_")`，检查当前位置还在不在列族范围内 |
| `iter.Next()` | 转发给 badger，移到下一个 key |
| `iter.Item().Key()` | 从 `*badger.Item` 取 key，**截掉 `cf_` 前缀** |
| `iter.Item().Value()` | 直接从 `*badger.Item` 取值（badger 内部从 value log 拷贝） |

## 三、P1 用到的数据结构

### CFItem —— 自动去前缀的 Item 包装

```go
type CFItem struct {
    item      *badger.Item   // badger 原始 Item
    prefixLen int            // 列族前缀长度（如 len("default_") = 8）
}

func (i *CFItem) Key() []byte {
    return i.item.Key()[i.prefixLen:]   // "default_foo" → "foo"
}
```

Key() 自动截掉前缀，Value() 直接转发给 badger。

### BadgerIterator —— 列族迭代器

```go
type BadgerIterator struct {
    iter   *badger.Iterator  // badger 原始迭代器
    prefix string            // 列族前缀，如 "default_"
}
```

Seek / ValidForPrefix 操作自动在用户 key 前拼接 `prefix`。ValidForPrefix 支持**二级前缀过滤**（在列族前缀基础上再追加子前缀），P1 暂不用。

## 四、P1 不需要管的函数

| 函数 | 原因 |
| --- | --- |
| `GetCF(db, cf, key)` | 内部自动创建事务。Reader 场景用 `GetCFFromTxn` 更合适 |
| `GetMeta` / `PutMeta` | Raft 元数据，P2+ 用 |
| `DeleteRange` | 内部工具 |
| `KeyWithCF` | 被其他函数内部调用，不需手动调 |

## 五、列族常量（write_batch.go）

```go
engine_util.CfDefault = "default"   // P1 主要用
engine_util.CfWrite   = "write"     // P4 事务
engine_util.CfLock    = "lock"      // P4 事务
```

## 六、常见坑

| 坑 | 说明 |
| --- | --- |
| 直接用 badger 原始 API | 必须走 engine_util，否则列族前缀没加上 |
| 忘记 `txn.Discard()` | Reader.Close 里必须调，否则内存泄漏 |
| 忘记关迭代器 | badger 要求 txn.Discard 前所有 iterator 必须 Close，否则 panic |
| key 不存在当错误 | `GetCFFromTxn` 返回 `ErrKeyNotFound`，raw_api 里要转为 nil, nil |
| 迭代器 key 已去前缀 | `CFItem.Key()` 已截掉前缀，不需要手动处理 |
