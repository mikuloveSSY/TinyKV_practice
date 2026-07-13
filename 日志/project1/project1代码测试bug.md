# Project1 代码测试 Bug 记录

## Bug 1：RawScan 忘记关闭迭代器

### 现象

```text
panic: Unclosed iterator at time of Txn.Discard.
goroutine 110 [running]:
testing.tRunner.func1.2({0xd133c0, 0xedbe50})
        /home/mikulove/anaconda3/envs/tinykv_env/go/src/testing/testing.go:1974 +0x232
testing.tRunner.func1()
        /home/mikulove/anaconda3/envs/tinykv_env/go/src/testing/testing.go:1977 +0x349
panic({0xd133c0?, 0xedbe50?})
        /home/mikulove/anaconda3/envs/tinykv_env/go/src/runtime/panic.go:860 +0x13a
github.com/Connor1996/badger.(*Txn).Discard(0x428285?)
        /home/mikulove/go/pkg/mod/github.com/!connor1996/badger@v1.5.1-0.20220222053432-2d2cbf472c77/transaction.go:422 +0x9a
github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage.(*StandaloneReader).Close(0x3ca56abf2101?)
        /home/mikulove/TinyKV_practice/tinykv/kv/storage/standalone_storage/standalone_storage.go:61 +0x16
github.com/pingcap-incubator/tinykv/kv/server.(*Server).RawScan(0x3ca56aa58140?, {0x106ef56?, 0x3ca56adbbee0?}, 0x3ca56adbbd08)
        /home/mikulove/TinyKV_practice/tinykv/kv/server/raw_api.go:92 +0x344
github.com/pingcap-incubator/tinykv/kv/server.TestRawScan1(0x3ca56ac02008)
        /home/mikulove/TinyKV_practice/tinykv/kv/server/server_test.go:226 +0x6f8
testing.tRunner(0x3ca56ac02008, 0xed4888)
        /home/mikulove/anaconda3/envs/tinykv_env/go/src/testing/testing.go:2036 +0xea
created by testing.(*T).Run in goroutine 1
        /home/mikulove/anaconda3/envs/tinykv_env/go/src/testing/testing.go:2101 +0x4c5
FAIL    github.com/pingcap-incubator/tinykv/kv/server   6.888s
FAIL
make: *** [Makefile:55: project1] Error 1
```

### 原因

`RawScan` 里通过 `reader.IterCF()` 创建了 badger 迭代器，但只写了 `defer reader.Close()`，漏掉了 `defer iter.Close()`。

执行顺序：

1. `defer reader.Close()` 触发 `txn.Discard()`
2. badger 的 `Discard()` 检测到还有迭代器没关闭 → panic

### 教训

使用迭代器必须配对 `defer iter.Close()`。badger 要求在事务 `Discard` 之前关闭所有迭代器，否则直接 panic。这和数据库里"先关游标再关连接"是一个道理。

### 迭代器和事务的生命周期

```text
Reader() 创建 txn（快照）
  │
  ├─ IterCF(...) → 创建迭代器，挂在同一个 txn 下
  │    └─ for { ... }  ← 迭代器活跃中
  │    └─ iter.Close()  ← 迭代器先释放
  │
  └─ reader.Close() → txn.Discard() → 快照释放
```

关闭顺序：**先 iter.Close()，后 reader.Close()**。迭代器内部引用了 txn，txn 一旦 Discard，迭代器的数据来源就断了，badger 选择直接 panic 而不是静默出错。defer 的 LIFO 顺序（后注册的先执行）正好满足：先注册 `defer reader.Close()`，后注册 `defer iter.Close()`，退出时 iter 先关，reader 后关。

### 成功反馈

```text
GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/server -run 1
=== RUN   TestRawGet1
--- PASS: TestRawGet1 (0.88s)
=== RUN   TestRawGetNotFound1
--- PASS: TestRawGetNotFound1 (0.97s)
=== RUN   TestRawPut1
--- PASS: TestRawPut1 (1.01s)
=== RUN   TestRawGetAfterRawPut1
--- PASS: TestRawGetAfterRawPut1 (1.21s)
=== RUN   TestRawGetAfterRawDelete1
--- PASS: TestRawGetAfterRawDelete1 (1.06s)
=== RUN   TestRawDelete1
--- PASS: TestRawDelete1 (1.14s)
=== RUN   TestRawScan1
--- PASS: TestRawScan1 (1.07s)
=== RUN   TestRawScanAfterRawPut1
--- PASS: TestRawScanAfterRawPut1 (1.02s)
=== RUN   TestRawScanAfterRawDelete1
--- PASS: TestRawScanAfterRawDelete1 (1.12s)
=== RUN   TestIterWithRawDelete1
--- PASS: TestIterWithRawDelete1 (1.12s)
PASS
ok      github.com/pingcap-incubator/tinykv/kv/server   10.634s
```
