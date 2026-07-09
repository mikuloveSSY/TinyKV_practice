# Project1 StandaloneKV

在本项目中，你将构建一个支持列族（column family）的独立键值存储 gRPC 服务。独立（Standalone）意味着只有单个节点，而非分布式系统。列族（下文简称 CF）是一个类似键命名空间的概念，即不同列族中相同键的值是不同的。你可以简单地将多个列族视为多个独立的迷你数据库。它用于在 Project4 中支持事务模型，届时你将明白为什么 TinyKV 需要 CF 的支持。

该服务支持四种基本操作：Put/Delete/Get/Scan。它维护一个简单的键值对数据库。键和值都是字符串。`Put` 替换数据库中指定 CF 下某个键的值，`Delete` 删除指定 CF 下某个键的值，`Get` 获取指定 CF 下某个键的当前值，`Scan` 获取指定 CF 下一系列键的当前值。

该项目可分为两步，包括：
1. 实现一个独立的存储引擎。
2. 实现原始的键值服务处理函数。

### 代码

gRPC 服务器在 `kv/main.go` 中初始化，它包含一个 `tinykv.Server`，该 Server 提供了一个名为 `TinyKv` 的 gRPC 服务。该服务由 `proto/proto/tinykvpb.proto` 中的 protocol-buffer 定义，rpc 请求和响应的细节定义在 `proto/proto/kvrpcpb.proto` 中。

通常，你不需要修改 proto 文件，因为所有必要字段都已经为你定义好了。但如果你仍然需要修改，你可以修改 proto 文件并运行 `make proto` 来更新 `proto/pkg/xxx/xxx.pb.go` 中相关的生成 Go 代码。

此外，`Server` 依赖于一个 `Storage` 接口，这是你需要为独立存储引擎实现的接口，位于 `kv/storage/standalone_storage/standalone_storage.go` 中。一旦在 `StandaloneStorage` 中实现了 `Storage` 接口，你就可以用它来实现 `Server` 的原始键值服务。

#### 实现独立存储引擎

第一个任务是封装 badger 键值 API。gRPC 服务器的服务依赖于 `Storage` 接口，该接口定义在 `kv/storage/storage.go` 中。在此上下文中，独立存储引擎只是 badger 键值 API 的一个封装，通过以下两个方法提供：

```go
type Storage interface {
    // 其他内容
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

`Write` 应提供一种将一系列修改应用到内部状态（在此情况下即 badger 实例）的方法。

`Reader` 应返回一个 `StorageReader`，该 reader 支持在快照上进行键值对的点查询和范围扫描操作。

你现在不需要考虑 `kvrpcpb.Context`，它会在后续项目中使用。

> 提示：
>
> - 你应该使用 `badger.Txn` 来实现 `Reader` 函数，因为 badger 提供的事务处理器可以提供键和值的一致性快照。
> - Badger 不支持列族。engine_util 包（`kv/util/engine_util`）通过在键前添加前缀来模拟列族。例如，属于特定列族 `cf` 的键 `key` 会被存储为 `${cf}_${key}`。它封装了 `badger` 以提供带 CF 的操作，并且还提供了许多有用的辅助函数。因此，你应该通过 `engine_util` 提供的方法来进行所有读写操作。请阅读 `util/engine_util/doc.go` 了解更多信息。
> - TinyKV 使用了 badger 原始版本的一个 fork，包含一些修复，因此请使用 `github.com/Connor1996/badger` 而非 `github.com/dgraph-io/badger`。
> - 不要忘记对 badger.Txn 调用 `Discard()`，并在丢弃之前关闭所有迭代器。

#### 实现服务处理函数

本项目的最后一步是使用已实现的存储引擎来构建原始键值服务处理函数，包括 RawGet / RawScan / RawPut / RawDelete。处理函数已经为你定义好了，你只需要在 `kv/server/raw_api.go` 中填充实现。完成后，记得运行 `make project1` 来通过测试套件。
