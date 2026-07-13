# Project1 代码注释

# Standalone_storage

## （1）为什么要`struct`存 `*badger.DB`？

`StandAloneStorage` 最终要作为 `Storage` 接口传给 `Server`：

```go
// kv/main.go
storage := standalone_storage.NewStandAloneStorage(conf)
server := server.NewServer(storage)
```

`Server` 对存储层的所有操作（`Write`、`Reader`、`Stop`）都是通过 `Storage` 接口调用的。而 `Storage` 接口要求实现四个方法：

```go
// tinykv/kv/storage/storage.go
type Storage interface {
    Start() error
    Stop() error
    Write(ctx, batch) error
    Reader(ctx) (StorageReader, error)
}
```

你必须在某个东西上绑定这些方法，它才算实现了接口。**用 struct，就是为了给它挂方法（类似于c++的成员函数，func后面括号里的代表接收者）**：

```go
type StandAloneStorage struct {
    db *badger.DB   // 存状态：badger 数据库实例
}

func (s *StandAloneStorage) Write(...)  { s.db... }   // 方法：通过 s.db 操作 badger
func (s *StandAloneStorage) Reader(...) { s.db... }
func (s *StandAloneStorage) Stop()      { s.db.Close() }
```

每个方法通过 `s.db` 访问同一个 badger 实例——如果不挂在 struct 上，后续 Write/Reader/Stop 之间就没法共享同一个数据库连接。

> Go 的面向对象：**struct 存状态，方法（带接收者）定义行为。组合起来满足接口。**

## （2）`&StandAloneStorage{db: db}` 是什么语法？

这是 Go 的标准构造写法，一句话完成三个动作：**创建 + 赋值 + 返回指针**。

```go
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
    db := engine_util.CreateDB(conf.DBPath, false)  // 1. 拿到 badger 指针
    return &StandAloneStorage{db: db}               // 2. 创建 struct + 赋值 + 返回指针
}
```

拆解：

```go
StandAloneStorage{db: db}    // 创建 struct 值，{字段名: 值} 直接初始化字段
&StandAloneStorage{db: db}   // 取地址，变成指针（返回值类型要求指针）
```

**为什么前面有 `&`？**

函数签名要求返回 `*StandAloneStorage`（指针），所以必须取地址。C++ 里返回局部变量的指针是 UB（悬空），但 Go 的编译器会自动做逃逸分析——发现你返回了指针，就把这个值分配到堆上，GC 负责清理，完全安全。

**等价写法对比：**

```go
// ✅ Go 首选：一行到位
return &StandAloneStorage{db: db}

// ⚠️ 也可以，但啰嗦
s := &StandAloneStorage{}
s.db = db
return s
```

> 这种 `&Struct{字段: 值}` 是 Go 社区的惯用写法，不是偷懒。

## （3）为什么 `Start()` 什么都不做还得写？

```go
func (s *StandAloneStorage) Start() error {
    return nil
}
```

**原因：Go 接口要求实现全部方法，少一个编译就过不了。**

`Server` 要求的 `Storage` 类型定义了 4 个方法——`Start`、`Stop`、`Write`、`Reader`——你的 struct 必须全部实现，哪怕方法体是空的。

当对`Storage`类型赋值实现了的`StandAloneStorage`类型时，编译时会检查`StandAloneStorage`里有无这四个方法，若有则编译通过，则对`Storage`类型调用这四个方法时实际上调用的就是`StandAloneStorage`里的具体实现

类似 C++ 的纯虚类：

```cpp
class Storage {                    // Go: type Storage interface {
public:
    virtual error Start() = 0;    // Go: Start() error
};
// 继承就必须实现 Start()，哪怕写个空函数
```

go的接口与c++虚函数不同的是：c++的派生类里实现虚函数是需要显式声明继承的基类；而go里面只要两者对上就可以，是隐式的。

**那为什么 badger 不需要 Start？**

badger 的 API 里不存在 "启动" 这一步——`badger.Open()` 返回的就是一个已经打开、立即可读写的实例。TinyKV 框架之所以在接口里留了 `Start()`，是为 P2 的 `RaftStorage` 做准备——Raft 节点需要专门的初始化（起网络监听、启动后台 goroutine 循环），这些不能塞在构造函数里，得留一个独立的启动入口。P1 的 `StandAloneStorage` 没这个需求，所以空实现就行。

## （4）`Reader()` 的三层架构：快照 + 列族翻译 + 存储

`Reader()` 的作用是**拍一张数据库快照，返回一个能对这快照进行列族操作的对象**。

### 调用链路

```text
RawGet("default", "foo")
  → reader.GetCF("default", "foo")              // 上：你的 StandaloneReader
    → engine_util.GetCFFromTxn(txn, "default", "foo")  // 中：engine_util 翻译列族
      → txn.Get("default_foo")                  // 下：badger 的实际磁盘读
```

三层各司其职：

| 层       | 谁                                                     | 做什么                                                  |
| -------- | ------------------------------------------------------ | ------------------------------------------------------- |
| 快照创建 | `Reader()` 调 `s.db.NewTransaction(false)`         | 拍快照，包一层外壳                                      |
| 列族翻译 | `engine_util`（`GetCFFromTxn`、`NewCFIterator`） | `"default"+"foo"` → `"default_foo"`，拼前缀/去前缀 |
| 磁盘存储 | `badger`（`txn.Get`）                              | 真正的数据持久化和检索                                  |

### Reader 需要一个"壳"：StandaloneReader

`Reader()` 返回的不是裸的 `*badger.Txn`，而是包了壳的 `StandaloneReader`。因为 badger 不认识列族，直接暴露出 `txn.Get(key)` 会让上层（raw_api）被迫自己拼 `"default_foo"`——这属于硬编码，扩展性差，操作散落到各个函数里。

所以自定义一个 struct 挂上三个方法，全部内部走 engine_util：

| 方法               | 内部调用                                   | 作用                |
| ------------------ | ------------------------------------------ | ------------------- |
| `GetCF(cf, key)` | `engine_util.GetCFFromTxn(txn, cf, key)` | 单点查询            |
| `IterCF(cf)`     | `engine_util.NewCFIterator(cf, txn)`     | 范围遍历（Scan 用） |
| `Close()`        | `txn.Discard()`                          | 释放快照资源        |

### 为什么不直接返回 `*badger.Txn`？

`Reader()` 完全可以写成：

```go
func (s *StandAloneStorage) Reader(...) (*badger.Txn, error) {
    return s.db.NewTransaction(false), nil
}
```

但这样上层 raw_api 就得直接操作 badger 原始 API：自己拼列族前缀、自己判断 `ErrKeyNotFound`、自己 `Discard`。读逻辑散落在各个 handler 里，存储引擎一换全得改。

所以包一层 `StandaloneReader`，好处：

1. **隐藏底层细节** — 上层只认 `GetCF`/`IterCF`/`Close`，不知道也不关心底下是 badger 还是别的引擎
2. **列族自动处理** — `StandaloneReader` 内部走 engine_util，上层不用操心 `"default_foo"` 前缀
3. **声明周期自动管理** — `Close()` 一把清，不靠上层记得调 `txn.Discard()`
4. **换引擎只换壳** — badger → RocksDB？只改 `StandaloneReader` 内部实现，上层无感

### txn 是快照

`NewTransaction(false)` 创建的是一个**只读事务**——badger 在调用瞬间把数据库状态拍成一张照片，后续所有读操作（GetCF、IterCF）都基于这张照片，不受并发写入干扰。这是 badger 的 MVCC 机制提供的保证。

## （5）`Write()`：Modify 包装与类型拆包

`Write()` 接收一批操作（`[]Modify`），遍历每个 Modify，判断是写入还是删除，调对应的 engine_util 执行。

### Modify：统一包装盒

写和删参数不同（Put 有 Value，Delete 没有），为了放进同一个列表传给 Write，框架用一个 `Modify` struct 包起来：

```go
type Modify struct {
    Data interface{}   // 装 Put 或 Delete，interface{} = 万能盒子
}
type Put struct {
    Key, Value []byte
    Cf         string
}
type Delete struct {
    Key []byte
    Cf  string
}
```

外部构造时，`interface{}` 自动存储类型标签：

```go
Modify{Data: Put{Key: "k", Value: "v", Cf: "default"}}   // 标签 = Put
Modify{Data: Delete{Key: "k", Cf: "default"}}             // 标签 = Delete
```

### 类型 switch 拆盒

Write 遍历 batch，用类型 switch 同时完成**判断 + 转换 + 取值**：

```go
for _, m := range batch {                      // _ = 忽略索引，m = 当前 Modify
    switch data := m.Data.(type) {             // .(type) = 读标签，跳对应 case
    case storage.Put:
        // 进了这里，data 自动是 storage.Put 类型
        engine_util.PutCF(s.db, data.Cf, data.Key, data.Value)
    case storage.Delete:
        // 进了这里，data 自动是 storage.Delete 类型
        engine_util.DeleteCF(s.db, data.Cf, data.Key)
    }
}
```

### 为什么 Reader 有 StandaloneReader 中间层，Write 没有？

Reader 额外包一层，不是偶然，而是一条因果链：

**1. 读操作需要快照一致性 → 事务必须长命**

`GetCF` 和 `IterCF` 可能被多次调用（同一请求里读多个 key，或者 Scan 遍历），必须看到**同一个快照**的数据。如果每次读都开新事务，两个 `GetCF` 可能读到不同版本，Scan 遍历到一半也可能被写入打断。

**2. 事务长命 → 不能用 engine_util 自动管理**

`engine_util.GetCF(db, cf, key)` 内部自己开一个 `db.View`，读完就关。这种事务瞬生瞬灭的模式没法共享快照。只能自己手动 `NewTransaction(false)` 创建 txn，用 `GetCFFromTxn`（你传 txn 进去），最后手动 `Discard`。

**3. 手动管理 txn + 列族隔离 → 需要 StandaloneReader**

裸的 `*badger.Txn` 不认识列族前缀，如果直接暴露给 raw_api，上层就得自己拼 `"default_foo"`，越了层。所以需要一个壳：对内管理 txn 生命周期（创建/释放），对外提供带 CF 的接口（`GetCF`/`IterCF`/`Close`），内部走 engine_util 翻译列族。

**反过来，Write 没有这些需求：**

- 不存在"多个写操作必须共享一个事务"的一致性要求（P1 简化，真正的原子事务放 P4）
- `engine_util.PutCF` 内部 `db.Update` 自动开事务 → 写入 → 提交，一气呵成
- 不需要手动管事务生命周期，也就不需要中间层

| 对比项       | Write                | Reader                           |
| ------------ | -------------------- | -------------------------------- |
| 一致性要求   | 无（P1 简化）        | 快照一致性（多次读共享同一视图） |
| 事务创建     | engine_util 内部自动 | 你手动`NewTransaction`         |
| 事务生命周期 | 单次调用即结束       | 跨多次调用                       |
| txn 归属     | engine_util 管       | 你管（包在 StandaloneReader 里） |

# Raw_api

### (1) RawGet：读一个键

```go
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
    reader, err := server.storage.Reader(nil)    // 拍快照，返回创建的read事务
    if err != nil {
        return nil, err
    }
    defer reader.Close()                         // 函数退出前自动释放快照

    val, _ := reader.GetCF(req.Cf, req.Key)      // 读数据，忽略 error（不存在不是错）

    return &kvrpcpb.RawGetResponse{
        Value:    val,
        NotFound: val == nil,
    }, nil
}
```

**defer 机制**：`defer reader.Close()` 不是立即执行，而是"推迟到函数 return 前执行"。保证快照一定被释放，不论从哪个分支返回。类似 C++ RAII 析构。

**kvrpcpb.RawGetResponse**：这是 proto 文件自动生成的 struct，有 7 个字段但你只需填 2 个：

| 字段         | 填的值           | 含义                            |
| ------------ | ---------------- | ------------------------------- |
| `Value`    | `val`          | 读到的值（nil = 不存在）        |
| `NotFound` | `val == nil`   | 告诉客户端键存不存在            |
| 其余 5 个    | 不填（自动零值） | RegionError、Error 等 P1 用不上 |

### (2) RawPut：写一个键值对（RawDelete同理）

**Modify 构造解析**

```go
storage.Modify{                      // 外层：包装盒
    Data: storage.Put{               // 内层：Put 实例，整个塞进 Data（interface{}）
        Key:   req.Key,              // 字段赋值，从请求中取
        Value: req.Value,
        Cf:    req.Cf,
    },
}
```

- `Modify{...}` 和 `Put{...}` 是嵌套初始化。Go 的 struct 字面量可以无限嵌套
- `Data: storage.Put{...}` — 创建一个 Put 实例，整体赋给 Data。interface{} 接受任何类型
- 字段名与 Modify 并列（Key/Value/Cf）是**错误写法**；正确写法是字段**包在 Put 里面**

注意	`storage.Modify{...}`的storage是包名，而`error := server.storage.Write(...)`的storage是成员名。

`RawPutResponse` 的 proto 定义里没有业务字段，所以空着就行。`RawDelete` 同理。

### (3) RawScan：范围遍历的逐层转发

```go
for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
    key := iter.Item().Key()
    val, _ := iter.Item().Value()
    kvs = append(kvs, &kvrpcpb.KvPair{Key: key, Value: val})
}
```

这一行串联了多层包装，每层各管一件事：

| 调用 | 实际动作 | 哪一层 |
| --- | --- | --- |
| `iter.Seek(key)` | `"default_" + "foo"` 拼前缀，传给 badger 定位 | engine_util |
| `iter.Valid()` | 调 `ValidForPrefix("default_")`，检查当前 key 是否还在列族范围内 | engine_util → badger |
| `iter.Next()` | badger 迭代器移到下一个 key | badger |
| `iter.Item().Key()` | 从 `*badger.Item` 取 key，按 `prefixLen` 截掉 `"default_"` 前缀 | CFItem |
| `iter.Item().Value()` | 直接读取 `*badger.Item` 的 value，不做前缀处理 | CFItem → badger |

**为什么 Seeker / Valid 要拼前缀，但 Key() 要去前缀？**

- 拼前缀：badger 底层存的都是 `"default_foo"` 格式，只有前缀匹配的 key 才是目标列族
- 去前缀：上层只关心原始 key `"foo"`，前缀是实现细节，不应泄露

**for 循环的迭代器三段式**：

```go
for iter.Seek(startKey); iter.Valid(); iter.Next() { ... }
//   └─ 初始化：跳到起点 ──┘ └─ 条件：还在范围内？──┘ └─ 迭代：下一个 ──┘
```

`Item()` 返回的 `*badger.Item` 被包在 `CFItem` 里，Key() 自动去前缀，Value() 直接转发。

**KvPair 的创建与存储**：

```go
kvs = append(kvs, &kvrpcpb.KvPair{Key: key, Value: value})
```

每次循环：创建 `KvPair` 值 → `&` 取地址 → `append` 将指针存入 `kvs` 切片。因为取了地址，Go 自动把 `KvPair` 分配到堆上，生命周期超出单次循环，由 GC 管理。`key` 和 `value` 是 badger 内部从 value log 拷贝出来的 `[]byte`，`Close` 释放快照后数据不受影响。
