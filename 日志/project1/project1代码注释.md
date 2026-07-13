# Project1 代码注释

## （1）为什么要`struct`存 `*badger.DB`？

`StandAloneStorage` 最终要作为 `Storage` 接口传给 `Server`：

```go
// kv/main.go
storage := standalone_storage.NewStandAloneStorage(conf)
server := server.NewServer(storage)
```

`Server` 对存储层的所有操作（`Write`、`Reader`、`Stop`）都是通过 `Storage` 接口调用的。而 `Storage` 接口要求实现四个方法：

```go
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

`Server` 要求的 `Storage` 接口定义了 4 个方法——`Start`、`Stop`、`Write`、`Reader`——你的 struct 必须全部实现，哪怕方法体是空的。

类似 C++ 的纯虚类：

```cpp
class Storage {                    // Go: type Storage interface {
public:
    virtual error Start() = 0;    // Go: Start() error
};
// 继承就必须实现 Start()，哪怕写个空函数
```

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

### Reader 的"壳"：StandaloneReader

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

### 与 C++ 对比

C++ 用 `std::variant` + `std::visit`，Go 把"类型枚举 + 分发 + 自动转换"内置到了 `switch .(type)` 语法里，少写 `dynamic_cast` 和 null 检查。
