# 本地对象存储引擎（localobjectstore）

纯 Go、零外部依赖的本地对象存储。核心目标是先把**对象分段写入、校验与原子发布**
做对：对象身份、版本与读取入口从第一天起保持稳定，将来加入清理策略或复制时
都不会改变任何已经发布的版本。

## 设计要点

### 稳定的对象身份与版本

- 版本 ID 直接是**全量对象内容的 SHA-256**。版本一经发布，其 ID、字节内容、
  大小永不变；相同内容天然去重。
- blob 以内容寻址存放（`objects/ab/cd/<sha256>`，只读 `0444`），只增不改。
  清理（GC）或复制只会搬运/删除整份不可变 blob，不能重写已发布版本。
- 键（对象名）与版本解耦：`keys/<hex(key)>/pointer` 只记录“当前版本”。旧版本
  仍可用版本 ID 经 `ReadVersion` 读出，覆盖发布不会让老版本消失。

### 分段写入与校验

- `CreateUpload` 创建会话；`PutPart(n, reader)` 逐个写入部件，部件号从 1 开始，
  完成时必须连续。每个部件单独 fsync、记录长度与 SHA-256。
- `CompleteUpload` 先把所有部件拼成 `staged`，逐部件与拼接后整体双重校验，
  再走发布流程。读者在发布前永远看不到部件或半成品。
  - 部件不全（如缺第 2 段）→ `ErrInvalidPart`，不产生任何对象。

### 原子发布与同名并发胜负

- 发布先把 `staged` 硬链接进内容存储并复验，然后用 `临时文件 + fsync + rename`
  原子替换键指针（父目录 fsync），崩溃只会出现“旧版本”或“新版本”，没有中间态。
- 每个键一把跨进程 `flock`（`keys/.../lock`），串行化发布。
- 乐观并发 `IfMatch(expectedVersion)` 明确决定胜负：
  - `IfMatch("")`：仅允许新建，键已存在 → `ErrConflict`。
  - `IfMatch(v)`：仅当当前版本仍是 `v` 才赢，否则 `ErrConflict`。

  同名并发发布因此恰好一个赢家，输家得到确定的冲突错误。

### 崩溃恢复与残片隔离

- 会话状态机：`receiving → assembling → assembled → committed`，状态写入
  `meta.json`（原子写）。
- 每个上传会话持有 `.live` 跨进程文件锁；进程被 kill / 掉电时内核自动释放。
  恢复扫描只处理**没有活锁**的会话，绝不误删其它进程正在写的上传。
- `Open` 时自动执行 `Recover`：
  - `receiving` / `assembling` 且无活锁：崩溃残片，整体移入 `quarantine/`，读者
    始终不可见。
  - `assembled`：校验部件与拼接字节，通过则安装 blob，并按原始 `IfMatch` 条件
    原子补发指针；校验失败则隔离。重启后已完成对象可找回。
  - `committed`：指针与 blob 已是持久事实，只清理可能残留的会话目录。
- 恢复可重复执行（幂等），不会重复发布或重复隔离。

### 读取即校验损坏

- `ReadObject` 返回当前版本，读取过程中边读边算 SHA-256，到 EOF 时比对版本 ID
  与长度；磁盘位腐、截断、篡改 → `ErrCorrupt`。
- `ReadVersion(version)` 读取任意历史不可变版本；`VerifyVersion(version)` 只做
  完整性校验。

## 磁盘布局

```
root/
  store.lock                      打开期恢复串行锁
  objects/ab/cd/<sha256>          不可变、只读、内容寻址的版本 blob
  keys/<hex(key)>/pointer         当前版本指针（tmp+rename 原子替换）
  keys/<hex(key)>/lock            每键跨进程发布锁
  uploads/<id>/
    .live                         活会话 flock（进程死亡自动释放）
    meta.json                     部件清单与状态机
    parts/00000001 ...            已 fsync 的部件
    staged                        校验过的完整拼接结果
  quarantine/<id>-<reason>-<rand> 被隔离的崩溃残片
```

## 用法（Go API）

```go
s, _ := objectstore.Open(ctx, "/var/lib/objstore")

u, _ := s.CreateUpload("photos/1.jpg", objectstore.IfMatch("")) // 新建
_, _ = u.PutPart(1, part1Reader)
_, _ = u.PutPart(2, part2Reader)
v, err := u.CompleteUpload()      // v.Version == 内容 SHA-256

cur, rc, _ := s.ReadObject("photos/1.jpg") // 读时校验损坏
defer rc.Close()
io.Copy(os.Stdout, rc)

old, _ := s.ReadVersion(previousVersionID) // 老版本依旧可读
```

CLI：`go run ./cmd/objstore put -root ./data -key k -file f [-if-match V|!]`，
另有 `get`、`stat`、`get-version`、`verify`、`recover`。

## 测试

`go test ./... -race` 覆盖：

- 小文件（含 1 字节）与多段拼接的往返、版本=内容哈希；
- 部件不连续失败、同部件幂等重传；
- 进程内 8 路与跨 6 进程同名并发发布：恰好 1 个赢家，其余确定冲突；
- 主动中断：子进程在 `after_assemble` 注入 `os.Exit`（模拟掉电，无清理），重启后
  找回已组装对象、隔离另一个写到一半的残片，且二次重启状态稳定；
- 直接篡改 blob 字节后 `VerifyVersion` 与 `ReadObject` 均报 `ErrCorrupt`；
- 中止上传后无任何半成品对象可见。
