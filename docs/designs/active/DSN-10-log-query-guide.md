# [B01] 日志查询指南

> **阅读顺序**（`[前缀]` 表示阅读顺序）：
> 1. 📄 `docs/A01-sscc-refactor-plan.md` [A01] — SSCC 模块化重构总览
> 2. 📄 `docs/A02-lock-retry-mechanism.md` [A02] — 锁与重试机制
> 3. 📄 `docs/A03-CR-priority-optimization.md` [A03] — CR 提交优先级优化
> 4. 📄 `docs/A04-onchain-retry-limit.md` [A04] — 链上重试次数限制
> 5. 📄 `docs/A05-hotkey-retry-design.md` [A05] — HotKey 链式重试（数据层）
> 6. 📄 `docs/A06-lock-priority-coordination.md` [A06] — 锁优先级协调（协调层）
>
> **运维辅助**（与主线并行的独立文档）：
> - 📄 `docs/B01-log-query-guide.md` [B01] **（本文）** — 日志查询指南
> - 📄 `docs/B02-log-lifecycle.md` [B02] — 交易生命周期日志大全

> 节点：10.7.95.199 | 端口：10022 | 用户：zjnu

---

## 一、SSH 连接

```bash
ssh -p 10022 zjnu@10.7.95.199
```

---

## 二、日志位置与结构

日志在实验节点上，由实验脚本自动收集到各机器。**在本机（服务器）上通过 `ssc_grep.sh` 统一查询所有远程节点的日志。**

### 日志目录

```bash
# 当前实验配置的日志目录
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/
ls
# → 按实验参数命名的子目录，如：
#   shard=4_validator=4_ssc=4_delay=10_rate=100_vpn=4/
#   shard=4_validator=4_ssc=4_delay=10_rate=300_vpn=4/
```

### 子目录结构

每个子目录对应一次实验运行，包含各节点的日志文件：

```
shard=4_validator=4_ssc=4_delay=10_rate=100_vpn=4/
├── ssc-validator-0.log      # 节点 0 日志
├── ssc-validator-1.log      # 节点 1 日志
├── ssc-validator-2.log      # 节点 2 日志
├── ...
├── bootnode.log             # 启动节点日志
└── r.log                    # 汇总日志（可能为空）
```

---

## 三、ssc_grep.sh 脚本用法

**核心用法**：读取 `.env` 配置，自动进入当前配置对应的日志目录，跨所有 `ssc-validator-*.log` 文件 grep。

```bash
cd ~/go/src/github.com/harmony-one/logs/harmony-sscc/

# 基本 grep
bash ssc_grep.sh "关键词"

# 带管道过滤
bash ssc_grep.sh "关键词" | grep "过滤条件"

# 按交易 hash 查
bash ssc_grep.sh "0x84e921b0f873ea7111e66709e281040ee3de04d48e813c420b48d6604e163049" | head -20

# 带上下文看前后行
bash ssc_grep.sh "retry tx blocked" | tail -5

# 统计出现次数
bash ssc_grep.sh "关键词" | wc -l
```

### 当前配置确认

ssc_grep.sh 读取 `../../.env` 决定用哪个日志目录：

```bash
cat ~/go/src/github.com/harmony-one/.env
# → SHARD_NUM=4, VALIDATOR=4, SSC=1, RATE=100, DELAY=20, ...
```

需要换实验配置时，修改 `.env` 中的参数。

---

## 四、关键查询模板

### 4.1 查交易是否卡死

```bash
bash ssc_grep.sh "retry tx blocked" | grep "port.*9120" | head -5
```

返回：
```
conflictKey=0x... holderTx=0x... fromCommitted=true/false conflictType=temp-write/committed-write
```

- `fromCommitted=true` → 链上锁（stateLockManager）挡路
- `fromCommitted=false` → TempLockView 临时锁挡路
- `conflictType=temp-write` → TempLockView 临时写锁
- `conflictType=committed-write` → 链上已提交写锁

### 4.2 查交易的完整生命周期

```bash
# 按阶段依次 grep，确认卡在哪一步
bash ssc_grep.sh "0xHASH" | grep "add a cross shard Tx"        # Phase 1: 入池
bash ssc_grep.sh "0xHASH" | grep "start simulate cx transaction" # Phase 2: 开始模拟
bash ssc_grep.sh "0xHASH" | grep "simulation accomplished"       # Phase 2c: 模拟完成
bash ssc_grep.sh "0xHASH" | grep "begin to verify simulation"    # Phase 4: 链上验证
bash ssc_grep.sh "0xHASH" | grep "send CXTCommitVote"            # Phase 4b: 发投票
bash ssc_grep.sh "0xHASH" | grep "leader close transaction"      # Phase 7: 交易终结
```

### 4.3 查特定 port（节点）的日志

```bash
# port 9120 在 10.7.95.203 上（shard 0 leader）
# port 9000 在 10.7.95.200 上（shard 3 leader）
# port 9080 在 10.7.95.202 上（shard 2 leader）

bash ssc_grep.sh "关键词" | grep "port.*9120"   # 只看 shard 0
bash ssc_grep.sh "关键词" | grep "port.*9000"   # 只看 shard 3
```

### 4.4 查 holderTx（锁持有者）的状态

```bash
# 查 holderTx 是否已 close
bash ssc_grep.sh "0xHOLDER" | grep -E "leader close transaction|commit or rollback with proof|closeTransaction"

# 如果没结果，说明 holderTx 没走到 close，锁可能残留
```

### 4.5 统计卡死规模

```bash
# 全部 retry tx blocked 计数
bash ssc_grep.sh "retry tx blocked" | wc -l

# 按冲突类型统计
bash ssc_grep.sh "retry tx blocked" | grep -o '"conflictType":"[^"]*"' | sort | uniq -c

# 按来源统计（committed vs temp）
bash ssc_grep.sh "retry tx blocked" | grep -o '"fromCommitted":[a-z]*' | sort | uniq -c

# 按 holderTx 统计（找到最占锁的 holder）
bash ssc_grep.sh "retry tx blocked" | grep -o '"holderTx":"0x[^"]*"' | sort | uniq -c | sort -rn | head -10
```

---

## 五、日志字段说明

每条 JSON 日志包含以下常用字段：

| 字段 | 含义 |
|------|------|
| `level` | 日志级别：info / warn / error |
| `port` | 节点端口：9120(shard0)、9122(shard0 validator)、9000(shard3)、9080(shard2) 等 |
| `ip` | 节点 IP |
| `txHash` | 交易哈希 |
| `message` | 日志消息内容 |
| `time` | 时间戳（格式：`2026-06-12T02:58:08.04097249+08:00`）|
| `conflictKey` | 锁冲突的 key |
| `holderTx` | 锁持有者的交易哈希 |
| `fromCommitted` | `true`=链上锁，`false`=TempLockView 临时锁 |
| `conflictType` | 冲突类型：`committed-write` / `temp-write` / `read-after-temp-write` |
| `simulationNum` | 模拟轮次编号 |
| `readSetSize` | 读集大小 |
| `writeSetSize` | 写集大小 |
| `caller` | 代码位置（文件:行号） |

---

## 六、快速诊断流程

发现交易卡死时，按以下顺序排查：

```
1. 统计卡死规模
   bash ssc_grep.sh "retry tx blocked" | wc -l

2. 找到最卡的 holderTx
   bash ssc_grep.sh "retry tx blocked" | grep -o '"holderTx":"0x[^"]*"' | sort | uniq -c | sort -rn | head -3

3. 查 holder 是否 close
   bash ssc_grep.sh "0xHOLDER" | grep -E "leader close transaction|commit or rollback with proof"
   → 有结果 = holder 已 close，但 TempLockView 锁残留
   → 无结果 = holder 本身卡在重试流程里

4. 查 holder 的完整路径
   bash ssc_grep.sh "0xHOLDER" | grep -E "simulation has been started|TempLockView pre-check|retry commit success|failed to get last simulation|retry tx blocked"

5. 判断断点
   - 只到 "TempLockView pre-check failed" → 没进链上
   - 有 "retry commit success" + 无后续 → startReSimulation 卡住
   - 有 "retry commit success" + "recall simulation" → 重试执行中
   - 有 "leader close transaction" → 交易已终结，锁应释放
```

---

*编写于 2026-06-12*
