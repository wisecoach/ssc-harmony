# AGENTS — 规范说明

本文只收录 Agent 在本仓库中需要遵守的**规范与规则**，不做项目介绍。项目背景、架构、文档目录等请查阅 `README.md` 与 `docs/`。

## 开发与验证规范

- **禁止本地 `go test` / `make test`**：无本地测试环境，且存在引用旧 API 的陈旧 `*_test.go`（会编进测试二进制导致编译失败），属**预存问题**，不要被误导。
- 本地只做编译/静态验证：
  ```bash
  go build ./ssc/...   # 本地验证入口：编译全部生产代码与改动
  go vet ./ssc/...     # 静态/类型检查；遇 BLS CGo 报错用 grep -v bls.h 过滤
  ```
- **行为验证的唯一方式**：远程部署重编译 binary → 跑实验 → 分析日志。改代码后的验收**永远以远程实验结果为准**，不是本地测试套件。
- **实验授权**：用户已明确授权——在完成某次代码调整后，Agent 可**自行执行下面的「同步代码 + 跑实验」两步流程**做验证，不必每次重复询问。仅在以下情况需先向用户确认：
  - 不确定实验环境当前是否空闲（worker 节点是否被他人占用）、或不确定是否会影响正在运行的任务；
  - 首次在陌生配置 / 新拓扑下跑实验；
  - 用户临时明确要求先确认。
  - 注意：实验会重建并重启整条 worker 区块链网络、清空旧日志，属破坏性操作，务必在确认节点空闲后再跑。

## 同步代码与跑实验（两步流程）

> 用户确认的标准做法：改动代码后自行执行以下两步即可完成远程验证。
> 步骤 2 内部已包含：远程编译、分发 binary 给 worker 节点、控制 worker 启动区块链网络、
> 控制客户端发送实验交易、下载 worker 日志、分析日志产出结果。

1. **同步代码**（在本地仓库 `/mnt/E/gowork/src/github.com/wisecoach/harmony-sscc` 执行）：
   ```bash
   . sync_code.sh && uploadCode zjnu@10.7.95.199
   ```
2. **跑实验**（远程 `zjnu@10.7.95.199`，在
   `~/go/src/github.com/harmony-one/harmony-sscc` 下执行）：
   ```bash
   conda activate cli-py && . auto_test.sh && test_single
   ```

- 实验参数由仓库上级 `.env`（`SHARD_NUM / VALIDATOR / SSC / DELAY / RATE / EXPERIMENT_TYPE / VALIDATOR_PER_NODE` 等）决定；改配置后记得与目标实验一致。
- 跑完的日志与结果目录、以及后续日志查看方式见下方「远程实验环境连接规范」与项目内既有 handoff/脚本（如 `remote_ssc_retry_stats.py`、`ssc_grep.sh`）。
- **部署前务必校验二进制新鲜度**：重编译后确认新改动已进 `bin/harmony`（例如 `nm bin/harmony | grep <新符号>`），避免用陈旧 binary 跑实验得出无效结论（曾踩过：源码已改而 `bin/` 仍是旧 build）。

## 日志规范

- **禁止对 SSC 日志裸 grep**，一律用 `ssc_grep.sh`（SSC）/ `zero_grep.sh`（Harmony 节点）。
- 任何用于计数/统计的日志必须用 **Info** 级别，不要用 Debug。
- 新增需要计数/grep 的目标时，补上对应的 Info 级日志。

## 文档规范

- 修改某系统前，先阅读相关 **active DSN**；完整索引见 `docs/README.md`。
- 以仓库 `docs/` 为**唯一权威来源**，改动前必须读原始 DSN 全文。
- hindsight 记忆库（bank = `harmony-sscc`）只是 `docs/README.md` 的**可检索镜像、非权威**，仅用于快速定位“某设计讲什么/和谁相关”，不可替代原始文档。

## 代码风格规范

- Go 1.22.5，GOPATH 布局。
- 日志格式：结构化 JSON，`utils.SSCLogger().Info().Str("key","val").Msg("message")`。
- 计时用 `defer` + `time.Since(t0)`，需 `import time`。
- 除非被迫，不改 `vendor/`；依赖走 `go.mod`。
- Git：从 `ssc_shared_address_space` 分支切出，提交带描述性前缀。

## 远程实验环境连接规范（SSH 必须按此方式）

- 远程：`zjnu@10.7.95.199 -p 10022`，仅 SSH key 认证、无密码。
- **必须 `-F /dev/null`**：本机 `ssh_config.d` 权限损坏，不带会报 `Bad owner or permissions`。
- **必须显式 `-i ~/.ssh/id_ed25519 -o IdentitiesOnly=yes`**：199 只授权这把 key；不要依赖默认发现，也不要试其他 key。
- 连接可能间歇性不稳：失败/无输出就重试；大批量命令尽量合并成一条远程命令执行。
- SSH / SCP 统一按如下参数：
  ```bash
  SSH_KEY=~/.ssh/id_ed25519
  # SSH
  ssh -F /dev/null -i "$SSH_KEY" -p 10022 -o ConnectTimeout=15 -o BatchMode=yes \
      -o StrictHostKeyChecking=no -o IdentitiesOnly=yes zjnu@10.7.95.199 '<cmd>'
  # SCP 上传/拉取
  scp -F /dev/null -i "$SSH_KEY" -P 10022 -o StrictHostKeyChecking=no -o IdentitiesOnly=yes \
      <local_file> zjnu@10.7.95.199:<remote_path>
  ```
- 验证连通：`ssh ... 'echo AUTH_OK; whoami'` → 期望 `AUTH_OK` + `zjnu`。
- 远程 conda 环境为 `cli-py`；用 SSH 跑命令时注意避免 conda 环境污染（必要时用 `env -i` 方式启动）。
