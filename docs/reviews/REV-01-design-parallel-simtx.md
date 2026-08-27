---
id: REV-01
title: DSN-45/46/47 设计评审（并行 SimTx 优化）
type: REV
status: done
author: Evaluator
reviewed: [DSN-45, DSN-46, DSN-47, EXP-07]
verdict: conditional-approve
created: 2026-08-27
refs: [DSN-45, DSN-46, DSN-47, EXP-07, BUG-09, BUG-10, BUG-11, BUG-13]
---

## 评审范围

- DSN-45：SimTx 批量并行验证（leader 先行）
- DSN-46：专用跨分片内部交易池（去 nonce + 进池仲裁分组 + DAG）
- DSN-47：专用内部交易结构（SSCInternalTx）+ 区块承载 + SSCVM 原生
- EXP-07：ChainPatch 机制研究报告
- 对照：DSN-23/26/34/35/37/40/41、BUG-09/10/11/13、当前实现（patchpool.go / retry_scheduler.go / verify.go / worker.go / bodyv2.go）

## 评审结论

**条件通过**：设计方向正确（去 nonce 串行 + 并行 execVerify + 进池仲裁），但需先解决 R1–R2 两个硬性矛盾，并对 R3–R11 逐项明确后才可进入实现。本轮已按决议修正文档并提交（895fb2fb1）。

---

## 必选修改（已解决）

### R1. DSN-45“validator 不动” vs DSN-47 改区块格式 —— 矛盾
- **问题**：45 强调 validator 保持原样，但 47 改 `BodyV2`/RLP，validator 必须能解码新区块。
- **决议**：目标架构为 **DSN-47+46+45 全栈**；DSN-47 是底层改动，**所有节点一起升级**；并行只在 leader 产块侧，validator 复用同一 `ProcessInternal` + 同一区块顺序复算。
- **落点**：DSN-45 §1、§4.5、§4.7；DSN-47 §4.5（R8 影响面清单）。

### R2. DSN-45 用 nonce 取数 vs DSN-46 去 nonce —— 口径冲突
- **问题**：45 Phase 0“按 nonce 取前缀”，46 去 nonce。
- **决议**：全栈下 Phase 0 取内部池 `batches[0]`；nonce 前缀仅作“未启用内部池”的 legacy 回退。
- **落点**：DSN-45 §4.4。

---

## 应解决项（已解决）

### R3. lockState 串行会吃掉并行收益
- 实测 lockState≈2ms ≈ execVerify，若串行只能 ~2x。
- **决议**：Phase 3 跨组并行 lockState（不同组无公共 key，sync.Map 写安全）、组内串行；receipt/gas/root 仍按块序串行装配。
- **落点**：DSN-45 §4.4、§4.6。

### R4. 进池仲裁到出块间锁状态会变
- **决议**：进池=初筛；出块对 `batches[0]` 做一次只读最终复核再打包。
- **落点**：DSN-46 §4.4、§4.6。

### R5 + R10. receipt/gas/state-root 重建没定义
- **决议**：`SSCInternalTx` 走**标准 ApplyTransaction 机制**（产生 receipt、累计 gas、更新 nonce、并入 state root），仅执行入口换成 `SSCVM.ProcessInternal`；收据/gas/root 全复用，不特判。
- **落点**：DSN-45 §4.4.1；DSN-47 §4.6。

### R6. Batch 之间 key 冲突
- **决议**：每块只提案 `batches[0]`（单批，内部全局互不冲突）→ 全部可并行；预算有余再串行提案 `batches[1]`。
- **落点**：DSN-46 §4.4。

### R7. DAG 就绪通知机制 + 索引重复
- **决议**：就绪通知复用 `OnBlockCommitted`（每块有界扫描 dagWaiting）；内部池 `keyIndex` 与 retryScheduler `subscriber`/`keyIndex` **统一为一份**（池持有、retry 引用）。
- **落点**：DSN-46 §4.3、§4.6、§8。

### R8. RLP 区块格式影响面未列全
- **决议**：补全清单——block hash / state root / extblockV2 / P2P / DB / RPC / 跨分片 header / sync，全节点协调升级。
- **落点**：DSN-47 §4.5。

### R9. protobuf 迁移影响签名字节
- **决议**：protobuf 只用于线上/区块载荷；签名仍用独立规范字节（沿用 `CXTSimulation.Bytes()` 语义），与线上编码解耦。
- **落点**：DSN-47 §4.5。

### R11. 二维数组 vs 一维
- **决议**：RLP 用**一维 `[]*SSCInternalTx`**（顺序=执行顺序），类型分桶在内存执行时做；二维列为可选。
- **落点**：DSN-47 §4.3。

---

## 遗留开放项（实现前需确认，不阻塞设计定稿）

| 项 | 说明 | 建议 |
|---|---|---|
| protobuf 迁移具体工作量 | `CXTSimulation` 等 JSON→protobuf 的范围 | 按 R9 只换线上载荷，先做最小集（SimTx/CRTx） |
| 跨分片内部池顺序一致性 | 各分片内部池独立 | 沿用现有跨分片协调，出块以区块顺序为权威 |
| `batches[0]` 容量/上限联动 | 500 上限与 1s 预算的关系 | 实验参数，先按 500+800ms 起测 |

---

## 评分

| 维度 | 评分 | 说明 |
|:-----|:----:|:-----|
| 设计完整性 | 4/5 | 三层职责清晰，边界已收敛 |
| 一致性 | 4/5 | R1/R2 矛盾已解决 |
| 可实施性 | 3/5 | 依赖 protobuf/RLP 迁移，工作量待量化 |
| 风险控制 | 4/5 | 复用历史 BUG-09/10/11 教训，开关默认关 |
| 文档质量 | 4/5 | DSN-45/46/47 + EXP-07 齐全 |

## 评审记录

- 2026-08-27：初评 R1–R11，提交 REV-01
- 文档修正提交：`895fb2fb1`（按 R2–R11 修订）
