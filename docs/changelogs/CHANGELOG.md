# 变更记录

所有值得记录的项目变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

---

## [Unreleased]

### Added
- EXP-01: stateLock 锁争抢分析与优化方案
- 全链路计时埋点：CommitOrRollbackWithProof (commitTx/closeTx)、closeTransaction (stateLock/simCleanup/verCleanup/totalClose)、子函数 (Simulator.Cleanup / Verifier.Cleanup / StaleTx / RemoveOnChainPatch / Timer.RemoveTx / PatchPool.Remove / RemoveFromPassivePool)

### Changed
- 文档目录重构：按 template 结构划分为 designs/active|archived、research/、issues/closed/、briefs/、decisions/、changelogs/

### Fixed
-

### Removed
-

---

## [0.1.0] - 2026-06-21

### Added
- 项目初始化
