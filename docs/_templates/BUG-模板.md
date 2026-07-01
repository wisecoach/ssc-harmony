# BUG-{序号}-{slug}.md — 缺陷报告模板

> **产出角色**: Evaluator 或 BOSS
> **消费角色**: Implementer
> **状态流转**: `active → done | blocked`
> **前置关联**: REV 评审过程中发现，或 BOSS 验收时发现

---

```markdown
---
id: BUG-01
title: {缺陷标题}
type: BUG
status: active
priority: P0
reporter: Evaluator
module: {文件路径}
severity: critical
created: YYYY-MM-DD
refs: [DSN-01, REV-01]
---

## 问题描述
{简洁描述}

例如：
用户登录接口 POST /api/auth/login 在连续 5 次错误密码后不返回
429 状态码，而是返回 500 Internal Server Error。

## 复现步骤
1. 启动服务
2. 用任意邮箱 + 错误密码连续请求 POST /api/auth/login 5 次
3. 观察第 5 次响应

## 预期行为
第 5 次应返回 HTTP 429 Too Many Requests，触发限流。

## 实际行为
返回 HTTP 500，服务端日志显示 `KeyError: 'login_attempts'`
（Redis 连接失败时计数器未初始化）。

## 根因分析
{如果已知}

`backend/app/auth/rate_limit.py:42` — 当 Redis 不可用时，
`get_attempts()` 返回 `None` 而非 `0`，导致后续 `attempts + 1` 触发 TypeError。

## 修复方案
{建议的修复}

在 `get_attempts()` 中添加 `return attempts or 0` 兜底逻辑。

## 验证方法
- [ ] 关闭 Redis → 连续请求登录 5 次 → 应返回 429 而非 500
- [ ] 开启 Redis → 正常限流逻辑不受影响
```
