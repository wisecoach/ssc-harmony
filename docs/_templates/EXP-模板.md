# EXP-{序号}-{slug}.md — 探索报告模板

> **产出角色**: Designer
> **消费角色**: BOSS, Designer
> **状态**: 固定 `done`
> **前置关联**: DSN 设计前的技术调研

---

```markdown
---
id: EXP-01
title: {探索标题}
type: EXP
status: done
author: Designer
created: YYYY-MM-DD
conclusion: feasible
refs: []
---

## 问题
{要探索什么技术/方案}

例如：
TaskFlow 需要"主管查看团队仪表盘"功能。
需要确定：用轮询还是 WebSocket 来实现数据刷新？

## 方法
{怎么探索的}

例如：
对比两种方案在 TaskFlow 场景下的实现复杂度、用户体验和运维成本。
- 方案 A：前端每 30 秒轮询 GET /api/dashboard/team
- 方案 B：WebSocket 实时推送

## 发现
{关键发现}

| 维度 | 轮询 | WebSocket |
|:-----|:----:|:---------:|
| 实现复杂度 | 低，前端一个 setInterval | 高，需要 ASGI + 前端 WS 客户端 |
| 实时性 | 30s 延迟 | 秒级 |
| 服务端负载 | 每个用户每 30s 一次请求 | 长连接，内存占用 |
| 部署 | 无额外依赖 | 需要 ASGI 服务器（uvicorn） |

## 结论
`feasible` — 轮询方案可行

在 TaskFlow 的场景中（内部系统，10-50 用户，仪表盘数据变化不频繁），
30s 轮询完全满足需求，且实现成本远低于 WebSocket。

## 建议
- 第一版用轮询，未来如果用户量增长或需要即时通知再升级 WebSocket
- 轮询间隔做成可配置项（环境变量）
- 后端做好缓存（仪表盘数据 30s 有效），减少数据库压力
```
