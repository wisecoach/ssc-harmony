# ENHANCE-{序号}-{slug}.md — 增强建议模板

> **产出角色**: Designer 或 BOSS
> **消费角色**: Implementer
> **状态流转**: `planned → active → done | cancelled`
> **前置关联**: 通常在 DSN 实现之后提出

---

```markdown
---
id: ENHANCE-01
title: {增强标题}
type: ENHANCE
status: planned
priority: P3
proposer: Designer
scope: backend
created: YYYY-MM-DD
refs: [DSN-01]
---

## 现状
{当前有什么不足}

例如：
仪表盘查询 `GET /api/dashboard/team` 每次请求都执行 4 表 JOIN，
随着任务量增长，响应时间从 200ms 上升到 1.2s。

## 建议
{怎么改进}

引入 Redis 缓存仪表盘聚合数据，设置 60s TTL。
主管查看时命中缓存直接返回，避免重复 JOIN。

## 收益
{改完有什么好处}

- 仪表盘响应时间从 1.2s 降到 <10ms（缓存命中时）
- 数据库负载降低约 80%（仪表盘是最高频的查询）

## 代价
{改动成本估计}

- 新增 Redis 依赖（Docker Compose 加一个 service）
- 缓存失效逻辑：任务状态变更时需主动 invalidate
- 估计改动量：后端 ~50 行，部署配置 ~10 行
```
