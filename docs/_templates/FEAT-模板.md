# FEAT-{序号}-{slug}.md — 功能请求模板

> **产出角色**: BOSS
> **消费角色**: Designer
> **状态流转**: `planned → active → done | cancelled`
> **前置关联**: 无（BOSS 直接提出）

---

```markdown
---
id: FEAT-01
title: {功能标题}
type: FEAT
status: planned
priority: P2
proposer: BOSS
scope: frontend
created: YYYY-MM-DD
---

## 需求描述
{要做什么功能}

例如：
任务列表支持拖拽排序。主管在给员工排任务优先级时，
可以直接拖拽任务卡片调整顺序。

## 动机
{为什么要做}

目前任务列表按创建时间排序，主管想调整优先级只能逐个修改
每个任务的 due_date，非常低效。

## 期望效果
- 主管在任务列表页直接拖拽卡片改变顺序
- 排序结果自动保存，无需额外点击"保存"按钮
- 员工看到的是主管排好的顺序

## 优先级理由
{为什么是这个优先级}

P2 — 改善体验但非阻塞性。当前通过 due_date 排序可勉强工作，
但拖拽排序能显著提升主管日常操作效率。

## 技术影响
{如果了解}

可能涉及：
- 前端：引入拖拽库（如 dnd-kit）
- 后端：新增 PATCH /api/tasks/reorder 接口
- 数据库：tasks 表新增 `sort_order` 字段
```
