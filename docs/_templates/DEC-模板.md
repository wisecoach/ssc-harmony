# DEC-{序号}-{slug}.md — 决策记录模板

> **产出角色**: Designer
> **消费角色**: 所有人
> **状态**: 固定 `done`（写了就是定稿，不可修改）
> **前置关联**: DSN 设计过程中产生

---

```markdown
---
id: DEC-01
title: {决策标题}
type: DEC
status: done
author: Designer
created: YYYY-MM-DD
decided_at: YYYY-MM-DD
alternatives: [方案A, 方案B, 方案C]
refs: [DSN-01]
---

## 背景
{需要做决策的上下文}

例如：
TaskFlow 需要在 React 和 Vue 之间选择前端框架。
两个框架都能满足需求，需要选择一个作为标准。

## 选项
| 选项 | 优点 | 缺点 |
|------|------|------|
| React 18 | 生态最大，TypeScript 支持好，团队成员熟悉 | 包体积较大 |
| Vue 3 | 上手快，文档全中文，性能好 | 团队经验少，生态略小 |
| Svelte | 编译时框架，运行时极小 | 生态最小，招人困难 |

## 选择
**React 18**

核心理由：
1. 团队 3 人中有 2 人有 React 项目经验，学习成本最低
2. React 生态最丰富（UI 库、状态管理、路由），减少重复造轮子
3. TypeScript 支持成熟，与后端共享类型定义

## 后果
### 正面
- 开发速度有保障，组件库用 Ant Design 开箱即用

### 制约
- 后续新项目也默认 React，除非有强理由变更
- 需要关注 React 19 的 breaking changes 对升级的影响
```
