# DSN-{序号}-{slug}.md — 设计文档模板

> **产出角色**: Designer
> **消费角色**: Implementer, Evaluator
> **状态流转**: `planned → active → done | blocked | cancelled`
> **前置关联**: REQ → DSN

---

```markdown
---
id: DSN-01
title: {设计标题}
type: DSN
status: active
priority: P1
author: Designer
created: YYYY-MM-DD
updated: YYYY-MM-DD
scope: [backend, frontend]
refs: [REQ-01]
---

## 1. 概述
{一句话说清要设计什么}

例如：
为 TaskFlow 任务管理系统设计整体架构，包括前端 SPA、
后端 REST API、数据库模型和部署方案。

## 2. 现状与问题
{当前系统的问题或从零开始的背景}

例如：
项目从零开始。需要一个轻量但可扩展的架构，支持快速迭代。

## 3. 设计方案

### 3.1 技术选型
| 层 | 选择 | 理由 |
|:---|:-----|:-----|
| 前端 | React + TypeScript | 生态丰富，类型安全 |
| 后端 | FastAPI (Python) | 开发效率高，性能足够 |
| 数据库 | PostgreSQL | 用户数据需要关系型 |
| 部署 | Docker Compose | 单机简化运维 |

### 3.2 系统架构
```
Browser ──→ Nginx ──→ React SPA (静态)
               │
               └──→ /api/* ──→ FastAPI ──→ PostgreSQL
```

### 3.3 API 设计
```
POST   /api/auth/register     # 注册
POST   /api/auth/login        # 登录
GET    /api/tasks              # 任务列表（支持筛选）
POST   /api/tasks              # 创建任务
PATCH  /api/tasks/{id}         # 更新任务
DELETE /api/tasks/{id}         # 删除任务
GET    /api/users/{id}/tasks   # 某用户的任务
GET    /api/dashboard/team     # 团队仪表盘（主管）
```

### 3.4 数据模型
```sql
users (id, email, password_hash, role, display_name, created_at)
tasks (id, title, description, status, assignee_id, creator_id,
       due_date, created_at, updated_at)
```

## 4. 备选方案

### 方案 A：React SPA + FastAPI + PostgreSQL（选择）
- 优点：开发速度快，技术栈熟悉，部署简单
- 缺点：SPA 首屏加载较慢，SEO 不友好（内部系统不关心）

### 方案 B：Next.js 全栈
- 优点：SSR 首屏快，前后端一体
- 缺点：项目初期过度工程化，学习曲线高

## 5. 关键决策
- **React 而非 Vue**：团队 React 经验更丰富
- **FastAPI 而非 Django**：轻量，API 专用，OpenAPI 文档自动生成
- **PostgreSQL 而非 SQLite**：虽然初期单机，但用户数据需要完整性约束

## 6. 风险与边界
- 第一版不做 WebSocket 实时推送（用户刷新获取最新状态）
- 不做第三方登录（OAuth），邮箱注册即可
- 不做国际化（i18n），中文即可
```
