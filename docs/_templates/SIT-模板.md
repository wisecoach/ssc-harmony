# SIT-{序号}-{slug}.md — 现状审计模板

> **产出角色**: Designer（项目接管时一次性产出）
> **消费角色**: 所有人
> **状态**: 固定 `done`（一次性，后续不修改）
> **前置关联**: 无

---

```markdown
---
id: SIT-01
title: {项目名} 代码现状审计
type: SIT
status: done
author: Designer
created: YYYY-MM-DD
project: {项目名}
audited_at: YYYY-MM-DD
---

## 概况

| 字段 | 值 |
|------|-----|
| 项目 | TaskFlow |
| 语言 | Python (backend), TypeScript (frontend) |
| 框架 | FastAPI, React |
| 数据库 | PostgreSQL |
| 代码规模 | 约 8,000 行 |
| 目录结构 | `backend/`, `frontend/`, `docker/` |

## 模块清单

| 模块 | 路径 | 说明 | 可动性 |
|:-----|:-----|:-----|:------:|
| 用户认证 | `backend/app/auth/` | JWT 登录注册 | ✅ |
| 任务 CRUD | `backend/app/tasks/` | 核心业务 | ⚠️ 小心 |
| 仪表盘 | `backend/app/dashboard/` | 聚合查询 | ✅ |
| 前端页面 | `frontend/src/pages/` | React 路由页面 | ✅ |
| 前端组件 | `frontend/src/components/` | 可复用 UI 组件 | ✅ |
| 数据库迁移 | `backend/migrations/` | Alembic | ❌ 不动 |

## 关键发现
- {重要的发现，好的和坏的}

例如：
- ✅ 后端 API 有完整的 OpenAPI 文档（`/docs`）
- ✅ 前端使用 TypeScript，类型覆盖率高
- ⚠️ 缺少自动化测试（无 pytest / jest 配置）
- ⚠️ `dashboard/` 聚合查询性能差，单个 SQL 关联 4 张表
- ❌ `.env` 文件被提交到 Git，含数据库密码

## 风险点
- {已知风险}

例如：
- 数据库密码泄露风险（Git 历史中存在明文密码）
- 缺少 CI/CD，部署依赖手动操作

## 开放问题
- {未解决的问题}

例如：
- 原开发者离职，无交接文档
- Docker Compose 配置中端口硬编码，可能与宿主机冲突
```
