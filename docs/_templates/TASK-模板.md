# TASK-{序号}-{slug}.md — 任务模板

> **产出角色**: Orchestrator
> **消费角色**: Implementer
> **状态流转**: `active → done | blocked`
> **前置关联**: DSN → TASK

---

```markdown
---
id: TASK-001
title: {任务标题}
type: TASK
status: active
priority: P1
author: Orchestrator
assigned_to: Implementer
parent: DSN-01
created: YYYY-MM-DD
refs: [DSN-01]
---

## 任务描述
{要做什么，简洁明确}

例如：
实现用户注册和登录功能。
- 后端：POST /api/auth/register 和 POST /api/auth/login
- 前端：注册页 + 登录页 + Token 存储

## 涉及文件
- `backend/app/auth/router.py` — 新增路由
- `backend/app/auth/service.py` — 新增认证逻辑
- `backend/app/models/user.py` — 新增用户模型
- `frontend/src/pages/Login.tsx` — 登录页
- `frontend/src/pages/Register.tsx` — 注册页

## 实现要点
- {关键细节提醒}

例如：
- 密码使用 bcrypt 哈希存储，不存明文
- JWT Token 有效期 24 小时
- 前端登录后将 Token 存 localStorage
- 注册后自动跳转登录页

## 验收条件
- [ ] 用户可用邮箱 + 密码注册
- [ ] 已注册用户可登录获取 Token
- [ ] 错误密码/未注册邮箱返回明确错误信息
- [ ] 前端表单有输入校验（邮箱格式、密码长度 ≥ 6）

## 完成后
通知 Orchestrator → 安排 Evaluator 评审（产出 REV-XX）
```
