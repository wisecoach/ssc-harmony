# REV-{序号}-{slug}.md — 评审报告模板

> **产出角色**: Evaluator
> **消费角色**: Orchestrator, Implementer
> **状态**: 固定 `done`
> **前置关联**: TASK 实现完成后

---

```markdown
---
id: REV-01
title: {评审标题}
type: REV
status: done
author: Evaluator
reviewed: [DSN-01, TASK-001]
verdict: approved
created: YYYY-MM-DD
refs: [DSN-01, TASK-001]
---

## 评审范围
{审了什么}

例如：
- TASK-001：用户注册和登录功能
- 涉及文件：`backend/app/auth/`, `frontend/src/pages/Login.tsx`, `frontend/src/pages/Register.tsx`
- 对照文档：DSN-01 API 设计 §3.3

## 必选修改
{必须修复的问题，否则不通过}

1. **密码强度校验缺失** — `POST /api/auth/register` 接受 "123" 作为密码
   - 建议：最小长度 8 位，至少含字母 + 数字
   - 位置：`backend/app/auth/service.py:28`

2. **Token 未设置过期时间** — JWT 默认无过期，安全风险
   - 建议：设置 `exp` claim 为 24 小时
   - 位置：`backend/app/auth/jwt.py:15`

## 推荐修改
{建议但非必须}

- 登录失败返回 "邮箱或密码错误" 而非分开提示（防枚举攻击）
- 前端密码输入框加"显示/隐藏"切换按钮

## 评分
| 维度 | 评分 | 说明 |
|:-----|:----:|:-----|
| 功能正确性 | 8/10 | 基本功能正常，但缺少边界校验 |
| 设计对齐 | 9/10 | API 端点完全对齐 DSN-01 |
| 代码质量 | 7/10 | 逻辑清晰，但缺少输入校验和错误处理 |
| 安全性 | 6/10 | JWT 无过期 + 弱密码是安全隐患 |

## 结论
**needs-changes** — 2 项必选修改需处理后重新评审。
```
