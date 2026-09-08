# 第三方组件说明

YxWoof 参考了部分开源实现，使用下列第三方组件构建消费者端 AI 客服。第三方组件版权与许可属于其各自权利人；依赖安装时保留各发行包所附的 LICENSE/NOTICE 文件。

| 组件 | 用途 | 发行包声明 |
|---|---|---|
| FastAPI、SQLAlchemy、Pydantic、LangGraph | 服务、持久化、校验、编排 | MIT |
| pgvector Python 客户端、jieba | 向量访问、中文分词 | MIT |
| rank-bm25 | 词法检索 | Apache-2.0 |
| HTTPX | HTTP 传输 | BSD-3-Clause |
| Psycopg | PostgreSQL 驱动 | LGPL-3.0-only |

前端与其他传递依赖的准确版本及许可信息见 frontend/package-lock.json 和各依赖发行包；后端版本见 backend/requirements.lock。PostgreSQL 与 pgvector 扩展随其容器镜像提供，保留各自的许可声明。

本文只说明第三方依赖，不为 YxWoof 项目自身新增许可证。
