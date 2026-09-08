# 公开源码启动与配置

## 1. 启动空环境

需要 Docker Engine、Docker Compose v2 和 Python 3.12。源码目录即 Compose 的工作目录。

```bash
python3 scripts/bootstrap.py
docker compose build
docker compose up -d db
docker compose run --rm app alembic upgrade head
docker compose up -d app
```

浏览器访问 http://127.0.0.1:8780。首次启动页面显示“尚未配置演示业务数据”，头像及“联系客服”可使用；未导入账号前不能创建演示会话。

服务器部署时使用 SSH 隧道访问该本机端口。数据库不发布宿主机端口。端口冲突时修改私有 .env 中的 YXWOOF_PORT。所有运行数据保存在 .runtime；不要提交该目录或 .env。

```bash
docker compose ps
docker compose logs --tail 100 app
docker compose stop
```

停止保留数据。备份时停止应用写入，再使用 pg_dump 输出到仓库外的私有备份目录；恢复前检查目标数据库，勿将备份提交到 GitHub。

## 2. 配置模型

.env.example 仅列出参数名称，实际凭证由部署者填写到本机 .env。后端读取 MODEL_API_KEY；直接运行后端时也支持 MODEL_API_KEY_FILE（文件只包含密钥）。前端不接触凭证。

- MODEL_BASE_URL：接口共同前缀；HTTPS，仅允许本机测试使用 HTTP。
- MODEL_CHAT_NAME、MODEL_EMBEDDING_NAME、MODEL_RERANK_NAME：三个能力对应的模型名称。
- config/runtime.json 的 model_api 对象也可配置地址和名称；非空环境变量优先。
- chat_extra、embedding_extra、rerank_extra：供应商扩展参数，默认空对象；不可覆盖消息、输入、模型、结构化输出或其他受控业务字段。

模型服务必须符合以下现有契约，不是任意供应商即插即用：

| 能力 | POST 路径 | 返回要求 |
|---|---|---|
| 结构化对话 | /chat/completions | 支持 response_format.json_schema；choices[0].message.content 为符合约定的 JSON 字符串 |
| 向量 | /embeddings | data 按 index 对齐，每项 embedding 恰为 1024 维 |
| 重排 | /rerank | results 数组包含文档 index，索引必须在本次候选范围内 |

在 pricing 中配置统一预算单位、输入/缓存输入/输出的每百万 token 单价，以及保守的输入/输出预占单价、向量/重排每调用预留量。预占不得低于计费单价，金额必须有限且非负，向量/重排预留必须大于零。确认后将 configured 设为 true，填写 price_version，最后显式设置 ai_enabled=true。

缺少配置时不派发模型请求。没有可核算用量时保留预留额度，不按零费用释放。向量和重排调用保持费用未知预留；接入方应依据供应商上限设置足够的预留量。默认 AI 关闭，通知关闭，不会自动使用任何已有账号。

配置变更后重建应用容器使环境变量生效：

```bash
docker compose up -d --force-recreate app
```

不要将带有真实地址或账户信息的配置修改提交回仓库。启用模型后的调用费用由所配置的服务提供方结算。

## 3. 接入自己的业务数据

仓库不附带订单、知识库、消费者或评测集。数据库结构见 backend/app/models.py；导入输入契约见 backend/app/import_data.py 的 Bundle。

JSON 顶层必须为 consumers、orders、knowledge 三个数组：

- consumers：id、name。界面仅识别固定演示标识 lin、chen；其他账号不会通过演示入口公开枚举。
- orders：id、consumer_id、merchant_id、merchant_name、sku、product、spec、price_cents、status、ordered_date；可选 delivery_days、version、logistics。金额单位为分；delivered 表示已签收，delivery_days 是当前原型的签收天数事实。物流条目需提供 time、text。
- knowledge：id、merchant_id、sku、title、kind、content、policy_version、valid_from、valid_until；可选 rules、enabled、consumer_visible。有效期使用带时区的 ISO 时间；content 为一段完整 Markdown，最多 8192 字符。每条资料是一段检索单元，正文及结构化规则由提供者维护。
- 资格政策的 kind 为 policy；rules 必须提供非负整数 return_days；完好未使用、原因和明确确认由领域代码强制要求，不通过政策字段关闭。同商户与商品存在冲突政策时，系统拒绝作出资格承诺。
- 不接受导入会话、申请、日志、模型账单或向量；重复主键拒绝，整个批次在一个事务中处理，不覆盖已有记录。

将自行准备的 JSON 放在仓库外。以下命令的 JSON_SOURCE 应指向该文件：

```bash
docker compose run --rm -v "$JSON_SOURCE:/import/business.json:ro" app python -m app.import_data --file /import/business.json
docker compose run --rm app python -m app.retrieval
```

第一条仅导入数据。第二条建立知识向量，会调用已配置的向量服务；须先完成模型与预算设置。未索引资料不生成有据回答。完成后在页面点击“重新检查配置”。

本版本的账号选择器只用于受控原型，不是生产认证系统；接入真实消费者前需要替换为业务方的服务端可信身份认证，保留当前范围授权校验。

## 4. 异常与通知

消费者故障出口统一为“抱歉，服务暂时不可用，请稍后重试。”；帮助入口不依赖模型或数据库。

独立异常日志位于 .runtime/incidents/exceptions.jsonl，待通知事故位于 pending。日志只保留关联标识、错误分类和代码位置，不记录凭证、原始异常消息、SQL 参数或消费者正文。

config/notifications.json 默认 enabled=false、endpoint 为空。需要对接时自行填写 HTTPS 接收地址并开启；接收方必须返回 accepted=true 和匹配的 incident_id。发送携带 Idempotency-Key，失败退避重试，八次失败进入死信。密钥可通过进程环境变量 YXWOOF_NOTIFY_TOKEN 配置，不能写入源码。

```bash
docker compose run --rm app python -m app.notifications
```

通知只通过显式研发命令触发，没有管理员页面，也不会自动发出通知。

## 5. 源码检查

```bash
docker compose run --rm app pytest -q
cd frontend
npm ci
npm test
npm run build
```

公开版测试只验证接口契约、安全退出及无数据启动等可复现行为，不附带原项目的评测集或原始运行结果。README 中的产品演示和历史成绩来自原运行环境。
