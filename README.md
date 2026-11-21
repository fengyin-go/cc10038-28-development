# 多源日志采集与解析中心

管理多来源日志接入、字段解析、批量去重和水位推进的数据采集平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8013/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5193/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8013`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源登记 | `source_registry` | 数据源 | 来源标识、接入协议、连接凭证 |
| 采集批次 | `ingest_batch` | 采集批次 | 批次编号、所属数据源、采集时间窗口 |
| 字段解析规则 | `field_parser` | 解析规则 | 规则名称、适用数据源、输入格式 |
| 原始日志记录 | `raw_records` | 日志记录 | 批次编号、原始行号、采集时间戳 |
| 标准化明细 | `parsed_entity` | 标准化记录 | 来源批次、原始记录ID、解析规则版本 |
| 去重引擎 | `dedup_engine` | 去重策略 | 策略名称、去重键字段、去重窗口 |
| 水位管理器 | `watermark_mgr` | 采集水位 | 数据源、最后成功批次、当前水位值 |
| 重试队列 | `retry_queue` | 重试任务 | 失败记录ID、失败原因、重试次数 |
| 格式适配层 | `format_adapter` | 格式适配 | 输入格式名、适配器类型、目标格式 |
| 质量校验规则 | `quality_check` | 校验规则 | 规则名称、校验类型、阈值参数 |
| 归档策略 | `data_archive` | 归档策略 | 策略名称、归档周期、保留时长 |
| 限流控制器 | `rate_limiter` | 限流规则 | 数据源、每秒上限、突发并发 |
| 解析操作日志 | `parse_journal` | 解析日志 | 操作类型、批次编号、执行时间 |
| RPA采集流程 | `rpa_workflow` | 采集流程 | 流程名称、目标系统、触发方式 |
| Schema版本 | `schema_version` | Schema版本 | 版本号、适用数据源、字段定义 |
| 采集告警策略 | `alert_policy` | 告警策略 | 策略名称、监控指标、告警阈值 |
| 数据导出 | `data_export` | 导出任务 | 导出范围、目标格式、过滤条件 |
| 回溯重放引擎 | `replay_engine` | 重放任务 | 历史批次范围、新解析规则版本、重放状态 |
| 聚合查询视图 | `aggregation_view` | 聚合视图 | 视图名称、聚合维度、指标定义 |
| 采集血缘 | `data_lineage` | 血缘关系 | 来源批次、解析规则版本、标准化版本 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 行级来源链与交付来源清单

批次上的“来源配置版本”只说明批次用了哪个版本，不足以证明交付文件里的每一行
来自哪条原始日志。`backend/app/services/provenance.py` 补齐了逐行的来源关系：

- **采集时钉版本**：数据源登记、验证或启动采集时，把接入协议、连接凭证摘要、
  采集区域等真实配置固化成 `source_config_versions` 快照；解析时把解析规则固化
  成 `parser_rule_versions` 快照。后续复验或规则更新不会回写历史行。
- **行级来源链**：每条标准化明细对应一条 `row_lineage`，记录原始记录 ID、批次、
  采集版本、解析版本、原始正文摘要、转换凭证摘要和标准化摘要。
- **来源清单**：导出时把来源链复制成只读切片并生成 `delivery_manifests`，文件
  行只引用同一份切片；核验结果回写清单，并支持由任一分组反查输入明细和原始正文。
- **版本冲突**：补采期间同一来源可能有两个验证版本，解析规则也可能已更新；
  历史行仍按自己的采集/解析版本处理，冲突只在来源清单上标注，不影响核对。
- **并发与等待**：复验会先收回旧的公开切片；导出和核验共用同一把锁，等待核验
  的日志按自己的版本保留在等待切片里，不会进入公开文件。
- **交付闸门**：文件与来源清单必须同时核验通过（`交付状态=可交付`）才允许下载；
  下载前会重新核对一次正文和摘要，不一致立即阻断。

相关接口（前缀 `/api/pipeline`）：

| 接口 | 说明 |
| --- | --- |
| `GET /provenance/versions` | 查看固化的来源配置版本与解析规则版本 |
| `GET /provenance/rows` | 查看行级来源链，可按批次、来源、冲突过滤 |
| `GET /deliveries` | 来源清单列表（公开/等待/异常切片与交付状态） |
| `GET /deliveries/{id}` | 交付明细：分组清单、公开文件行、全部切片 |
| `POST /deliveries/{id}/verify` | 文件与来源同时核验，结果回写来源清单 |
| `GET /deliveries/{id}/trace?group=` | 由分组/切片/批次定位明细、原始正文和当时配置 |
| `GET /deliveries/{id}/download` | 下载文件，仅公开已核验切片；未通过核验返回 409 |

前端“交付来源清单”页面（`/deliveries`）可以生成导出、查看分组、逐组追溯原始
正文，并在核验后下载新旧两份统计进行对照。

领域：数据工程
子方向：数据采集、爬虫与RPA
