"""业务路由汇总：流水线固定路径优先注册，避免被通用资源路由的
`/api/{module}/{entry_id}` 动态路径抢先匹配（例如 /api/pipeline/deliveries）。"""
from __future__ import annotations

from app.routers import pipeline_router, resource

ROUTERS = [pipeline_router, resource]
