"""数据源登记领域服务：通用列表/登记之外，验证连通时固化来源配置版本。"""
from __future__ import annotations

from typing import Any

from app.services import provenance
from app.services.base import BaseModuleService


class SourceRegistryService(BaseModuleService):
    MODULE = "source_registry"
    ENTITY = "数据源"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["来源标识", "接入协议", "连接凭证", "采集区域", "状态"]
    REQUIRED_FIELDS = ["来源标识", "接入协议", "连接凭证"]
    STATUS_ORDER = ["待验证", "已验证", "已停用"]
    ACTION_RULES = {"验证连通": "已验证", "停用接入": "已停用"}
    NEGATIVE_ACTIONS = ["停用接入"]

    def create_entry(self, values: dict[str, Any]):
        entry, missing = super().create_entry(values)
        if entry is not None:
            entry["source_version"] = 1
            provenance.ensure_source_version(entry, trigger="登记")
        return entry, missing

    def run_action(self, entry_id: int, action: str):
        entry, message = super().run_action(entry_id, action)
        if entry is not None and action == "验证连通":
            # 页面上点“验证连通”同样要留下真实配置快照，否则后续采集
            # 无法证明用的是哪一份接入配置。版本号由快照生成器统一推进。
            entry["validated_at"] = provenance.now_iso()
            snapshot = provenance.ensure_source_version(
                entry, force_new=True, trigger="验证")
            entry["source_version"] = snapshot.get("版本号", entry.get("source_version", 1))
            entry["source_version_id"] = snapshot.get("id")
            entry["source_config_digest"] = snapshot.get("配置摘要", "")
        return entry, message
