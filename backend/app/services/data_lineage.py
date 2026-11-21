"""采集血缘领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services import provenance


class DataLineageService(BaseModuleService):
    MODULE = "data_lineage"
    ENTITY = "血缘关系"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["来源批次", "解析规则版本", "标准化版本", "下游消费标识", "记录时间"]
    REQUIRED_FIELDS = ["来源批次", "解析规则版本", "标准化版本"]
    STATUS_ORDER = ["已记录", "待验证", "验证异常"]
    ACTION_RULES = {"验证血缘": "已记录", "标记断裂": "验证异常"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]):
        """手工登记血缘时，从来源清单里补上文件摘要和来源摘要。"""
        entry, missing = super().create_entry(values)
        if entry is not None:
            export_id = str(values.get("export_id") or "").strip()
            manifest = provenance.manifest_for_export(export_id) if export_id else None
            if manifest is not None:
                entry["manifest_id"] = manifest.get("id")
                entry["文件摘要"] = manifest.get("文件摘要", "")
                entry["来源摘要"] = manifest.get("来源摘要", "")
                entry["分组数"] = manifest.get("分组数", 0)
                entry["切片数"] = len(manifest.get("切片IDs", []))
            else:
                entry["pending"] = True
                entry["abnormal"] = True
                entry["核验说明"] = "下游消费标识无法定位到来源清单"
        return entry, missing

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        manifests = provenance.store.rows(provenance.DELIVERY_MANIFESTS)
        snapshot["deliveries"] = {
            "total": len(manifests),
            "ready": sum(1 for row in manifests
                         if row.get("交付状态") == provenance.DELIVERY_READY),
            "pending": sum(1 for row in manifests
                           if row.get("交付状态") == provenance.DELIVERY_PENDING),
            "blocked": sum(1 for row in manifests
                           if row.get("交付状态") == provenance.DELIVERY_BLOCKED),
            "public_rows": sum(int(row.get("公开切片数", 0) or 0) for row in manifests),
        }
        snapshot["row_lineage"] = len(
            [row for row in provenance.store.rows(provenance.ROW_LINEAGE)
             if row.get("文件ID", "") == ""])
        return snapshot
