"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from app.services import provenance as prov
from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def _freeze_source_version(source_entry: dict[str, Any], trigger: str) -> dict[str, Any]:
    """采集/登记时固化数据源的真实接入配置快照。"""
    snapshot = prov.ensure_source_version(source_entry, trigger=trigger)
    source_entry["source_version"] = snapshot.get("版本号", source_entry.get("source_version", 1))
    source_entry["source_version_id"] = snapshot.get("id")
    source_entry["source_config_digest"] = snapshot.get("配置摘要", "")
    return snapshot


def _source_snapshot_for_raw(raw: dict[str, Any]) -> dict[str, Any]:
    """解析时按原始行自己的采集版本取来源快照，而不是当前最新配置。"""
    version_id = raw.get("source_version_id")
    snapshot = prov.source_version_by_id(version_id)
    if snapshot is not None:
        return snapshot
    snapshot = prov.source_version_row(
        str(raw.get("source_id", "")), raw.get("source_version", 1))
    if snapshot is not None:
        return snapshot
    # 兼容没有版本快照的历史数据：用当前数据源配置补一份快照并把它钉在原始行上。
    for source in store.rows("source_registry"):
        if source.get("来源标识") == raw.get("source_id"):
            snapshot = prov.ensure_source_version(source, trigger="解析补录")
            raw["source_version"] = snapshot.get("版本号")
            raw["source_version_id"] = snapshot.get("id")
            raw["source_config_digest"] = snapshot.get("配置摘要", "")
            raw["source_config_snapshot"] = snapshot.get("配置快照", "")
            return snapshot
    return {
        "id": None,
        "版本号": raw.get("source_version", 1),
        "配置摘要": raw.get("source_config_digest", ""),
        "配置快照": raw.get("source_config_snapshot", ""),
        "连接凭证摘要": "",
        "来源标识": raw.get("source_id", ""),
        "规则名称": "",
    }


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        _freeze_source_version(entry, "登记")
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                # 复验会生成新的来源配置版本；历史批次和来源链仍按采集时版本处理。
                snapshot = _freeze_source_version(s, "验证")
                return {
                    "ok": True,
                    "message": f"数据源 {source_id} 已验证",
                    "entry": s,
                    "source_version": snapshot.get("版本标签"),
                }
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        # 采集时把实际生效的来源配置钉在批次上；之后数据源复验不会改写这份快照。
        # 补采可能在同一来源上留下两个验证版本，这里取的是“此刻正在使用”的那一个。
        source_snapshot = _freeze_source_version(source_entry, "采集")
        with prov.lineage_lock:
            batch_entry = {
                "id": _next_id("ingest_batch"),
                "批次编号": batch_no,
                "所属数据源": source_id,
                "采集时间窗口": time_window,
                "记录总数": str(len(raw_logs) if raw_logs else 0),
                "批次状态": time_window,
                "status": "采集中",
                "pending": True,
                "abnormal": False,
                "batch_version": 1,
                "source_version": source_snapshot.get("版本号", source_entry.get("source_version", 1)),
                "source_version_id": source_snapshot.get("id"),
                "source_version_label": source_snapshot.get("版本标签", ""),
                "source_config_digest": source_snapshot.get("配置摘要", ""),
                "source_config_snapshot": source_snapshot.get("配置快照", ""),
                "started_at": _now_iso(),
            }
            batches.append(batch_entry)

            # 生成原始日志记录
            raw_records_list = store.rows("raw_records")
            created_ids: list[int] = []
            if raw_logs:
                for idx, log in enumerate(raw_logs):
                    raw_id = _next_id("raw_records")
                    record = {
                        "id": raw_id,
                        "批次编号": batch_no,
                        "原始行号": str(idx + 1),
                        "采集时间戳": _now_iso(),
                        "原始正文": log.get("body", str(log)),
                        "解析状态": "未解析",
                        "status": "待解析",
                        "pending": True,
                        "abnormal": False,
                        "batch_version": 1,
                        "source_id": source_id,
                        "source_version": batch_entry["source_version"],
                        "source_version_id": source_snapshot.get("id"),
                        "source_config_digest": source_snapshot.get("配置摘要", ""),
                        "source_config_snapshot": source_snapshot.get("配置快照", ""),
                        "ingested_at": _now_iso(),
                    }
                    raw_records_list.append(record)
                    created_ids.append(raw_id)

            batch_entry["记录总数"] = str(len(created_ids))
            batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}
        # 已经解析过的原始行不再重复解析：重复解析会给同一行生成第二份明细，
        # 交付文件里就会出现两条来源相同但 ID 不同的记录。
        batch_raw = [r for r in batch_raw if r.get("status") != "已解析"]
        if not batch_raw:
            return {"ok": True, "message": f"批次 {batch_no} 的原始日志均已解析",
                    "success": 0, "failure": 0, "parsed_ids": [], "failed_ids": []}

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 解析时钉住规则的真实配置快照；后续规则更新不会回写已产出的明细。
        parser_snapshot = prov.ensure_parser_version(rule, trigger="解析")

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        with prov.lineage_lock:
            for raw in batch_raw:
                raw_id = raw["id"]
                try:
                    # 模拟字段解析：将原始正文按规则打散
                    body = raw.get("原始正文", "")
                    fields = self._parse_body(body, rule)
                    ent_id = _next_id("parsed_entity")
                    entity = {
                        "id": ent_id,
                        "来源批次": batch_no,
                        "原始记录ID": str(raw_id),
                        "解析规则版本": parser_snapshot.get("版本标签", rule.get("规则名称", "unknown")),
                        "解析规则": rule.get("规则名称"),
                        "解析版本": parser_snapshot.get("版本号"),
                        "解析版本ID": parser_snapshot.get("id"),
                        "解析配置摘要": parser_snapshot.get("配置摘要", ""),
                        "解析配置快照": parser_snapshot.get("配置快照", ""),
                        "标准化字段集": json.dumps(fields, ensure_ascii=False),
                        "校验状态": "",
                        "status": "待校验",
                        "pending": True,
                        "abnormal": False,
                        "parser_rule": rule.get("规则名称"),
                        "parser_version": parser_snapshot.get("版本号"),
                        "parser_version_label": parser_snapshot.get("版本标签"),
                        "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                        "parsed_at": _now_iso(),
                        "content_digest": _stable_digest(fields),
                    }
                    # 行级来源链：先钉住原始正文、来源版本和解析版本，再挂到明细上。
                    prov.record_row_lineage(
                        raw, entity, _source_snapshot_for_raw(raw), parser_snapshot)
                    entity["lineage_id"] = prov.base_lineage_for_entity(ent_id).get("id")
                    parsed_entities.append(entity)
                    raw["status"] = "已解析"
                    raw["pending"] = False
                    raw["解析状态"] = "已解析"
                    raw["parsed_entity_id"] = ent_id
                    success_count += 1
                    parsed_ids.append(ent_id)
                except Exception:
                    raw["status"] = "解析失败"
                    raw["解析状态"] = "解析失败"
                    raw["abnormal"] = True
                    failure_count += 1
                    failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                prov.mark_base_lineage(entity["id"], True)
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                prov.mark_base_lineage(entity["id"], False, "标准化明细校验失败")
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
        batch_no: str | None = None,
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。

        指定 batch_no 时视图只统计该批次的输入，这样新旧两份统计各自对应
        一次接入配置采集的日志，下载后可以按各自的来源清单核对。
        """
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}
        member_ids: list[int] = []
        batch_nos: set[str] = set()

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            if batch_no and entity.get("来源批次") != batch_no:
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)
            member_ids.append(int(entity["id"]))
            if entity.get("来源批次"):
                batch_nos.add(str(entity["来源批次"]))

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标字段": metric_field,
            "聚合函数": agg_func,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            # 视图固化自己的成员明细和来源批次，导出时按同一批输入生成来源清单，
            # 避免把视图刷新之后才校验通过的日志带进旧文件。
            "member_entity_ids": member_ids,
            "batch_nos": sorted(batch_nos),
            "refreshed_at": _now_iso(),
            "source_records": len(member_ids),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
        batch_no: str | None = None,
        dim_field: str | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据，并固化行级来源清单。

        导出时把每个输入明细的行级来源链复制成这次交付的切片：文件行和来源清单
        读取同一份切片，等待核验的日志不会被写进公开文件。未指定视图时按
        dim_field（默认取标准化明细的 level 字段）分组。
        """
        with prov.lineage_lock:
            exports = store.rows("data_export")
            lineages = store.rows("data_lineage")

            # 查找目标视图
            aggs = store.rows("aggregation_view")
            view = None
            for a in aggs:
                if a.get("视图名称") == view_name:
                    view = a
                    break

            results_data = view.get("results", {}) if view else {}
            if view:
                dim_field = str(view.get("聚合维度", "") or dim_field or "")
            dim_field = dim_field or "level"
            metric_def = str(view.get("指标定义", "count")) if view else "count"
            metric_field = metric_def.split("(")[-1].rstrip(")") if "(" in metric_def else ""

            targets = self._export_targets(batch_no, filters, view)
            export_id = _next_id("data_export")
            export_entry = {
                "id": export_id,
                "导出范围": view_name,
                "目标格式": export_format,
                "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
                "导出状态": "已完成",
                "文件路径": f"/exports/{view_name}.{export_format}",
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "results": results_data,
                "record_count": len(results_data),
                "source_record_count": len(targets),
                "交付状态": prov.DELIVERY_PENDING,
                "delivery_status": prov.DELIVERY_PENDING,
                "exported_at": _now_iso(),
            }
            exports.append(export_entry)

            # 复制本次交付的行级来源切片。
            for entity in targets:
                base = prov.base_lineage_for_entity(entity["id"])
                if base is None:
                    continue
                fields = prov.json_value(entity.get("标准化字段集"), {})
                group_value = str(fields.get(dim_field, "")) if dim_field else ""
                if not group_value:
                    group_value = str(base.get("分组值", ""))
                prov.clone_lineage_for_export(
                    base, export_id, group_value, dim_field or "分组")
                entity.setdefault("export_ids", []).append(export_id)

            manifest = prov.build_manifest_for_export(
                export_id,
                view_name=view_name,
                export_format=export_format,
                filters=filters,
                dim_field=dim_field or "分组",
                metric_field=metric_field,
                agg_func=metric_def.split("(")[0] if "(" in metric_def else "count",
            )
            export_entry["manifest_id"] = manifest.get("id")
            export_entry["文件摘要"] = manifest.get("文件摘要", "")
            export_entry["来源摘要"] = manifest.get("来源摘要", "")
            export_entry["核验状态"] = manifest.get("核验状态", prov.VERIFY_PENDING)
            export_entry["public_row_count"] = manifest.get("公开切片数", 0)
            export_entry["pending_row_count"] = manifest.get("等待切片数", 0)
            export_entry["file_digest"] = manifest.get("文件摘要", "")
            export_entry["lineage_digest"] = manifest.get("来源摘要", "")

            # 记录血缘：来源清单的摘要和文件摘要进入血缘表，便于从血缘回到清单。
            lineage_entry = {
                "id": _next_id("data_lineage"),
                "来源批次": view_name,
                "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
                "标准化版本": view_name,
                "下游消费标识": f"export-{export_id}",
                "记录时间": _now_iso(),
                "status": "已记录",
                "pending": manifest.get("交付状态") != prov.DELIVERY_READY,
                "abnormal": False,
                "export_id": str(export_id),
                "export_format": export_format,
                "manifest_id": manifest.get("id"),
                "文件摘要": manifest.get("文件摘要", ""),
                "来源摘要": manifest.get("来源摘要", ""),
                "分组数": manifest.get("分组数", 0),
                "切片数": len(manifest.get("切片IDs", [])),
            }
            lineages.append(lineage_entry)

            return {
                "ok": True,
                "message": (
                    f"数据导出 {view_name}.{export_format} 已生成，"
                    "等待文件与来源同时核验"
                ),
                "export": export_entry,
                "lineage": lineage_entry,
                "manifest": manifest,
            }

    def _export_targets(
        self,
        batch_no: str | None,
        filters: dict[str, str] | None = None,
        view: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """选出本次交付的输入明细：只取已校验通过且有行级来源链的明细。

        有聚合视图时以视图刷新时固化的成员明细为准，保证“分组统计”和“文件行”
        来自同一批输入；视图刷新之后才校验通过的日志不会混进旧文件。
        """
        parsed = store.rows("parsed_entity")
        member_ids = None
        if view is not None and "member_entity_ids" in view:
            member_ids = {int(item) for item in view.get("member_entity_ids") or []}
        targets = []
        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            if member_ids is not None and int(entity["id"]) not in member_ids:
                continue
            if batch_no and entity.get("来源批次") != batch_no:
                continue
            if prov.base_lineage_for_entity(entity["id"]) is None:
                continue
            if filters:
                fields = prov.json_value(entity.get("标准化字段集"), {})
                if any(str(fields.get(key, "")) != str(value)
                       for key, value in filters.items()):
                    continue
            targets.append(entity)
        return targets

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        suspended_files: set[int] = set()
        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            batch_no = str(entity.get("来源批次", ""))
            if history_batch_range and batch_no != history_batch_range:
                continue
            # 复验窗口内先收回旧切片：还不能确认新版本对得上时，旧文件不允许继续公开。
            suspended_files.update(
                prov.suspend_deliveries_for_entity(
                    entity["id"], f"复验中：{new_parser_rule} 待核验"))
            prov.mark_revalidating(entity["id"])
            entity["status"] = "待校验"
            entity["校验状态"] = "待校验"
            entity["pending"] = True
            entity["解析规则版本"] = new_parser_rule
            entity["replay_id"] = str(replay_id)
            entity["replayed_at"] = _now_iso()
            replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"
        replay_entry["suspended_exports"] = sorted(suspended_files)

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
            "suspended_exports": sorted(suspended_files),
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            # 已登记的数据源复采时也要先确认仍然可用。
            existing = [s for s in store.rows("source_registry")
                        if s.get("来源标识") == source_id]
            if not existing or existing[0].get("status") != "已验证":
                return {"ok": False, "message": reg["message"]}
            reg = {"ok": True, "message": "数据源已登记并复用", "entry": existing[0]}
        else:
            self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()