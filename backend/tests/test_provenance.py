"""行级来源链与来源清单测试：采集版本/解析版本固化、版本冲突、复验与导出并发、
历史交付持久化和“文件与来源同时核验后才允许交付”的交付闸门。"""
from __future__ import annotations

import pytest

from app.services import provenance as prov
from app.store import store

BATCH_LOGS = [
    {"body": "2026-02-01T10:00:00|INFO|started"},
    {"body": "2026-02-01T10:01:00|WARN|memory high"},
    {"body": "2026-02-01T10:02:00|INFO|healthy"},
]


def _prepare(pipeline, source_id: str = "prov-src", logs=None):
    """登记 + 两次验证（同一来源两个验证版本）+ 采集解析校验。"""
    pipeline.register_source(source_id, "http", "beijing")
    pipeline.validate_source(source_id)
    pipeline.validate_source(source_id)
    batch = pipeline.start_ingest_batch(source_id, "2026-02-01", logs or BATCH_LOGS)
    batch_no = batch["batch"]["批次编号"]
    pipeline.parse_batch(batch_no)
    pipeline.validate_entities(batch_no)
    pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
    return batch_no


def _export(pipeline, batch_no: str) -> int:
    return pipeline.build_export(f"view-{batch_no}", "csv")["export"]["id"]


class TestRowLineage:
    def test_each_entity_has_row_lineage(self, pipeline):
        batch_no = _prepare(pipeline)
        entities = [p for p in store.rows("parsed_entity") if p["来源批次"] == batch_no]
        assert entities
        for entity in entities:
            base = prov.base_lineage_for_entity(entity["id"])
            assert base is not None
            assert base["来源批次"] == batch_no
            assert base["原始记录ID"] == int(entity["原始记录ID"])
            assert base["采集版本"] == 3
            assert base["解析版本"] >= 1

    def test_lineage_points_to_raw_body(self, pipeline):
        batch_no = _prepare(pipeline)
        base = [row for row in store.rows(prov.ROW_LINEAGE) if row["文件ID"] == ""][0]
        raw = store.find("raw_records", base["原始记录ID"])
        assert raw is not None
        assert prov.stable_digest(raw["原始正文"]) == base["原始正文摘要"]

    def test_source_versions_persist_after_revalidation(self, pipeline):
        batch_no = _prepare(pipeline)
        base = [row for row in store.rows(prov.ROW_LINEAGE) if row["文件ID"] == ""][0]
        pinned = base["来源配置版本ID"]
        # 采集之后再复验一次数据源：历史来源链仍指向采集时的版本。
        pipeline.validate_source("prov-src")
        snapshot = prov.source_version_by_id(pinned)
        assert snapshot is not None
        assert snapshot["版本号"] == base["采集版本"]
        assert snapshot["配置快照"]
        assert prov.base_lineage_for_entity(base["标准化记录ID"])["来源配置版本ID"] == pinned

    def test_version_conflict_flagged_without_rewriting(self, pipeline):
        batch_no = _prepare(pipeline)
        rule = [p for p in store.rows("field_parser") if p["status"] == "已发布"][0]
        pipeline.validate_source("prov-src")          # 来源出现更新的验证版本
        rule["输出字段列表"] = "ts,lvl,msg"            # 解析规则内容更新
        prov.ensure_parser_version(rule)              # 生成新的解析版本

        base = [row for row in store.rows(prov.ROW_LINEAGE) if row["文件ID"] == ""][0]
        ok, notes = prov.check_lineage_consistency(base)
        assert ok
        assert base["版本冲突"] is True
        assert "仍按采集版本" in base["冲突说明"]
        # 冲突提示不改写本行使用的版本。
        assert base["采集版本"] == 3
        assert base["解析版本"] == 1


class TestDeliveryManifest:
    def test_export_freezes_slices_and_requires_verification(self, pipeline):
        batch_no = _prepare(pipeline)
        export_id = _export(pipeline, batch_no)
        manifest = prov.manifest_for_export(export_id)
        assert manifest["交付状态"] == prov.DELIVERY_PENDING
        assert manifest["公开切片数"] == 0
        assert manifest["等待切片数"] == 3
        assert prov.render_delivery_csv(manifest).count("\n") == 1  # 只有表头

        manifest = prov.verify_delivery(export_id)
        assert manifest["交付状态"] == prov.DELIVERY_READY
        assert manifest["核验状态"] == prov.VERIFY_OK
        assert manifest["文件行数"] == 3
        assert set(manifest["分组"]) == {"INFO", "WARN"}

    def test_file_rows_share_the_manifest_source(self, pipeline):
        batch_no = _prepare(pipeline)
        export_id = _export(pipeline, batch_no)
        manifest = prov.verify_delivery(export_id)
        lineage_ids = {
            row["id"] for row in store.rows(prov.ROW_LINEAGE)
            if row["文件ID"] == str(export_id)
        }
        assert {row["lineage_id"] for row in manifest["文件行"]} == lineage_ids
        for file_row in manifest["文件行"]:
            base = store.find(prov.ROW_LINEAGE, file_row["source_lineage_id"])
            assert base is not None
            assert base["文件ID"] == ""
            assert base["原始正文摘要"] == file_row["raw_body_digest"]

    def test_trace_from_any_group_finds_inputs(self, pipeline):
        batch_no = _prepare(pipeline)
        export_id = _export(pipeline, batch_no)
        prov.verify_delivery(export_id)

        trace = prov.delivery_trace(export_id, group="WARN")
        assert len(trace["trace"]) == 1
        item = trace["trace"][0]
        assert item["原始正文一致"] is True
        assert item["标准化摘要一致"] is True
        assert item["原始日志"]["原始正文"].endswith("memory high")
        assert item["采集批次"]["批次编号"] == batch_no
        assert item["采集时来源配置"]["版本标签"] == "v3"
        assert item["解析时规则配置"]["配置快照"]

    def test_trace_by_batch(self, pipeline):
        batch_no = _prepare(pipeline)
        export_id = _export(pipeline, batch_no)
        prov.verify_delivery(export_id)
        trace = prov.delivery_trace(export_id, batch_no=batch_no)
        assert len(trace["trace"]) == 3
        for item in trace["trace"]:
            assert item["采集批次"]["批次编号"] == batch_no
            assert item["采集时来源配置"]["版本标签"] == "v3"
            assert item["解析时规则配置"]["版本标签"] == "v1"

    def test_download_recheck_blocks_tampered_body(self, pipeline):
        batch_no = _prepare(pipeline)
        export_id = _export(pipeline, batch_no)
        prov.verify_delivery(export_id)
        base = [row for row in store.rows(prov.ROW_LINEAGE) if row["文件ID"] == ""][0]
        raw = store.find("raw_records", base["原始记录ID"])
        raw["原始正文"] = "tampered|INFO|changed"

        manifest = prov.recheck_delivery(export_id)
        assert manifest["交付状态"] == prov.DELIVERY_BLOCKED
        assert manifest["异常切片数"] >= 1
        assert manifest["文件行数"] < 3


class TestRevalidationConcurrency:
    def test_replay_suspends_public_slices(self, pipeline):
        batch_no = _prepare(pipeline)
        export_id = _export(pipeline, batch_no)
        prov.verify_delivery(export_id)

        result = pipeline.replay_batch(batch_no, "replayer-v9")
        assert export_id in result["suspended_exports"]
        manifest = prov.manifest_for_export(export_id)
        assert manifest["交付状态"] == prov.DELIVERY_PENDING
        assert manifest["公开切片数"] == 0
        assert manifest["等待切片数"] == 3
        # 等待中的日志仍按自己的版本保留在切片上。
        detail = prov.delivery_detail(export_id)
        assert {row["采集版本"] for row in detail["切片"]} == {3}

    def test_pending_logs_stay_out_of_public_file(self, pipeline):
        batch_no = _prepare(pipeline)
        entities = [p for p in store.rows("parsed_entity") if p["来源批次"] == batch_no]
        pending_id = entities[0]["id"]
        export_id = _export(pipeline, batch_no)
        entities[0]["status"] = "待校验"
        entities[0]["校验状态"] = "待校验"
        prov.mark_revalidating(pending_id)

        manifest = prov.verify_delivery(export_id)
        assert manifest["交付状态"] == prov.DELIVERY_READY
        assert manifest["文件行数"] == 2
        assert manifest["等待切片数"] == 1
        assert all(
            row["parsed_entity_id"] != pending_id for row in manifest["文件行"]
        )

    def test_partial_public_slice_per_group(self, pipeline):
        batch_no = _prepare(pipeline)
        info_entities = [
            p for p in store.rows("parsed_entity")
            if p["来源批次"] == batch_no and "INFO" in p["标准化字段集"]
        ]
        export_id = _export(pipeline, batch_no)

        for entity in info_entities:
            entity["status"] = "待校验"
            entity["校验状态"] = "待校验"
            prov.mark_revalidating(entity["id"])

        manifest = prov.verify_delivery(export_id)
        assert manifest["交付状态"] == prov.DELIVERY_READY
        groups = manifest["分组"]
        assert groups["INFO"]["公开记录数"] == 0
        assert groups["INFO"]["等待记录数"] == 2
        assert groups["WARN"]["公开记录数"] == 1
        assert all(row["group"] != "INFO" for row in manifest["文件行"])


class TestPersistence:
    def test_historical_delivery_keeps_original_config(self, pipeline):
        batch_no = _prepare(pipeline)
        export_id = _export(pipeline, batch_no)
        before = prov.verify_delivery(export_id)
        source_snapshot = [row for row in store.rows(prov.SOURCE_VERSIONS)
                           if row["来源标识"] == "prov-src" and row["版本号"] == 3][0]
        parser_snapshot = store.rows(prov.PARSER_VERSIONS)[-1]

        # 之后配置发生变化，历史清单读取的仍是当时的快照。
        pipeline.validate_source("prov-src")
        rule = [p for p in store.rows("field_parser") if p["status"] == "已发布"][0]
        rule["输出字段列表"] = "a,b,c"
        prov.ensure_parser_version(rule)

        after = prov.delivery_detail(export_id)
        trace = prov.delivery_trace(export_id, group="INFO")
        assert after["文件摘要"] == before["文件摘要"]
        assert after["来源摘要"] == before["来源摘要"]
        item = trace["trace"][0]
        assert item["采集时来源配置"]["配置快照"] == source_snapshot["配置快照"]
        assert item["解析时规则配置"]["配置快照"] == parser_snapshot["配置快照"]
        # 版本冲突被标注出来，但历史交付本身仍可核对。
        assert item["版本冲突"] is True
        assert item["原始正文一致"] is True
        assert item["标准化摘要一致"] is True

    def test_internal_tables_not_listed_as_business_modules(self, pipeline):
        _prepare(pipeline)
        assert "row_lineage" not in store.module_names()
        assert "delivery_manifests" not in store.module_names()
        assert "data_lineage" in store.module_names()

    def test_lineage_snapshot_exposes_delivery_counts(self, pipeline):
        batch_no = _prepare(pipeline)
        export_id = _export(pipeline, batch_no)
        prov.verify_delivery(export_id)
        from app.services import SERVICES
        snapshot = SERVICES["data_lineage"].domain_snapshot()
        assert snapshot["row_lineage"] >= 3
        assert snapshot["deliveries"]["total"] >= 1
        assert snapshot["deliveries"]["ready"] >= 1
        assert snapshot["deliveries"]["public_rows"] >= 3


class TestTwoAccessConfigurations:
    """使用两次接入配置采集日志并刷新分组后，新旧两份统计各自可核对。"""

    def _run_batch(self, pipeline, source_id, window, logs, view_name):
        batch = pipeline.start_ingest_batch(source_id, window, logs)
        assert batch["ok"] is True, batch["message"]
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(
            view_name, "level", "time", "count", batch_no=batch_no)
        return batch, batch_no

    def test_two_configs_two_exports_each_traceable(self, pipeline):
        pipeline.register_source("cfg-src", "http", "beijing")
        pipeline.validate_source("cfg-src")

        batch1, batch_no1 = self._run_batch(
            pipeline, "cfg-src", "2026-03-01", BATCH_LOGS[:2], "view-old")
        assert batch1["batch"]["source_version"] == 2

        # 第二次接入配置：重新验证后再采集，来源版本推进。
        pipeline.validate_source("cfg-src")
        batch2, batch_no2 = self._run_batch(
            pipeline, "cfg-src", "2026-03-02", BATCH_LOGS[1:], "view-new")
        assert batch2["batch"]["source_version"] == 3
        assert batch_no2 != batch_no1

        export_old = pipeline.build_export("view-old", "csv")["export"]["id"]
        export_new = pipeline.build_export("view-new", "csv")["export"]["id"]
        prov.verify_delivery(export_old)
        prov.verify_delivery(export_new)

        old = prov.delivery_detail(export_old)
        new = prov.delivery_detail(export_new)

        # 旧统计只包含第一次接入配置采集的行，新统计只包含第二次。
        assert {row["来源批次"] for row in old["切片"]} == {batch_no1}
        assert {row["来源批次"] for row in new["切片"]} == {batch_no2}
        assert {row["采集版本"] for row in old["切片"]} == {2}
        assert {row["采集版本"] for row in new["切片"]} == {3}

        # 两份文件都能由分组定位到标准化明细、原始正文和采集批次。
        for export_id, batch_no, version in (
            (export_old, batch_no1, 2),
            (export_new, batch_no2, 3),
        ):
            trace = prov.delivery_trace(export_id, batch_no=batch_no)
            assert trace["trace"]
            for item in trace["trace"]:
                assert item["原始正文一致"] is True
                assert item["标准化摘要一致"] is True
                assert item["采集批次"]["批次编号"] == batch_no
                assert item["采集时来源配置"]["版本标签"] == f"v{version}"

        # 旧统计在新配置采集之后仍标注版本冲突，但历史来源保持可核对。
        old_trace = prov.delivery_trace(export_old, batch_no=batch_no1)
        assert all(item["版本冲突"] is True for item in old_trace["trace"])
        assert all(item["采集版本"] == 2 for item in old_trace["trace"])
