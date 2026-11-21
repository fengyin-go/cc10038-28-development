"""流水线路由：暴露跨模块数据采集流水线的 HTTP 接口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services import provenance
from app.services.pipeline import pipeline

router = APIRouter(prefix="/api/pipeline", tags=["采集流水线"])


class SourceRegisterPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    protocol: str = "http"
    region: str = "default"


class IngestBatchPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)


class ParseBatchPayload(BaseModel):
    batch_no: str = Field(..., min_length=1)
    parser_rule_name: str | None = None


class DedupPayload(BaseModel):
    dedup_key_fields: list[str] = Field(..., min_length=1)
    window: str = "batch"
    retain_strategy: str = "keep_first"


class AggregationPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    dim_field: str = Field(..., min_length=1)
    metric_field: str = "count"
    agg_func: str = "count"
    batch_no: str | None = None


class ExportPayload(BaseModel):
    view_name: str = Field(..., min_length=1)
    export_format: str = "csv"
    filters: dict[str, str] | None = None
    batch_no: str | None = None
    dim_field: str | None = None


class WatermarkPayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    batch_no: str = Field(..., min_length=1)
    watermark_value: str = Field(..., min_length=1)


class ReplayPayload(BaseModel):
    history_batch_range: str = Field(..., min_length=1)
    new_parser_rule: str = Field(..., min_length=1)


class SchemaEvolvePayload(BaseModel):
    version: str = Field(..., min_length=1)
    data_source: str = Field(..., min_length=1)
    field_defs: list[str] = Field(..., min_length=1)
    backward_compatible: bool = True


class FullPipelinePayload(BaseModel):
    source_id: str = Field(..., min_length=1)
    time_window: str = Field(..., min_length=1)
    raw_logs: list[dict[str, Any]] = Field(default_factory=list)
    parser_rule: str | None = None


@router.post("/source/register")
def pipeline_register_source(payload: SourceRegisterPayload) -> dict[str, Any]:
    return pipeline.register_source(payload.source_id, payload.protocol, payload.region)


@router.post("/source/validate")
def pipeline_validate_source(source_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    return pipeline.validate_source(source_id)


@router.post("/batch/start")
def pipeline_start_batch(payload: IngestBatchPayload) -> dict[str, Any]:
    return pipeline.start_ingest_batch(payload.source_id, payload.time_window, payload.raw_logs)


@router.post("/batch/parse")
def pipeline_parse_batch(payload: ParseBatchPayload) -> dict[str, Any]:
    return pipeline.parse_batch(payload.batch_no, payload.parser_rule_name)


@router.post("/batch/recover")
def pipeline_recover_batch(batch_no: str = Query(..., min_length=1)) -> dict[str, Any]:
    return pipeline.recover_batch(batch_no)


@router.post("/dedup/run")
def pipeline_run_dedup(payload: DedupPayload) -> dict[str, Any]:
    return pipeline.run_dedup(payload.dedup_key_fields, payload.window, payload.retain_strategy)


@router.post("/validate")
def pipeline_validate_entities(batch_no: str | None = Query(default=None)) -> dict[str, Any]:
    return pipeline.validate_entities(batch_no)


@router.post("/aggregation/refresh")
def pipeline_refresh_aggregation(payload: AggregationPayload) -> dict[str, Any]:
    return pipeline.refresh_aggregation(
        payload.view_name,
        payload.dim_field,
        payload.metric_field,
        payload.agg_func,
        payload.batch_no,
    )


@router.post("/export/build")
def pipeline_build_export(payload: ExportPayload) -> dict[str, Any]:
    return pipeline.build_export(
        payload.view_name,
        payload.export_format,
        payload.filters,
        payload.batch_no,
        payload.dim_field,
    )


@router.post("/watermark/advance")
def pipeline_advance_watermark(payload: WatermarkPayload) -> dict[str, Any]:
    return pipeline.advance_watermark(payload.source_id, payload.batch_no, payload.watermark_value)


@router.post("/replay/batch")
def pipeline_replay_batch(payload: ReplayPayload) -> dict[str, Any]:
    return pipeline.replay_batch(payload.history_batch_range, payload.new_parser_rule)


@router.post("/schema/evolve")
def pipeline_evolve_schema(payload: SchemaEvolvePayload) -> dict[str, Any]:
    return pipeline.evolve_schema(
        payload.version, payload.data_source, payload.field_defs, payload.backward_compatible
    )


@router.post("/full")
def pipeline_run_full(payload: FullPipelinePayload) -> dict[str, Any]:
    return pipeline.run_full_pipeline(
        payload.source_id, payload.time_window, payload.raw_logs, payload.parser_rule
    )


@router.get("/snapshot")
def pipeline_get_snapshot() -> dict[str, Any]:
    """获取所有模块的数据快照，用于跨模块数据流验证。"""
    from app.services.base import BaseModuleService
    from app.services import SERVICES
    snapshots: dict[str, Any] = {}
    for key, svc in SERVICES.items():
        snapshots[key] = svc.domain_snapshot()
    return {"modules": snapshots}


# ---------- 行级来源链与来源清单 ----------


@router.get("/provenance/versions")
def provenance_versions(
    source_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """查看采集时固化的来源配置版本和解析规则版本。"""
    source_rows = provenance.store.rows(provenance.SOURCE_VERSIONS)
    parser_rows = provenance.store.rows(provenance.PARSER_VERSIONS)
    if source_id:
        source_rows = [row for row in source_rows if row.get("来源标识") == source_id]
    return {"sources": source_rows, "parsers": parser_rows}


@router.get("/provenance/rows")
def provenance_rows(
    batch_no: str | None = Query(default=None),
    source_id: str | None = Query(default=None),
    conflict_only: bool = Query(default=False),
) -> dict[str, Any]:
    """行级来源链：任一分组都能在这里定位到输入明细和原始正文。"""
    rows = [row for row in provenance.store.rows(provenance.ROW_LINEAGE)
            if row.get("文件ID", "") == ""]
    if batch_no:
        rows = [row for row in rows if str(row.get("来源批次")) == batch_no]
    if source_id:
        rows = [row for row in rows if str(row.get("数据源")) == source_id]
    if conflict_only:
        rows = [row for row in rows if row.get("版本冲突")]
    return {"items": rows, "total": len(rows)}


@router.get("/deliveries")
def list_deliveries() -> dict[str, Any]:
    """来源清单列表：每次交付的文件行数、公开/等待/异常切片与交付状态。"""
    items = provenance.list_deliveries()
    return {"items": items, "total": len(items)}


@router.get("/deliveries/{export_id}")
def delivery_detail(
    export_id: int,
    include_pending: bool = Query(default=True),
) -> dict[str, Any]:
    detail = provenance.delivery_detail(export_id, include_pending)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"交付文件 {export_id} 不存在")
    return detail


@router.post("/deliveries/{export_id}/verify")
def verify_delivery(export_id: int) -> dict[str, Any]:
    """文件与来源记录同时核验，核验结果回写来源清单。"""
    manifest = provenance.verify_delivery(export_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"交付文件 {export_id} 不存在")
    return {
        "ok": manifest.get("核验状态") == provenance.VERIFY_OK,
        "message": f"交付文件 {export_id} 核验完成：{manifest.get('交付状态')}",
        "manifest": manifest,
    }


@router.get("/deliveries/{export_id}/trace")
def delivery_trace(
    export_id: int,
    group: str | None = Query(default=None),
    slice_id: str | None = Query(default=None),
    batch_no: str | None = Query(default=None),
) -> dict[str, Any]:
    """由任一分组（或切片/批次）定位输入明细、原始正文和采集时配置。"""
    trace = provenance.delivery_trace(
        export_id, group=group, slice_id=slice_id, batch_no=batch_no)
    if trace is None:
        raise HTTPException(status_code=404, detail=f"交付文件 {export_id} 不存在")
    return trace


@router.get("/deliveries/{export_id}/download")
def download_delivery(export_id: int) -> StreamingResponse:
    """下载交付文件：只公开已核验切片；文件与来源未同时核验时拒绝对外交付。"""
    manifest = provenance.manifest_for_export(export_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"交付文件 {export_id} 不存在")
    # 导出后原始正文或明细可能被修正，下载前重新核对，避免公开已经对不上的切片。
    manifest = provenance.recheck_delivery(export_id) or manifest
    if manifest.get("交付状态") != provenance.DELIVERY_READY:
        raise HTTPException(
            status_code=409,
            detail=(
                f"交付文件 {export_id} 当前为{manifest.get('交付状态')}，"
                "文件与来源尚未同时核验通过，不能对外交付"
            ),
        )
    content = provenance.render_delivery_csv(manifest)
    return StreamingResponse(
        iter([content]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{manifest.get("文件名称")}"',
            "X-Manifest-Id": str(manifest.get("id")),
            "X-File-Digest": str(manifest.get("文件摘要", "")),
            "X-Lineage-Digest": str(manifest.get("来源摘要", "")),
        },
    )