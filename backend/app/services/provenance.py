"""行级来源链、来源清单与交付核验。

批次记录“采集时使用的来源配置版本”只能说明批次级别用了哪个版本；从采集、
解析到交付文件之间仍然缺少可以逐行核对的来源关系。本模块补齐这条链路：

- 采集时钉住来源配置快照，解析时钉住解析规则快照；
- 每条标准化明细都生成一条行级来源链，记录原始记录、批次、来源版本、解析版本、
  原始正文摘要和转换凭证摘要；
- 导出文件时按分组把行级来源链固化成来源清单，文件行只引用同一份来源链；
- 核验结果回写来源清单，并支持由任一分组反查输入明细和原始正文；
- 复验和导出并发时，导出只公开已经核实的切片，等待中的日志仍按自己的版本处理；
- 文件和来源清单必须同时核验通过才允许对外交付。

所有数据都写回 store 中的持久表，重新进入历史交付时读取的是当时的快照，
而不是当前最新配置。
"""
from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import threading
import time
from typing import Any

from app.store import store

ROW_LINEAGE = "row_lineage"
SOURCE_VERSIONS = "source_config_versions"
PARSER_VERSIONS = "parser_rule_versions"
DELIVERY_MANIFESTS = "delivery_manifests"

VERIFY_PENDING = "待核验"
VERIFY_OK = "已核验"
VERIFY_FAILED = "核验异常"

DELIVERY_READY = "可交付"
DELIVERY_PENDING = "待核验"
DELIVERY_BLOCKED = "交付阻断"

# 复验、导出和核验共同使用一把可重入锁：同一时刻只有一个动作能观察并固化
# “哪些切片已经核实”，从而避免把核验中的切片写进公开文件。
lineage_lock = threading.RLock()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def stable_digest(data: Any) -> str:
    serialized = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def content_digest(data: Any) -> str:
    return stable_digest(data)


def json_text(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)


def json_value(raw: Any, default: Any) -> Any:
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw or "")
    except (TypeError, ValueError):
        return default


def next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def version_label(version: Any) -> str:
    text = str(version or "").strip()
    if not text:
        return ""
    return text if text.lower().startswith("v") else f"v{text}"


def version_number(value: Any, default: int = 1) -> int:
    text = str(value or "").strip().lower()
    if text.startswith("v"):
        text = text[1:]
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return default


def _source_version_config(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "来源标识": entry.get("来源标识", ""),
        "接入协议": entry.get("接入协议", ""),
        "采集区域": entry.get("采集区域", ""),
        "连接凭证": entry.get("连接凭证", ""),
        "状态": entry.get("状态", ""),
    }


def ensure_source_version(
    source_entry: dict[str, Any],
    *,
    force_new: bool = False,
    trigger: str = "采集",
) -> dict[str, Any]:
    """把数据源在当前时刻的真实接入配置固化成一个不可回写的版本快照。"""
    with lineage_lock:
        source_id = str(source_entry.get("来源标识", ""))
        config = _source_version_config(source_entry)
        digest = stable_digest(config)
        current = version_number(source_entry.get("source_version", 1), 1)

        rows = store.rows(SOURCE_VERSIONS)
        same = [
            row for row in rows
            if row.get("来源标识") == source_id
            and int(row.get("版本号", 0) or 0) == current
            and row.get("配置摘要") == digest
            and row.get("状态") == config["状态"]
        ]
        if same and not force_new:
            return same[-1]

        latest = max(
            (int(row.get("版本号", 0) or 0) for row in rows if row.get("来源标识") == source_id),
            default=0,
        )
        target_version = current if not force_new else max(current, latest) + 1
        if any(row.get("来源标识") == source_id for row in rows) and target_version <= latest:
            target_version = latest + 1
        source_entry["source_version"] = target_version

        for row in rows:
            if row.get("来源标识") == source_id:
                row["是否当前"] = False

        entry = {
            "id": next_id(SOURCE_VERSIONS),
            "来源标识": source_id,
            "版本号": target_version,
            "版本标签": version_label(target_version),
            "接入协议": config["接入协议"],
            "采集区域": config["采集区域"],
            "连接凭证摘要": stable_digest(config["连接凭证"]),
            "配置摘要": digest,
            "配置快照": json_text(config),
            "状态": config["状态"],
            "是否当前": True,
            "触发动作": trigger,
            "记录时间": now_iso(),
            "status": config["状态"] or "待验证",
            "pending": config["状态"] != "已验证",
            "abnormal": False,
            # 英文别名便于跨模块按来源链字段过滤。
            "source_id": source_id,
            "source_version": target_version,
        }
        rows.append(entry)
        return entry


def source_version_row(source_id: str, version: Any) -> dict[str, Any] | None:
    number = version_number(version, 1)
    for row in store.rows(SOURCE_VERSIONS):
        if row.get("来源标识") == source_id and int(row.get("版本号", 0) or 0) == number:
            return row
    return None


def source_version_by_id(version_id: Any) -> dict[str, Any] | None:
    try:
        return store.find(SOURCE_VERSIONS, int(version_id))
    except (TypeError, ValueError):
        return None


def _parser_config(rule: dict[str, Any]) -> dict[str, Any]:
    return {
        "规则名称": rule.get("规则名称", ""),
        "适用数据源": rule.get("适用数据源", ""),
        "输入格式": rule.get("输入格式", ""),
        "输出字段列表": rule.get("输出字段列表", ""),
        "解析引擎": rule.get("解析引擎", ""),
        "状态": rule.get("status", ""),
    }


def ensure_parser_version(
    rule: dict[str, Any],
    *,
    force_new: bool = False,
    trigger: str = "解析",
) -> dict[str, Any]:
    """把解析规则在解析时刻的真实配置固化成一个版本快照。

    规则内容被直接改动但版本号没有同步时，自动生成新的版本号，避免同一个版本号
    对应两份不同的解析配置。
    """
    with lineage_lock:
        name = str(rule.get("规则名称", ""))
        config = _parser_config(rule)
        digest = stable_digest(config)
        declared = version_number(rule.get("rule_version", 0), 0)

        rows = store.rows(PARSER_VERSIONS)
        for row in rows:
            if (
                row.get("规则名称") == name
                and int(row.get("版本号", 0) or 0) == declared
                and row.get("配置摘要") == digest
                and not force_new
                and declared > 0
            ):
                rule["rule_version"] = declared
                return row

        latest = max(
            (int(row.get("版本号", 0) or 0) for row in rows if row.get("规则名称") == name),
            default=0,
        )
        target = declared if declared > 0 else latest + 1
        if force_new:
            target = max(declared, latest) + 1
        elif target <= latest:
            # 规则内容变化但版本号没有递增：说明解析规则发生了更新。
            target = latest + 1
        if target <= 0:
            target = 1

        for row in rows:
            if row.get("规则名称") == name:
                row["是否当前"] = False

        entry = {
            "id": next_id(PARSER_VERSIONS),
            "规则名称": name,
            "版本号": target,
            "版本标签": version_label(target),
            "适用数据源": config["适用数据源"],
            "输入格式": config["输入格式"],
            "输出字段列表": config["输出字段列表"],
            "解析引擎": config["解析引擎"],
            "配置摘要": digest,
            "配置快照": json_text(config),
            "是否当前": True,
            "触发动作": trigger,
            "记录时间": now_iso(),
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "rule_name": name,
            "parser_version": target,
        }
        rows.append(entry)
        rule["rule_version"] = target
        rule["规则版本"] = version_label(target)
        rule["rule_version_id"] = entry["id"]
        return entry


def parser_version_row(rule_name: str, version: Any) -> dict[str, Any] | None:
    number = version_number(version, 1)
    for row in store.rows(PARSER_VERSIONS):
        if row.get("规则名称") == rule_name and int(row.get("版本号", 0) or 0) == number:
            return row
    return None


def parser_version_by_id(version_id: Any) -> dict[str, Any] | None:
    try:
        return store.find(PARSER_VERSIONS, int(version_id))
    except (TypeError, ValueError):
        return None


def _active_source_version(source_id: str, version: Any) -> tuple[int, str]:
    entry = None
    for row in store.rows("source_registry"):
        if row.get("来源标识") == source_id:
            entry = row
            break
    current = version_number(entry.get("source_version", version) if entry else version, 1)
    notes: list[str] = []
    row_version = version_number(version, 1)
    if row_version and current != row_version:
        notes.append(f"来源存在更新的验证版本 v{current}，本行仍按采集版本 v{row_version} 处理")
    return (current != row_version if row_version else False), "；".join(notes)


def _active_parser_version(rule_name: str, version: Any) -> tuple[int, str]:
    latest = max(
        (int(row.get("版本号", 0) or 0) for row in store.rows(PARSER_VERSIONS)
         if row.get("规则名称") == rule_name),
        default=0,
    )
    row_version = version_number(version, 1)
    notes: list[str] = []
    if latest and row_version and latest != row_version:
        notes.append(f"解析规则已更新到 v{latest}，本行仍按解析版本 v{row_version} 处理")
    return (latest != row_version if row_version else False), "；".join(notes)


def conflict_state(
    source_id: str,
    source_version: Any,
    rule_name: str,
    parser_version: Any,
) -> tuple[bool, str]:
    source_conflict, source_note = _active_source_version(source_id, source_version)
    parser_conflict, parser_note = _active_parser_version(rule_name, parser_version)
    notes = [note for note in (source_note, parser_note) if note]
    return source_conflict or parser_conflict, "；".join(notes)


def refresh_conflict(row: dict[str, Any]) -> None:
    """按当前生效版本刷新冲突标记：只做提示，不改写本行使用的版本。

    补采期间同一来源可能出现新的验证版本、解析规则也可能已更新；历史行仍按
    采集/解析时的版本处理，冲突只说明“本行不是最新版本”，不影响它自身的核对。
    """
    has_conflict, note = conflict_state(
        str(row.get("数据源", "")),
        row.get("采集版本"),
        str(row.get("解析规则", "")),
        row.get("解析版本"),
    )
    row["版本冲突"] = has_conflict
    row["conflict"] = has_conflict
    row["冲突说明"] = note


def record_row_lineage(
    raw: dict[str, Any],
    entity: dict[str, Any],
    source_version: dict[str, Any],
    parser_version: dict[str, Any],
) -> dict[str, Any]:
    """为一条标准化明细生成行级来源链；重复调用返回同一条基础来源行。"""
    with lineage_lock:
        entity_id = int(entity["id"])
        for row in store.rows(ROW_LINEAGE):
            if row.get("文件ID", "") == "" and row.get("标准化记录ID") == entity_id:
                return row

        raw_body = str(raw.get("原始正文", ""))
        fields = json_value(entity.get("标准化字段集"), {})
        body_digest = stable_digest(raw_body)
        entity_digest = content_digest(fields)
        source_id = str(raw.get("source_id") or source_version.get("来源标识", ""))
        parser_name = str(parser_version.get("规则名称", ""))
        source_ver = source_version.get("版本号", raw.get("source_config_version", 1))
        parser_ver = parser_version.get("版本号", entity.get("parser_version", 1))
        has_conflict, conflict_note = conflict_state(
            source_id, source_ver, parser_name, parser_ver,
        )

        group_value = ""
        for candidate in ("level", "分组", "status", "message"):
            if candidate in fields:
                group_value = str(fields.get(candidate, ""))
                break

        entry = {
            "id": next_id(ROW_LINEAGE),
            "来源批次": str(raw.get("批次编号", "")),
            "数据源": source_id,
            "原始记录ID": int(raw["id"]),
            "标准化记录ID": entity_id,
            "原始行号": str(raw.get("原始行号", "")),
            "采集版本": int(source_ver),
            "采集版本标签": version_label(source_ver),
            "来源配置版本ID": source_version.get("id"),
            "来源配置摘要": source_version.get("配置摘要", ""),
            "转换凭证摘要": source_version.get("连接凭证摘要", ""),
            "解析规则": parser_name,
            "解析版本": int(parser_ver),
            "解析版本标签": version_label(parser_ver),
            "解析版本ID": parser_version.get("id"),
            "解析配置摘要": parser_version.get("配置摘要", ""),
            "分组键": "",
            "分组值": group_value,
            "原始正文摘要": body_digest,
            "标准化摘要": entity_digest,
            "文件ID": "",
            "来源行ID": "",
            "核验状态": VERIFY_PENDING,
            "版本冲突": has_conflict,
            "conflict": has_conflict,
            "冲突说明": conflict_note,
            "采集快照": source_version.get("配置快照", ""),
            "解析快照": parser_version.get("配置快照", ""),
            "记录时间": now_iso(),
            "status": VERIFY_PENDING,
            "pending": True,
            "abnormal": False,
            # 英文别名，方便外部按稳定字段名定位来源链。
            "batch_no": str(raw.get("批次编号", "")),
            "source_id": source_id,
            "raw_record_id": int(raw["id"]),
            "parsed_entity_id": entity_id,
            "source_version": int(source_ver),
            "parser_rule": parser_name,
            "parser_version": int(parser_ver),
            "raw_body_digest": body_digest,
            "content_digest": entity_digest,
            "group_key": "",
            "group_value": group_value,
            "file_id": "",
            "verify_status": VERIFY_PENDING,
        }
        store.rows(ROW_LINEAGE).append(entry)
        return entry


def base_lineage_for_entity(entity_id: Any) -> dict[str, Any] | None:
    try:
        target = int(entity_id)
    except (TypeError, ValueError):
        return None
    for row in store.rows(ROW_LINEAGE):
        if row.get("文件ID", "") == "" and row.get("标准化记录ID") == target:
            return row
    return None


def mark_base_lineage(entity_id: Any, verified: bool, note: str = "") -> None:
    with lineage_lock:
        for row in store.rows(ROW_LINEAGE):
            if row.get("文件ID", "") != "":
                continue
            if row.get("标准化记录ID") != int(entity_id):
                continue
            row["核验状态"] = VERIFY_OK if verified else VERIFY_FAILED
            row["verify_status"] = row["核验状态"]
            row["status"] = row["核验状态"]
            row["pending"] = not verified
            row["abnormal"] = not verified
            row["核验说明"] = note
            row["核验时间"] = now_iso() if verified else ""


def mark_revalidating(entity_id: Any) -> None:
    with lineage_lock:
        for row in store.rows(ROW_LINEAGE):
            if row.get("文件ID", "") != "":
                continue
            if row.get("标准化记录ID") != int(entity_id):
                continue
            row["核验状态"] = VERIFY_PENDING
            row["verify_status"] = VERIFY_PENDING
            row["status"] = VERIFY_PENDING
            row["pending"] = True
            row["核验说明"] = "复验进行中，等待重新核验"


def clone_lineage_for_export(
    base: dict[str, Any],
    export_id: int,
    group_value: str,
    dim_field: str,
) -> dict[str, Any]:
    """把基础来源行固化成某一次交付的只读切片。"""
    with lineage_lock:
        entry = copy.deepcopy(base)
        entry["id"] = next_id(ROW_LINEAGE)
        entry["来源行ID"] = str(base.get("id", ""))
        entry["文件ID"] = str(export_id)
        entry["file_id"] = str(export_id)
        entry["分组键"] = dim_field
        entry["分组值"] = str(group_value)
        entry["group_key"] = dim_field
        entry["group_value"] = str(group_value)
        entry["核验状态"] = VERIFY_PENDING
        entry["verify_status"] = VERIFY_PENDING
        entry["status"] = VERIFY_PENDING
        entry["pending"] = True
        entry["abnormal"] = False
        entry["核验说明"] = ""
        entry["核验时间"] = ""
        entry["导出时间"] = now_iso()
        store.rows(ROW_LINEAGE).append(entry)
        return entry


# ---------------------------------------------------------------------------
# 来源清单：由行级来源链生成、核验，并作为交付闸门
# ---------------------------------------------------------------------------


def _entity_fields(entity: dict[str, Any] | None) -> dict[str, Any]:
    if not entity:
        return {}
    return json_value(entity.get("标准化字段集"), {})


def _batch_row(batch_no: str) -> dict[str, Any] | None:
    for row in store.rows("ingest_batch"):
        if str(row.get("批次编号", "")) == str(batch_no):
            return row
    return None


def check_lineage_consistency(row: dict[str, Any]) -> tuple[bool, list[str]]:
    """检查一条来源链引用到的正文、明细、来源快照、解析快照和批次是否对得上。"""
    # 冲突标记是“是否最新版本”的实时判断，每次核对前刷新一次；
    # 它只影响来源清单里的提示，不影响本行使用的版本和能否核对。
    refresh_conflict(row)
    notes: list[str] = []
    raw_id = row.get("原始记录ID")
    entity_id = row.get("标准化记录ID")
    raw = store.find("raw_records", int(raw_id)) if raw_id not in (None, "") else None
    entity = store.find("parsed_entity", int(entity_id)) if entity_id not in (None, "") else None

    if raw is None:
        notes.append(f"原始记录 {raw_id} 不存在")
    else:
        if str(raw.get("批次编号", "")) != str(row.get("来源批次", "")):
            notes.append("原始记录的采集批次与来源链不一致")
        if stable_digest(str(raw.get("原始正文", ""))) != row.get("原始正文摘要"):
            notes.append("原始正文摘要与来源链不一致")

    if entity is None:
        notes.append(f"标准化明细 {entity_id} 不存在")
    else:
        if str(entity.get("来源批次", "")) != str(row.get("来源批次", "")):
            notes.append("标准化明细的来源批次与来源链不一致")
        if content_digest(_entity_fields(entity)) != row.get("标准化摘要"):
            notes.append("标准化字段摘要与来源链不一致")

    source_version = (
        source_version_by_id(row.get("来源配置版本ID"))
        or source_version_row(str(row.get("数据源", "")), row.get("采集版本"))
    )
    if source_version is None:
        notes.append("采集时的来源配置版本不存在")
    elif source_version.get("配置摘要") != row.get("来源配置摘要"):
        notes.append("来源配置快照与采集时版本不一致")

    parser_version = (
        parser_version_by_id(row.get("解析版本ID"))
        or parser_version_row(str(row.get("解析规则", "")), row.get("解析版本"))
    )
    if parser_version is None:
        notes.append("解析时的规则版本不存在")
    elif parser_version.get("配置摘要") != row.get("解析配置摘要"):
        notes.append("解析配置快照与解析时版本不一致")

    if _batch_row(str(row.get("来源批次", ""))) is None:
        notes.append("来源批次不存在")

    return (not notes), notes


def evaluate_base(row: dict[str, Any]) -> tuple[str, str]:
    """计算一条基础来源链的核验状态（不改写）。"""
    ok, notes = check_lineage_consistency(row)
    if not ok:
        return VERIFY_FAILED, "；".join(notes)
    entity = store.find("parsed_entity", int(row.get("标准化记录ID", 0)))
    if entity is None:
        return VERIFY_FAILED, "标准化明细不存在"
    if entity.get("status") == "校验通过":
        return VERIFY_OK, ""
    if entity.get("status") == "待校验":
        return VERIFY_PENDING, "标准化明细尚未通过校验"
    return VERIFY_FAILED, f"标准化明细状态为{entity.get('status', '未知')}"


def evaluate_slice(row: dict[str, Any]) -> tuple[str, str]:
    """计算一条交付切片的核验状态：正文/明细/快照对得上且基础链已核验。"""
    ok, notes = check_lineage_consistency(row)
    if not ok:
        return VERIFY_FAILED, "；".join(notes)

    base_id = row.get("来源行ID")
    base = store.find(ROW_LINEAGE, int(base_id)) if base_id not in (None, "") else None
    if base is None:
        return VERIFY_FAILED, "基础来源链缺失，无法核对实际输入"
    if base.get("原始正文摘要") != row.get("原始正文摘要"):
        return VERIFY_FAILED, "切片的原始正文摘要与基础来源链不一致"
    if base.get("标准化摘要") != row.get("标准化摘要"):
        return VERIFY_FAILED, "切片的标准化摘要与基础来源链不一致"

    status = base.get("核验状态")
    if status == VERIFY_OK:
        return VERIFY_OK, ""
    if status == VERIFY_FAILED:
        return VERIFY_FAILED, f"基础来源链核验异常：{base.get('核验说明', '')}".strip()
    return VERIFY_PENDING, f"基础来源链待核验：{base.get('核验说明', '')}".strip()


def set_verify(row: dict[str, Any], state: str, note: str = "") -> None:
    row["核验状态"] = state
    row["verify_status"] = state
    row["status"] = state
    row["pending"] = state == VERIFY_PENDING
    row["abnormal"] = state == VERIFY_FAILED
    row["核验说明"] = note
    row["核验时间"] = now_iso() if state != VERIFY_PENDING else ""


def suspend_deliveries_for_entity(entity_id: Any, note: str) -> list[int]:
    """实体进入复验/重放时，先收回复验范围内的公开切片，避免旧文件继续被下载。

    只收回这条明细对应的切片；同一文件中其他仍然已核验的切片继续公开，
    交付状态按剩余公开切片重算。
    """
    with lineage_lock:
        affected: set[int] = set()
        for row in store.rows(ROW_LINEAGE):
            if row.get("标准化记录ID") != int(entity_id):
                continue
            if row.get("文件ID", "") == "":
                continue
            if row.get("核验状态") == VERIFY_OK:
                set_verify(row, VERIFY_PENDING, note)
            affected.add(int(row.get("文件ID", 0) or 0))
        for file_id in affected:
            if file_id:
                recompute_manifest(file_id)
        return sorted(affected)


def _slice_ref(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "来源链ID": row.get("来源行ID"),
        "切片ID": row.get("id"),
        "原始记录ID": row.get("原始记录ID"),
        "标准化记录ID": row.get("标准化记录ID"),
        "采集版本": row.get("采集版本"),
        "解析版本": row.get("解析版本"),
        "原始正文摘要": row.get("原始正文摘要"),
        "标准化摘要": row.get("标准化摘要"),
        "核验状态": row.get("核验状态"),
        "版本冲突": row.get("版本冲突", False),
    }


def _group_key(row: dict[str, Any]) -> str:
    value = str(row.get("分组值", ""))
    return value if value else "未分组"


def _metric_value(entity: dict[str, Any] | None, metric_field: str) -> float:
    fields = _entity_fields(entity)
    value = fields.get(metric_field)
    try:
        return float(value) if value not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _file_row(row: dict[str, Any]) -> dict[str, Any]:
    entity = store.find("parsed_entity", int(row.get("标准化记录ID", 0)))
    fields = _entity_fields(entity)
    return {
        "lineage_id": row.get("id"),
        "source_lineage_id": row.get("来源行ID"),
        "group": _group_key(row),
        "batch_no": row.get("来源批次"),
        "raw_record_id": row.get("原始记录ID"),
        "parsed_entity_id": row.get("标准化记录ID"),
        "source_version": row.get("采集版本"),
        "parser_rule": row.get("解析规则"),
        "parser_version": row.get("解析版本"),
        "raw_body_digest": row.get("原始正文摘要"),
        "content_digest": row.get("标准化摘要"),
        "fields": fields,
    }


def recompute_manifest(export_id: int, *, draft: bool = False) -> dict[str, Any] | None:
    """按当前切片状态重建来源清单的分组、公开文件行和摘要。

    draft=True 表示清单还在生成或复验过程中：此时文件行只是“将来会公开的行”，
    交付状态保持待核验；draft=False 表示一次真正的文件/来源双核验，只有公开行数
    大于零且没有异常切片时才允许交付。
    """
    with lineage_lock:
        manifest = manifest_for_export(export_id)
        export = store.find("data_export", int(export_id))
        if manifest is None or export is None:
            return manifest
        slices = [
            row for row in store.rows(ROW_LINEAGE)
            if str(row.get("文件ID", "")) == str(export_id)
        ]
        dim_field = manifest.get("聚合维度", "")
        metric_field = manifest.get("指标字段", "")
        groups: dict[str, dict[str, Any]] = {}
        file_rows: list[dict[str, Any]] = []
        for row in sorted(slices, key=lambda item: int(item.get("id", 0))):
            key = _group_key(row)
            group = groups.setdefault(key, {
                "分组值": key,
                "聚合维度": dim_field or "分组",
                "公开记录数": 0,
                "等待记录数": 0,
                "异常记录数": 0,
                "指标合计": 0.0,
                "切片IDs": [],
                "公开切片IDs": [],
                "来源引用": [],
            })
            group["切片IDs"].append(row.get("id"))
            group["来源引用"].append(_slice_ref(row))
            state = row.get("核验状态")
            if state == VERIFY_OK:
                group["公开记录数"] += 1
                group["公开切片IDs"].append(row.get("id"))
                entity = store.find("parsed_entity", int(row.get("标准化记录ID", 0)))
                group["指标合计"] += _metric_value(entity, metric_field)
                file_rows.append(_file_row(row))
            elif state == VERIFY_FAILED:
                group["异常记录数"] += 1
            else:
                group["等待记录数"] += 1

        for key, group in groups.items():
            group["记录数"] = (
                group["公开记录数"] + group["等待记录数"] + group["异常记录数"]
            )
            group["是否公开"] = group["公开记录数"] > 0
            group["版本冲突"] = any(ref.get("版本冲突") for ref in group["来源引用"])
            group["源引用冲突"] = group["版本冲突"]
            if group["异常记录数"]:
                group["状态"] = VERIFY_FAILED
            elif group["等待记录数"]:
                group["状态"] = VERIFY_PENDING
            else:
                group["状态"] = VERIFY_OK
            group["来源摘要"] = stable_digest(group["来源引用"])
            group_file_rows = [
                item for item in file_rows if item["group"] == key
            ]
            group["文件摘要"] = stable_digest(group_file_rows)

        public_count = len(file_rows)
        pending_count = sum(1 for row in slices if row.get("核验状态") == VERIFY_PENDING)
        failed_count = sum(1 for row in slices if row.get("核验状态") == VERIFY_FAILED)

        manifest["分组"] = groups
        manifest["分组数"] = len(groups)
        manifest["切片IDs"] = [row.get("id") for row in slices]
        # 等待核验的切片不是文件行；公开行数只统计已验证切片。
        manifest["文件行数"] = public_count
        manifest["公开切片数"] = public_count
        manifest["等待切片数"] = pending_count
        manifest["异常切片数"] = failed_count
        manifest["来源摘要"] = stable_digest([_slice_ref(row) for row in slices])
        manifest["更新时间"] = now_iso()

        if draft:
            # 生成或复验进行中的清单：还没有做文件/来源双核验，一律按待核验处理。
            manifest["文件行"] = []
            manifest["file_rows"] = []
            manifest["文件摘要"] = ""
            manifest["核验状态"] = VERIFY_PENDING
            manifest["交付状态"] = DELIVERY_PENDING
        else:
            manifest["文件行"] = file_rows
            manifest["file_rows"] = file_rows
            manifest["文件摘要"] = stable_digest(file_rows)
            if failed_count:
                manifest["核验状态"] = VERIFY_FAILED
                manifest["交付状态"] = DELIVERY_BLOCKED
            elif public_count == 0:
                manifest["核验状态"] = VERIFY_PENDING
                manifest["交付状态"] = DELIVERY_PENDING
            else:
                manifest["核验状态"] = VERIFY_OK
                manifest["交付状态"] = DELIVERY_READY

        manifest["status"] = manifest["核验状态"]
        manifest["pending"] = manifest["交付状态"] != DELIVERY_READY
        manifest["abnormal"] = manifest["核验状态"] == VERIFY_FAILED

        export["交付状态"] = manifest["交付状态"]
        export["delivery_status"] = manifest["交付状态"]
        export["manifest_id"] = manifest["id"]
        export["公开行数"] = public_count
        export["等待行数"] = pending_count
        export["异常行数"] = failed_count
        export["文件摘要"] = manifest["文件摘要"]
        export["来源摘要"] = manifest["来源摘要"]
        export["核验状态"] = manifest["核验状态"]
        export["public_row_count"] = public_count
        export["pending_row_count"] = pending_count
        export["file_digest"] = manifest["文件摘要"]
        export["lineage_digest"] = manifest["来源摘要"]
        return manifest


def manifest_for_export(export_id: Any) -> dict[str, Any] | None:
    try:
        target = int(export_id)
    except (TypeError, ValueError):
        return None
    for row in store.rows(DELIVERY_MANIFESTS):
        if int(row.get("id", 0) or 0) == target:
            return row
    return None


def create_manifest(
    export_id: int,
    *,
    view_name: str,
    export_format: str,
    filters: dict[str, str] | None,
    dim_field: str,
    metric_field: str,
    agg_func: str,
) -> dict[str, Any]:
    with lineage_lock:
        existing = manifest_for_export(export_id)
        if existing is not None:
            return existing
        manifest = {
            "id": export_id,
            "文件ID": str(export_id),
            "文件名称": f"{view_name}.{export_format}"
            if view_name else f"{export_id}.{export_format}",
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json_text(filters or {}),
            "聚合维度": dim_field,
            "指标字段": metric_field,
            "聚合函数": agg_func,
            "分组": {},
            "分组数": 0,
            "切片IDs": [],
            "文件行": [],
            "file_rows": [],
            "文件行数": 0,
            "公开切片数": 0,
            "等待切片数": 0,
            "异常切片数": 0,
            "文件摘要": "",
            "来源摘要": "",
            "核验状态": VERIFY_PENDING,
            "交付状态": DELIVERY_PENDING,
            "核验时间": "",
            "生成时间": now_iso(),
            "更新时间": now_iso(),
            "status": VERIFY_PENDING,
            "pending": True,
            "abnormal": False,
            "export_id": export_id,
            "view_name": view_name,
            "export_format": export_format,
        }
        store.rows(DELIVERY_MANIFESTS).append(manifest)
        return manifest


def verify_delivery(export_id: int) -> dict[str, Any] | None:
    """同时核验“文件行”和“来源记录”，并把结果回写来源清单。"""
    with lineage_lock:
        manifest = manifest_for_export(export_id)
        if manifest is None:
            return None
        slices = [
            row for row in store.rows(ROW_LINEAGE)
            if str(row.get("文件ID", "")) == str(export_id)
        ]
        for row in slices:
            state, note = evaluate_slice(row)
            set_verify(row, state, note)
        manifest = recompute_manifest(int(export_id))
        if manifest is not None:
            manifest["核验时间"] = now_iso()
        return manifest


def build_manifest_for_export(
    export_id: int,
    *,
    view_name: str,
    export_format: str,
    filters: dict[str, str] | None,
    dim_field: str,
    metric_field: str,
    agg_func: str,
) -> dict[str, Any]:
    manifest = create_manifest(
        export_id,
        view_name=view_name,
        export_format=export_format,
        filters=filters,
        dim_field=dim_field,
        metric_field=metric_field,
        agg_func=agg_func,
    )
    return recompute_manifest(int(export_id), draft=True) or manifest


def list_deliveries() -> list[dict[str, Any]]:
    result = []
    for manifest in store.rows(DELIVERY_MANIFESTS):
        result.append({
            "id": manifest.get("id"),
            "文件名称": manifest.get("文件名称"),
            "导出范围": manifest.get("导出范围"),
            "目标格式": manifest.get("目标格式"),
            "文件行数": manifest.get("文件行数", 0),
            "公开切片数": manifest.get("公开切片数", 0),
            "等待切片数": manifest.get("等待切片数", 0),
            "异常切片数": manifest.get("异常切片数", 0),
            "分组数": manifest.get("分组数", 0),
            "文件摘要": manifest.get("文件摘要", ""),
            "来源摘要": manifest.get("来源摘要", ""),
            "核验状态": manifest.get("核验状态"),
            "交付状态": manifest.get("交付状态"),
            "生成时间": manifest.get("生成时间"),
            "核验时间": manifest.get("核验时间", ""),
            "status": manifest.get("status"),
            "pending": manifest.get("pending"),
        })
    return result


def delivery_detail(export_id: int, include_pending: bool = True) -> dict[str, Any] | None:
    manifest = manifest_for_export(export_id)
    if manifest is None:
        return None
    slices = [
        row for row in store.rows(ROW_LINEAGE)
        if str(row.get("文件ID", "")) == str(export_id)
    ]
    for row in slices:
        refresh_conflict(row)
    if not include_pending:
        slices = [row for row in slices if row.get("核验状态") == VERIFY_OK]
    detail = copy.deepcopy(manifest)
    detail["切片"] = [copy.deepcopy(row) for row in slices]
    detail["slices"] = detail["切片"]
    detail["文件摘要"] = stable_digest(manifest.get("文件行", []))
    return detail


def delivery_trace(
    export_id: int,
    *,
    group: str | None = None,
    slice_id: str | None = None,
    batch_no: str | None = None,
) -> dict[str, Any] | None:
    """由任一分组、切片或批次定位输入明细、原始正文和采集批次。"""
    manifest = manifest_for_export(export_id)
    if manifest is None:
        return None
    slices = [
        row for row in store.rows(ROW_LINEAGE)
        if str(row.get("文件ID", "")) == str(export_id)
    ]
    if group is not None:
        slices = [row for row in slices if _group_key(row) == group]
    if slice_id is not None:
        slices = [row for row in slices if str(row.get("id")) == str(slice_id)]
    if batch_no is not None:
        slices = [row for row in slices if str(row.get("来源批次")) == str(batch_no)]

    traces = []
    for row in sorted(slices, key=lambda item: int(item.get("id", 0))):
        refresh_conflict(row)
        raw = store.find("raw_records", int(row.get("原始记录ID", 0)))
        entity = store.find("parsed_entity", int(row.get("标准化记录ID", 0)))
        batch = _batch_row(str(row.get("来源批次", "")))
        source_version = (
            source_version_by_id(row.get("来源配置版本ID"))
            or source_version_row(str(row.get("数据源", "")), row.get("采集版本"))
        )
        parser_version = (
            parser_version_by_id(row.get("解析版本ID"))
            or parser_version_row(str(row.get("解析规则", "")), row.get("解析版本"))
        )
        traces.append({
            "分组值": row.get("分组值"),
            "切片ID": row.get("id"),
            "来源链ID": row.get("来源行ID"),
            "核验状态": row.get("核验状态"),
            "版本冲突": row.get("版本冲突", False),
            "冲突说明": row.get("冲突说明", ""),
            "采集版本": row.get("采集版本"),
            "解析版本": row.get("解析版本"),
            "原始正文摘要": row.get("原始正文摘要"),
            "标准化摘要": row.get("标准化摘要"),
            "采集批次": copy.deepcopy(batch) if batch else None,
            "标准化明细": {
                "id": entity.get("id"),
                "来源批次": entity.get("来源批次"),
                "原始记录ID": entity.get("原始记录ID"),
                "解析规则": entity.get("parser_rule"),
                "解析版本": entity.get("parser_version"),
                "标准化字段集": entity.get("标准化字段集"),
                "校验状态": entity.get("校验状态"),
            } if entity else None,
            "原始日志": {
                "id": raw.get("id"),
                "批次编号": raw.get("批次编号"),
                "原始行号": raw.get("原始行号"),
                "原始正文": raw.get("原始正文"),
                "采集时间戳": raw.get("采集时间戳"),
            } if raw else None,
            "采集时来源配置": {
                "版本号": source_version.get("版本号"),
                "版本标签": source_version.get("版本标签"),
                "配置快照": source_version.get("配置快照"),
                "连接凭证摘要": source_version.get("连接凭证摘要"),
                "状态": source_version.get("状态"),
            } if source_version else None,
            "解析时规则配置": {
                "版本号": parser_version.get("版本号"),
                "版本标签": parser_version.get("版本标签"),
                "配置快照": parser_version.get("配置快照"),
                "状态": parser_version.get("状态"),
            } if parser_version else None,
            "原始正文一致": bool(
                raw and stable_digest(str(raw.get("原始正文", ""))) == row.get("原始正文摘要")
            ),
            "标准化摘要一致": bool(
                entity and content_digest(_entity_fields(entity)) == row.get("标准化摘要")
            ),
        })
    return {
        "export_id": int(export_id),
        "文件名称": manifest.get("文件名称"),
        "核验状态": manifest.get("核验状态"),
        "交付状态": manifest.get("交付状态"),
        "筛选": {"group": group, "slice_id": slice_id, "batch_no": batch_no},
        "trace": traces,
    }


def render_delivery_csv(manifest: dict[str, Any]) -> str:
    """只渲染来源清单里已经核实的公开切片。"""
    rows = manifest.get("文件行", [])
    columns: list[str] = []
    for row in rows:
        for key in row.get("fields", {}):
            if key not in columns:
                columns.append(key)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "lineage_id", "source_lineage_id", "group", "batch_no",
        "raw_record_id", "parsed_entity_id", "source_version",
        "parser_rule", "parser_version", *columns,
    ])
    for row in rows:
        values = [
            row.get("lineage_id"),
            row.get("source_lineage_id"),
            row.get("group"),
            row.get("batch_no"),
            row.get("raw_record_id"),
            row.get("parsed_entity_id"),
            row.get("source_version"),
            row.get("parser_rule"),
            row.get("parser_version"),
            *(row.get("fields", {}).get(column, "") for column in columns),
        ]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return output.getvalue()


def recheck_delivery(export_id: int) -> dict[str, Any] | None:
    """下载前再核对一次公开切片：正文/明细/快照被改动时立刻阻断交付。"""
    with lineage_lock:
        manifest = manifest_for_export(export_id)
        if manifest is None:
            return None
        changed = False
        for row in store.rows(ROW_LINEAGE):
            if str(row.get("文件ID", "")) != str(export_id):
                continue
            if row.get("核验状态") != VERIFY_OK:
                continue
            state, note = evaluate_slice(row)
            if state != VERIFY_OK:
                set_verify(row, state, note)
                changed = True
        if not changed:
            return manifest
        return recompute_manifest(int(export_id))
