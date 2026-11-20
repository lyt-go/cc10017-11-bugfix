"""CDC捕获 时间线接口：返回作业时间序列结构。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.change_capture import ChangeCaptureService

router = APIRouter(prefix="/api/change_capture", tags=["CDC捕获"])
service = ChangeCaptureService()
LIST_FIELDS = ["配置编号", "源数据库", "源表名", "快照模式", "捕获列", "变更序号", "增量阶段", "目标主题"]
STATUSES = ["草稿", "待校验", "运行中", "已停止"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=change_capture.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"CDC配置 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="CDC配置已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.get("/{entry_id}/snapshot")
def get_merge_snapshot(entry_id: int) -> dict:
    """读取该CDC配置写入的终态合并数据快照，核对删除序号与来源批次。"""
    snapshot = service.get_merge_snapshot_for_entry(entry_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"CDC配置 {entry_id} 未关联增量合并策略")
    return snapshot


@router.post("/{entry_id}/capture")
def capture_changes(entry_id: int, payload: dict[str, Any] | None = None) -> ActionResult:
    """把捕获到的变更交给增量合并，写入同一份终态快照。"""
    body = payload or {}
    records = body.get("records")
    result = service.capture_changes(
        entry_id,
        records=[record for record in records if isinstance(record, dict)] if isinstance(records, list) else None,
        change_seq=body.get("变更序号", body.get("change_seq")),
        source_version=body.get("源表版本", body.get("source_version")),
        batch_id=body.get("batch_id") if isinstance(body.get("batch_id"), str) else None,
        interrupt=body.get("interrupt") if isinstance(body.get("interrupt"), str) else None,
    )
    if not result.get("ok"):
        return ActionResult(ok=False, message=str(result.get("error", "CDC增量捕获失败")))
    return ActionResult(ok=True, message=str(result.get("message", "CDC变更已写入终态合并数据快照")), entry=result)


@router.post("/{entry_id}/capture-demo")
def capture_demo_changes(entry_id: int) -> ActionResult:
    """按两批时序演示：删除序号 100 先发布，序号 90 的更新迟到。"""
    result = service.capture_demo_changes(entry_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=str(result.get("error", "CDC增量捕获失败")))
    return ActionResult(ok=True, message=str(result.get("message")), entry=result)
