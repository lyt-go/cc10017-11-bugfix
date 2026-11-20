"""增量合并接口：维护合并策略，覆盖提交校验、标记生效、停用策略等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.upsert_merge import UpsertMergeService

router = APIRouter(prefix="/api/upsert_merge", tags=["增量合并"])

service = UpsertMergeService()

LIST_FIELDS = ["策略编号", "目标表名", "合并键列", "插入列", "更新列", "删除标记", "排序字段", "合并引擎"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、策略编号关键字与状态过滤增量合并列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="upsert_merge.csv"'})


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条合并策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"合并策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条合并策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="合并策略已登记", entry=entry)


@router.get("/{entry_id}/snapshot", response_model=dict)
def get_merge_snapshot(entry_id: int) -> dict:
    """读取终态合并数据快照：保留删除标记、最终排序值、来源批次与发布版本。"""
    snapshot = service.get_merge_snapshot(entry_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"合并策略 {entry_id} 不存在或已归档")
    return snapshot


@router.get("/{entry_id}/downstream", response_model=dict)
def get_downstream_records(entry_id: int) -> dict:
    """读取下游可见记录，用于核对与终态快照是否同版本。"""
    downstream = service.get_downstream_records(entry_id)
    if downstream is None:
        raise HTTPException(status_code=404, detail=f"合并策略 {entry_id} 不存在或已归档")
    return downstream


@router.post("/{entry_id}/merge", response_model=ActionResult)
def run_merge(entry_id: int, payload: dict[str, object] | None = None) -> ActionResult:
    """提交一批增量变更：按变更序号归并并发布终态快照。"""
    body = payload or {}
    records = body.get("records")
    if not isinstance(records, list):
        records = []
    result = service.merge_batch(
        entry_id,
        records=records,
        source_version=body.get("source_version"),
        batch_id=body.get("batch_id") if isinstance(body.get("batch_id"), str) else None,
        interrupt=body.get("interrupt") if isinstance(body.get("interrupt"), str) else None,
    )
    if not result.get("ok"):
        return ActionResult(ok=False, message=str(result.get("error", "增量合并失败")))
    return ActionResult(ok=True, message=str(result.get("message", "增量合并已应用")), entry=result)


@router.post("/{entry_id}/merge-demo", response_model=ActionResult)
def run_merge_demo(entry_id: int) -> ActionResult:
    """运行两批演示变更：较新的删除先到、次新的更新迟到。"""
    result = service.run_demo_merge(entry_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=str(result.get("error", "增量合并失败")))
    return ActionResult(ok=True, message=str(result.get("message")), entry=result)


@router.post("/{entry_id}/snapshot/retry", response_model=ActionResult)
def retry_merge_snapshot(entry_id: int) -> ActionResult:
    """发布中断后重试：从已落盘的终态快照补齐下游并追加后续批次。"""
    result = service.retry_merge_snapshot(entry_id)
    if not result.get("ok"):
        return ActionResult(ok=False, message=str(result.get("error", "重试失败")))
    return ActionResult(ok=True, message=str(result.get("message", "已补齐")), entry=result)


@router.post("/{entry_id}/snapshot/interrupt", response_model=ActionResult)
def interrupt_merge_snapshot(entry_id: int, payload: dict[str, object] | None = None) -> ActionResult:
    """模拟一次发布中断，供补齐路径验证；仅在演示环境使用。"""
    stage = (payload or {}).get("stage")
    result = service.interrupt_merge_snapshot(entry_id, str(stage) if stage else None)
    if "error" in result:
        return ActionResult(ok=False, message=str(result.get("error")))
    return ActionResult(ok=True, message=str(result.get("message", "发布已中断")), entry=result)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条合并策略执行提交校验、标记生效、停用策略；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
