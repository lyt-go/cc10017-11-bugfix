"""增量合并业务规则：状态流转、字段校验与筛选口径都收在这里。

增量合并的另一半是终态合并数据快照：两批变更先按到达顺序入批，
但发布时统一按 ``变更序号 → 源表版本 → 删除优先 → 排序值`` 归并，
保证较新的删除不会被迟到的旧更新翻回有效；快照与下游记录同版本落盘。
"""
from __future__ import annotations

from typing import Any

from app.services.merge_snapshot import demo_merge_records, merge_snapshot_service
from app.store import store

MODULE = "upsert_merge"
LIST_FIELDS = ["策略编号", "目标表名", "合并键列", "插入列", "更新列", "删除标记", "排序字段", "合并引擎"]
REQUIRED_FIELDS = ["策略编号", "目标表名", "合并键列"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]


class UpsertMergeService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("策略编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"合并策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于增量合并可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"合并策略已{action}"

    # ───────────────────────────────
    # 终态合并数据快照
    # ───────────────────────────────

    def merge_batch(
        self,
        entry_id: int,
        *,
        records: list[dict[str, Any]] | None = None,
        source_version: Any = None,
        batch_id: str | None = None,
        source: str = "upsert_merge",
        cdc_id: int | None = None,
        interrupt: str | None = None,
    ) -> dict[str, Any]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": f"合并策略 {entry_id} 不存在或已归档"}
        if entry.get("status") != "已生效":
            return {"ok": False, "error": f"合并策略 {entry.get('策略编号')} 尚未生效，不能承接增量合并"}

        payload = [dict(record) for record in (records or []) if isinstance(record, dict)]
        if not payload:
            return {"ok": False, "error": "没有提交变更记录"}

        result = merge_snapshot_service.submit_batch(
            entry,
            records=payload,
            source=source,
            source_version=source_version,
            batch_id=batch_id,
            cdc_id=cdc_id,
            interrupt=interrupt,
        )
        if result.get("ok") and not result.get("duplicate"):
            entry["_last_merge"] = {
                "batch": result.get("batch"),
                "publication": result.get("publication"),
                "merged_at": (result.get("batch") or {}).get("submitted_at"),
            }
        return result

    def run_demo_merge(self, entry_id: int) -> dict[str, Any]:
        """按两批演示变更走一遍发布会话：先删除、后迟到更新。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": f"合并策略 {entry_id} 不存在或已归档"}
        if entry.get("status") != "已生效":
            return {"ok": False, "error": f"合并策略 {entry.get('策略编号')} 尚未生效，不能执行增量合并"}

        applied: list[dict[str, Any]] = []
        for batch_no in (1, 2):
            result = self.merge_batch(entry_id, records=demo_merge_records(batch_no))
            if not result.get("ok"):
                return result
            applied.append(result.get("batch"))
        return {
            "ok": True,
            "message": "两批变更已合并，较新的删除保持终态",
            "batches": applied,
            "snapshot": merge_snapshot_service.get_snapshot(entry),
            "downstream": merge_snapshot_service.get_downstream(entry),
            "consistency": merge_snapshot_service.consistency(entry),
        }

    def get_merge_snapshot(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        snapshot = merge_snapshot_service.get_snapshot(entry)
        snapshot["batches"] = merge_snapshot_service.get_batches(entry)
        snapshot["downstream"] = merge_snapshot_service.get_downstream(entry)
        return snapshot

    def get_downstream_records(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return merge_snapshot_service.get_downstream(entry)

    def retry_merge_snapshot(self, entry_id: int) -> dict[str, Any]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": f"合并策略 {entry_id} 不存在或已归档"}
        return merge_snapshot_service.retry(entry)

    def interrupt_merge_snapshot(self, entry_id: int, stage: str | None = None) -> dict[str, Any]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": f"合并策略 {entry_id} 不存在或已归档"}
        return merge_snapshot_service.interrupt_publish(entry, stage=stage or "snapshot_written")
