"""终态合并数据快照：版本化归并、原子发布与中断补齐。

增量合并不再按“到达顺序”直接覆盖，而是为每个合并键保留一条终态记录：

* 比较顺序固定为 ``变更序号 → 源表版本 → 删除优先 → 排序值 → 到达次序``，
  因此迟到的旧更新不会把更晚发生的删除重新恢复成有效记录；
* 快照记录始终保留 ``删除标记`` 与 ``最终排序值``，删除只翻转标记，不物理移除；
* 同一份快照与下游可见记录在同一个发布版本里写入，发布完成前不会被查询到一半；
* 发布中断后会留下一条待补齐的发布记录，重试时从已落盘的快照补齐下游，
  必要时按全部来源批次重建，保证既不残留也不丢失记录。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.store import store

BATCH_TABLE = "merge_batches"
SNAPSHOT_TABLE = "merge_snapshot_records"
DOWNSTREAM_TABLE = "merge_downstream_records"
PUBLICATION_TABLE = "merge_snapshot_publications"

PUBLISHED = "已发布"
PUBLISHING = "发布中"
INTERRUPTED = "已中断"

STAGE_BEFORE_SNAPSHOT = "before_snapshot"
STAGE_SNAPSHOT_WRITTEN = "snapshot_written"
STAGE_PUBLISHED = "published"

DELETE = "DELETE"
UPSERT = "UPSERT"

_KEY_FIELDS = ("merge_key", "业务标识", "订单键", "order_id", "主键")
_SEQ_FIELDS = ("变更序号", "change_seq", "changeSeq", "sequence", "seq", "lsn", "scn")
_VERSION_FIELDS = ("源表版本", "source_version", "sourceVersion", "table_version")
_SORT_FIELDS = ("排序值", "sort_value", "updated_at", "更新时间", "update_time")
_OP_FIELDS = ("操作类型", "operation", "op", "change_type", "变更类型")
_DELETE_FIELDS = ("is_deleted", "删除标记", "deleted", "isDeleted")
_TRUE_TEXT = {"true", "1", "是", "y", "yes", "已删除", "删除", "deleted", "delete"}


def demo_merge_records(batch: int) -> list[dict[str, Any]]:
    """两批演示变更：先到的是较新的删除，后到的是序号更小的迟到更新。

    第二批更新虽然 ``updated_at`` 更晚，但变更序号 90 小于删除的 100，
    终态必须保持删除；同时带入一条正常更新，验证快照并非一律删除。
    """
    if batch == 1:
        return [{
            "id": "ORD-20260101-001",
            "status": "已取消",
            "amount": 199.0,
            "updated_at": "2026-01-01T10:00:00",
            "is_deleted": True,
            "变更序号": 100,
            "源表版本": "v5",
        }]
    return [
        {
            "id": "ORD-20260101-001",
            "status": "已支付",
            "amount": 199.0,
            "updated_at": "2026-01-01T10:30:00",
            "is_deleted": False,
            "变更序号": 90,
            "源表版本": "v5",
        },
        {
            "id": "ORD-20260101-002",
            "status": "已发货",
            "amount": 88.0,
            "updated_at": "2026-01-01T10:20:00",
            "is_deleted": False,
            "变更序号": 95,
            "源表版本": "v5",
        },
    ]


def demo_delete_conflict_records(batch: int) -> list[dict[str, Any]]:
    """只含同一订单键的冲突：删除序号 100 先发布，序号 90 的更新迟到。

    CDC 核对用这条最小样例，下游最终只应读到删除结果。
    """
    return [
        record for record in demo_merge_records(batch)
        if record["id"] == "ORD-20260101-001"
    ]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _pick(record: dict[str, Any], fields: tuple[str, ...]) -> Any:
    for field in fields:
        if field in record and record[field] is not None:
            return record[field]
    lowered = {str(key).casefold(): value for key, value in record.items()}
    for field in fields:
        value = lowered.get(field.casefold())
        if value is not None:
            return value
    return None


def _as_delete(record: dict[str, Any]) -> bool:
    for field in _DELETE_FIELDS:
        if field not in record:
            continue
        value = record[field]
        if isinstance(value, bool):
            if value:
                return True
            continue
        if str(value).strip().casefold() in _TRUE_TEXT:
            return True
    operation = str(_pick(record, _OP_FIELDS) or "").strip().upper()
    return operation in {"DELETE", "DEL", "D", "REMOVE", "删除"}


def _ordered_rank(value: Any) -> tuple[int, float, str]:
    """把序号/版本/排序值归一成可比较三元组，数字优先、文本兜底。"""
    if value is None or str(value).strip() == "":
        return (0, 0.0, "")
    text = str(value).strip()
    try:
        return (2, float(text), "")
    except ValueError:
        return (1, 0.0, text)


class MergeSnapshotService:
    """终态快照的唯一写入点；CDC 捕获与增量合并都通过这里发布。"""

    # ───────────────────────────────
    # 批次与发布
    # ───────────────────────────────

    def submit_batch(
        self,
        policy: dict[str, Any],
        *,
        records: list[dict[str, Any]],
        source: str = "upsert_merge",
        source_version: Any = None,
        batch_id: str | None = None,
        cdc_id: int | None = None,
        interrupt: str | None = None,
    ) -> dict[str, Any]:
        policy_id = int(policy.get("id", 0))
        merge_key = str(policy.get("合并键列") or "id")
        version_value = source_version if source_version not in (None, "") else _pick(policy, _VERSION_FIELDS)

        raw_records = [dict(record) for record in (records or []) if isinstance(record, dict)]
        if not raw_records:
            return {"ok": False, "error": "没有可合并的变更记录"}

        if not batch_id:
            payload = json.dumps(raw_records, sort_keys=True, ensure_ascii=False, default=str)
            digest = hashlib.sha1(f"{policy_id}:{version_value}:{payload}".encode("utf-8")).hexdigest()[:12]
            batch_id = f"B{policy_id}-{digest}"

        # 上一次发布中断时先补齐，避免后续批次越过待发布的终态。
        recovered = self._complete_pending(policy)

        existing = self._find_batch(policy_id, batch_id)
        if existing is not None:
            if interrupt:
                result = self.interrupt_publish(policy, stage=interrupt)
                result["duplicate"] = True
                result["batch"] = self._batch_summary(existing)
                return result
            return {
                "ok": True,
                "duplicate": True,
                "message": f"批次 {batch_id} 已应用，重复提交不会改变终态",
                "batch": self._batch_summary(existing),
                "snapshot": self.get_snapshot(policy),
                "downstream": self.get_downstream(policy),
                "consistency": self.consistency(policy),
                "recovered": recovered,
            }

        arrival = self._next_arrival(policy_id)
        normalized: list[dict[str, Any]] = []
        bad_indexes: list[int] = []
        for index, record in enumerate(raw_records):
            item = self._normalize(record, merge_key, version_value, batch_id, source, cdc_id, arrival + index)
            if not item["key"]:
                bad_indexes.append(index)
                continue
            normalized.append(item)
        if bad_indexes:
            return {"ok": False, "error": f"第 {bad_indexes} 条记录缺少合并键 {merge_key}"}

        batch = {
            "batch_id": batch_id,
            "policy_id": policy_id,
            "target_table": policy.get("目标表名"),
            "merge_key": merge_key,
            "source": source,
            "cdc_id": cdc_id,
            "source_version": version_value,
            "records": normalized,
            "record_count": len(normalized),
            "change_seq_max": max((r["seq_num"] for r in normalized if r["seq_num"] is not None), default=None),
            "delete_seq_max": max((r["seq_num"] for r in normalized if r["deleted"] and r["seq_num"] is not None), default=None),
            "submitted_at": _now(),
        }
        store.rows(BATCH_TABLE).append(batch)

        result = self.publish(policy, trigger_batch_id=batch_id, interrupt=interrupt)
        result["batch"] = self._batch_summary(batch)
        result["recovered"] = recovered
        return result

    def publish(
        self,
        policy: dict[str, Any],
        *,
        trigger_batch_id: str | None = None,
        interrupt: str | None = None,
    ) -> dict[str, Any]:
        policy_id = int(policy.get("id", 0))
        batches = self._batches(policy_id)
        rows = self._build_snapshot_rows(policy, batches)
        version = self._next_version(policy_id)
        publication = {
            "publication_id": max((int(p.get("publication_id", 0)) for p in self._publications(policy_id)), default=0) + 1,
            "version": version,
            "policy_id": policy_id,
            "target_table": policy.get("目标表名"),
            "batch_ids": [batch["batch_id"] for batch in batches],
            "trigger_batch_id": trigger_batch_id,
            "record_count": len(rows),
            "deleted_count": sum(1 for row in rows if row["is_deleted"]),
            "status": PUBLISHING,
            "stage": STAGE_BEFORE_SNAPSHOT,
            "started_at": _now(),
            "published_at": None,
            "interrupted_at": None,
        }
        store.rows(PUBLICATION_TABLE).append(publication)

        if interrupt == STAGE_BEFORE_SNAPSHOT:
            self._mark_interrupted(publication)
            return self._publish_result(False, policy, publication, "发布在写入终态快照前中断，可重试补齐")

        self._write_table_rows(SNAPSHOT_TABLE, policy, rows, version)
        publication["stage"] = STAGE_SNAPSHOT_WRITTEN

        if interrupt == STAGE_SNAPSHOT_WRITTEN:
            self._mark_interrupted(publication)
            return self._publish_result(
                False, policy, publication,
                "终态快照已写入但下游尚未发布，重试会补齐下游记录",
            )

        self._write_table_rows(DOWNSTREAM_TABLE, policy, rows, version)
        publication["status"] = PUBLISHED
        publication["stage"] = STAGE_PUBLISHED
        publication["published_at"] = _now()
        return self._publish_result(True, policy, publication, "终态合并数据快照与下游记录已同版本发布")

    def retry(self, policy: dict[str, Any]) -> dict[str, Any]:
        """重试待补齐的发布；没有待补齐项时做一次一致性自检。"""
        pending = self._pending_publication(int(policy.get("id", 0)))
        if pending is not None:
            recovered = self._complete_pending(policy)
            return {
                "ok": True,
                "recovered": recovered,
                "message": "已补齐中断的终态合并数据快照发布",
                "snapshot": self.get_snapshot(policy),
                "downstream": self.get_downstream(policy),
                "consistency": self.consistency(policy),
            }

        check = self.consistency(policy)
        if check["consistent"]:
            return {
                "ok": True,
                "recovered": False,
                "message": "终态合并数据快照与下游记录一致，无需补齐",
                "snapshot": self.get_snapshot(policy),
                "downstream": self.get_downstream(policy),
                "consistency": check,
            }

        result = self.publish(policy)
        result["recovered"] = True
        result["message"] = "检测到快照与下游不一致，已按来源批次重新发布"
        return result

    def interrupt_publish(self, policy: dict[str, Any], *, stage: str = STAGE_SNAPSHOT_WRITTEN) -> dict[str, Any]:
        """按指定阶段模拟一次发布中断，供重试补齐路径使用。"""
        if stage not in (STAGE_BEFORE_SNAPSHOT, STAGE_SNAPSHOT_WRITTEN):
            stage = STAGE_SNAPSHOT_WRITTEN
        if not self._batches(int(policy.get("id", 0))):
            return {"ok": False, "error": "没有可发布的来源批次"}
        return self.publish(policy, interrupt=stage)

    # ───────────────────────────────
    # 查询
    # ───────────────────────────────

    def get_snapshot(self, policy: dict[str, Any]) -> dict[str, Any]:
        policy_id = int(policy.get("id", 0))
        rows = sorted(self._table_rows(SNAPSHOT_TABLE, policy_id), key=lambda row: str(row.get("merge_key", "")))
        publication = self._latest_publication(policy_id)
        pending = publication if publication and publication.get("status") != PUBLISHED else None
        return {
            "policy_id": policy_id,
            "策略编号": policy.get("策略编号"),
            "target_table": policy.get("目标表名"),
            "merge_key": policy.get("合并键列"),
            "snapshot_version": max((int(row.get("快照版本", 0)) for row in rows), default=0),
            "downstream_version": self._table_version(DOWNSTREAM_TABLE, policy_id),
            "published_at": publication.get("published_at") if publication else None,
            "pending_publication": self._publication_summary(pending) if pending else None,
            "record_count": len(rows),
            "deleted_count": sum(1 for row in rows if row.get("is_deleted")),
            "items": [dict(row) for row in rows],
            "records": [dict(row) for row in rows],
            "visible": [dict(row) for row in rows if not row.get("is_deleted")],
            "deleted": [dict(row) for row in rows if row.get("is_deleted")],
            "consistency": self.consistency(policy),
        }

    def get_downstream(self, policy: dict[str, Any]) -> dict[str, Any]:
        policy_id = int(policy.get("id", 0))
        rows = sorted(self._table_rows(DOWNSTREAM_TABLE, policy_id), key=lambda row: str(row.get("merge_key", "")))
        return {
            "policy_id": policy_id,
            "target_table": policy.get("目标表名"),
            "version": self._table_version(DOWNSTREAM_TABLE, policy_id),
            "items": [dict(row) for row in rows],
            "visible": [dict(row) for row in rows if not row.get("is_deleted")],
            "deleted": [dict(row) for row in rows if row.get("is_deleted")],
            "consistency": self.consistency(policy),
        }

    def consistency(self, policy: dict[str, Any]) -> dict[str, Any]:
        policy_id = int(policy.get("id", 0))
        snapshot_rows = self._table_rows(SNAPSHOT_TABLE, policy_id)
        downstream_rows = self._table_rows(DOWNSTREAM_TABLE, policy_id)
        snapshot_version = max((int(row.get("快照版本", 0)) for row in snapshot_rows), default=0)
        downstream_version = max((int(row.get("快照版本", 0)) for row in downstream_rows), default=0)

        snapshot_map = {str(row.get("merge_key")): self._signature(row) for row in snapshot_rows}
        downstream_map = {str(row.get("merge_key")): self._signature(row) for row in downstream_rows}
        keys = sorted(set(snapshot_map) | set(downstream_map))
        divergent = [
            key for key in keys
            if snapshot_map.get(key) != downstream_map.get(key)
        ]

        pending = self._pending_publication(policy_id)
        consistent = not divergent and snapshot_version == downstream_version and pending is None
        reasons: list[str] = []
        if pending is not None:
            reasons.append(f"发布版本 {pending.get('version')} 尚未完成（{pending.get('stage')}）")
        if snapshot_version != downstream_version:
            reasons.append(f"快照版本 {snapshot_version} 与下游版本 {downstream_version} 不一致")
        if divergent:
            reasons.append(f"{len(divergent)} 个合并键在快照与下游之间口径分裂")

        return {
            "consistent": consistent,
            "snapshot_version": snapshot_version,
            "downstream_version": downstream_version,
            "divergent_keys": divergent,
            "pending_publication": self._publication_summary(pending) if pending else None,
            "reason": "；".join(reasons) if reasons else "终态快照与下游记录同版本一致",
        }

    def get_batches(self, policy: dict[str, Any]) -> list[dict[str, Any]]:
        return [self._batch_summary(batch) for batch in self._batches(int(policy.get("id", 0)))]

    # ───────────────────────────────
    # 归并
    # ───────────────────────────────

    def _normalize(
        self,
        record: dict[str, Any],
        merge_key: str,
        batch_version: Any,
        batch_id: str,
        source: str,
        cdc_id: int | None,
        arrival: int,
    ) -> dict[str, Any]:
        key_value = _pick(record, (merge_key, *_KEY_FIELDS))
        key = "" if key_value is None else str(key_value).strip()
        deleted = _as_delete(record)
        operation = str(_pick(record, _OP_FIELDS) or "").strip().upper()
        if deleted:
            operation = DELETE
        elif not operation:
            operation = UPSERT

        seq_value = _pick(record, _SEQ_FIELDS)
        seq_num: float | None = None
        try:
            if seq_value is not None and str(seq_value).strip() != "":
                seq_num = float(str(seq_value).strip())
        except ValueError:
            seq_num = None
        version_value = _pick(record, _VERSION_FIELDS)
        if version_value in (None, ""):
            version_value = batch_version
        sort_value = _pick(record, _SORT_FIELDS)

        rank = (
            _ordered_rank(seq_value),
            _ordered_rank(version_value),
            2 if deleted else 1,
            _ordered_rank(sort_value),
            arrival,
        )
        return {
            "key": key,
            "deleted": deleted,
            "operation": DELETE if deleted else UPSERT,
            "raw_operation": operation,
            "seq": seq_value,
            "seq_num": seq_num,
            "version": version_value,
            "sort_value": sort_value,
            "payload": dict(record),
            "batch_id": batch_id,
            "source": source,
            "cdc_id": cdc_id,
            "arrival": arrival,
            "rank": rank,
        }

    def _build_snapshot_rows(self, policy: dict[str, Any], batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        winners: dict[str, dict[str, Any]] = {}
        for batch in batches:
            for record in batch.get("records", []):
                current = winners.get(record["key"])
                if current is None or record["rank"] > current["rank"]:
                    winners[record["key"]] = record

        rows: list[dict[str, Any]] = []
        for index, key in enumerate(sorted(winners), start=1):
            winner = winners[key]
            rows.append({
                # 快照以业务键作为对外标识，原始订单键不会因为重建而丢失。
                "id": key,
                "snapshot_id": index,
                "policy_id": int(policy.get("id", 0)),
                "target_table": policy.get("目标表名"),
                "merge_key": key,
                "business_key": key,
                "业务标识": key,
                "订单键": key,
                "is_deleted": winner["deleted"],
                "删除标记": winner["deleted"],
                "deleted": winner["deleted"],
                "排序值": winner["sort_value"],
                "最终排序值": winner["sort_value"],
                "变更序号": winner["seq"],
                "源表版本": winner["version"],
                "来源批次": winner["batch_id"],
                "来源类型": winner["source"],
                "cdc_id": winner["cdc_id"],
                "操作": winner["operation"],
                "状态": "已删除" if winner["deleted"] else "有效",
                "payload": winner["payload"],
            })
        return rows

    # ───────────────────────────────
    # 发布落盘
    # ───────────────────────────────

    def _publish_result(self, ok: bool, policy: dict[str, Any], publication: dict[str, Any], message: str) -> dict[str, Any]:
        return {
            "ok": ok,
            "interrupted": not ok,
            "message": message,
            "publication": self._publication_summary(publication),
            "snapshot": self.get_snapshot(policy),
            "downstream": self.get_downstream(policy),
            "consistency": self.consistency(policy),
        }

    def _complete_pending(self, policy: dict[str, Any]) -> dict[str, Any] | None:
        policy_id = int(policy.get("id", 0))
        pending = self._pending_publication(policy_id)
        if pending is None:
            return None

        version = int(pending.get("version", 0))
        rows = self._table_rows(SNAPSHOT_TABLE, policy_id)
        if pending.get("stage") == STAGE_BEFORE_SNAPSHOT or not rows:
            rows = self._build_snapshot_rows(policy, self._batches(policy_id))
            self._write_table_rows(SNAPSHOT_TABLE, policy, rows, version)
            pending["stage"] = STAGE_SNAPSHOT_WRITTEN

        snapshot_rows = [dict(row) for row in rows]
        self._write_table_rows(DOWNSTREAM_TABLE, policy, snapshot_rows, version)
        pending["status"] = PUBLISHED
        pending["stage"] = STAGE_PUBLISHED
        pending["published_at"] = _now()
        pending["record_count"] = len(snapshot_rows)
        pending["deleted_count"] = sum(1 for row in snapshot_rows if row.get("is_deleted"))
        return {"publication": self._publication_summary(pending), "message": "待补齐的发布已恢复"}

    def _write_table_rows(self, table: str, policy: dict[str, Any], rows: list[dict[str, Any]], version: int) -> None:
        policy_id = int(policy.get("id", 0))
        kept = [row for row in store.rows(table) if int(row.get("policy_id", 0)) != policy_id]
        for row in rows:
            item = dict(row)
            item["policy_id"] = policy_id
            item["快照版本"] = version
            item["snapshot_version"] = version
            kept.append(item)
        store.replace(table, kept)

    def _mark_interrupted(self, publication: dict[str, Any]) -> None:
        publication["status"] = INTERRUPTED
        publication["interrupted_at"] = _now()

    # ───────────────────────────────
    # 内部查询
    # ───────────────────────────────

    def _batches(self, policy_id: int) -> list[dict[str, Any]]:
        return [batch for batch in store.rows(BATCH_TABLE) if int(batch.get("policy_id", 0)) == policy_id]

    def _find_batch(self, policy_id: int, batch_id: str) -> dict[str, Any] | None:
        for batch in self._batches(policy_id):
            if batch.get("batch_id") == batch_id:
                return batch
        return None

    def _next_arrival(self, policy_id: int) -> int:
        return max(
            (int(record.get("arrival", 0)) for batch in self._batches(policy_id) for record in batch.get("records", [])),
            default=0,
        ) + 1

    def _publications(self, policy_id: int) -> list[dict[str, Any]]:
        return [pub for pub in store.rows(PUBLICATION_TABLE) if int(pub.get("policy_id", 0)) == policy_id]

    def _latest_publication(self, policy_id: int) -> dict[str, Any] | None:
        publications = self._publications(policy_id)
        if not publications:
            return None
        return max(publications, key=lambda pub: int(pub.get("version", 0)))

    def _pending_publication(self, policy_id: int) -> dict[str, Any] | None:
        latest = self._latest_publication(policy_id)
        if latest is not None and latest.get("status") != PUBLISHED:
            return latest
        return None

    def _next_version(self, policy_id: int) -> int:
        versions = [int(pub.get("version", 0)) for pub in self._publications(policy_id)]
        versions.append(self._table_version(SNAPSHOT_TABLE, policy_id))
        versions.append(self._table_version(DOWNSTREAM_TABLE, policy_id))
        return max(versions, default=0) + 1

    def _table_rows(self, table: str, policy_id: int) -> list[dict[str, Any]]:
        return [row for row in store.rows(table) if int(row.get("policy_id", 0)) == policy_id]

    def _table_version(self, table: str, policy_id: int) -> int:
        return max((int(row.get("快照版本", 0)) for row in self._table_rows(table, policy_id)), default=0)

    def _batch_summary(self, batch: dict[str, Any]) -> dict[str, Any]:
        return {
            "batch_id": batch.get("batch_id"),
            "来源批次": batch.get("batch_id"),
            "policy_id": batch.get("policy_id"),
            "target_table": batch.get("target_table"),
            "source": batch.get("source"),
            "cdc_id": batch.get("cdc_id"),
            "source_version": batch.get("source_version"),
            "源表版本": batch.get("source_version"),
            "record_count": batch.get("record_count", 0),
            "change_seq_max": batch.get("change_seq_max"),
            "变更序号": batch.get("change_seq_max"),
            "delete_seq_max": batch.get("delete_seq_max"),
            "submitted_at": batch.get("submitted_at"),
        }

    def _publication_summary(self, publication: dict[str, Any] | None) -> dict[str, Any] | None:
        if publication is None:
            return None
        return {
            "publication_id": publication.get("publication_id"),
            "version": publication.get("version"),
            "status": publication.get("status"),
            "stage": publication.get("stage"),
            "batch_ids": list(publication.get("batch_ids", [])),
            "record_count": publication.get("record_count", 0),
            "deleted_count": publication.get("deleted_count", 0),
            "started_at": publication.get("started_at"),
            "published_at": publication.get("published_at"),
            "interrupted_at": publication.get("interrupted_at"),
        }

    def _signature(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            bool(row.get("is_deleted")),
            row.get("排序值"),
            row.get("变更序号"),
            row.get("源表版本"),
            row.get("来源批次"),
            int(row.get("快照版本", 0)),
        )


merge_snapshot_service = MergeSnapshotService()
