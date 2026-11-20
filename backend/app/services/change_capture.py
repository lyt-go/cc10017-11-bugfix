"""CDC捕获业务规则：配置管理、增量捕获与下游主题联动。

增强：CDC启动时校验目标主题状态，捕获增量后触发合并策略检查。
捕获产物必须向同一份终态合并数据快照提供变更序号与源表版本，
跨模块交接时保留来源批次，避免快照与下游记录落在不同版本。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from app.services.merge_snapshot import demo_delete_conflict_records, demo_merge_records, merge_snapshot_service
from app.store import store

MODULE = "change_capture"
REQUIRED_FIELDS = ["配置编号", "源数据库", "源表名"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已停止"]
ACTION_RULES = {"提交校验": "待校验", "启动捕获": "运行中", "停止捕获": "已停止"}
NEGATIVE_ACTIONS = []


class ChangeCaptureService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get("配置编号") or row.get("源表名") or f"CDC-{row.get('id')}"),
                "code": str(row.get("配置编号", "")),
                "source_table": str(row.get("源表名", "")),
                "has_merge_policy": self._find_upsert(row.get("源表名")) is not None,
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        # 校验目标主题
        target_topic = values.get("目标主题")
        if target_topic:
            topic = self._find_topic(target_topic)
            if not topic:
                return None, [f"目标主题 {target_topic} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        for f in ["快照模式", "捕获列", "变更序号", "增量阶段", "目标主题", "源表版本"]:
            entry[f] = values.get(f, "")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"CDC配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于CDC捕获可执行范围"
        target = ACTION_RULES[action]

        if action == "启动捕获":
            target_topic = entry.get("目标主题")
            topic = self._find_topic(target_topic) if target_topic else None
            if not topic:
                return None, f"目标主题 {target_topic} 未注册"
            if topic.get("status") != "已上线":
                return None, f"目标主题 {target_topic} 未上线"
            # 检查UPSERT策略
            source_table = entry.get("源表名")
            upsert = self._find_upsert(source_table)
            if not upsert:
                return None, f"源表 {source_table} 未配置增量合并策略"
            # 生成捕获产物
            self._generate_capture_artifact(entry, topic, upsert)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"CDC配置已{action}"

    # ───────────────────────────────
    # 终态合并数据快照交接
    # ───────────────────────────────

    def capture_changes(
        self,
        entry_id: int,
        *,
        records: list[dict[str, Any]] | None = None,
        change_seq: Any = None,
        source_version: Any = None,
        batch_id: str | None = None,
        interrupt: str | None = None,
    ) -> dict[str, Any]:
        """把 CDC 捕获到的变更交给增量合并，写入同一份终态快照。

        ``变更序号`` 与 ``源表版本`` 缺省取配置上的最新值，并随来源批次一起交接；
        未显式给批次号时按配置编号 + 序号生成，重复投递不会重复改变终态。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": f"CDC配置 {entry_id} 不存在或已归档"}

        upsert = self._find_upsert(entry.get("源表名"))
        if not upsert:
            return {"ok": False, "error": f"源表 {entry.get('源表名')} 未配置增量合并策略"}
        if upsert.get("status") != "已生效":
            return {"ok": False, "error": f"增量合并策略 {upsert.get('策略编号')} 尚未生效，不能接收CDC变更"}

        seq_value = change_seq if change_seq not in (None, "") else entry.get("变更序号")
        version_value = source_version if source_version not in (None, "") else entry.get("源表版本")
        if version_value in (None, ""):
            version_value = upsert.get("源表版本")
        if version_value in (None, ""):
            version_value = f"cdc-{entry.get('配置编号')}-{seq_value or '0'}"

        payload = [dict(record) for record in (records or []) if isinstance(record, dict)]
        if not payload:
            payload = demo_merge_records(1)
            for record in payload:
                record.setdefault("变更序号", seq_value)
                record.setdefault("源表版本", version_value)
        for record in payload:
            if record.get("变更序号") in (None, "") and seq_value not in (None, ""):
                record["变更序号"] = seq_value
            if record.get("源表版本") in (None, "") and version_value not in (None, ""):
                record["源表版本"] = version_value

        if not batch_id:
            batch_id = f"CDC-{entry.get('配置编号')}-{seq_value or '0'}"

        result = merge_snapshot_service.submit_batch(
            upsert,
            records=payload,
            source="change_capture",
            source_version=version_value,
            batch_id=batch_id,
            cdc_id=int(entry.get("id", 0)),
            interrupt=interrupt,
        )
        result["cdc"] = self._capture_summary(entry, upsert, batch_id, seq_value, version_value)
        if result.get("ok"):
            self._attach_handoff(entry, upsert, batch_id, seq_value, version_value, result)

        self._ensure_artifact_table("cdc_capture_artifacts")
        store.rows("cdc_capture_artifacts").append({
            "cdc_id": entry.get("id"),
            "cdc_code": entry.get("配置编号"),
            "source_db": entry.get("源数据库"),
            "source_table": entry.get("源表名"),
            "target_topic": entry.get("目标主题"),
            "merge_policy": upsert.get("策略编号"),
            "merge_engine": upsert.get("合并引擎"),
            "change_seq": seq_value,
            "source_version": version_value,
            "batch_id": batch_id,
            "captured_at": datetime.now().isoformat(),
            "stage": "streaming",
        })
        return result

    def capture_demo_changes(self, entry_id: int) -> dict[str, Any]:
        """按 CDC 的时序跑两批：删除序号 100 先到，序号 90 的更新迟到。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": f"CDC配置 {entry_id} 不存在或已归档"}

        results: list[dict[str, Any]] = []
        for batch_no, seq in ((1, 100), (2, 90)):
            result = self.capture_changes(
                entry_id,
                records=demo_delete_conflict_records(batch_no),
                change_seq=seq,
                batch_id=f"CDC-{entry.get('配置编号')}-DEMO-{batch_no}",
            )
            if not result.get("ok"):
                return result
            results.append(result)

        upsert = self._find_upsert(entry.get("源表名"))
        snapshot = merge_snapshot_service.get_snapshot(upsert) if upsert else None
        return {
            "ok": True,
            "message": "两批CDC变更已写入终态合并数据快照，删除序号压过迟到更新",
            "batches": [result.get("batch") for result in results],
            "snapshot": snapshot,
            "downstream": merge_snapshot_service.get_downstream(upsert) if upsert else None,
            "consistency": merge_snapshot_service.consistency(upsert) if upsert else None,
        }

    def get_merge_snapshot_for_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        upsert = self._find_upsert(entry.get("源表名"))
        if upsert is None:
            return None
        snapshot = merge_snapshot_service.get_snapshot(upsert)
        snapshot["cdc"] = {
            "cdc_id": entry.get("id"),
            "配置编号": entry.get("配置编号"),
            "变更序号": entry.get("变更序号"),
            "源表版本": entry.get("源表版本"),
        }
        snapshot["batches"] = [
            batch for batch in merge_snapshot_service.get_batches(upsert)
            if batch.get("source") == "change_capture" and batch.get("cdc_id") == entry.get("id")
        ]
        snapshot["downstream"] = merge_snapshot_service.get_downstream(upsert)
        return snapshot

    def _attach_handoff(
        self,
        entry: dict[str, Any],
        upsert: dict[str, Any],
        batch_id: str,
        change_seq: Any,
        source_version: Any,
        result: dict[str, Any],
    ) -> None:
        handoff = {
            "cdc_id": entry.get("id"),
            "config_code": entry.get("配置编号"),
            "merge_policy": upsert.get("策略编号"),
            "policy_id": upsert.get("id"),
            "batch_id": batch_id,
            "source_batch": batch_id,
            "change_seq": change_seq,
            "source_version": source_version,
            "publication_version": (result.get("publication") or {}).get("version"),
            "status": (result.get("publication") or {}).get("status"),
            "handed_off_at": datetime.now().isoformat(),
        }
        entry["_last_handoff"] = handoff
        if change_seq not in (None, ""):
            entry["变更序号"] = str(change_seq)
        if source_version not in (None, ""):
            entry["源表版本"] = str(source_version)
        store.rows("cdc_merge_handoffs").append(dict(handoff))

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _find_upsert(self, table_name: str) -> dict[str, Any] | None:
        for row in store.rows("upsert_merge"):
            if row.get("目标表名") == table_name:
                return row
        return None

    def _generate_capture_artifact(self, entry: dict[str, Any], topic: dict[str, Any], upsert: dict[str, Any]) -> None:
        artifact = {
            "cdc_id": entry.get("id"),
            "cdc_code": entry.get("配置编号"),
            "source_db": entry.get("源数据库"),
            "source_table": entry.get("源表名"),
            "target_topic": topic.get("主题编号"),
            "merge_policy": upsert.get("策略编号"),
            "merge_engine": upsert.get("合并引擎"),
            "change_seq": entry.get("变更序号"),
            "source_version": entry.get("源表版本") or upsert.get("源表版本"),
            "batch_id": f"CDC-{entry.get('配置编号')}-{entry.get('变更序号') or '0'}",
            "captured_at": datetime.now().isoformat(),
            "stage": "streaming",
        }
        self._ensure_artifact_table("cdc_capture_artifacts")
        store.rows("cdc_capture_artifacts").append(artifact)
        entry["_last_capture"] = artifact

    def _capture_summary(
        self,
        entry: dict[str, Any],
        upsert: dict[str, Any],
        batch_id: str,
        change_seq: Any,
        source_version: Any,
    ) -> dict[str, Any]:
        return {
            "cdc_id": entry.get("id"),
            "配置编号": entry.get("配置编号"),
            "源表名": entry.get("源表名"),
            "策略编号": upsert.get("策略编号"),
            "batch_id": batch_id,
            "来源批次": batch_id,
            "变更序号": change_seq,
            "源表版本": source_version,
        }

    def get_capture_artifacts(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("cdc_capture_artifacts")
        return [a for a in store.rows("cdc_capture_artifacts") if a.get("cdc_id") == entry_id]

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
