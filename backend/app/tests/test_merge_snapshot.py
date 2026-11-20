"""终态合并数据快照测试：迟到更新与较新删除的终态、发布一致性与中断补齐。"""
from __future__ import annotations

import unittest

from app.pipeline_engine import pipeline_engine
from app.services.change_capture import ChangeCaptureService
from app.services.merge_snapshot import merge_snapshot_service
from app.services.upsert_merge import UpsertMergeService
from app.store import store


def _fresh_policy() -> tuple[UpsertMergeService, dict]:
    svc = UpsertMergeService()
    entry, missing = svc.create_entry({
        "策略编号": "UPSE-SNAP", "目标表名": "orders_snap", "合并键列": "id",
        "插入列": "id,status", "更新列": "status", "删除标记": "is_deleted",
        "排序字段": "updated_at", "合并引擎": "merge-on-read",
    })
    assert entry is not None and not missing
    entry["status"] = "已生效"
    return svc, entry


class TestTerminalSnapshot(unittest.TestCase):
    def test_newer_delete_wins_over_late_update(self) -> None:
        svc, policy = _fresh_policy()
        svc.merge_batch(policy["id"], records=[{
            "id": "ORD-1", "status": "已取消", "updated_at": "2026-01-01T10:00:00",
            "is_deleted": True, "变更序号": 100, "源表版本": "v5",
        }])
        result = svc.merge_batch(policy["id"], records=[{
            "id": "ORD-1", "status": "已支付", "updated_at": "2026-01-02T10:00:00",
            "is_deleted": False, "变更序号": 90, "源表版本": "v5",
        }])
        self.assertTrue(result["ok"])
        snapshot = result["snapshot"]
        item = snapshot["items"][0]
        # 迟到的更新排序值更晚，但变更序号更小，终态必须仍是删除。
        self.assertTrue(item["is_deleted"])
        self.assertEqual(item["变更序号"], 100)
        self.assertEqual(item["最终排序值"], "2026-01-01T10:00:00")
        self.assertEqual(item["业务标识"], "ORD-1")
        self.assertEqual(snapshot["deleted_count"], 1)
        self.assertEqual(snapshot["visible"], [])

    def test_snapshot_and_downstream_share_version(self) -> None:
        svc, policy = _fresh_policy()
        svc.run_demo_merge(policy["id"])
        snapshot = svc.get_merge_snapshot(policy["id"])
        downstream = svc.get_downstream_records(policy["id"])
        self.assertEqual(snapshot["snapshot_version"], downstream["version"])
        self.assertTrue(snapshot["consistency"]["consistent"])
        self.assertEqual([row["merge_key"] for row in snapshot["items"]],
                         [row["merge_key"] for row in downstream["items"]])
        self.assertEqual([row["is_deleted"] for row in downstream["visible"]], [False] * len(downstream["visible"]))

    def test_interrupted_publish_is_completed_on_retry(self) -> None:
        svc, policy = _fresh_policy()
        svc.run_demo_merge(policy["id"])
        interrupted = svc.interrupt_merge_snapshot(policy["id"], "snapshot_written")
        self.assertFalse(interrupted["ok"])
        self.assertFalse(interrupted["consistency"]["consistent"])
        self.assertEqual(interrupted["consistency"]["divergent_keys"],
                         [row["merge_key"] for row in svc.get_merge_snapshot(policy["id"])["items"]])

        retried = svc.retry_merge_snapshot(policy["id"])
        self.assertTrue(retried["ok"])
        self.assertTrue(retried["consistency"]["consistent"])
        # 补齐后记录不缺失，业务标识仍然保留。
        keys = [row["merge_key"] for row in retried["snapshot"]["items"]]
        self.assertIn("ORD-20260101-001", keys)
        self.assertIn("ORD-20260101-002", keys)

    def test_later_batch_recovers_pending_publication(self) -> None:
        svc, policy = _fresh_policy()
        svc.run_demo_merge(policy["id"])
        svc.interrupt_merge_snapshot(policy["id"], "snapshot_written")
        result = svc.merge_batch(policy["id"], records=[{
            "id": "ORD-3", "status": "已发货", "updated_at": "2026-01-03T00:00:00",
            "is_deleted": False, "变更序号": 120, "源表版本": "v5",
        }])
        self.assertTrue(result["ok"])
        self.assertIsNotNone(result["recovered"])
        self.assertTrue(result["consistency"]["consistent"])
        self.assertEqual(result["snapshot"]["record_count"], 3)

    def test_snapshot_deleted_but_downstream_valid_is_divergence(self) -> None:
        """判重边界：快照已删除、下游仍读有效记录，必须判为口径分裂。"""
        svc, policy = _fresh_policy()
        svc.merge_batch(policy["id"], records=[{
            "id": "ORD-B1", "status": "已支付", "updated_at": "2026-01-01T00:00:00",
            "is_deleted": False, "变更序号": 10, "源表版本": "v1",
        }])
        # 删除批次写到终态快照后中断，下游仍停留在上一版的有效记录。
        interrupted = svc.merge_batch(policy["id"], records=[{
            "id": "ORD-B1", "status": "已取消", "updated_at": "2026-01-02T00:00:00",
            "is_deleted": True, "变更序号": 20, "源表版本": "v1",
        }], interrupt="snapshot_written")
        self.assertTrue(interrupted["interrupted"])
        snapshot = svc.get_merge_snapshot(policy["id"])
        downstream = svc.get_downstream_records(policy["id"])
        self.assertEqual(snapshot["snapshot_version"], downstream["version"] + 1)
        self.assertTrue(snapshot["items"][0]["is_deleted"])
        self.assertFalse(downstream["items"][0]["is_deleted"])
        self.assertIn("ORD-B1", snapshot["consistency"]["divergent_keys"])
        self.assertFalse(snapshot["consistency"]["consistent"])

        # 补齐后下游只剩删除终态，不再返回有效记录。
        retried = svc.retry_merge_snapshot(policy["id"])
        self.assertTrue(retried["consistency"]["consistent"])
        self.assertTrue(retried["downstream"]["items"][0]["is_deleted"])
        self.assertEqual(retried["downstream"]["visible"], [])

    def test_duplicate_batch_does_not_change_terminal_state(self) -> None:
        svc, policy = _fresh_policy()
        records = [{"id": "ORD-1", "status": "已支付", "updated_at": "2026-01-01T00:00:00",
                    "is_deleted": False, "变更序号": 10, "源表版本": "v1"}]
        first = svc.merge_batch(policy["id"], records=records)
        second = svc.merge_batch(policy["id"], records=records)
        self.assertTrue(second.get("duplicate"))
        self.assertEqual(first["snapshot"]["snapshot_version"], second["snapshot"]["snapshot_version"])
        self.assertEqual(second["snapshot"]["record_count"], 1)


class TestCdcHandoff(unittest.TestCase):
    def test_cdc_capture_and_merge_share_snapshot(self) -> None:
        svc = ChangeCaptureService()
        result = svc.capture_demo_changes(1)
        self.assertTrue(result["ok"])
        snapshot = svc.get_merge_snapshot_for_entry(1)
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["snapshot_version"], snapshot["downstream_version"])
        self.assertTrue(snapshot["consistency"]["consistent"])
        # 删除序号 100 压过迟到的序号 90。
        deleted = snapshot["deleted"][0]
        self.assertEqual(deleted["merge_key"], "ORD-20260101-001")
        self.assertEqual(int(deleted["变更序号"]), 100)
        self.assertEqual(snapshot["cdc"]["变更序号"], "90")
        # 同一订单键先删除后迟到更新：下游最终只读到删除结果。
        downstream = merge_snapshot_service.get_downstream(
            store.find("upsert_merge", 1))
        visible_keys = [row["merge_key"] for row in downstream["visible"]]
        self.assertNotIn("ORD-20260101-001", visible_keys)
        self.assertEqual(visible_keys, [])
        self.assertEqual([row["merge_key"] for row in downstream["deleted"]],
                         ["ORD-20260101-001"])

    def test_cdc_demo_downstream_reads_only_delete(self) -> None:
        """CDC演示只带同一订单键：下游最终只读到删除结果。"""
        svc = ChangeCaptureService()
        result = svc.capture_demo_changes(2)
        self.assertTrue(result["ok"])
        self.assertEqual(result["downstream"]["visible"], [])
        self.assertEqual(
            [(row["merge_key"], row["is_deleted"]) for row in result["downstream"]["items"]],
            [("ORD-20260101-001", True)],
        )
        self.assertEqual(result["snapshot"]["deleted_count"], 1)

    def test_handoff_preserves_source_batch_and_sequence(self) -> None:
        svc = ChangeCaptureService()
        result = svc.capture_changes(
            1,
            records=[{"id": "ORD-H1", "status": "已支付", "updated_at": "2026-01-05T00:00:00",
                      "is_deleted": False}],
            change_seq=101,
            source_version="v6",
            batch_id="CDC-CHAN-0001-101",
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["batch"]["来源批次"], "CDC-CHAN-0001-101")
        entry = store.find("change_capture", 1)
        handoff = entry["_last_handoff"]
        self.assertEqual(handoff["batch_id"], "CDC-CHAN-0001-101")
        self.assertEqual(handoff["change_seq"], 101)
        self.assertEqual(handoff["source_version"], "v6")
        snapshot = svc.get_merge_snapshot_for_entry(1)
        item = next(row for row in snapshot["items"] if row["merge_key"] == "ORD-H1")
        self.assertEqual(item["来源类型"], "change_capture")
        self.assertEqual(item["源表版本"], "v6")
        self.assertEqual(item["变更序号"], 101)

    def test_pipeline_upsert_simulation_writes_snapshot(self) -> None:
        result = pipeline_engine.run_cdc_upsert_simulation(1)
        self.assertTrue(result["ok"])
        self.assertIn("snapshot", result)
        snapshot = result["snapshot"]
        self.assertGreater(snapshot["record_count"], 0)
        self.assertEqual(snapshot["snapshot_version"], snapshot["downstream_version"])
        self.assertTrue(any(row["is_deleted"] for row in snapshot["items"]))


if __name__ == "__main__":
    unittest.main()
