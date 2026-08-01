from pathlib import Path

from bot import Database, humanize_bytes


def test_activity_log_and_quota_management(tmp_path):
    db = Database(str(tmp_path / "test.db"))

    db.log_activity("create_vps", target="vps-001", detail="created", actor="123")
    logs = db.get_recent_activity(limit=5)
    assert logs[0]["action"] == "create_vps"
    assert logs[0]["target"] == "vps-001"

    db.set_user_quota("123", 5)
    assert db.get_user_quota("123") == 5
    assert db.get_quota_limit("123", default=2) == 5


def test_snapshot_tracking_and_cleanup(tmp_path):
    db = Database(str(tmp_path / "test.db"))

    db.add_snapshot("vps-001", "snap-001", "/tmp/snap.tar", "123", 1024)
    snapshots = db.list_snapshots("vps-001")
    assert snapshots[0]["snapshot_id"] == "snap-001"
    assert db.delete_snapshot("snap-001") is True
    assert db.list_snapshots("vps-001") == []


def test_humanize_bytes_formats_sizes():
    assert humanize_bytes(1024) == "1.0 KB"
    assert humanize_bytes(1536) == "1.5 KB"
