"""
Unit tests for PersistentStorage, log indexing, and snapshot compaction.
"""
import tempfile
from pathlib import Path

import pytest
from raftzero.storage import PersistentStorage
from raftzero.types import LogEntry


def test_storage_basic_operations():
    storage = PersistentStorage("node-1")
    assert storage.last_log_index == 0
    assert storage.last_log_term == 0
    assert storage.current_term == 0
    assert storage.voted_for is None

    storage.set_term_and_vote(1, "node-1")
    assert storage.current_term == 1
    assert storage.voted_for == "node-1"

    e1 = LogEntry(term=1, index=1, command={"op": "SET", "key": "x", "val": 10})
    e2 = LogEntry(term=1, index=2, command={"op": "SET", "key": "y", "val": 20})
    storage.append_entries([e1, e2])

    assert storage.last_log_index == 2
    assert storage.last_log_term == 1
    assert storage.get_term_at(1) == 1
    assert storage.get_term_at(2) == 1
    assert storage.get_entry(1) == e1
    assert storage.get_entry(2) == e2


def test_storage_truncation_on_conflict():
    storage = PersistentStorage("node-1")
    e1 = LogEntry(term=1, index=1, command="cmd1")
    e2 = LogEntry(term=1, index=2, command="cmd2")
    e3 = LogEntry(term=1, index=3, command="cmd3")
    storage.append_entries([e1, e2, e3])
    assert storage.last_log_index == 3

    # Conflict at index 2 with term 2
    e2_new = LogEntry(term=2, index=2, command="cmd2_alt")
    storage.append_entries([e2_new])

    assert storage.last_log_index == 2
    assert storage.get_entry(2) == e2_new
    assert storage.get_entry(3) is None


def test_storage_snapshot_compaction():
    storage = PersistentStorage("node-1")
    entries = [LogEntry(term=1, index=i, command=f"cmd{i}") for i in range(1, 6)]
    storage.append_entries(entries)
    assert storage.last_log_index == 5

    # Compact up to index 3
    snapshot_bytes = b"test-snapshot-bytes"
    storage.compact_log(last_included_index=3, last_included_term=1, snapshot=snapshot_bytes)

    assert storage.last_included_index == 3
    assert storage.last_included_term == 1
    assert storage.last_log_index == 5
    assert storage.get_term_at(3) == 1
    assert storage.get_entry(3) is None  # Entry 3 was discarded into snapshot
    assert storage.get_entry(4).command == "cmd4"
    assert storage.get_entry(5).command == "cmd5"


def test_storage_persistence_wal():
    with tempfile.TemporaryDirectory() as tmpdir:
        storage1 = PersistentStorage("node-persisted", storage_dir=tmpdir)
        storage1.set_term_and_vote(3, "candidate-A")
        e1 = LogEntry(term=3, index=1, command={"op": "SET", "key": "p", "val": 999})
        storage1.append_entries([e1])
        storage1.compact_log(1, 3, b"wal-snap")

        # Reload from same directory
        storage2 = PersistentStorage("node-persisted", storage_dir=tmpdir)
        assert storage2.current_term == 3
        assert storage2.voted_for == "candidate-A"
        assert storage2.last_included_index == 1
        assert storage2.last_included_term == 3
        assert storage2.snapshot_data == b"wal-snap"
