"""
raftzero.storage
~~~~~~~~~~~~~~~~
Persistent storage abstraction, Write-Ahead Log (WAL), and snapshot compaction.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from raftzero.types import LogEntry


class PersistentStorage:
    """
    Manages stable persistent state for a Raft node.
    Ensures state transitions (term updates, votes, log appends) survive crashes.
    Supports log compaction and snapshot storage.
    """

    def __init__(self, node_id: str, storage_dir: Optional[str] = None) -> None:
        self.node_id = node_id
        self.storage_dir = Path(storage_dir) if storage_dir else None
        
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.entries: List[LogEntry] = []  # Index 0 corresponds to last_included_index
        
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        self.snapshot_data: bytes = b""

        if self.storage_dir:
            self.storage_dir.mkdir(parents=True, exist_ok=True)
            self._state_file = self.storage_dir / f"{self.node_id}_state.json"
            self._snapshot_file = self.storage_dir / f"{self.node_id}_snapshot.bin"
            self._load()

    @property
    def last_log_index(self) -> int:
        if self.entries:
            return self.entries[-1].index
        return self.last_included_index

    @property
    def last_log_term(self) -> int:
        if self.entries:
            return self.entries[-1].term
        return self.last_included_term

    def get_term_at(self, index: int) -> int:
        if index == self.last_included_index:
            return self.last_included_term
        offset = index - (self.last_included_index + 1)
        if 0 <= offset < len(self.entries):
            return self.entries[offset].term
        raise IndexError(f"Log index {index} out of range (last_included={self.last_included_index}, len={len(self.entries)})")

    def get_entry(self, index: int) -> Optional[LogEntry]:
        offset = index - (self.last_included_index + 1)
        if 0 <= offset < len(self.entries):
            return self.entries[offset]
        return None

    def get_entries_from(self, start_index: int) -> List[LogEntry]:
        if start_index <= self.last_included_index:
            offset = 0
        else:
            offset = start_index - (self.last_included_index + 1)
        if offset < 0:
            offset = 0
        return list(self.entries[offset:])

    def append_entries(self, new_entries: List[LogEntry]) -> None:
        for entry in new_entries:
            offset = entry.index - (self.last_included_index + 1)
            if offset < len(self.entries):
                # Truncate conflicting log entries
                self.entries = self.entries[:offset]
            self.entries.append(entry)
        self._persist()

    def set_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        self.current_term = term
        self.voted_for = voted_for
        self._persist()

    def compact_log(self, last_included_index: int, last_included_term: int, snapshot: bytes) -> None:
        """Compact logs strictly up to last_included_index."""
        if last_included_index <= self.last_included_index:
            return
        offset = last_included_index - self.last_included_index
        if offset < len(self.entries):
            self.entries = self.entries[offset:]
        else:
            self.entries = []
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.snapshot_data = snapshot
        self._persist()

    def install_snapshot(self, last_included_index: int, last_included_term: int, snapshot: bytes) -> None:
        self.last_included_index = last_included_index
        self.last_included_term = last_included_term
        self.snapshot_data = snapshot
        self.entries = []
        self._persist()

    def _persist(self) -> None:
        if not self.storage_dir:
            return
        payload = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "entries": [asdict(e) for e in self.entries],
        }
        temp_file = self.storage_dir / f"{self.node_id}_state.tmp"
        with open(temp_file, "w") as f:
            json.dump(payload, f)
        temp_file.replace(self._state_file)

        if self.snapshot_data:
            snap_tmp = self.storage_dir / f"{self.node_id}_snap.tmp"
            with open(snap_tmp, "wb") as f:
                f.write(self.snapshot_data)
            snap_tmp.replace(self._snapshot_file)

    def _load(self) -> None:
        if self._state_file.exists():
            with open(self._state_file, "r") as f:
                data = json.load(f)
            self.current_term = data.get("current_term", 0)
            self.voted_for = data.get("voted_for", None)
            self.last_included_index = data.get("last_included_index", 0)
            self.last_included_term = data.get("last_included_term", 0)
            self.entries = [LogEntry(**e) for e in data.get("entries", [])]

        if self._snapshot_file.exists():
            with open(self._snapshot_file, "rb") as f:
                self.snapshot_data = f.read()
