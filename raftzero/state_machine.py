"""
raftzero.state_machine
~~~~~~~~~~~~~~~~~~~~~~
Deterministic Key-Value State Machine with atomic CAS, linearizable deduplication, and snapshots.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple


class StateMachine:
    """
    Deterministic state machine interface applied sequentially by Raft commits.
    """

    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        # client_id -> (last_seq_num, last_response)
        self._client_sessions: Dict[str, Tuple[int, Any]] = {}

    def apply(self, command: Any, client_id: Optional[str] = None, seq_num: int = 0) -> Any:
        """
        Applies a command deterministically.
        Supports linearizable deduplication for client sessions.
        """
        if client_id and seq_num > 0:
            if client_id in self._client_sessions:
                last_seq, last_resp = self._client_sessions[client_id]
                if seq_num <= last_seq:
                    return last_resp

        result = self._execute(command)

        if client_id and seq_num > 0:
            self._client_sessions[client_id] = (seq_num, result)

        return result

    def _execute(self, command: Any) -> Any:
        if not isinstance(command, dict):
            return None

        op = command.get("op", "").upper()
        key = str(command.get("key", ""))

        if op == "SET":
            val = command.get("val")
            self._data[key] = val
            return val

        elif op == "GET":
            return self._data.get(key, None)

        elif op == "DEL":
            return self._data.pop(key, None) is not None

        elif op == "CAS":
            # Compare-and-swap
            expected = command.get("expected")
            new_val = command.get("val")
            current = self._data.get(key, None)
            if current == expected:
                self._data[key] = new_val
                return True
            return False

        elif op == "INCR":
            # Atomic increment
            delta = int(command.get("delta", 1))
            curr = int(self._data.get(key, 0))
            new_val = curr + delta
            self._data[key] = new_val
            return new_val

        elif op == "APPEND":
            val = command.get("val", "")
            curr = self._data.get(key, "")
            if isinstance(curr, list):
                curr.append(val)
                self._data[key] = curr
                return list(curr)
            else:
                new_str = str(curr) + str(val)
                self._data[key] = new_str
                return new_str

        elif op == "NOOP":
            return None

        return None

    def take_snapshot(self) -> bytes:
        payload = {
            "data": self._data,
            "client_sessions": {k: list(v) for k, v in self._client_sessions.items()},
        }
        return json.dumps(payload, sort_keys=True).encode("utf-8")

    def restore_snapshot(self, snapshot_data: bytes) -> None:
        if not snapshot_data:
            self._data = {}
            self._client_sessions = {}
            return
        payload = json.loads(snapshot_data.decode("utf-8"))
        self._data = payload.get("data", {})
        self._client_sessions = {
            k: (v[0], v[1]) for k, v in payload.get("client_sessions", {}).items()
        }

    def get_state_copy(self) -> Dict[str, Any]:
        return dict(self._data)
