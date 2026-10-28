"""
raftzero.types
~~~~~~~~~~~~~~
Core data types, message schemas, and state representations for the Raft consensus protocol.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple


class Role(Enum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"


@dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    command: Any  # E.g. {"op": "SET", "key": "k", "val": "v"} or no-op
    client_id: Optional[str] = None
    seq_num: int = 0


@dataclass
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool
    from_id: str = ""


@dataclass
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int
    from_id: str = ""
    conflict_term: Optional[int] = None
    conflict_index: Optional[int] = None


@dataclass
class InstallSnapshotArgs:
    term: int
    leader_id: str
    last_included_index: int
    last_included_term: int
    data: bytes


@dataclass
class InstallSnapshotReply:
    term: int
    from_id: str = ""


@dataclass
class ClientRequest:
    command: Any
    client_id: str
    seq_num: int


@dataclass
class ClientResponse:
    success: bool
    leader_id: Optional[str]
    result: Any = None
    error: Optional[str] = None
