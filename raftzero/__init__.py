"""
raftzero
~~~~~~~~
Deterministic Distributed Raft Consensus Engine & Byzantine Fault-Injection Simulation Runtime.
"""
from __future__ import annotations

from raftzero.chaos import ByzantineInterceptor, ChaosHarness
from raftzero.cluster import RaftCluster
from raftzero.invariants import InvariantAuditor, InvariantViolation
from raftzero.network import NetworkPacket, NetworkRouter
from raftzero.node import RaftNode
from raftzero.state_machine import StateMachine
from raftzero.storage import PersistentStorage
from raftzero.types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    ClientRequest,
    ClientResponse,
    InstallSnapshotArgs,
    InstallSnapshotReply,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
    Role,
)

__version__ = "0.1.0"
__all__ = [
    "RaftCluster",
    "RaftNode",
    "Role",
    "LogEntry",
    "RequestVoteArgs",
    "RequestVoteReply",
    "AppendEntriesArgs",
    "AppendEntriesReply",
    "InstallSnapshotArgs",
    "InstallSnapshotReply",
    "ClientRequest",
    "ClientResponse",
    "PersistentStorage",
    "StateMachine",
    "NetworkRouter",
    "NetworkPacket",
    "InvariantAuditor",
    "InvariantViolation",
    "ChaosHarness",
    "ByzantineInterceptor",
]
