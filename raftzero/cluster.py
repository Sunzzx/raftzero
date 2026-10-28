"""
raftzero.cluster
~~~~~~~~~~~~~~~~
Cluster coordinator and discrete-event simulation harness.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple

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
    RequestVoteArgs,
    RequestVoteReply,
    Role,
)


class RaftCluster:
    """
    Manages an ensemble of Raft nodes in a deterministic simulated universe.
    Coordinates tick progressions, message dispatch, crashes, and recoveries.
    """

    def __init__(
        self,
        node_ids: List[str],
        storage_dir: Optional[str] = None,
        heartbeat_interval: float = 50.0,
        election_timeout_range: Tuple[float, float] = (150.0, 300.0),
        router: Optional[NetworkRouter] = None,
        seed: Optional[int] = None,
    ) -> None:
        self.node_ids = list(node_ids)
        self.storage_dir = storage_dir
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout_range = election_timeout_range
        self.router = router or NetworkRouter(rng_seed=seed)
        self.seed = seed

        self.nodes: Dict[str, RaftNode] = {}
        self.crashed_nodes: Set[str] = set()
        self.time_ms: float = 0.0
        self._client_seq_counter: int = 0

        for i, nid in enumerate(node_ids):
            node_seed = (seed + i * 1000) if seed is not None else None
            storage = PersistentStorage(nid, storage_dir=storage_dir)
            sm = StateMachine()
            node = RaftNode(
                node_id=nid,
                peers=node_ids,
                storage=storage,
                state_machine=sm,
                election_timeout_range=election_timeout_range,
                heartbeat_interval=heartbeat_interval,
                rng_seed=node_seed,
            )
            self.nodes[nid] = node

    def step(self, dt_ms: float = 10.0) -> None:
        """Advances cluster simulation by dt_ms."""
        self.time_ms += dt_ms

        # 1. Tick all alive nodes
        for nid, node in self.nodes.items():
            if nid not in self.crashed_nodes:
                node.tick(dt_ms)

        # 2. Collect outbox messages and route to network
        for nid, node in self.nodes.items():
            if nid not in self.crashed_nodes:
                while node.outbox:
                    dst, rpc_name, payload = node.outbox.pop(0)
                    self.router.send(
                        current_time_ms=self.time_ms,
                        src=nid,
                        dst=dst,
                        rpc_name=rpc_name,
                        payload=payload,
                        is_reply=False,
                    )

        # 3. Deliver all ready network packets
        ready_packets = self.router.pop_ready_packets(self.time_ms)
        for pkt in ready_packets:
            self._dispatch_packet(pkt)

    def _dispatch_packet(self, pkt: NetworkPacket) -> None:
        """Dispatches a delivered packet to its destination node."""
        if pkt.dst in self.crashed_nodes:
            return
        target_node = self.nodes.get(pkt.dst)
        if not target_node:
            return

        if pkt.is_reply:
            if pkt.rpc_name == "RequestVoteReply" and isinstance(pkt.payload, RequestVoteReply):
                target_node.handle_request_vote_reply(pkt.payload)
            elif pkt.rpc_name == "AppendEntriesReply" and isinstance(pkt.payload, AppendEntriesReply):
                target_node.handle_append_entries_reply(pkt.payload)
            elif pkt.rpc_name == "InstallSnapshotReply" and isinstance(pkt.payload, InstallSnapshotReply):
                target_node.handle_install_snapshot_reply(pkt.payload)
        else:
            # Incoming RPC request -> invokes handler and enqueues reply
            if pkt.rpc_name == "RequestVote" and isinstance(pkt.payload, RequestVoteArgs):
                reply = target_node.handle_request_vote(pkt.payload)
                self.router.send(
                    current_time_ms=self.time_ms,
                    src=pkt.dst,
                    dst=pkt.src,
                    rpc_name="RequestVoteReply",
                    payload=reply,
                    is_reply=True,
                )
            elif pkt.rpc_name == "AppendEntries" and isinstance(pkt.payload, AppendEntriesArgs):
                reply = target_node.handle_append_entries(pkt.payload)
                self.router.send(
                    current_time_ms=self.time_ms,
                    src=pkt.dst,
                    dst=pkt.src,
                    rpc_name="AppendEntriesReply",
                    payload=reply,
                    is_reply=True,
                )
            elif pkt.rpc_name == "InstallSnapshot" and isinstance(pkt.payload, InstallSnapshotArgs):
                reply = target_node.handle_install_snapshot(pkt.payload)
                self.router.send(
                    current_time_ms=self.time_ms,
                    src=pkt.dst,
                    dst=pkt.src,
                    rpc_name="InstallSnapshotReply",
                    payload=reply,
                    is_reply=True,
                )

    def run_for(self, duration_ms: float, step_dt_ms: float = 10.0) -> None:
        """Runs simulation forward for duration_ms."""
        target_time = self.time_ms + duration_ms
        while self.time_ms < target_time:
            step = min(step_dt_ms, target_time - self.time_ms)
            self.step(step)

    def run_until(self, condition: Callable[[RaftCluster], bool], max_ms: float = 2000.0, step_dt_ms: float = 10.0) -> bool:
        """Steps simulation until condition returns True or max_ms elapses."""
        target_time = self.time_ms + max_ms
        while self.time_ms < target_time:
            if condition(self):
                return True
            self.step(step_dt_ms)
        return condition(self)

    # -------------------------------------------------------------------------
    # Node Lifecycle: Crashes & Restarts
    # -------------------------------------------------------------------------

    def crash_node(self, node_id: str) -> None:
        """Simulates sudden server crash (volatile memory lost, WAL remains). """
        self.crashed_nodes.add(node_id)

    def restart_node(self, node_id: str) -> None:
        """Recovers a crashed server from persistent storage and state machine."""
        if node_id not in self.crashed_nodes:
            return
        self.crashed_nodes.remove(node_id)
        old_node = self.nodes[node_id]
        
        # Reload from storage
        storage = PersistentStorage(node_id, storage_dir=self.storage_dir)
        sm = StateMachine()
        if storage.snapshot_data:
            sm.restore_snapshot(storage.snapshot_data)
        
        new_node = RaftNode(
            node_id=node_id,
            peers=self.node_ids,
            storage=storage,
            state_machine=sm,
            election_timeout_range=self.election_timeout_range,
            heartbeat_interval=self.heartbeat_interval,
            rng_seed=self.seed,
        )
        # Re-apply any committed entries in storage if last_applied < last_included_index
        new_node.commit_index = storage.last_included_index
        new_node.last_applied = storage.last_included_index
        new_node.current_time_ms = self.time_ms
        new_node.reset_election_timer()
        self.nodes[node_id] = new_node

    # -------------------------------------------------------------------------
    # Cluster Inspection & Client Requests
    # -------------------------------------------------------------------------

    def get_leader(self) -> Optional[RaftNode]:
        """Returns currently recognized leader, if any."""
        leaders = [
            n for nid, n in self.nodes.items()
            if nid not in self.crashed_nodes and n.role == Role.LEADER
        ]
        if len(leaders) == 1:
            return leaders[0]
        return None

    def execute_command(
        self,
        command: Any,
        client_id: str = "client-1",
        seq_num: Optional[int] = None,
        timeout_ms: float = 2000.0,
    ) -> ClientResponse:
        """
        Submits command to the current cluster leader and awaits consensus commit.
        """
        if seq_num is None:
            self._client_seq_counter += 1
            seq_num = self._client_seq_counter

        leader = self.get_leader()
        if not leader:
            # Wait briefly for leader election
            if not self.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0):
                return ClientResponse(success=False, leader_id=None, error="No leader elected")
            leader = self.get_leader()

        req = ClientRequest(command=command, client_id=client_id, seq_num=seq_num)
        is_leader, leader_id, assigned_idx = leader.submit_command(req)
        if not is_leader or assigned_idx is None:
            return ClientResponse(success=False, leader_id=leader_id, error="Node rejected leadership")

        # Step simulation until leader commits this index or timeout
        def is_committed(c: RaftCluster) -> bool:
            cur_leader = c.get_leader()
            if not cur_leader or cur_leader.node_id != leader.node_id:
                return False
            return assigned_idx in cur_leader.client_commit_callbacks

        if self.run_until(is_committed, max_ms=timeout_ms):
            _, result = leader.client_commit_callbacks[assigned_idx]
            return ClientResponse(success=True, leader_id=leader.node_id, result=result)

        return ClientResponse(success=False, leader_id=leader.node_id, error="Consensus commit timeout")
