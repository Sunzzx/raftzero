"""
raftzero.chaos
~~~~~~~~~~~~~~
Byzantine Fault Injection, Network Chaos, and Adversarial Fuzzing Harness.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from raftzero.cluster import RaftCluster
from raftzero.invariants import InvariantAuditor, InvariantViolation
from raftzero.network import NetworkPacket
from raftzero.types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
    Role,
)


class ChaosHarness:
    """
    Orchestrates deterministic chaos scenarios, adversarial attacks,
    and invariant auditing on a RaftCluster.
    """

    def __init__(self, cluster: RaftCluster, seed: Optional[int] = None) -> None:
        self.cluster = cluster
        self.auditor = InvariantAuditor(cluster)
        self.rng = random.Random(seed)

    def run_chaos_fuzzing(
        self,
        duration_ms: float = 3000.0,
        step_dt_ms: float = 10.0,
        client_ops: int = 20,
        partition_interval_ms: float = 500.0,
        crash_interval_ms: float = 700.0,
    ) -> Dict[str, Any]:
        """
        Executes an intensive chaos experiment with concurrent client transactions,
        random network partitions, node crashes/recoveries, and invariant audits at every tick.
        """
        time_started = self.cluster.time_ms
        target_time = time_started + duration_ms
        
        last_partition_time = time_started
        last_crash_time = time_started
        
        op_counter = 0
        successful_ops = 0
        audit_count = 0

        while self.cluster.time_ms < target_time:
            # 1. Periodically toggle network partitions
            if self.cluster.time_ms - last_partition_time >= partition_interval_ms:
                last_partition_time = self.cluster.time_ms
                if self.cluster.router.disabled_links:
                    self.cluster.router.heal_partitions()
                else:
                    # Create majority/minority split
                    nodes = list(self.cluster.node_ids)
                    self.rng.shuffle(nodes)
                    split_idx = (len(nodes) // 2) + 1  # 3 vs 2 for 5-node cluster
                    maj = nodes[:split_idx]
                    min_group = nodes[split_idx:]
                    self.cluster.router.partition(maj, min_group)

            # 2. Periodically crash or restart a node
            if self.cluster.time_ms - last_crash_time >= crash_interval_ms:
                last_crash_time = self.cluster.time_ms
                if self.cluster.crashed_nodes:
                    # Restart one
                    to_restart = self.rng.choice(list(self.cluster.crashed_nodes))
                    self.cluster.restart_node(to_restart)
                else:
                    # Crash one random non-leader or leader
                    to_crash = self.rng.choice(self.cluster.node_ids)
                    self.cluster.crash_node(to_crash)

            # 3. Inject client operations if quota remains
            if op_counter < client_ops and self.rng.random() < 0.15:
                op_counter += 1
                leader = self.cluster.get_leader()
                if leader:
                    key = f"key_{op_counter % 5}"
                    val = f"val_{op_counter}"
                    cmd = {"op": "SET", "key": key, "val": val}
                    req_res = self.cluster.execute_command(
                        command=cmd,
                        client_id=f"client_{op_counter % 3}",
                        seq_num=op_counter,
                        timeout_ms=300.0,
                    )
                    if req_res.success:
                        successful_ops += 1

            # 4. Advance time
            self.cluster.step(step_dt_ms)
            
            # 5. Formally audit invariants at each tick
            self.auditor.audit()
            audit_count += 1

        # Final heal and catchup
        self.cluster.router.heal_partitions()
        for c in list(self.cluster.crashed_nodes):
            self.cluster.restart_node(c)
        self.cluster.run_for(500.0)
        self.auditor.audit()

        return {
            "duration_ms": duration_ms,
            "audits_performed": audit_count,
            "client_ops_attempted": op_counter,
            "client_ops_succeeded": successful_ops,
            "invariants_held": True,
        }


# -----------------------------------------------------------------------------
# Byzantine Adversary Injector
# -----------------------------------------------------------------------------

class ByzantineInterceptor:
    """
    Adversarial network interceptor that simulates malicious Byzantine behaviors:
    - Forged high terms
    - Equivocating votes
    - Conflicting log entry injection
    """

    def __init__(self, target_node: str, attack_mode: str = "term_inflation") -> None:
        self.target_node = target_node
        self.attack_mode = attack_mode
        self.attacks_injected = 0

    def __call__(self, packet: NetworkPacket) -> Optional[NetworkPacket]:
        if packet.src != self.target_node:
            return packet

        if self.attack_mode == "term_inflation":
            # Maliciously inflate term by +1000
            if isinstance(packet.payload, RequestVoteArgs):
                self.attacks_injected += 1
                packet.payload.term += 1000
            elif isinstance(packet.payload, AppendEntriesArgs):
                self.attacks_injected += 1
                packet.payload.term += 1000
            return packet

        elif self.attack_mode == "split_vote":
            # Grant vote to everyone regardless of state
            if isinstance(packet.payload, RequestVoteReply):
                self.attacks_injected += 1
                packet.payload.vote_granted = True
            return packet

        elif self.attack_mode == "drop_all":
            # Blackhole
            self.attacks_injected += 1
            return None

        return packet
