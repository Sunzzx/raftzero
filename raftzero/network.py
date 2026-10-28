"""
raftzero.network
~~~~~~~~~~~~~~~~
Deterministic discrete-event network simulator with latency, drops, reordering, and partition matrices.
"""
from __future__ import annotations

import heapq
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


@dataclass(order=True)
class NetworkPacket:
    deliver_time_ms: float
    seq: int
    src: str = field(compare=False)
    dst: str = field(compare=False)
    rpc_name: str = field(compare=False)
    payload: Any = field(compare=False)
    is_reply: bool = field(default=False, compare=False)


class NetworkRouter:
    """
    Simulated deterministic network transport supporting arbitrary topology partitions,
    asymmetric links, latency distributions, and packet drop/duplication fuzzing.
    """

    def __init__(
        self,
        min_latency_ms: float = 5.0,
        max_latency_ms: float = 20.0,
        drop_rate: float = 0.0,
        dup_rate: float = 0.0,
        rng_seed: Optional[int] = None,
    ) -> None:
        self.min_latency_ms = min_latency_ms
        self.max_latency_ms = max_latency_ms
        self.drop_rate = drop_rate
        self.dup_rate = dup_rate
        self.rng = random.Random(rng_seed)

        self._packet_queue: List[NetworkPacket] = []
        self._seq_counter: int = 0
        
        # Blocked links: (src, dst) in disabled_links means packets are dropped
        self.disabled_links: Set[Tuple[str, str]] = set()
        
        # Packet interceptor for Byzantine / chaos hooks: (packet) -> Optional[packet]
        self.interceptor: Optional[Callable[[NetworkPacket], Optional[NetworkPacket]]] = None

    def partition(self, group_a: List[str], group_b: List[str], bidirectional: bool = True) -> None:
        """Partitions group_a from group_b."""
        for a in group_a:
            for b in group_b:
                self.disabled_links.add((a, b))
                if bidirectional:
                    self.disabled_links.add((b, a))

    def heal_partitions(self) -> None:
        """Restores full network connectivity."""
        self.disabled_links.clear()

    def isolate_node(self, node_id: str, all_nodes: List[str]) -> None:
        """Completely isolates a node from all peers."""
        others = [n for n in all_nodes if n != node_id]
        self.partition([node_id], others, bidirectional=True)

    def send(
        self,
        current_time_ms: float,
        src: str,
        dst: str,
        rpc_name: str,
        payload: Any,
        is_reply: bool = False,
    ) -> None:
        """Enqueues a message for simulated transmission."""
        if (src, dst) in self.disabled_links:
            # Dropped due to network partition
            return

        if self.drop_rate > 0.0 and self.rng.random() < self.drop_rate:
            # Dropped due to packet loss
            return

        delay = self.rng.uniform(self.min_latency_ms, self.max_latency_ms)
        deliver_time = current_time_ms + delay
        self._seq_counter += 1
        
        packet = NetworkPacket(
            deliver_time_ms=deliver_time,
            seq=self._seq_counter,
            src=src,
            dst=dst,
            rpc_name=rpc_name,
            payload=payload,
            is_reply=is_reply,
        )

        if self.interceptor:
            intercepted = self.interceptor(packet)
            if intercepted is None:
                return
            packet = intercepted

        heapq.heappush(self._packet_queue, packet)

        # Duplicate packet simulation
        if self.dup_rate > 0.0 and self.rng.random() < self.dup_rate:
            dup_delay = delay + self.rng.uniform(1.0, 10.0)
            self._seq_counter += 1
            dup_pkt = NetworkPacket(
                deliver_time_ms=current_time_ms + dup_delay,
                seq=self._seq_counter,
                src=src,
                dst=dst,
                rpc_name=rpc_name,
                payload=payload,
                is_reply=is_reply,
            )
            heapq.heappush(self._packet_queue, dup_pkt)

    def pop_ready_packets(self, current_time_ms: float) -> List[NetworkPacket]:
        """Pops all packets whose delivery time <= current_time_ms."""
        ready: List[NetworkPacket] = []
        while self._packet_queue and self._packet_queue[0].deliver_time_ms <= current_time_ms:
            pkt = heapq.heappop(self._packet_queue)
            if (pkt.src, pkt.dst) not in self.disabled_links:
                ready.append(pkt)
        return ready

    @property
    def pending_count(self) -> int:
        return len(self._packet_queue)
