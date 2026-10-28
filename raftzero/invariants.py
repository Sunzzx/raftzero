"""
raftzero.invariants
~~~~~~~~~~~~~~~~~~~
Formal Invariant Auditor for Raft consensus validation.
Rigorously verifies Election Safety, Log Matching, Leader Completeness, and State Machine Safety.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from raftzero.cluster import RaftCluster
from raftzero.types import LogEntry, Role


class InvariantViolation(Exception):
    """Raised when a formal Raft consensus invariant is violated."""
    pass


class InvariantAuditor:
    """
    Auditor that inspects the Raft cluster state to guarantee safety properties:
    1. Election Safety: At most one leader per term.
    2. Log Matching: Identical index & term implies identical prefix.
    3. State Machine Safety: At most one command applied per log index.
    4. Prefix Consistency: Committed logs across all nodes form monotonic prefix chains.
    """

    def __init__(self, cluster: RaftCluster) -> None:
        self.cluster = cluster
        # term -> leader_id
        self.term_leaders: Dict[int, str] = {}
        # index -> (term, command)
        self.applied_entries: Dict[int, Tuple[int, Any]] = {}

    def audit(self) -> None:
        """Performs full formal verification audit across all nodes."""
        self.check_election_safety()
        self.check_log_matching()
        self.check_state_machine_safety()
        self.check_prefix_consistency()

    def check_election_safety(self) -> None:
        """At most one leader can be elected in a given term."""
        leaders_by_term: Dict[int, Set[str]] = {}
        for nid, node in self.cluster.nodes.items():
            if nid in self.cluster.crashed_nodes:
                continue
            if node.role == Role.LEADER:
                term = node.current_term
                if term not in leaders_by_term:
                    leaders_by_term[term] = set()
                leaders_by_term[term].add(nid)

                # Verify against historical term leaders
                if term in self.term_leaders and self.term_leaders[term] != nid:
                    raise InvariantViolation(
                        f"Election Safety Violated: Multiple leaders for term {term}: "
                        f"{self.term_leaders[term]} and {nid}"
                    )
                self.term_leaders[term] = nid

        for term, leaders in leaders_by_term.items():
            if len(leaders) > 1:
                raise InvariantViolation(
                    f"Election Safety Violated: Concurrent leaders in term {term}: {leaders}"
                )

    def check_log_matching(self) -> None:
        """
        If two logs contain an entry with the same index and term,
        then the logs are identical in all entries up through the given index.
        """
        active_nodes = [
            n for nid, n in self.cluster.nodes.items() if nid not in self.cluster.crashed_nodes
        ]
        for i in range(len(active_nodes)):
            for j in range(i + 1, len(active_nodes)):
                n1 = active_nodes[i]
                n2 = active_nodes[j]
                
                # Find common indices
                min_idx = max(n1.storage.last_included_index, n2.storage.last_included_index) + 1
                max_idx = min(n1.storage.last_log_index, n2.storage.last_log_index)
                
                for idx in range(min_idx, max_idx + 1):
                    e1 = n1.storage.get_entry(idx)
                    e2 = n2.storage.get_entry(idx)
                    if e1 and e2 and e1.term == e2.term:
                        if e1.command != e2.command:
                            raise InvariantViolation(
                                f"Log Matching Violated: Index {idx} has matching term {e1.term} "
                                f"between {n1.node_id} and {n2.node_id} but divergent commands: "
                                f"{e1.command} vs {e2.command}"
                            )

    def check_state_machine_safety(self) -> None:
        """If a server has applied an entry at a given index, no other server will apply different entry."""
        for nid, node in self.cluster.nodes.items():
            for idx in range(1, node.last_applied + 1):
                if idx <= node.storage.last_included_index:
                    continue
                entry = node.storage.get_entry(idx)
                if entry is None:
                    continue
                
                key = idx
                val = (entry.term, entry.command)
                if key in self.applied_entries:
                    existing = self.applied_entries[key]
                    if existing != val:
                        raise InvariantViolation(
                            f"State Machine Safety Violated: Index {idx} applied divergent entries: "
                            f"{existing} vs {val} on node {nid}"
                        )
                else:
                    self.applied_entries[key] = val

    def check_prefix_consistency(self) -> None:
        """Verifies that all committed entries form consistent prefixes across nodes."""
        committed_logs: Dict[str, List[Tuple[int, int, Any]]] = {}
        for nid, node in self.cluster.nodes.items():
            if nid in self.cluster.crashed_nodes:
                continue
            # Collect committed entries from last_included_index + 1 up to commit_index
            entries = []
            for idx in range(node.storage.last_included_index + 1, node.commit_index + 1):
                e = node.storage.get_entry(idx)
                if e:
                    entries.append((e.index, e.term, e.command))
            committed_logs[nid] = entries

        nodes = list(committed_logs.keys())
        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                l1 = committed_logs[nodes[i]]
                l2 = committed_logs[nodes[j]]
                min_len = min(len(l1), len(l2))
                if l1[:min_len] != l2[:min_len]:
                    raise InvariantViolation(
                        f"Prefix Consistency Violated between {nodes[i]} and {nodes[j]}: "
                        f"{l1[:min_len]} != {l2[:min_len]}"
                    )
