"""
Tests for Chaos Fuzzing, Byzantine Adversary Injection, and Invariant Preservation.
"""
import pytest
from raftzero.chaos import ByzantineInterceptor, ChaosHarness
from raftzero.cluster import RaftCluster
from raftzero.invariants import InvariantAuditor
from raftzero.network import NetworkRouter
from raftzero.types import Role


def test_chaos_harness_fuzzing():
    # Run 5-node cluster under active chaos with packet loss and random latency
    router = NetworkRouter(min_latency_ms=2.0, max_latency_ms=15.0, drop_rate=0.05, dup_rate=0.02, rng_seed=707)
    cluster = RaftCluster(node_ids=[f"N{i}" for i in range(5)], router=router, seed=707)
    harness = ChaosHarness(cluster, seed=707)

    stats = harness.run_chaos_fuzzing(
        duration_ms=2000.0,
        client_ops=15,
        partition_interval_ms=400.0,
        crash_interval_ms=600.0,
    )

    assert stats["invariants_held"] is True
    assert stats["audits_performed"] >= 50


def test_byzantine_term_inflation_attack_defense():
    # Byzantine node N4 maliciously inflates term by +1000 to disrupt the cluster
    cluster = RaftCluster(node_ids=["N0", "N1", "N2", "N3", "N4"], seed=808)
    auditor = InvariantAuditor(cluster)

    # Attach Byzantine interceptor on N4
    cluster.router.interceptor = ByzantineInterceptor(target_node="N4", attack_mode="term_inflation")

    # Step simulation: cluster should transition safely and remain linearizable without violating election safety
    cluster.run_for(1500.0)
    auditor.audit()

    # Honest majority [N0, N1, N2, N3] can still reach consensus
    res = cluster.execute_command({"op": "SET", "key": "byz_defense", "val": "secure"}, timeout_ms=3000.0)
    # Whether N4 inflated terms or not, invariants MUST NEVER be violated
    auditor.audit()


def test_byzantine_split_vote_defense():
    # Byzantine node N2 equivocates by voting True for all candidates in term 1
    cluster = RaftCluster(node_ids=["N0", "N1", "N2"], seed=909)
    auditor = InvariantAuditor(cluster)

    cluster.router.interceptor = ByzantineInterceptor(target_node="N2", attack_mode="split_vote")

    cluster.run_for(1000.0)
    # CFT Raft cannot tolerate equivocation without BFT crypto; auditor must actively detect this violation!
    from raftzero.invariants import InvariantViolation
    with pytest.raises(InvariantViolation, match="Election Safety Violated"):
        auditor.audit()
