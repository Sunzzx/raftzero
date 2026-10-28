"""
Advanced Raft consensus scenarios, fast recovery, and concurrent edge-cases.
"""
import pytest
from raftzero.cluster import RaftCluster
from raftzero.invariants import InvariantAuditor
from raftzero.network import NetworkRouter
from raftzero.types import LogEntry, Role


def test_fast_log_conflict_backoff():
    # Node 1 has diverging term entries; verify fast recovery conflict indexing
    cluster = RaftCluster(node_ids=["N0", "N1", "N2"], seed=111)
    auditor = InvariantAuditor(cluster)

    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    leader = cluster.get_leader()
    assert leader is not None

    # Write 5 entries
    for i in range(5):
        cluster.execute_command({"op": "SET", "key": f"k_{i}", "val": i})
    cluster.run_for(100.0)
    auditor.audit()

    # Isolate follower N2
    followers = [nid for nid in cluster.node_ids if nid != leader.node_id]
    isolated_id = followers[0]
    cluster.router.isolate_node(isolated_id, cluster.node_ids)

    # Write 10 more entries to remaining quorum
    for i in range(5, 15):
        cluster.execute_command({"op": "SET", "key": f"k_{i}", "val": i})
    auditor.audit()

    # Reconnect isolated node
    cluster.router.heal_partitions()
    cluster.run_until(
        lambda cl: cl.get_leader() is not None and cl.nodes[isolated_id].storage.last_log_index == 15,
        max_ms=1500.0,
    )
    cluster.run_for(100.0)
    auditor.audit()

    # Ensure isolated follower jumped forward quickly and matched leader
    isolated_node = cluster.nodes[isolated_id]
    assert isolated_node.storage.last_log_index == 15
    assert isolated_node.commit_index == 15
    assert isolated_node.state_machine.apply({"op": "GET", "key": "k_14"}) == 14


def test_duplicate_and_delayed_rpc_resilience():
    # Network with heavy packet duplication (15%) and delay
    router = NetworkRouter(min_latency_ms=5.0, max_latency_ms=25.0, drop_rate=0.02, dup_rate=0.15, rng_seed=222)
    cluster = RaftCluster(node_ids=["N0", "N1", "N2"], router=router, seed=222)
    auditor = InvariantAuditor(cluster)

    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    auditor.audit()

    # Submit multiple writes under duplicate delivery
    for i in range(8):
        res = cluster.execute_command({"op": "INCR", "key": "dedup_counter", "delta": 1})
        assert res.success is True

    cluster.run_for(300.0)
    auditor.audit()

    # Check that deduplication and idempotence ensured exact counter value
    for nid, node in cluster.nodes.items():
        assert node.state_machine.apply({"op": "GET", "key": "dedup_counter"}) == 8


def test_multi_node_simultaneous_crash_recovery():
    # 5-node cluster with 2 simultaneous crashes (f = 2 tolerable out of 5)
    cluster = RaftCluster(node_ids=[f"N{i}" for i in range(5)], seed=333)
    auditor = InvariantAuditor(cluster)

    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    leader = cluster.get_leader()
    assert leader is not None

    # Crash 2 non-leader nodes
    crashed = [nid for nid in cluster.node_ids if nid != leader.node_id][:2]
    for c in crashed:
        cluster.crash_node(c)

    # Cluster of 3 remaining nodes must still commit writes
    res = cluster.execute_command({"op": "SET", "key": "resilient", "val": "yes"})
    assert res.success is True
    auditor.audit()

    # Recover both crashed nodes
    for c in crashed:
        cluster.restart_node(c)

    cluster.run_for(400.0)
    auditor.audit()

    # Both recovered nodes must synchronize and apply committed value
    for c in crashed:
        assert cluster.nodes[c].state_machine.apply({"op": "GET", "key": "resilient"}) == "yes"


def test_cas_atomic_transaction():
    cluster = RaftCluster(node_ids=["N0", "N1", "N2"], seed=444)
    auditor = InvariantAuditor(cluster)

    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    
    # Initial SET
    cluster.execute_command({"op": "SET", "key": "account_bal", "val": 1000})
    auditor.audit()

    # Valid CAS
    res_cas1 = cluster.execute_command({"op": "CAS", "key": "account_bal", "expected": 1000, "val": 1500})
    assert res_cas1.success is True
    assert res_cas1.result is True

    # Invalid CAS (stale expectation)
    res_cas2 = cluster.execute_command({"op": "CAS", "key": "account_bal", "expected": 1000, "val": 2000})
    assert res_cas2.success is True
    assert res_cas2.result is False  # Rejected because value is 1500

    cluster.run_for(100.0)
    auditor.audit()

    for nid, node in cluster.nodes.items():
        assert node.state_machine.apply({"op": "GET", "key": "account_bal"}) == 1500
