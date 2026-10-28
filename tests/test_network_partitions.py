"""
Tests for Network Partitions, Split-Brain prevention, and post-partition healing.
"""
import pytest
from raftzero.cluster import RaftCluster
from raftzero.invariants import InvariantAuditor
from raftzero.types import Role


def test_majority_minority_split_brain_prevention():
    # 5-node cluster
    cluster = RaftCluster(node_ids=["N0", "N1", "N2", "N3", "N4"], seed=505)
    auditor = InvariantAuditor(cluster)

    # Elect initial leader
    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    auditor.audit()
    initial_leader = cluster.get_leader()
    assert initial_leader is not None

    # Write 1 command before partition
    res_pre = cluster.execute_command({"op": "SET", "key": "pre_split", "val": 100})
    assert res_pre.success is True
    auditor.audit()

    # Partition: Group A (3 nodes) vs Group B (2 nodes)
    group_a = ["N0", "N1", "N2"]
    group_b = ["N3", "N4"]
    cluster.router.partition(group_a, group_b)

    # Advance time to allow any re-election in partitioned sub-clusters
    cluster.run_for(600.0)
    auditor.audit()

    # Group A has 3 nodes (majority of 5), so it MUST be able to elect a leader and commit
    # Determine leader in Group A
    group_a_leaders = [
        cluster.nodes[nid] for nid in group_a if cluster.nodes[nid].role == Role.LEADER
    ]
    assert len(group_a_leaders) <= 1
    if not group_a_leaders:
        cluster.run_until(
            lambda c: any(c.nodes[nid].role == Role.LEADER for nid in group_a),
            max_ms=1000.0,
        )
        group_a_leaders = [
            cluster.nodes[nid] for nid in group_a if cluster.nodes[nid].role == Role.LEADER
        ]
    assert len(group_a_leaders) == 1
    maj_leader = group_a_leaders[0]

    # Group B has 2 nodes (< majority of 5), so it CANNOT elect a leader or commit entries
    group_b_leaders = [
        cluster.nodes[nid] for nid in group_b if cluster.nodes[nid].role == Role.LEADER
    ]
    assert len(group_b_leaders) == 0

    # Write to majority partition succeeds
    res_maj = cluster.execute_command(
        command={"op": "SET", "key": "maj_key", "val": "majority_val"},
        client_id="client_maj",
        seq_num=1,
    )
    assert res_maj.success is True
    auditor.audit()

    # Heal the network partition
    cluster.router.heal_partitions()
    cluster.run_for(1000.0)
    auditor.audit()

    # All 5 nodes must have caught up and applied the majority commit
    for nid, node in cluster.nodes.items():
        assert node.state_machine.apply({"op": "GET", "key": "maj_key"}) == "majority_val"
        assert node.state_machine.apply({"op": "GET", "key": "pre_split"}) == 100


def test_asymmetric_partition():
    # N0 can send to N1, but N1 cannot send to N0
    cluster = RaftCluster(node_ids=["N0", "N1", "N2"], seed=606)
    auditor = InvariantAuditor(cluster)

    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    auditor.audit()

    # Disable link N1 -> N0 only
    cluster.router.disabled_links.add(("N1", "N0"))
    cluster.run_for(400.0)
    auditor.audit()

    # Consensus can still function via N2 or new leader election
    cluster.execute_command({"op": "SET", "key": "asymm", "val": 42})
    auditor.audit()

    cluster.router.heal_partitions()
    cluster.run_for(300.0)
    auditor.audit()
