"""
Integration tests for core Raft consensus protocol, leader election, and log replication.
"""
import pytest
from raftzero.cluster import RaftCluster
from raftzero.invariants import InvariantAuditor
from raftzero.types import Role


def test_leader_election_3_nodes():
    cluster = RaftCluster(node_ids=["N0", "N1", "N2"], seed=101)
    auditor = InvariantAuditor(cluster)

    # Run until a leader is elected
    elected = cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    assert elected is True

    auditor.audit()
    leader = cluster.get_leader()
    assert leader is not None
    assert leader.role == Role.LEADER
    assert leader.current_term >= 1

    # Confirm other nodes are followers
    followers = [n for nid, n in cluster.nodes.items() if nid != leader.node_id]
    assert len(followers) == 2
    for f in followers:
        assert f.role == Role.FOLLOWER
        assert f.current_term == leader.current_term


def test_replicated_client_writes():
    cluster = RaftCluster(node_ids=["N0", "N1", "N2"], seed=202)
    auditor = InvariantAuditor(cluster)

    # Elect leader
    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    leader = cluster.get_leader()
    assert leader is not None

    # Submit commands
    res1 = cluster.execute_command({"op": "SET", "key": "counter", "val": 10}, client_id="c1", seq_num=1)
    assert res1.success is True
    assert res1.result == 10

    res2 = cluster.execute_command({"op": "INCR", "key": "counter", "delta": 5}, client_id="c1", seq_num=2)
    assert res2.success is True
    assert res2.result == 15

    auditor.audit()

    # Check that entries replicated to followers
    cluster.run_for(100.0)
    for nid, node in cluster.nodes.items():
        assert node.commit_index >= 2
        assert node.state_machine.apply({"op": "GET", "key": "counter"}) == 15


def test_leader_failover_on_crash():
    cluster = RaftCluster(node_ids=["N0", "N1", "N2", "N3", "N4"], seed=303)
    auditor = InvariantAuditor(cluster)

    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    leader1 = cluster.get_leader()
    assert leader1 is not None
    old_term = leader1.current_term
    old_leader_id = leader1.node_id

    # Crash the leader
    cluster.crash_node(old_leader_id)

    # Step simulation to trigger new election among remaining 4 nodes
    new_elected = cluster.run_until(
        lambda c: c.get_leader() is not None and c.get_leader().node_id != old_leader_id,
        max_ms=1500.0,
    )
    assert new_elected is True

    auditor.audit()
    leader2 = cluster.get_leader()
    assert leader2 is not None
    assert leader2.node_id != old_leader_id
    assert leader2.current_term > old_term

    # New leader successfully processes writes
    res = cluster.execute_command({"op": "SET", "key": "post_failover", "val": "active"}, client_id="c2", seq_num=1)
    assert res.success is True
    auditor.audit()


def test_install_snapshot_to_lagging_node():
    cluster = RaftCluster(node_ids=["N0", "N1", "N2"], seed=404)
    auditor = InvariantAuditor(cluster)

    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    leader = cluster.get_leader()
    assert leader is not None

    # Isolate follower N2
    followers = [nid for nid in ["N0", "N1", "N2"] if nid != leader.node_id]
    lagging_id = followers[0]
    cluster.router.isolate_node(lagging_id, cluster.node_ids)

    # Commit 10 operations between leader and remaining node
    for i in range(1, 11):
        res = cluster.execute_command({"op": "SET", "key": f"k_{i}", "val": i}, client_id="c3", seq_num=i)
        assert res.success is True

    # Leader creates snapshot and discards logs up to commit_index
    leader.create_snapshot()
    assert leader.storage.last_included_index >= 10

    # Reconnect lagging node
    cluster.router.heal_partitions()
    
    # Step simulation so leader detects lagging node and sends InstallSnapshot
    cluster.run_for(600.0)
    auditor.audit()

    lagging_node = cluster.nodes[lagging_id]
    assert lagging_node.storage.last_included_index >= 10
    assert lagging_node.state_machine.apply({"op": "GET", "key": "k_10"}) == 10
