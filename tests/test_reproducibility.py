"""
Tests for Deterministic Replay and bit-for-bit reproducibility.
"""
import pytest
from raftzero.cluster import RaftCluster


def test_deterministic_cluster_replay():
    # Run simulation A with seed 4242
    c_a = RaftCluster(node_ids=["N0", "N1", "N2"], seed=4242)
    c_a.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    for i in range(5):
        c_a.execute_command({"op": "SET", "key": f"key_{i}", "val": i * 10})
    c_a.run_for(200.0)

    # Run simulation B with identical seed 4242
    c_b = RaftCluster(node_ids=["N0", "N1", "N2"], seed=4242)
    c_b.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    for i in range(5):
        c_b.execute_command({"op": "SET", "key": f"key_{i}", "val": i * 10})
    c_b.run_for(200.0)

    # Assert bit-for-bit identical state across both runs
    assert c_a.get_leader().node_id == c_b.get_leader().node_id
    assert c_a.get_leader().current_term == c_b.get_leader().current_term
    assert c_a.get_leader().commit_index == c_b.get_leader().commit_index

    for nid in ["N0", "N1", "N2"]:
        assert c_a.nodes[nid].state_machine._data == c_b.nodes[nid].state_machine._data
        assert c_a.nodes[nid].storage.last_log_index == c_b.nodes[nid].storage.last_log_index
        assert c_a.nodes[nid].commit_index == c_b.nodes[nid].commit_index
