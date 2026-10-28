"""
Unit tests for Key-Value StateMachine, atomic CAS, deduplication, and snapshots.
"""
import pytest
from raftzero.state_machine import StateMachine


def test_state_machine_crud_operations():
    sm = StateMachine()
    
    # SET & GET
    assert sm.apply({"op": "SET", "key": "alpha", "val": 42}) == 42
    assert sm.apply({"op": "GET", "key": "alpha"}) == 42
    assert sm.apply({"op": "GET", "key": "missing"}) is None

    # INCR
    assert sm.apply({"op": "INCR", "key": "alpha", "delta": 8}) == 50
    assert sm.apply({"op": "INCR", "key": "counter", "delta": 1}) == 1

    # CAS
    assert sm.apply({"op": "CAS", "key": "alpha", "expected": 999, "val": 100}) is False
    assert sm.apply({"op": "GET", "key": "alpha"}) == 50
    assert sm.apply({"op": "CAS", "key": "alpha", "expected": 50, "val": 100}) is True
    assert sm.apply({"op": "GET", "key": "alpha"}) == 100

    # DEL
    assert sm.apply({"op": "DEL", "key": "alpha"}) is True
    assert sm.apply({"op": "GET", "key": "alpha"}) is None
    assert sm.apply({"op": "DEL", "key": "alpha"}) is False


def test_state_machine_deduplication():
    sm = StateMachine()
    client_id = "client-123"

    # First execution
    res1 = sm.apply({"op": "INCR", "key": "counter", "delta": 5}, client_id=client_id, seq_num=1)
    assert res1 == 5

    # Replayed execution with same seq_num
    res2 = sm.apply({"op": "INCR", "key": "counter", "delta": 5}, client_id=client_id, seq_num=1)
    assert res2 == 5  # Cached response returned without executing again
    assert sm.apply({"op": "GET", "key": "counter"}) == 5

    # New execution with next seq_num
    res3 = sm.apply({"op": "INCR", "key": "counter", "delta": 5}, client_id=client_id, seq_num=2)
    assert res3 == 10


def test_state_machine_snapshot_and_restore():
    sm1 = StateMachine()
    sm1.apply({"op": "SET", "key": "k1", "val": "v1"})
    sm1.apply({"op": "SET", "key": "k2", "val": [1, 2, 3]})
    sm1.apply({"op": "SET", "key": "k3", "val": {"nested": True}}, client_id="c1", seq_num=5)

    snap_data = sm1.take_snapshot()
    assert isinstance(snap_data, bytes)

    # Restore in fresh state machine
    sm2 = StateMachine()
    sm2.restore_snapshot(snap_data)
    assert sm2.apply({"op": "GET", "key": "k1"}) == "v1"
    assert sm2.apply({"op": "GET", "key": "k2"}) == [1, 2, 3]
    
    # Deduplication state also restored
    res_replayed = sm2.apply({"op": "SET", "key": "k3", "val": "changed"}, client_id="c1", seq_num=5)
    assert res_replayed == {"nested": True}
