"""
raftzero.cli
~~~~~~~~~~~~
Interactive Terminal Visualizer and Benchmark CLI for RaftZero.
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Dict, List

from raftzero.chaos import ChaosHarness
from raftzero.cluster import RaftCluster
from raftzero.invariants import InvariantAuditor
from raftzero.types import Role


# ANSI color codes
RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
DIM = "\033[2m"


def render_cluster_table(cluster: RaftCluster) -> str:
    lines = []
    lines.append(f"{BOLD}=== RaftZero Cluster State (t={cluster.time_ms:.1f}ms) ==={RESET}")
    lines.append(f"{DIM}┌────────┬───────────┬──────────────┬──────┬────────┬─────────┬────────┬────────────────────────┐{RESET}")
    lines.append(f"{DIM}│{RESET} {BOLD}Node{RESET}   {DIM}│{RESET} {BOLD}Status{RESET}    {DIM}│{RESET} {BOLD}Role{RESET}         {DIM}│{RESET} {BOLD}Term{RESET} {DIM}│{RESET} {BOLD}Commit{RESET} {DIM}│{RESET} {BOLD}Applied{RESET} {DIM}│{RESET} {BOLD}LogLen{RESET} {DIM}│{RESET} {BOLD}State Machine (K-V){RESET}      {DIM}│{RESET}")
    lines.append(f"{DIM}├────────┼───────────┼──────────────┼──────┼────────┼─────────┼────────┼────────────────────────┤{RESET}")
    
    for nid in sorted(cluster.node_ids):
        is_crashed = nid in cluster.crashed_nodes
        node = cluster.nodes[nid]
        
        if is_crashed:
            status_str = f"{RED}CRASHED  {RESET}"
            role_str = f"{DIM}DEAD        {RESET}"
            term_str = f"{DIM}---{RESET}"
            commit_str = f"{DIM}---{RESET}"
            applied_str = f"{DIM}---{RESET}"
            log_len_str = f"{DIM}---{RESET}"
            kv_str = f"{DIM}unreachable{RESET}"
        else:
            status_str = f"{GREEN}ALIVE    {RESET}"
            if node.role == Role.LEADER:
                role_str = f"{GREEN}{BOLD}👑 LEADER    {RESET}"
            elif node.role == Role.CANDIDATE:
                role_str = f"{YELLOW}🗳️ CANDIDATE {RESET}"
            else:
                role_str = f"{CYAN}🛡️ FOLLOWER  {RESET}"
            
            term_str = f"{node.current_term:>4}"
            commit_str = f"{node.commit_index:>6}"
            applied_str = f"{node.last_applied:>7}"
            log_len_str = f"{node.storage.last_log_index:>6}"
            
            sm_items = list(node.state_machine._data.items())
            if sm_items:
                kv_str = ", ".join(f"{k}={v}" for k, v in sm_items[:3])
                if len(sm_items) > 3:
                    kv_str += "..."
            else:
                kv_str = "<empty>"
            kv_str = kv_str[:22].ljust(22)

        lines.append(
            f"{DIM}│{RESET} {nid:<6} {DIM}│{RESET} {status_str} {DIM}│{RESET} {role_str} {DIM}│{RESET} {term_str} {DIM}│{RESET} {commit_str} {DIM}│{RESET} {applied_str} {DIM}│{RESET} {log_len_str} {DIM}│{RESET} {kv_str} {DIM}│{RESET}"
        )
    lines.append(f"{DIM}└────────┴───────────┴──────────────┴──────┴────────┴─────────┴────────┴────────────────────────┘{RESET}")
    return "\n".join(lines)


def run_demo() -> None:
    print(f"\n{BOLD}{CYAN}⚡ RaftZero: Deterministic Distributed Raft Consensus Engine{RESET}")
    print(f"{DIM}Initializing 5-node cluster [N0, N1, N2, N3, N4]...{RESET}\n")

    node_ids = ["N0", "N1", "N2", "N3", "N4"]
    cluster = RaftCluster(node_ids=node_ids, seed=42)
    auditor = InvariantAuditor(cluster)

    print(render_cluster_table(cluster))
    
    # Step 1: Leader election
    print(f"\n{BOLD}[Step 1] Advancing time to trigger randomized leader election...{RESET}")
    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    auditor.audit()
    leader = cluster.get_leader()
    print(f"{GREEN}✔ Leader elected successfully:{RESET} {BOLD}{leader.node_id}{RESET} in Term {leader.current_term}")
    print(render_cluster_table(cluster))

    # Step 2: Replicated Client Writes
    print(f"\n{BOLD}[Step 2] Executing replicated client state-machine transactions...{RESET}")
    ops = [
        {"op": "SET", "key": "cluster_name", "val": "raftzero-core"},
        {"op": "SET", "key": "quorum_size", "val": 3},
        {"op": "INCR", "key": "tx_counter", "delta": 10},
        {"op": "CAS", "key": "quorum_size", "expected": 3, "val": 5},
    ]
    for i, op in enumerate(ops):
        res = cluster.execute_command(command=op, client_id="client-main", seq_num=i + 1)
        print(f"  -> Tx {i+1} {op['op']} result: {GREEN}{res.result}{RESET} (Committed by {res.leader_id})")
        auditor.audit()

    print(render_cluster_table(cluster))

    # Step 3: Network Partition (Majority / Minority split)
    print(f"\n{BOLD}[Step 3] Simulating network split: {YELLOW}[N0, N1, N2]{RESET} vs {RED}[N3, N4]{RESET}{RESET}")
    cluster.router.partition(["N0", "N1", "N2"], ["N3", "N4"])
    cluster.run_for(300.0)
    auditor.audit()
    
    # Issue write into majority partition
    print(f"\n{BOLD}[Step 4] Submitting write while partitioned into majority partition...{RESET}")
    res_part = cluster.execute_command(
        command={"op": "SET", "key": "partition_state", "val": "majority_active"},
        client_id="client-part",
        seq_num=100,
    )
    print(f"  -> Majority partition commit success: {GREEN}{res_part.success}{RESET} (Leader: {res_part.leader_id})")
    auditor.audit()
    print(render_cluster_table(cluster))

    # Step 5: Heal partition & Catch-up
    print(f"\n{BOLD}[Step 5] Healing network partition and converging logs...{RESET}")
    cluster.router.heal_partitions()
    cluster.run_for(400.0)
    auditor.audit()
    print(render_cluster_table(cluster))

    # Step 6: Log Snapshot Compaction
    print(f"\n{BOLD}[Step 6] Compacting log via State Machine Snapshots...{RESET}")
    leader = cluster.get_leader()
    if leader:
        leader.create_snapshot()
        print(f"  -> Snapshot created at index {leader.storage.last_included_index} (Remaining entries: {len(leader.storage.entries)})")
    cluster.run_for(200.0)
    auditor.audit()

    print(f"\n{BOLD}{GREEN}✔ All Raft formal invariants (Election Safety, Log Matching, State Machine Safety) verified!{RESET}\n")


def run_benchmark() -> None:
    print(f"\n{BOLD}{CYAN}🚀 RaftZero: High-Performance Consensus Benchmark{RESET}")
    node_ids = ["N0", "N1", "N2", "N3", "N4"]
    cluster = RaftCluster(node_ids=node_ids, seed=1337)
    auditor = InvariantAuditor(cluster)
    
    # Warm up election
    cluster.run_until(lambda c: c.get_leader() is not None, max_ms=1000.0)
    auditor.audit()
    leader = cluster.get_leader()
    print(f"  Leader elected: {leader.node_id} (Term {leader.current_term})")

    # Measure replicated transactions throughput
    total_ops = 50
    t0 = time.perf_counter()
    for i in range(total_ops):
        cluster.execute_command(
            command={"op": "INCR", "key": "bench_counter", "delta": 1},
            client_id="bench-worker",
            seq_num=i + 1,
        )
        cluster.step(5.0)
    cluster.run_for(50.0)
    t1 = time.perf_counter()
    
    elapsed = t1 - t0
    ops_sec = total_ops / elapsed if elapsed > 0 else 0
    
    auditor.audit()
    counter_val = cluster.nodes[node_ids[0]].state_machine._data.get("bench_counter")
    
    print(f"  {GREEN}Consensus Transactions:{RESET} {total_ops} ops in {elapsed*1000:.1f}ms ({ops_sec:.1f} ops/sec)")
    print(f"  {GREEN}Final Replicated Counter:{RESET} {counter_val}")
    print(f"  {GREEN}Formal Invariant Audits:{RESET} 100% PASS\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="RaftZero Consensus CLI & Visualizer")
    subparsers = parser.add_subparsers(dest="command", help="Sub-command to run")
    
    subparsers.add_parser("demo", help="Run interactive Raft consensus visualizer")
    subparsers.add_parser("benchmark", help="Run high-performance consensus benchmark")
    subparsers.add_parser("chaos", help="Run chaos fuzzing and invariant validation")
    
    args = parser.parse_args()
    if args.command == "benchmark":
        run_benchmark()
    elif args.command == "chaos":
        print(f"{BOLD}{CYAN}🌪️  Running RaftZero Chaos Fuzzing Suite...{RESET}")
        cluster = RaftCluster(node_ids=[f"N{i}" for i in range(5)], seed=999)
        harness = ChaosHarness(cluster, seed=999)
        res = harness.run_chaos_fuzzing(duration_ms=2500.0, client_ops=15)
        print(f"{GREEN}✔ Chaos Completed:{RESET} {res['audits_performed']} audits passed under dynamic partitions & crashes!")
    else:
        run_demo()


if __name__ == "__main__":
    main()
