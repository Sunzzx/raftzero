"""
raftzero.node
~~~~~~~~~~~~~
Fully-featured, mathematically rigorous Raft consensus node implementation.
Implements state machine replication, leader election, log replication, and snapshotting.
"""
from __future__ import annotations

import random
from typing import Any, Dict, List, Optional, Set, Tuple

from raftzero.state_machine import StateMachine
from raftzero.storage import PersistentStorage
from raftzero.types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    ClientRequest,
    ClientResponse,
    InstallSnapshotArgs,
    InstallSnapshotReply,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
    Role,
)


class RaftNode:
    """
    A Raft consensus engine node operating according to the Raft specification
    (Ongaro & Ousterhout 2014) with snapshotting and fast log recovery.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        storage: Optional[PersistentStorage] = None,
        state_machine: Optional[StateMachine] = None,
        election_timeout_range: Tuple[float, float] = (150.0, 300.0),  # ms
        heartbeat_interval: float = 50.0,  # ms
        rng_seed: Optional[int] = None,
    ) -> None:
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.cluster_size = len(peers) if node_id in peers else len(peers) + 1
        self.majority = (self.cluster_size // 2) + 1

        self.storage = storage or PersistentStorage(node_id)
        self.state_machine = state_machine or StateMachine()

        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self.rng = random.Random(rng_seed)

        # Volatile state on all servers
        self.role: Role = Role.FOLLOWER
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.leader_id: Optional[str] = None

        # Volatile state on candidates
        self.votes_received: Set[str] = set()

        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Clock & Timers (in ms)
        self.current_time_ms: float = 0.0
        self.last_heartbeat_time_ms: float = 0.0
        self.election_timeout_ms: float = self._random_election_timeout()

        # Outbox for RPC messages to send
        # tuples of (destination_id, rpc_name, rpc_args)
        self.outbox: List[Tuple[str, str, Any]] = []
        
        # Pending client requests (index -> (ClientRequest, callback_key))
        self.pending_client_requests: Dict[int, ClientRequest] = {}
        self.client_commit_callbacks: Dict[int, Any] = {}

    @property
    def current_term(self) -> int:
        return self.storage.current_term

    @property
    def voted_for(self) -> Optional[str]:
        return self.storage.voted_for

    def _random_election_timeout(self) -> float:
        low, high = self.election_timeout_range
        return self.rng.uniform(low, high)

    def reset_election_timer(self) -> None:
        self.last_heartbeat_time_ms = self.current_time_ms
        self.election_timeout_ms = self._random_election_timeout()

    # -------------------------------------------------------------------------
    # Time & Tick Progression
    # -------------------------------------------------------------------------

    def tick(self, elapsed_ms: float) -> None:
        """Advances virtual clock and triggers timers."""
        self.current_time_ms += elapsed_ms

        if self.role == Role.LEADER:
            if self.current_time_ms - self.last_heartbeat_time_ms >= self.heartbeat_interval:
                self.send_append_entries_all()
                self.last_heartbeat_time_ms = self.current_time_ms
        else:
            if self.current_time_ms - self.last_heartbeat_time_ms >= self.election_timeout_ms:
                self.start_election()

    # -------------------------------------------------------------------------
    # Leader Election
    # -------------------------------------------------------------------------

    def start_election(self) -> None:
        """Transition to Candidate and broadcast RequestVote RPCs."""
        self.role = Role.CANDIDATE
        new_term = self.current_term + 1
        self.storage.set_term_and_vote(new_term, self.node_id)
        self.votes_received = {self.node_id}
        self.leader_id = None
        self.reset_election_timer()

        if len(self.votes_received) >= self.majority:
            self.become_leader()
            return

        args = RequestVoteArgs(
            term=new_term,
            candidate_id=self.node_id,
            last_log_index=self.storage.last_log_index,
            last_log_term=self.storage.last_log_term,
        )
        for peer in self.peers:
            self.outbox.append((peer, "RequestVote", args))

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        """Processes incoming RequestVote RPC."""
        # 1. Rule: If term > current_term, step down to follower
        if args.term > self.current_term:
            self.become_follower(args.term)

        vote_granted = False
        if args.term == self.current_term:
            can_vote = (self.voted_for is None or self.voted_for == args.candidate_id)
            
            # Check election safety: candidate's log must be at least as up-to-date
            my_last_term = self.storage.last_log_term
            my_last_index = self.storage.last_log_index
            log_ok = (
                args.last_log_term > my_last_term
                or (args.last_log_term == my_last_term and args.last_log_index >= my_last_index)
            )
            
            if can_vote and log_ok:
                vote_granted = True
                self.storage.set_term_and_vote(self.current_term, args.candidate_id)
                self.reset_election_timer()

        return RequestVoteReply(
            term=self.current_term,
            vote_granted=vote_granted,
            from_id=self.node_id,
        )

    def handle_request_vote_reply(self, reply: RequestVoteReply) -> None:
        """Processes response to RequestVote RPC."""
        if reply.term > self.current_term:
            self.become_follower(reply.term)
            return

        if self.role == Role.CANDIDATE and reply.term == self.current_term and reply.vote_granted:
            self.votes_received.add(reply.from_id)
            if len(self.votes_received) >= self.majority:
                self.become_leader()

    def become_leader(self) -> None:
        """Initializes volatile leader state upon election."""
        self.role = Role.LEADER
        self.leader_id = self.node_id
        last_idx = self.storage.last_log_index
        for peer in self.peers:
            self.next_index[peer] = last_idx + 1
            self.match_index[peer] = 0
        self.last_heartbeat_time_ms = self.current_time_ms
        
        # Broadcast immediate heartbeat
        self.send_append_entries_all()

    def become_follower(self, new_term: int) -> None:
        """Transitions server back to Follower."""
        self.role = Role.FOLLOWER
        self.storage.set_term_and_vote(new_term, None)
        self.leader_id = None
        self.votes_received.clear()
        self.reset_election_timer()

    # -------------------------------------------------------------------------
    # Log Replication & AppendEntries
    # -------------------------------------------------------------------------

    def send_append_entries_all(self) -> None:
        """Broadcasts AppendEntries (or InstallSnapshot) to all peers."""
        for peer in self.peers:
            self.send_append_entries_to(peer)

    def send_append_entries_to(self, peer: str) -> None:
        next_idx = self.next_index.get(peer, self.storage.last_log_index + 1)

        # If peer is lagging behind our compacted log snapshot, send snapshot
        if next_idx <= self.storage.last_included_index:
            snap_args = InstallSnapshotArgs(
                term=self.current_term,
                leader_id=self.node_id,
                last_included_index=self.storage.last_included_index,
                last_included_term=self.storage.last_included_term,
                data=self.storage.snapshot_data,
            )
            self.outbox.append((peer, "InstallSnapshot", snap_args))
            return

        prev_log_index = next_idx - 1
        prev_log_term = self.storage.get_term_at(prev_log_index)
        entries = self.storage.get_entries_from(next_idx)

        args = AppendEntriesArgs(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index,
        )
        self.outbox.append((peer, "AppendEntries", args))

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        """Processes incoming AppendEntries RPC."""
        # 1. Reply false if term < current_term
        if args.term < self.current_term:
            return AppendEntriesReply(
                term=self.current_term,
                success=False,
                match_index=self.storage.last_log_index,
                from_id=self.node_id,
            )

        # If term > current_term or candidate seeing current term leader
        if args.term > self.current_term or self.role == Role.CANDIDATE:
            self.become_follower(args.term)

        self.leader_id = args.leader_id
        self.reset_election_timer()

        # 2. Reply false if log doesn't contain an entry at prev_log_index matching prev_log_term
        if args.prev_log_index > self.storage.last_log_index:
            # Follower's log is too short
            return AppendEntriesReply(
                term=self.current_term,
                success=False,
                match_index=self.storage.last_log_index,
                from_id=self.node_id,
                conflict_index=self.storage.last_log_index + 1,
                conflict_term=None,
            )

        if args.prev_log_index > self.storage.last_included_index:
            actual_term = self.storage.get_term_at(args.prev_log_index)
            if actual_term != args.prev_log_term:
                # Term mismatch at prev_log_index
                # Find the first index of the conflicting term for fast recovery
                conflict_term = actual_term
                first_index = args.prev_log_index
                while first_index > self.storage.last_included_index + 1:
                    if self.storage.get_term_at(first_index - 1) != conflict_term:
                        break
                    first_index -= 1
                return AppendEntriesReply(
                    term=self.current_term,
                    success=False,
                    match_index=self.storage.last_log_index,
                    from_id=self.node_id,
                    conflict_term=conflict_term,
                    conflict_index=first_index,
                )
        elif args.prev_log_index < self.storage.last_included_index:
            # Stale entry already compacted
            pass

        # 3. If an existing entry conflicts with a new one, delete existing entry and all following
        # 4. Append any new entries not already in the log
        if args.entries:
            self.storage.append_entries(args.entries)

        # 5. If leader_commit > commit_index, set commit_index = min(leader_commit, index of last new entry)
        if args.leader_commit > self.commit_index:
            self.commit_index = min(args.leader_commit, self.storage.last_log_index)
            self.apply_committed_entries()

        return AppendEntriesReply(
            term=self.current_term,
            success=True,
            match_index=self.storage.last_log_index,
            from_id=self.node_id,
        )

    def handle_append_entries_reply(self, reply: AppendEntriesReply) -> None:
        """Processes response from AppendEntries RPC on the leader."""
        if reply.term > self.current_term:
            self.become_follower(reply.term)
            return

        if self.role != Role.LEADER or reply.term != self.current_term:
            return

        peer = reply.from_id
        if reply.success:
            self.match_index[peer] = max(self.match_index.get(peer, 0), reply.match_index)
            self.next_index[peer] = self.match_index[peer] + 1
            self.check_and_update_commit_index()
        else:
            # Decrement next_index using fast recovery conflict info if available
            if reply.conflict_index is not None:
                self.next_index[peer] = max(1, reply.conflict_index)
            else:
                self.next_index[peer] = max(1, self.next_index.get(peer, 2) - 1)
            
            # Immediately retry sending AppendEntries
            self.send_append_entries_to(peer)

    def check_and_update_commit_index(self) -> None:
        """
        Leader rule: If there exists an N > commit_index such that a majority
        of match_index[i] >= N, and log[N].term == current_term, set commit_index = N.
        """
        for n in range(self.storage.last_log_index, self.commit_index, -1):
            if n <= self.storage.last_included_index:
                break
            entry = self.storage.get_entry(n)
            if entry and entry.term == self.current_term:
                # Count matches across cluster (self always matches its own log)
                count = 1
                for peer in self.peers:
                    if self.match_index.get(peer, 0) >= n:
                        count += 1
                if count >= self.majority:
                    self.commit_index = n
                    self.apply_committed_entries()
                    break

    # -------------------------------------------------------------------------
    # State Machine Execution & Snapshotting
    # -------------------------------------------------------------------------

    def apply_committed_entries(self) -> None:
        """Applies entries from last_applied + 1 to commit_index."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.storage.get_entry(self.last_applied)
            if entry is not None and entry.command is not None:
                result = self.state_machine.apply(
                    command=entry.command,
                    client_id=entry.client_id,
                    seq_num=entry.seq_num,
                )
                if self.last_applied in self.pending_client_requests:
                    req = self.pending_client_requests.pop(self.last_applied)
                    self.client_commit_callbacks[self.last_applied] = (req, result)

    def handle_install_snapshot(self, args: InstallSnapshotArgs) -> InstallSnapshotReply:
        """Processes incoming InstallSnapshot RPC."""
        if args.term < self.current_term:
            return InstallSnapshotReply(term=self.current_term, from_id=self.node_id)

        if args.term > self.current_term:
            self.become_follower(args.term)

        self.leader_id = args.leader_id
        self.reset_election_timer()

        if args.last_included_index > self.storage.last_included_index:
            self.storage.install_snapshot(
                last_included_index=args.last_included_index,
                last_included_term=args.last_included_term,
                snapshot=args.data,
            )
            self.state_machine.restore_snapshot(args.data)
            self.commit_index = max(self.commit_index, args.last_included_index)
            self.last_applied = max(self.last_applied, args.last_included_index)

        return InstallSnapshotReply(term=self.current_term, from_id=self.node_id)

    def handle_install_snapshot_reply(self, reply: InstallSnapshotReply) -> None:
        if reply.term > self.current_term:
            self.become_follower(reply.term)
            return
        if self.role == Role.LEADER and reply.term == self.current_term:
            peer = reply.from_id
            self.match_index[peer] = max(
                self.match_index.get(peer, 0), self.storage.last_included_index
            )
            self.next_index[peer] = self.match_index[peer] + 1

    def create_snapshot(self) -> None:
        """Compacts logs up to current last_applied into snapshot."""
        if self.last_applied <= self.storage.last_included_index:
            return
        snapshot_data = self.state_machine.take_snapshot()
        term = self.storage.get_term_at(self.last_applied)
        self.storage.compact_log(self.last_applied, term, snapshot_data)

    # -------------------------------------------------------------------------
    # Client Requests
    # -------------------------------------------------------------------------

    def submit_command(self, client_req: ClientRequest) -> Tuple[bool, Optional[str], Optional[int]]:
        """
        Submits command to Raft leader.
        Returns (is_leader, leader_id, assigned_index).
        """
        if self.role != Role.LEADER:
            return (False, self.leader_id, None)

        assigned_index = self.storage.last_log_index + 1
        entry = LogEntry(
            term=self.current_term,
            index=assigned_index,
            command=client_req.command,
            client_id=client_req.client_id,
            seq_num=client_req.seq_num,
        )
        self.storage.append_entries([entry])
        self.pending_client_requests[assigned_index] = client_req

        # Broadcast entry to peers
        self.send_append_entries_all()
        return (True, self.node_id, assigned_index)
