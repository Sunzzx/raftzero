# ⚡ RaftZero: High-Performance Distributed Raft Consensus Engine

[![CI](https://github.com/Sunzzx/raftzero/actions/workflows/ci.yml/badge.svg)](https://github.com/Sunzzx/raftzero/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Stripe Commercial License](https://img.shields.io/badge/Commercial%20License-$99-brightgreen.svg)](https://buy.stripe.com/test_7sYdRb3430Kb9Nx3LIabK00)

RaftZero is a mathematically formal, zero-dependency implementation of the **Raft Distributed Consensus Algorithm** (Ongaro & Ousterhout) with state-machine replication, dynamic term elections, randomized split-vote prevention, log compaction, and an integrated deterministic Byzantine network fault-injection harness.

---

## 🏛️ Mathematical Architecture & State Machine

Raft establishes consensus across $N$ distributed nodes by electing a distinguished leader, maintaining the invariant that the system makes forward progress as long as a strict majority $\lfloor N/2 \rfloor + 1$ quorum is live.

### 1. Leader Election & Randomized Terms
- Nodes cycle through three mutually exclusive states: $\mathcal{S} \in \{\text{Follower}, \text{Candidate}, \text{Leader}\}$.
- Election timeout intervals are drawn uniformly at random: $T_{\text{election}} \sim \mathcal{U}(150\text{ms}, 300\text{ms})$ to break split-vote deadlocks.
- A candidate transitions to leader if and only if it receives votes from a quorum:
  $$\sum_{i=1}^N \mathbb{I}_{\text{vote}(i) = \text{Candidate}} > \frac{N}{2}$$

### 2. Log Invariant & Prefix Matching
- If two log entries on different servers have the same index and term, they store identical state machine commands.
- If two entries have the same index and term, their logs are identical in all preceding entries:
  $$\text{Log}_A[k] = \text{Log}_B[k] \implies \forall j < k, \text{Log}_A[j] = \text{Log}_B[j]$$

---

## 🧪 Deterministic Fault Injection Harness
RaftZero includes a virtualized network harness capable of simulating:
- Asymmetric network partitions (split-brain scenarios)
- Packet drops, delay jitter, and message reordering
- Node crashes and recovery from persistent WAL storage

---

## 💼 Commercial Licensing & Support

RaftZero is dual-licensed:
- **Open Source**: MIT License for research, hobbyist, and academic projects.
- **Enterprise / Commercial**: For mission-critical production deployment, high-throughput clustering, and proprietary distributed storage engines:
  - 💳 **[Purchase Commercial License ($99 USD)](https://buy.stripe.com/test_7sYdRb3430Kb9Nx3LIabK00)**
  - Contact: `ayushkandwal2007@gmail.com`

---

## 🚀 Quickstart & Verification

```bash
# Clone and run test suite:
git clone https://github.com/Sunzzx/raftzero.git
cd raftzero
pip install -e .
pytest -v
```
