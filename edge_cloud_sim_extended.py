#!/usr/bin/env python3
"""Extended illustrative edge-cloud simulation with event-driven queues.

Compared with edge_cloud_sim.py, this version adds (1) shared ingress/egress
access links used by all routes, (2) multi-core CPU pools with per-core service
rates preserving each node's previous aggregate compute capacity, and (3) an
event calendar that enforces FIFO queue arrival order at each resource. All
infrastructure values remain illustrative, not measurements.
"""
from __future__ import annotations

import csv
import heapq
import math
import random
import statistics
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Node:
    name: str
    one_way_latency_s: float
    uplink_mbps: float
    downlink_mbps: float
    cores: int
    per_core_mi_s: float

    @property
    def aggregate_compute_mi_s(self) -> float:
        return self.cores * self.per_core_mi_s


@dataclass(frozen=True)
class Task:
    task_id: int
    arrival_s: float
    work_mi: float
    input_mb: float
    output_mb: float


NODES = [
    Node("edge_1", 0.010, 40.0, 80.0, 2, 450.0),
    Node("edge_2", 0.015, 25.0, 50.0, 2, 350.0),
    Node("cloud", 0.045, 100.0, 200.0, 8, 300.0),
]
ARRIVAL_RATES = [2.0, 6.0]
NETWORK_MULTIPLIERS = [1.0, 3.0]
SIMULATION_DURATION_S = 120.0
REPLICATIONS = 20
BASE_SEED = 20261009
DEFAULT_ACCESS_UP_Mbps = 100.0
DEFAULT_ACCESS_DOWN_Mbps = 100.0
MEAN_WORK_MI = 546.7
MEAN_INPUT_MB = 1.794
MEAN_OUTPUT_MB = 0.1654
T_CRITICAL_95_DF19 = 2.093024054
POLICIES = ["round_robin", "shortest_estimated_completion"]
# Supplementary policies (used only by supplementary_experiments.py).
#  - sec_mean_reservation: SEC whose queue reservations for already-assigned
#    tasks use population means for work and output size (only the input size,
#    observable on arrival, is exact), i.e. no hidden per-task information.
#  - weighted_round_robin: smooth weighted RR with weights proportional to
#    aggregate node compute (9:7:24).
EXTRA_POLICIES = ["sec_mean_reservation", "weighted_round_robin"]
ALL_POLICIES = POLICIES + EXTRA_POLICIES
WRR_WEIGHTS = [9, 7, 24]


def transfer_s(size_mb: float, bandwidth_mbps: float) -> float:
    if size_mb < 0 or bandwidth_mbps <= 0:
        raise ValueError("Transfer size must be nonnegative and bandwidth positive")
    return 8.0 * size_mb / bandwidth_mbps


def generate_tasks(rate: float, duration_s: float, seed: int) -> list[Task]:
    """Use exactly the base model's task stream for paired comparison."""
    rng = random.Random(seed)
    tasks: list[Task] = []
    t = 0.0
    while True:
        t += rng.expovariate(rate)
        if t > duration_s:
            break
        work = min(1800.0, max(80.0, rng.lognormvariate(math.log(450), 0.65)))
        input_mb = min(20.0, max(0.05, rng.lognormvariate(math.log(1.2), 0.9)))
        output_mb = min(5.0, max(0.01, rng.lognormvariate(math.log(0.12), 0.8)))
        tasks.append(Task(len(tasks), t, work, input_mb, output_mb))
    return tasks


def jitter_s(task_id: int, node_idx: int, stage: str, multiplier: float, seed: int) -> float:
    stage_code = 1 if stage == "up" else 2
    rng = random.Random(seed * 1000003 + task_id * 9176 + node_idx * 131 + stage_code)
    return rng.uniform(0.0, 0.20 * NODES[node_idx].one_way_latency_s * multiplier)


def expected_jitter_s(node_idx: int, multiplier: float) -> float:
    """Mean of the bounded U(0, 0.2 * propagation) jitter distribution."""
    return 0.10 * NODES[node_idx].one_way_latency_s * multiplier


class Link:
    """One FCFS link; propagation is applied outside this resource."""
    def __init__(self, name: str, start_field: str, finish_field: str):
        self.name = name
        self.start_field = start_field
        self.finish_field = finish_field
        self.busy = False
        self.waiting: deque[tuple[int, float, str]] = deque()

    def request(self, engine: "Engine", now: float, task_id: int,
                service_s: float, action: str) -> None:
        if self.busy:
            self.waiting.append((task_id, service_s, action))
        else:
            self._start(engine, now, task_id, service_s, action)

    def _start(self, engine: "Engine", now: float, task_id: int,
               service_s: float, action: str) -> None:
        self.busy = True
        engine.records[task_id][self.start_field] = now
        engine.push(now + service_s, 0, "link_done", (self.name, task_id, action))

    def complete(self, engine: "Engine", now: float) -> None:
        if self.waiting:
            task_id, service_s, action = self.waiting.popleft()
            self._start(engine, now, task_id, service_s, action)
        else:
            self.busy = False


class CpuPool:
    """FCFS queue feeding identical single-task CPU cores."""
    def __init__(self, node_idx: int, core_count: int):
        self.node_idx = node_idx
        self.idle = list(range(core_count))
        self.waiting: deque[tuple[int, float]] = deque()

    def request(self, engine: "Engine", now: float, task_id: int, work_mi: float) -> None:
        if self.idle:
            core = self.idle.pop(0)
            engine.start_cpu(now, self.node_idx, core, task_id, work_mi)
        else:
            self.waiting.append((task_id, work_mi))

    def complete(self, engine: "Engine", now: float, core: int) -> None:
        if self.waiting:
            task_id, work_mi = self.waiting.popleft()
            engine.start_cpu(now, self.node_idx, core, task_id, work_mi)
        else:
            self.idle.append(core)
            self.idle.sort()


class Engine:
    def __init__(self, tasks: list[Task], policy: str, multiplier: float, seed: int,
                 access_up_mbps: float, access_down_mbps: float):
        if policy not in ALL_POLICIES:
            raise ValueError(f"Unknown policy: {policy}")
        if multiplier <= 0 or access_up_mbps <= 0 or access_down_mbps <= 0:
            raise ValueError("Network multiplier and access link rates must be positive")
        self.tasks = tasks
        self.policy = policy
        self.multiplier = multiplier
        self.seed = seed
        self.access_up_mbps = access_up_mbps
        self.access_down_mbps = access_down_mbps
        self.events: list[tuple[float, int, int, str, Any]] = []
        self.sequence = 0
        self.records: dict[int, dict[str, Any]] = {}
        self.task_by_id = {t.task_id: t for t in tasks}
        self.rr_pointer = 0
        self.wrr_current = [0] * len(NODES)
        self.links: dict[str, Link] = {
            "access_up": Link("access_up", "access_up_start_s", "access_up_finish_s"),
            "access_down": Link("access_down", "access_down_start_s", "access_down_finish_s"),
        }
        for i in range(len(NODES)):
            self.links[f"node_up_{i}"] = Link(f"node_up_{i}", "node_up_start_s", "node_up_finish_s")
            self.links[f"node_down_{i}"] = Link(f"node_down_{i}", "node_down_start_s", "node_down_finish_s")
        self.cpu_pools = [CpuPool(i, node.cores) for i, node in enumerate(NODES)]

        # Predicted resource availability for the placement policy. Updates use
        # exact sizes of already-assigned tasks; the incoming task is estimated
        # using fixed population means.
        self.pred_access_up_free = 0.0
        self.pred_access_down_free = 0.0
        self.pred_node_up_free = [0.0] * len(NODES)
        self.pred_node_down_free = [0.0] * len(NODES)
        self.pred_cpu_free = [[0.0] * node.cores for node in NODES]

    def push(self, time_s: float, priority: int, kind: str, payload: Any) -> None:
        self.sequence += 1
        heapq.heappush(self.events, (time_s, priority, self.sequence, kind, payload))

    def _estimate_completion(self, task: Task, node_idx: int) -> tuple[float, tuple]:
        node = NODES[node_idx]
        delay = node.one_way_latency_s * self.multiplier
        access_up_start = max(task.arrival_s, self.pred_access_up_free)
        access_up_end = access_up_start + transfer_s(MEAN_INPUT_MB, self.access_up_mbps)
        node_up_start = max(access_up_end, self.pred_node_up_free[node_idx])
        node_up_end = node_up_start + transfer_s(MEAN_INPUT_MB, node.uplink_mbps)
        up_arrival = node_up_end + delay + expected_jitter_s(node_idx, self.multiplier)
        core = min(range(node.cores), key=lambda c: self.pred_cpu_free[node_idx][c])
        cpu_start = max(up_arrival, self.pred_cpu_free[node_idx][core])
        cpu_end = cpu_start + MEAN_WORK_MI / node.per_core_mi_s
        node_down_start = max(cpu_end, self.pred_node_down_free[node_idx])
        node_down_end = node_down_start + transfer_s(MEAN_OUTPUT_MB, node.downlink_mbps)
        down_arrival = node_down_end + delay + expected_jitter_s(node_idx, self.multiplier)
        access_down_start = max(down_arrival, self.pred_access_down_free)
        completion = access_down_start + transfer_s(MEAN_OUTPUT_MB, self.access_down_mbps)
        return completion, (access_up_end, node_up_end, core, cpu_end, node_down_end, completion)

    def _reserve_prediction(self, task: Task, node_idx: int) -> None:
        exact = self.policy != "sec_mean_reservation"
        work_mi = task.work_mi if exact else MEAN_WORK_MI
        output_mb = task.output_mb if exact else MEAN_OUTPUT_MB
        node = NODES[node_idx]
        delay = node.one_way_latency_s * self.multiplier
        access_up_start = max(task.arrival_s, self.pred_access_up_free)
        access_up_end = access_up_start + transfer_s(task.input_mb, self.access_up_mbps)
        self.pred_access_up_free = access_up_end
        node_up_start = max(access_up_end, self.pred_node_up_free[node_idx])
        node_up_end = node_up_start + transfer_s(task.input_mb, node.uplink_mbps)
        self.pred_node_up_free[node_idx] = node_up_end
        up_arrival = node_up_end + delay + expected_jitter_s(node_idx, self.multiplier)
        core = min(range(node.cores), key=lambda c: self.pred_cpu_free[node_idx][c])
        cpu_start = max(up_arrival, self.pred_cpu_free[node_idx][core])
        cpu_end = cpu_start + work_mi / node.per_core_mi_s
        self.pred_cpu_free[node_idx][core] = cpu_end
        node_down_start = max(cpu_end, self.pred_node_down_free[node_idx])
        node_down_end = node_down_start + transfer_s(output_mb, node.downlink_mbps)
        self.pred_node_down_free[node_idx] = node_down_end
        down_arrival = node_down_end + delay + expected_jitter_s(node_idx, self.multiplier)
        access_down_start = max(down_arrival, self.pred_access_down_free)
        self.pred_access_down_free = access_down_start + transfer_s(output_mb, self.access_down_mbps)

    def _choose_node(self, task: Task) -> int:
        if self.policy == "round_robin":
            selected = self.rr_pointer % len(NODES)
            self.rr_pointer += 1
            return selected
        if self.policy == "weighted_round_robin":
            for i, w in enumerate(WRR_WEIGHTS):
                self.wrr_current[i] += w
            selected = max(range(len(NODES)), key=self.wrr_current.__getitem__)
            self.wrr_current[selected] -= sum(WRR_WEIGHTS)
            return selected
        estimates = [self._estimate_completion(task, i)[0] for i in range(len(NODES))]
        return min(range(len(NODES)), key=estimates.__getitem__)

    def start_cpu(self, now: float, node_idx: int, core: int, task_id: int, work_mi: float) -> None:
        self.records[task_id]["compute_start_s"] = now
        self.records[task_id]["compute_core_id"] = core
        duration = work_mi / NODES[node_idx].per_core_mi_s
        self.push(now + duration, 0, "cpu_done", (node_idx, core, task_id))

    def run(self) -> list[dict[str, Any]]:
        for task in self.tasks:
            self.push(task.arrival_s, 1, "task_arrival", task.task_id)
        while self.events:
            now, _, _, kind, payload = heapq.heappop(self.events)
            if kind == "task_arrival":
                task = self.task_by_id[payload]
                node_idx = self._choose_node(task)
                self.records[task.task_id] = {
                    "policy": self.policy,
                    "task_id": task.task_id,
                    "node": NODES[node_idx].name,
                    "arrival_s": task.arrival_s,
                    "work_mi": task.work_mi,
                    "input_mb": task.input_mb,
                    "output_mb": task.output_mb,
                }
                self._reserve_prediction(task, node_idx)
                self.links["access_up"].request(
                    self, now, task.task_id, transfer_s(task.input_mb, self.access_up_mbps), "access_up_done")
                self.records[task.task_id]["node_idx"] = node_idx
            elif kind == "link_done":
                link_name, task_id, action = payload
                link = self.links[link_name]
                self.records[task_id][link.finish_field] = now
                link.complete(self, now)
                task = self.task_by_id[task_id]
                node_idx = self.records[task_id]["node_idx"]
                node = NODES[node_idx]
                if action == "access_up_done":
                    self.links[f"node_up_{node_idx}"].request(
                        self, now, task_id, transfer_s(task.input_mb, node.uplink_mbps), "node_up_done")
                elif action == "node_up_done":
                    delay = node.one_way_latency_s * self.multiplier
                    jitter = jitter_s(task_id, node_idx, "up", self.multiplier, self.seed)
                    self.push(now + delay + jitter, 1, "upload_arrival", task_id)
                elif action == "node_down_done":
                    delay = node.one_way_latency_s * self.multiplier
                    jitter = jitter_s(task_id, node_idx, "down", self.multiplier, self.seed)
                    self.push(now + delay + jitter, 1, "download_arrival", task_id)
                elif action == "access_down_done":
                    record = self.records[task_id]
                    record["completion_s"] = now
                    record["latency_ms"] = 1000.0 * (now - task.arrival_s)
                else:
                    raise RuntimeError(f"Unknown link action {action}")
            elif kind == "upload_arrival":
                task = self.task_by_id[payload]
                node_idx = self.records[payload]["node_idx"]
                self.records[payload]["upload_arrival_s"] = now
                self.cpu_pools[node_idx].request(self, now, payload, task.work_mi)
            elif kind == "cpu_done":
                node_idx, core, task_id = payload
                self.records[task_id]["compute_finish_s"] = now
                self.cpu_pools[node_idx].complete(self, now, core)
                node = NODES[node_idx]
                self.links[f"node_down_{node_idx}"].request(
                    self, now, task_id, transfer_s(self.task_by_id[task_id].output_mb, node.downlink_mbps), "node_down_done")
            elif kind == "download_arrival":
                task = self.task_by_id[payload]
                self.records[payload]["node_download_arrival_s"] = now
                self.links["access_down"].request(
                    self, now, payload, transfer_s(task.output_mb, self.access_down_mbps), "access_down_done")
            else:
                raise RuntimeError(f"Unknown event {kind}")
        result = []
        for task in self.tasks:
            rec = self.records[task.task_id]
            if "completion_s" not in rec:
                raise RuntimeError(f"Task {task.task_id} did not complete")
            rec["network_multiplier"] = self.multiplier
            rec["access_up_mbps"] = self.access_up_mbps
            rec["access_down_mbps"] = self.access_down_mbps
            result.append(rec)
        return result


def simulate(tasks: list[Task], policy: str, multiplier: float, seed: int,
             access_up_mbps: float = DEFAULT_ACCESS_UP_Mbps,
             access_down_mbps: float = DEFAULT_ACCESS_DOWN_Mbps) -> list[dict[str, Any]]:
    return Engine(tasks, policy, multiplier, seed, access_up_mbps, access_down_mbps).run()


def nearest_rank(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def ci95(values: list[float]) -> tuple[float, float, float]:
    if len(values) != REPLICATIONS:
        raise ValueError(f"95% t interval is configured for exactly {REPLICATIONS} replications")
    mean = statistics.mean(values)
    margin = T_CRITICAL_95_DF19 * statistics.stdev(values) / math.sqrt(len(values))
    return mean, mean - margin, mean + margin


def evaluate(rate: float, multiplier: float, duration_s: float,
             replications: int, access_up_mbps: float,
             access_down_mbps: float, keep_raw: bool,
             policies: list[str] | None = None) -> tuple[list[dict], list[dict], list[dict]]:
    policies = POLICIES if policies is None else policies
    if replications != REPLICATIONS:
        raise ValueError(f"This experiment uses exactly {REPLICATIONS} replications for its t intervals")
    rep_rows: list[dict] = []
    raw_rows: list[dict] = []
    routes: list[dict] = []
    for rep in range(replications):
        seed = BASE_SEED + rep
        tasks = generate_tasks(rate, duration_s, seed)
        for policy in policies:
            results = simulate(tasks, policy, multiplier, seed, access_up_mbps, access_down_mbps)
            latencies = [r["latency_ms"] for r in results]
            rep_rows.append({
                "arrival_rate_tasks_per_s": rate,
                "network_multiplier": multiplier,
                "duration_s": duration_s,
                "access_up_mbps": access_up_mbps,
                "access_down_mbps": access_down_mbps,
                "replication": rep + 1,
                "seed": seed,
                "policy": policy,
                "tasks": len(results),
                "mean_latency_ms": statistics.mean(latencies),
                "median_latency_ms": statistics.median(latencies),
                "p95_latency_ms": nearest_rank(latencies, 0.95),
                "p99_latency_ms": nearest_rank(latencies, 0.99),
                "last_completion_s": max(r["completion_s"] for r in results),
            })
            counts = {node.name: 0 for node in NODES}
            for r in results:
                counts[r["node"]] += 1
                if keep_raw:
                    raw_rows.append({
                        **{k: v for k, v in r.items() if k != "node_idx"},
                        "arrival_rate_tasks_per_s": rate,
                        "duration_s": duration_s,
                        "replication": rep + 1,
                        "seed": seed,
                        "policy": policy,
                    })
            for node_name, count in counts.items():
                routes.append({
                    "arrival_rate_tasks_per_s": rate,
                    "network_multiplier": multiplier,
                    "access_up_mbps": access_up_mbps,
                    "access_down_mbps": access_down_mbps,
                    "replication": rep + 1,
                    "policy": policy,
                    "node": node_name,
                    "tasks": count,
                    "share": count / len(results),
                })
    return rep_rows, raw_rows, routes


def summarize(rep_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    groups: dict[tuple, list[dict]] = {}
    for row in rep_rows:
        key = (row["arrival_rate_tasks_per_s"], row["network_multiplier"], row["access_up_mbps"], row["access_down_mbps"], row["policy"])
        groups.setdefault(key, []).append(row)
    summary = []
    for (rate, net, access_up, access_down, policy), rows in sorted(groups.items()):
        mean, lo, hi = ci95([r["mean_latency_ms"] for r in rows])
        summary.append({
            "arrival_rate_tasks_per_s": rate,
            "network_multiplier": net,
            "access_up_mbps": access_up,
            "access_down_mbps": access_down,
            "policy": policy,
            "replications": len(rows),
            "mean_latency_ms": mean,
            "ci95_low_ms": lo,
            "ci95_high_ms": hi,
            "mean_of_replication_medians_ms": statistics.mean(r["median_latency_ms"] for r in rows),
            "mean_of_replication_p95_ms": statistics.mean(r["p95_latency_ms"] for r in rows),
            "mean_last_completion_s": statistics.mean(r["last_completion_s"] for r in rows),
        })
    paired = []
    scenarios = sorted({(r["arrival_rate_tasks_per_s"], r["network_multiplier"], r["access_up_mbps"], r["access_down_mbps"]) for r in rep_rows})
    for rate, net, access_up, access_down in scenarios:
        rr = {r["replication"]: r for r in rep_rows if r["arrival_rate_tasks_per_s"] == rate and r["network_multiplier"] == net and r["access_up_mbps"] == access_up and r["access_down_mbps"] == access_down and r["policy"] == "round_robin"}
        sec = {r["replication"]: r for r in rep_rows if r["arrival_rate_tasks_per_s"] == rate and r["network_multiplier"] == net and r["access_up_mbps"] == access_up and r["access_down_mbps"] == access_down and r["policy"] == "shortest_estimated_completion"}
        diffs = [rr[k]["mean_latency_ms"] - sec[k]["mean_latency_ms"] for k in sorted(rr)]
        diff, lo, hi = ci95(diffs)
        rr_mean = statistics.mean(r["mean_latency_ms"] for r in rr.values())
        sec_mean = statistics.mean(r["mean_latency_ms"] for r in sec.values())
        paired.append({
            "arrival_rate_tasks_per_s": rate,
            "network_multiplier": net,
            "access_up_mbps": access_up,
            "access_down_mbps": access_down,
            "replications": len(diffs),
            "mean_paired_difference_rr_minus_sec_ms": diff,
            "paired_ci95_low_ms": lo,
            "paired_ci95_high_ms": hi,
            "relative_reduction_percent": 100 * (rr_mean - sec_mean) / rr_mean,
            "sec_better_replications": sum(d > 0 for d in diffs),
        })
    return summary, paired


def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        raise ValueError(f"No rows for {path}")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    reps_all: list[dict] = []
    raw_all: list[dict] = []
    routes_all: list[dict] = []
    for rate in ARRIVAL_RATES:
        for net in NETWORK_MULTIPLIERS:
            reps, raw, routes = evaluate(rate, net, SIMULATION_DURATION_S, REPLICATIONS,
                                         DEFAULT_ACCESS_UP_Mbps, DEFAULT_ACCESS_DOWN_Mbps, True)
            reps_all += reps
            raw_all += raw
            routes_all += routes
    summary, paired = summarize(reps_all)
    write_csv("extended_replications.csv", reps_all)
    write_csv("extended_summary.csv", summary)
    write_csv("extended_paired.csv", paired)
    write_csv("extended_routes.csv", routes_all)
    write_csv("extended_raw.csv", raw_all)

    # Access-capacity sensitivity: high arrival rate, low propagation multiplier.
    sensitivity_reps = []
    for cap in (50.0, 100.0, 200.0):
        rows, _, _ = evaluate(6.0, 1.0, SIMULATION_DURATION_S, REPLICATIONS, cap, cap, False)
        sensitivity_reps.extend(rows)
    sens_summary, sens_paired = summarize(sensitivity_reps)
    write_csv("extended_access_sensitivity_replications.csv", sensitivity_reps)
    write_csv("extended_access_sensitivity_summary.csv", sens_summary)
    write_csv("extended_access_sensitivity_paired.csv", sens_paired)

    print("Extended event-driven model; illustrative assumptions, not measured infrastructure")
    for row in paired:
        print(f"load={row['arrival_rate_tasks_per_s']:.0f}/s net=x{row['network_multiplier']:.0f} access={row['access_up_mbps']:.0f}Mbps: "
              f"RR={next(s['mean_latency_ms'] for s in summary if s['arrival_rate_tasks_per_s']==row['arrival_rate_tasks_per_s'] and s['network_multiplier']==row['network_multiplier'] and s['policy']=='round_robin'):.1f}ms; "
              f"SEC better by {row['relative_reduction_percent']:.1f}% ({row['sec_better_replications']}/{row['replications']})")
    print("Access-capacity sensitivity at 6 tasks/s, m=1:")
    for row in sens_paired:
        print(f"access={row['access_up_mbps']:.0f}Mbps reduction={row['relative_reduction_percent']:.1f}% "
              f"paired delta CI=[{row['paired_ci95_low_ms']:.1f},{row['paired_ci95_high_ms']:.1f}] ms")
    try:
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(1, 2, figsize=(9, 3.4), sharey=True)
        colors = {"round_robin": "#2563a6", "shortest_estimated_completion": "#e87516"}
        labels = {"round_robin": "Round-robin", "shortest_estimated_completion": "Shortest estimated completion"}
        for ax, net in zip(axes, NETWORK_MULTIPLIERS):
            for policy in POLICIES:
                points = sorted((r for r in summary if r["network_multiplier"] == net), key=lambda r: r["arrival_rate_tasks_per_s"])
                points = [r for r in points if r["policy"] == policy]
                x = [r["arrival_rate_tasks_per_s"] for r in points]
                y = [r["mean_latency_ms"] for r in points]
                lo = [a - r["ci95_low_ms"] for a, r in zip(y, points)]
                hi = [r["ci95_high_ms"] - a for a, r in zip(y, points)]
                ax.errorbar(x, y, yerr=[lo, hi], marker="o", linestyle="none", capsize=3,
                            label=labels[policy], color=colors[policy])
            ax.set_title(f"Propagation multiplier m={int(net)}")
            ax.set_xlabel("Task arrival rate (tasks/s)")
            ax.set_yscale("log")
            ax.grid(True, which="both", axis="y", alpha=0.25)
        axes[0].set_ylabel("Mean latency (ms, log scale)")
        axes[1].legend(fontsize=8)
        fig.tight_layout()
        fig.savefig("extended_plot.png", dpi=180)
    except ImportError:
        pass


if __name__ == "__main__":
    main()
