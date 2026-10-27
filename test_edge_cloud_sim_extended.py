import unittest
from unittest.mock import patch

import edge_cloud_sim_extended as sim


class ExtendedSimulationTests(unittest.TestCase):
    def test_shared_ingress_is_one_fcfs_bottleneck(self):
        tasks = [sim.Task(0, 0.0, 0.0, 1.0, 0.0), sim.Task(1, 0.01, 0.0, 1.0, 0.0)]
        rows = sim.simulate(tasks, "round_robin", 1.0, 11, 8.0, 8.0)
        by_id = {r["task_id"]: r for r in rows}
        self.assertAlmostEqual(by_id[0]["access_up_start_s"], 0.0)
        self.assertAlmostEqual(by_id[0]["access_up_finish_s"], 1.0)
        self.assertAlmostEqual(by_id[1]["access_up_start_s"], 1.0)
        self.assertAlmostEqual(by_id[1]["access_up_finish_s"], 2.0)

    def test_1mb_on_100mbps_access_link_takes_80ms(self):
        self.assertAlmostEqual(sim.transfer_s(1.0, 100.0), 0.08)
        tasks = [sim.Task(0, 0.0, 0.0, 1.0, 0.0)]
        row = sim.simulate(tasks, "round_robin", 1.0, 12, 100.0, 100.0)[0]
        self.assertAlmostEqual(row["access_up_finish_s"] - row["access_up_start_s"], 0.08)

    def test_full_duplex_directions_are_independent(self):
        original_nodes = sim.NODES
        try:
            sim.NODES = [sim.Node("test", 0.0, 100.0, 100.0, 1, 1.0)]
            tasks = [
                sim.Task(0, 0.0, 0.0, 1.0, 1.0),
                sim.Task(1, 0.2, 0.0, 1.0, 0.0),
            ]
            rows = sim.simulate(tasks, "round_robin", 1.0, 13, 100.0, 100.0)
            by_id = {r["task_id"]: r for r in rows}
            # Task 0's shared egress occupies [0.24, 0.32], while task 1 can
            # simultaneously use the independent shared ingress at t=0.20.
            self.assertAlmostEqual(by_id[0]["access_down_start_s"], 0.24)
            self.assertAlmostEqual(by_id[0]["access_down_finish_s"], 0.32)
            self.assertAlmostEqual(by_id[1]["access_up_start_s"], 0.2)
        finally:
            sim.NODES = original_nodes

    def test_propagation_and_jitter_are_applied_once_each_direction(self):
        original_nodes = sim.NODES
        try:
            sim.NODES = [sim.Node("test", 0.5, 1000.0, 1000.0, 1, 1.0)]
            tasks = [sim.Task(0, 0.0, 1.0, 0.0, 0.0)]
            row = sim.simulate(tasks, "round_robin", 1.0, 17, 1000.0, 1000.0)[0]
            self.assertAlmostEqual(row["node_up_finish_s"], 0.0)
            self.assertGreaterEqual(row["upload_arrival_s"], 0.5)
            self.assertLessEqual(row["upload_arrival_s"], 0.6)
            self.assertGreaterEqual(row["completion_s"], 2.0)
            self.assertLessEqual(row["completion_s"], 2.2)
        finally:
            sim.NODES = original_nodes

    def test_sec_predicts_expected_not_realized_future_jitter(self):
        original_nodes = sim.NODES
        try:
            sim.NODES = [sim.Node("test", 0.5, 1000.0, 1000.0, 1, 1.0)]
            task = sim.Task(0, 0.0, 1.0, 0.0, 0.0)
            engine = sim.Engine([task], "shortest_estimated_completion", 3.0, 31, 1000.0, 1000.0)
            with patch.object(sim, "jitter_s", side_effect=AssertionError("predictor read a future random draw")):
                estimate, _ = engine._estimate_completion(task, 0)
            # Mean input/output sizes traverse both access and node links;
            # expected propagation+jitter is applied once in each direction.
            expected = (
                2 * (0.5 * 3.0 + 0.1 * 0.5 * 3.0)
                + sim.MEAN_WORK_MI / sim.NODES[0].per_core_mi_s
                + 2 * sim.transfer_s(sim.MEAN_INPUT_MB, 1000.0)
                + 2 * sim.transfer_s(sim.MEAN_OUTPUT_MB, 1000.0)
            )
            self.assertAlmostEqual(estimate, expected)
        finally:
            sim.NODES = original_nodes

    def test_node_link_releases_after_serialization_not_propagation(self):
        original_nodes = sim.NODES
        try:
            sim.NODES = [sim.Node("test", 0.5, 8.0, 8.0, 1, 1.0)]
            tasks = [
                sim.Task(0, 0.0, 0.1, 1.0, 0.0),
                sim.Task(1, 0.01, 0.1, 1.0, 0.0),
            ]
            rows = sim.simulate(tasks, "round_robin", 1.0, 19, 800.0, 800.0)
            by_id = {r["task_id"]: r for r in rows}
            self.assertAlmostEqual(by_id[0]["node_up_finish_s"], 1.01)
            self.assertGreaterEqual(by_id[0]["upload_arrival_s"], 1.51)
            self.assertAlmostEqual(by_id[1]["node_up_start_s"], 1.01)
            self.assertLess(by_id[1]["node_up_start_s"], by_id[0]["upload_arrival_s"])
        finally:
            sim.NODES = original_nodes

    def test_two_cpu_cores_process_jobs_concurrently(self):
        original_nodes = sim.NODES
        try:
            sim.NODES = [sim.Node("test", 0.0, 1000.0, 1000.0, 2, 1.0)]
            tasks = [sim.Task(0, 0.0, 1.0, 0.0, 0.0), sim.Task(1, 0.01, 1.0, 0.0, 0.0)]
            rows = sim.simulate(tasks, "round_robin", 1.0, 23, 1000.0, 1000.0)
            by_id = {r["task_id"]: r for r in rows}
            self.assertAlmostEqual(by_id[0]["compute_start_s"], 0.0)
            self.assertAlmostEqual(by_id[1]["compute_start_s"], 0.01)
            self.assertAlmostEqual(by_id[0]["compute_finish_s"], 1.0)
            self.assertAlmostEqual(by_id[1]["compute_finish_s"], 1.01)
        finally:
            sim.NODES = original_nodes

    def test_finite_cohort_is_drained_after_arrival_window(self):
        original_nodes = sim.NODES
        try:
            sim.NODES = [sim.Node("test", 0.0, 8.0, 8.0, 1, 1.0)]
            task = sim.Task(0, 119.9, 0.0, 1.0, 0.0)
            row = sim.simulate([task], "round_robin", 1.0, 29, 8.0, 8.0)[0]
            self.assertGreater(row["completion_s"], 120.0)
        finally:
            sim.NODES = original_nodes

    def test_conservation_and_nonnegative_latency(self):
        tasks = sim.generate_tasks(2.0, 5.0, 20261009)
        for policy in sim.POLICIES:
            rows = sim.simulate(tasks, policy, 1.0, 20261009, 100.0, 100.0)
            self.assertEqual(len(rows), len(tasks))
            self.assertEqual(len({r["task_id"] for r in rows}), len(tasks))
            for row in rows:
                self.assertGreaterEqual(row["latency_ms"], 0.0)
                self.assertLessEqual(row["arrival_s"], row["access_up_start_s"])
                self.assertLessEqual(row["compute_start_s"], row["compute_finish_s"])
                self.assertLessEqual(row["compute_finish_s"], row["completion_s"])

    def test_t_interval_refuses_unmatched_replication_count(self):
        with self.assertRaises(ValueError):
            sim.ci95([1.0, 2.0, 3.0, 4.0, 5.0])
        with self.assertRaises(ValueError):
            sim.evaluate(2.0, 1.0, 10.0, 1, 100.0, 100.0, False)

    def test_summaries_separate_egress_capacity(self):
        rows = []
        for down in (50.0, 100.0):
            for rep in range(1, sim.REPLICATIONS + 1):
                for policy, offset in (("round_robin", 20.0), ("shortest_estimated_completion", 0.0)):
                    value = float(rep) + offset
                    rows.append({
                        "arrival_rate_tasks_per_s": 6.0,
                        "network_multiplier": 1.0,
                        "access_up_mbps": 100.0,
                        "access_down_mbps": down,
                        "replication": rep,
                        "policy": policy,
                        "mean_latency_ms": value,
                        "median_latency_ms": value,
                        "p95_latency_ms": value,
                        "last_completion_s": value,
                    })
        summary, paired = sim.summarize(rows)
        self.assertEqual(len(summary), 4)
        self.assertEqual(len(paired), 2)
        self.assertEqual({r["access_down_mbps"] for r in paired}, {50.0, 100.0})
        self.assertTrue(all(r["mean_paired_difference_rr_minus_sec_ms"] == 20.0 for r in paired))

    def test_paired_policy_run_is_deterministic(self):
        tasks = sim.generate_tasks(2.0, 5.0, 20261009)
        first = sim.simulate(tasks, "shortest_estimated_completion", 3.0, 20261009, 100.0, 100.0)
        second = sim.simulate(tasks, "shortest_estimated_completion", 3.0, 20261009, 100.0, 100.0)
        self.assertEqual(first, second)

    def test_weighted_round_robin_follows_weights(self):
        tasks = [sim.Task(i, 0.01 * i, 1.0, 0.01, 0.01) for i in range(40)]
        rows = sim.simulate(tasks, "weighted_round_robin", 1.0, 5, 100.0, 100.0)
        counts = {n.name: sum(r["node"] == n.name for r in rows) for n in sim.NODES}
        self.assertEqual(counts, {"edge_1": 9, "edge_2": 7, "cloud": 24})

    def test_sec_mean_reservation_does_not_use_hidden_sizes(self):
        node_name = sim.NODES
        a = sim.Task(0, 0.0, 1800.0, 1.0, 5.0)
        b = sim.Task(0, 0.0, 80.0, 1.0, 0.01)
        frees = []
        for t in (a, b):
            e = sim.Engine([t], "sec_mean_reservation", 1.0, 1, 100.0, 100.0)
            e._reserve_prediction(t, 2)
            frees.append((max(e.pred_cpu_free[2]), e.pred_node_down_free[2], e.pred_access_down_free))
        self.assertEqual(frees[0], frees[1])
        e = sim.Engine([a], "shortest_estimated_completion", 1.0, 1, 100.0, 100.0)
        e._reserve_prediction(a, 2)
        self.assertGreater(max(e.pred_cpu_free[2]), frees[0][0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
