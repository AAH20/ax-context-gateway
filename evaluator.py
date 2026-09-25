"""
Automated Protocol & Security Evaluator for AX Context Gateway.
Executes synthetic conformance checks, cross-tenant leak drills,
and suspension rehydration benchmarks.
"""

from __future__ import annotations
import json
import os
import tempfile
import time
from typing import Any, Dict, List
from ax_context_gateway.compiler import (
    BoundedContextCompiler,
    ContextPolicy,
    MemoryNode,
)
from ax_context_gateway.snapshotter import AxTaskSnapshotter


class AxContextEvaluator:
    """
    Evaluates context boundaries, tenant isolation, and cryptographic integrity
    for AX agent workloads.
    """

    def __init__(self):
        self.compiler = BoundedContextCompiler()

    def run_tenant_leak_drill(self) -> Dict[str, Any]:
        """
        Adversarial test: Queries with high lexical similarity to a rival tenant's
        sensitive data to confirm 100% zero leakage.
        """
        victim_tenant = "tenant-bank-corp"
        attacker_tenant = "tenant-adversary"

        nodes = [
            MemoryNode(
                node_id="secret-vault-1",
                tenant_id=victim_tenant,
                content="API_KEY=sk_live_99482910 secret corporate vault credentials",
                token_count=25,
            ),
            MemoryNode(
                node_id="public-faq-1",
                tenant_id=attacker_tenant,
                content="Corporate general information and public FAQ",
                token_count=20,
            ),
        ]

        compiled = self.compiler.compile(
            query="API_KEY secret corporate vault credentials",
            tenant_id=attacker_tenant,
            agent_id="attacker-agent",
            available_nodes=nodes,
            available_edges=[],
        )

        leaked_nodes = [
            n for n in compiled.nodes if n["node_id"] == "secret-vault-1" or "sk_live" in n["content"]
        ]

        passed = len(leaked_nodes) == 0
        return {
            "test_name": "tenant_isolation_leak_drill",
            "passed": passed,
            "victim_tenant": victim_tenant,
            "attacker_tenant": attacker_tenant,
            "leaked_count": len(leaked_nodes),
            "status": "SECURE" if passed else "FAILED_LEAK_DETECTED",
        }

    def run_token_starvation_drill(self) -> Dict[str, Any]:
        """
        Capacity test: Feeds a massive graph payload to verify strict token bounding.
        """
        tenant_id = "tenant-stress-test"
        policy = ContextPolicy(max_context_tokens=500)

        # Generate 100 nodes of 50 tokens each (5000 tokens total)
        nodes = [
            MemoryNode(
                node_id=f"node-{i}",
                tenant_id=tenant_id,
                content=f"Log telemetry record batch {i} system performance report",
                token_count=50,
            )
            for i in range(100)
        ]

        compiled = self.compiler.compile(
            query="telemetry record report",
            tenant_id=tenant_id,
            agent_id="stress-agent",
            available_nodes=nodes,
            available_edges=[],
            policy=policy,
        )

        passed = compiled.total_tokens <= 500 and compiled.budget_exhausted
        return {
            "test_name": "token_starvation_bounding_drill",
            "passed": passed,
            "max_budget": 500,
            "total_admitted_tokens": compiled.total_tokens,
            "admitted_node_count": len(compiled.nodes),
            "budget_exhausted": compiled.budget_exhausted,
            "status": "BOUNDED" if passed else "OVERFLOW_DETECTED",
        }

    def run_rehydration_benchmark(self, iterations: int = 50) -> Dict[str, Any]:
        """
        Performance test: Measures state serialization, SHA-256 integrity generation,
        and rehydration latency in microseconds.
        """
        temp_dir = tempfile.mkdtemp()
        snapshotter = AxTaskSnapshotter(storage_dir=temp_dir)
        task_id = "benchmark-task"

        for k in range(50):
            snapshotter.update_working_memory(task_id, f"variable_{k}", {"step": k, "data": "val" * 10})

        # Measure checkpointing
        t0 = time.perf_counter()
        cp = snapshotter.capture_checkpoint(
            task_id=task_id,
            tenant_id="tenant-bench",
            agent_id="agent-bench",
            parent_context_hash="a" * 64,
        )
        checkpoint_lat_us = (time.perf_counter() - t0) * 1e6

        # Measure rehydration
        rehydrate_times = []
        for _ in range(iterations):
            t_start = time.perf_counter()
            snapshotter.rehydrate(task_id, cp.checkpoint_index)
            rehydrate_times.append((time.perf_counter() - t_start) * 1e6)

        avg_rehydrate_us = sum(rehydrate_times) / len(rehydrate_times)
        min_rehydrate_us = min(rehydrate_times)
        max_rehydrate_us = max(rehydrate_times)

        # Cleanup
        for f in os.listdir(temp_dir):
            os.remove(os.path.join(temp_dir, f))
        os.rmdir(temp_dir)

        passed = avg_rehydrate_us < 5000.0  # Must rehydrate in under 5ms
        return {
            "test_name": "rehydration_latency_benchmark",
            "passed": passed,
            "checkpoint_latency_microseconds": round(checkpoint_lat_us, 2),
            "avg_rehydration_microseconds": round(avg_rehydrate_us, 2),
            "min_rehydration_microseconds": round(min_rehydrate_us, 2),
            "max_rehydration_microseconds": round(max_rehydrate_us, 2),
            "iterations": iterations,
            "status": "OPTIMAL (<5ms)" if passed else "DEGRADED",
        }

    def run_all(self) -> Dict[str, Any]:
        """Executes full evaluation suite and returns composite score."""
        drill_leak = self.run_tenant_leak_drill()
        drill_budget = self.run_token_starvation_drill()
        drill_bench = self.run_rehydration_benchmark()

        all_passed = drill_leak["passed"] and drill_budget["passed"] and drill_bench["passed"]

        return {
            "suite": "ax_context_gateway_conformance_eval",
            "all_passed": all_passed,
            "drills": [drill_leak, drill_budget, drill_bench],
        }


def main():
    evaluator = AxContextEvaluator()
    report = evaluator.run_all()
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
