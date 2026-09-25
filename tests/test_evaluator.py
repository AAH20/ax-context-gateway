"""
Unit tests for AxContextEvaluator and AxContextClient.
"""

from __future__ import annotations
import socket
import threading
import unittest
from ax_context_gateway.client import AxContextClient
from ax_context_gateway.compiler import (
    BoundedContextCompiler,
    MemoryNode,
)
from ax_context_gateway.evaluator import AxContextEvaluator
from ax_context_gateway.sidecar import UnixSocketMcpSidecar
from ax_context_gateway.snapshotter import AxTaskSnapshotter


class TestEvaluatorAndClient(unittest.TestCase):
    def test_evaluator_drills(self):
        evaluator = AxContextEvaluator()
        report = evaluator.run_all()
        self.assertTrue(report["all_passed"])
        self.assertEqual(len(report["drills"]), 3)

    def test_client_duplex_session(self):
        compiler = BoundedContextCompiler()
        pkg = compiler.compile(
            query="test",
            tenant_id="tenant-x",
            agent_id="agent-x",
            available_nodes=[MemoryNode("n1", "tenant-x", "sample data", 10)],
            available_edges=[],
        )
        snapshotter = AxTaskSnapshotter()
        sidecar = UnixSocketMcpSidecar("/tmp/mock.sock", "t1", pkg, snapshotter)

        # Direct test of sidecar dispatch_rpc
        tools = sidecar.dispatch_rpc({"id": 1, "method": "tools/list", "params": {}})
        self.assertIn("tools", tools["result"])

        call_resp = sidecar.dispatch_rpc(
            {"id": 2, "method": "tools/call", "params": {"name": "get_bounded_context", "arguments": {}}}
        )
        self.assertIn("content", call_resp["result"])

        delta_resp = sidecar.dispatch_rpc(
            {
                "id": 3,
                "method": "tools/call",
                "params": {"name": "record_memory_delta", "arguments": {"key": "state", "value": "active"}},
            }
        )
        self.assertIn("content", delta_resp["result"])


if __name__ == "__main__":
    unittest.main()
