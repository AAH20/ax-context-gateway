"""
Comprehensive Test Suite for AX Context Gateway.
Verifies tenant isolation, token bounds, Unix socket MCP, and suspension checkpointing.
"""

from __future__ import annotations
import json
import os
import shutil
import socket
import tempfile
import time
import unittest

from ax_context_gateway.compiler import (
    BoundedContextCompiler,
    ContextPolicy,
    MemoryEdge,
    MemoryNode,
)
from ax_context_gateway.controller import AxContextController
from ax_context_gateway.snapshotter import AxTaskSnapshotter


class TestAxContextGateway(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.socket_dir = os.path.join(self.temp_dir, "sockets")
        self.snapshot_dir = os.path.join(self.temp_dir, "snapshots")
        os.makedirs(self.socket_dir, exist_ok=True)
        os.makedirs(self.snapshot_dir, exist_ok=True)

        self.controller = AxContextController(
            snapshot_storage_dir=self.snapshot_dir,
            base_socket_dir=self.socket_dir,
        )

    def tearDown(self):
        # Stop all running sidecars
        for sidecar in list(self.controller._active_sidecars.values()):
            sidecar.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_tenant_isolation_and_tombstones(self):
        """Verifies strict cross-tenant segregation and tombstone honoring."""
        compiler = BoundedContextCompiler()

        nodes = [
            MemoryNode(
                node_id="n1",
                tenant_id="tenant-alpha",
                content="Alpha confidential database password and secrets",
                token_count=50,
            ),
            MemoryNode(
                node_id="n2",
                tenant_id="tenant-beta",
                content="Beta confidential database password and secrets",
                token_count=50,
            ),
            MemoryNode(
                node_id="n3",
                tenant_id="tenant-alpha",
                content="Alpha public incident reports",
                token_count=30,
            ),
        ]
        edges = []

        # Tombstone n1
        compiler.register_tombstone("tenant-alpha", "n1")

        # Compile for tenant-alpha
        pkg_alpha = compiler.compile(
            query="confidential database password",
            tenant_id="tenant-alpha",
            agent_id="agent-1",
            available_nodes=nodes,
            available_edges=edges,
        )

        # Assert no tenant-beta data leaked
        for n in pkg_alpha.nodes:
            self.assertNotEqual(n["node_id"], "n2")
            self.assertNotIn("Beta", n["content"])

        # Assert tombstoned node n1 was not admitted
        admitted_ids = [n["node_id"] for n in pkg_alpha.nodes]
        self.assertNotIn("n1", admitted_ids)
        self.assertIn("n3", admitted_ids)

    def test_token_budget_enforcement(self):
        """Ensures context compilation halts strictly when token budget is reached."""
        compiler = BoundedContextCompiler()
        policy = ContextPolicy(max_context_tokens=100)

        nodes = [
            MemoryNode(node_id="a1", tenant_id="t1", content="Cluster log 1", token_count=60),
            MemoryNode(node_id="a2", tenant_id="t1", content="Cluster log 2", token_count=50),
            MemoryNode(node_id="a3", tenant_id="t1", content="Cluster log 3", token_count=20),
        ]
        pkg = compiler.compile(
            query="Cluster log",
            tenant_id="t1",
            agent_id="analyst",
            available_nodes=nodes,
            available_edges=[],
            policy=policy,
        )

        # Total tokens should not exceed 100
        self.assertLessEqual(pkg.total_tokens, 100)
        self.assertTrue(pkg.budget_exhausted)
        self.assertEqual(len(pkg.nodes), 1)  # Only a1 (60) fits; adding a2 (50) would reach 110

    def test_unix_socket_mcp_zero_egress(self):
        """Tests zero-network Model Context Protocol over a streaming duplex socket."""
        task_id = "test-task-mcp-01"
        nodes = [
            MemoryNode(
                node_id="mem-1",
                tenant_id="tenant-finance",
                content="Ledger reconciliation discrepancy on account 4401",
                token_count=40,
            )
        ]
        self.controller.handle_task_admitted(
            task_id=task_id,
            tenant_id="tenant-finance",
            agent_id="reconciliation-bot",
            initial_query="Ledger reconciliation",
            available_nodes=nodes,
            available_edges=[],
        )

        sidecar = self.controller._active_sidecars[task_id]
        server_conn, client_conn = socket.socketpair()

        # Run sidecar handler in background thread
        import threading
        t = threading.Thread(target=sidecar.handle_connection, args=(server_conn,), daemon=True)
        t.start()

        def rpc_call(method: str, params: dict) -> dict:
            payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
            client_conn.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            data = ""
            while "\n" not in data:
                chunk = client_conn.recv(4096).decode("utf-8")
                if not chunk:
                    break
                data += chunk
            return json.loads(data.strip())

        # Test tools/list
        tools_resp = rpc_call("tools/list", {})
        self.assertIn("tools", tools_resp["result"])
        tool_names = [t["name"] for t in tools_resp["result"]["tools"]]
        self.assertIn("get_bounded_context", tool_names)
        self.assertIn("record_memory_delta", tool_names)

        # Test tool call: get_bounded_context
        ctx_resp = rpc_call("tools/call", {"name": "get_bounded_context", "arguments": {}})
        content_text = ctx_resp["result"]["content"][0]["text"]
        ctx_data = json.loads(content_text)
        self.assertEqual(ctx_data["tenant_id"], "tenant-finance")
        self.assertEqual(len(ctx_data["nodes"]), 1)

        # Test tool call: record_memory_delta
        delta_resp = rpc_call(
            "tools/call",
            {"name": "record_memory_delta", "arguments": {"key": "anomaly_detected", "value": True}},
        )
        self.assertEqual(delta_resp["result"]["content"][0]["type"], "text")

        client_conn.close()

    def test_suspension_checkpoint_and_rehydration(self):
        """Verifies AX task suspension, state hashing, and deterministic rehydration."""
        task_id = "test-task-suspend-02"
        self.controller.handle_task_admitted(
            task_id=task_id,
            tenant_id="tenant-corp",
            agent_id="patch-agent",
            initial_query="",
            available_nodes=[],
            available_edges=[],
        )

        # Simulate agent writing memory delta
        self.controller.snapshotter.update_working_memory(task_id, "step_count", 4)
        self.controller.snapshotter.update_working_memory(task_id, "last_action", "isolate_host")

        # Suspend task
        checkpoint = self.controller.handle_task_suspending(task_id)
        self.assertEqual(checkpoint.deltas["step_count"], 4)
        self.assertEqual(checkpoint.deltas["last_action"], "isolate_host")
        self.assertTrue(len(checkpoint.checkpoint_hash) == 64)

        # Verify binding state
        status = self.controller.get_binding_status(task_id)
        self.assertEqual(status["phase"], "Suspended")

        # Clear active memory to simulate pod eviction/sleeping
        self.controller.snapshotter._working_memory.pop(task_id, None)
        self.assertEqual(self.controller.snapshotter.get_working_memory(task_id), {})

        # Resume task
        rehydrated = self.controller.handle_task_resuming(task_id)
        self.assertEqual(rehydrated.checkpoint_hash, checkpoint.checkpoint_hash)
        self.assertEqual(
            self.controller.snapshotter.get_working_memory(task_id)["last_action"],
            "isolate_host",
        )

        # Finalize task
        final_info = self.controller.handle_task_finalized(task_id)
        self.assertEqual(final_info["phase"], "Finalized")
        self.assertFalse(os.path.exists(self.controller.get_socket_path(task_id)))

    def test_tampered_checkpoint_rejection(self):
        """Ensures corrupted or modified checkpoints fail cryptographic attestation."""
        snapshotter = AxTaskSnapshotter(storage_dir=self.snapshot_dir)
        task_id = "tamper-task"
        snapshotter.update_working_memory(task_id, "authorized", True)

        cp = snapshotter.capture_checkpoint(
            task_id=task_id,
            tenant_id="t1",
            agent_id="a1",
            parent_context_hash="000000",
        )

        # Tamper with file
        cp_file = os.path.join(self.snapshot_dir, f"{task_id}.cp_{cp.checkpoint_index}.json")
        with open(cp_file, "r") as f:
            data = json.load(f)
        data["deltas"]["authorized"] = False  # Tampered
        with open(cp_file, "w") as f:
            json.dump(data, f)

        # Rehydration must raise ValueError on digest mismatch
        with self.assertRaises(ValueError):
            snapshotter.rehydrate(task_id, cp.checkpoint_index)


if __name__ == "__main__":
    unittest.main()
