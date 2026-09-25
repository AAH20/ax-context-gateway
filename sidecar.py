"""
Unix Domain Socket MCP Sidecar for Google AX Sandboxes.
Provides zero-egress, zero-network Model Context Protocol (MCP) access
to bounded memory and GraphRAG context inside gVisor/isolated pods.
"""

from __future__ import annotations
import json
import os
import socket
import threading
from typing import Any, Callable, Dict, Optional
from ax_context_gateway.compiler import CompiledContextPackage
from ax_context_gateway.snapshotter import AxTaskSnapshotter


class UnixSocketMcpSidecar:
    """
    Lightweight JSON-RPC 2.0 MCP server listening on a local Unix socket.
    Mounted into the agent container via a shared emptyDir/Memory volume.
    """

    def __init__(
        self,
        socket_path: str,
        task_id: str,
        context_package: CompiledContextPackage,
        snapshotter: AxTaskSnapshotter,
    ):
        self.socket_path = socket_path
        self.task_id = task_id
        self.context_package = context_package
        self.snapshotter = snapshotter
        self._server_sock: Optional[socket.socket] = None
        self._is_running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Starts the Unix socket listener thread."""
        if os.path.exists(self.socket_path):
            try:
                os.remove(self.socket_path)
            except Exception:
                pass

        # Ensure parent directory exists
        os.makedirs(os.path.dirname(os.path.abspath(self.socket_path)), exist_ok=True)

        try:
            self._server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._server_sock.bind(self.socket_path)
            self._server_sock.listen(5)
            self._is_running = True

            self._thread = threading.Thread(target=self._serve_loop, daemon=True)
            self._thread.start()
        except PermissionError:
            # Fallback in restricted sandbox environments
            self._is_running = True

    def stop(self):
        """Stops the socket listener and cleans up socket file."""
        self._is_running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
        if os.path.exists(self.socket_path):
            try:
                os.remove(self.socket_path)
            except Exception:
                pass

    def _serve_loop(self):
        while self._is_running:
            try:
                conn, _ = self._server_sock.accept()
                client_thread = threading.Thread(
                    target=self.handle_connection, args=(conn,), daemon=True
                )
                client_thread.start()
            except Exception:
                break

    def handle_connection(self, conn: socket.socket):
        buffer = ""
        with conn:
            while self._is_running:
                try:
                    data = conn.recv(4096)
                except Exception:
                    break
                if not data:
                    break
                buffer += data.decode("utf-8")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        req = json.loads(line)
                        resp = self.dispatch_rpc(req)
                        conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
                    except Exception as ex:
                        err_resp = {
                            "jsonrpc": "2.0",
                            "id": None,
                            "error": {"code": -32603, "message": str(ex)},
                        }
                        conn.sendall((json.dumps(err_resp) + "\n").encode("utf-8"))

    def dispatch_rpc(self, req: Dict[str, Any]) -> Dict[str, Any]:
        rpc_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "result": {
                    "tools": [
                        {
                            "name": "get_bounded_context",
                            "description": "Retrieve pre-flight compiled GraphRAG context and token allocation.",
                            "inputSchema": {"type": "object", "properties": {}},
                        },
                        {
                            "name": "query_memory_graph",
                            "description": "Query nodes and edges from the admitted in-memory graph bundle.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {"keyword": {"type": "string"}},
                            },
                        },
                        {
                            "name": "record_memory_delta",
                            "description": "Record working state mutation for suspension checkpointing.",
                            "inputSchema": {
                                "type": "object",
                                "required": ["key", "value"],
                                "properties": {
                                    "key": {"type": "string"},
                                    "value": {},
                                },
                            },
                        },
                        {
                            "name": "get_session_metrics",
                            "description": "Inspect token consumption, budget limits, and context hash.",
                            "inputSchema": {"type": "object", "properties": {}},
                        },
                    ]
                },
            }

        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})

            if tool_name == "get_bounded_context":
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": json.dumps(self.context_package.to_dict())}
                        ]
                    },
                }

            elif tool_name == "query_memory_graph":
                kw = arguments.get("keyword", "").lower()
                matched_nodes = [
                    n
                    for n in self.context_package.nodes
                    if kw in n.get("content", "").lower()
                ]
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "result": {
                        "content": [
                            {"type": "text", "text": json.dumps({"matched_nodes": matched_nodes})}
                        ]
                    },
                }

            elif tool_name == "record_memory_delta":
                k = arguments["key"]
                v = arguments["value"]
                self.snapshotter.update_working_memory(self.task_id, k, v)
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps({"status": "recorded", "key": k}),
                            }
                        ]
                    },
                }

            elif tool_name == "get_session_metrics":
                working_mem = self.snapshotter.get_working_memory(self.task_id)
                metrics = {
                    "task_id": self.task_id,
                    "tenant_id": self.context_package.tenant_id,
                    "agent_id": self.context_package.agent_id,
                    "context_hash": self.context_package.context_hash,
                    "allocated_tokens": self.context_package.total_tokens,
                    "budget_exhausted": self.context_package.budget_exhausted,
                    "active_delta_keys": list(working_mem.keys()),
                }
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(metrics)}]
                    },
                }

            else:
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
                }

        return {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "error": {"code": -32601, "message": f"Method {method} not supported"},
        }
