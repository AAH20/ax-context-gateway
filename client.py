"""
Zero-Egress Python Client SDK for Google AX Agent Workloads.
Allows autonomous agents inside sandboxes to interact seamlessly with ax-context-gateway
over the local Unix domain socket MCP endpoint.
"""

from __future__ import annotations
import json
import os
import socket
from typing import Any, Dict, List, Optional


class AxContextClient:
    """
    Lightweight, dependency-free client for interacting with the local
    Unix socket MCP sidecar inside an isolated AX Task container.
    """

    def __init__(self, socket_path: Optional[str] = None):
        self.socket_path = socket_path or os.environ.get(
            "MCP_SOCKET_PATH", "/var/run/ax-context/mcp.sock"
        )
        self._rpc_id = 0

    @classmethod
    def from_env(cls) -> AxContextClient:
        return cls()

    def _call(self, tool_name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Sends an MCP JSON-RPC call over the Unix domain socket."""
        self._rpc_id += 1
        req = {
            "jsonrpc": "2.0",
            "id": self._rpc_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments or {},
            },
        }

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(self.socket_path)
            sock.sendall((json.dumps(req) + "\n").encode("utf-8"))

            buffer = ""
            while "\n" not in buffer:
                chunk = sock.recv(4096).decode("utf-8")
                if not chunk:
                    break
                buffer += chunk

        resp = json.loads(buffer.strip())
        if "error" in resp:
            raise RuntimeError(f"MCP RPC Error ({resp['error'].get('code')}): {resp['error'].get('message')}")

        result_content = resp["result"]["content"]
        if result_content and result_content[0].get("type") == "text":
            return json.loads(result_content[0]["text"])
        return resp["result"]

    def list_tools(self) -> List[Dict[str, Any]]:
        """Lists available MCP tools exposed by the sidecar."""
        self._rpc_id += 1
        req = {"jsonrpc": "2.0", "id": self._rpc_id, "method": "tools/list", "params": {}}

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(self.socket_path)
            sock.sendall((json.dumps(req) + "\n").encode("utf-8"))

            buffer = ""
            while "\n" not in buffer:
                chunk = sock.recv(4096).decode("utf-8")
                if not chunk:
                    break
                buffer += chunk

        resp = json.loads(buffer.strip())
        return resp.get("result", {}).get("tools", [])

    def get_bounded_context(self) -> Dict[str, Any]:
        """Retrieves the pre-flight compiled context, allocated tokens, and graph nodes."""
        return self._call("get_bounded_context")

    def query_memory_graph(self, keyword: str) -> List[Dict[str, Any]]:
        """Searches across admitted in-memory nodes matching the keyword."""
        res = self._call("query_memory_graph", {"keyword": keyword})
        return res.get("matched_nodes", [])

    def record_memory_delta(self, key: str, value: Any) -> Dict[str, Any]:
        """Records a state mutation to be safely snapshotted on AX task suspension."""
        return self._call("record_memory_delta", {"key": key, "value": value})

    def get_session_metrics(self) -> Dict[str, Any]:
        """Retrieves token limits, remaining budget, and context integrity digests."""
        return self._call("get_session_metrics")
