"""
AX Context Controller & Reconciler.
Orchestrates lifecycle events between Google AX Task transitions and AxContextBinding.
"""

from __future__ import annotations
import json
import logging
import os
import time
from typing import Dict, List, Optional
from ax_context_gateway.compiler import (
    BoundedContextCompiler,
    ContextPolicy,
    MemoryEdge,
    MemoryNode,
    CompiledContextPackage,
)
from ax_context_gateway.sidecar import UnixSocketMcpSidecar
from ax_context_gateway.snapshotter import AxTaskSnapshotter, TaskCheckpoint

logger = logging.getLogger("AxContextController")


class AxContextController:
    """
    Kubernetes Reconciler simulating the control plane binding between
    Google AX Tasks and AxContextBinding resources.
    """

    def __init__(
        self,
        snapshot_storage_dir: Optional[str] = None,
        base_socket_dir: Optional[str] = None,
    ):
        self.compiler = BoundedContextCompiler()
        self.snapshotter = AxTaskSnapshotter(storage_dir=snapshot_storage_dir)
        self.base_socket_dir = base_socket_dir or "/tmp/ax_context_sockets"
        os.makedirs(self.base_socket_dir, exist_ok=True)

        self._active_bindings: Dict[str, dict] = {}
        self._active_sidecars: Dict[str, UnixSocketMcpSidecar] = {}

    def get_socket_path(self, task_id: str) -> str:
        return os.path.join(self.base_socket_dir, f"{task_id}.sock")

    def handle_task_admitted(
        self,
        task_id: str,
        tenant_id: str,
        agent_id: str,
        initial_query: str,
        available_nodes: List[MemoryNode],
        available_edges: List[MemoryEdge],
        policy: Optional[ContextPolicy] = None,
    ) -> CompiledContextPackage:
        """
        Called when Google AX admits a task. Pre-compiles bounded context
        and initializes local zero-network MCP socket.
        """
        logger.info(f"Admitting task {task_id} for tenant {tenant_id}, agent {agent_id}")

        # Compile bounded context
        pkg = self.compiler.compile(
            query=initial_query,
            tenant_id=tenant_id,
            agent_id=agent_id,
            available_nodes=available_nodes,
            available_edges=available_edges,
            policy=policy,
        )

        sock_path = self.get_socket_path(task_id)
        sidecar = UnixSocketMcpSidecar(
            socket_path=sock_path,
            task_id=task_id,
            context_package=pkg,
            snapshotter=self.snapshotter,
        )
        sidecar.start()
        self._active_sidecars[task_id] = sidecar

        self._active_bindings[task_id] = {
            "task_id": task_id,
            "tenant_id": tenant_id,
            "agent_id": agent_id,
            "phase": "Bound",
            "context_hash": pkg.context_hash,
            "socket_path": sock_path,
            "allocated_tokens": pkg.total_tokens,
            "last_checkpoint_hash": None,
        }

        return pkg

    def handle_task_suspending(self, task_id: str) -> TaskCheckpoint:
        """
        Called when Google AX Agent Substrate signals task suspension.
        Captures memory deltas and halts socket activity.
        """
        binding = self._active_bindings.get(task_id)
        if not binding:
            raise KeyError(f"Task {task_id} not registered with controller")

        logger.info(f"Suspending task {task_id}. Capturing state checkpoint...")
        checkpoint = self.snapshotter.capture_checkpoint(
            task_id=task_id,
            tenant_id=binding["tenant_id"],
            agent_id=binding["agent_id"],
            parent_context_hash=binding["context_hash"],
        )

        binding["phase"] = "Suspended"
        binding["last_checkpoint_hash"] = checkpoint.checkpoint_hash
        return checkpoint

    def handle_task_resuming(self, task_id: str) -> TaskCheckpoint:
        """
        Called when Google AX wakes a suspended task. Rehydrates working memory
        and verifies hash integrity in sub-second time.
        """
        binding = self._active_bindings.get(task_id)
        if not binding:
            raise KeyError(f"Task {task_id} not registered with controller")

        logger.info(f"Resuming task {task_id}. Rehydrating state from checkpoint...")
        checkpoint = self.snapshotter.rehydrate(task_id)

        binding["phase"] = "Running"
        return checkpoint

    def handle_task_finalized(self, task_id: str) -> dict:
        """
        Called when Google AX task completes or terminates.
        Shuts down MCP sidecar and cleans up socket.
        """
        sidecar = self._active_sidecars.pop(task_id, None)
        if sidecar:
            sidecar.stop()

        binding = self._active_bindings.pop(task_id, None)
        if binding:
            binding["phase"] = "Finalized"
            return binding
        return {"task_id": task_id, "phase": "Unknown"}

    def get_binding_status(self, task_id: str) -> Optional[dict]:
        return self._active_bindings.get(task_id)


def main():
    print("AX Context Gateway Controller Initialized.")


if __name__ == "__main__":
    main()
