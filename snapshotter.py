"""
State Snapshotter & Rehydration Engine for Google AX Agent Workloads.
Handles durable state persistence during AX task suspension and fast resumption.
"""

from __future__ import annotations
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class WorkingMemoryDelta:
    key: str
    value: Any
    updated_at: float = field(default_factory=time.time)
    provenance_step: int = 0


@dataclass
class TaskCheckpoint:
    task_id: str
    tenant_id: str
    agent_id: str
    checkpoint_index: int
    parent_context_hash: str
    deltas: Dict[str, Any]
    checkpoint_hash: str
    created_at: float

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
            "checkpoint_index": self.checkpoint_index,
            "parent_context_hash": self.parent_context_hash,
            "deltas": self.deltas,
            "checkpoint_hash": self.checkpoint_hash,
            "created_at": self.created_at,
        }


class AxTaskSnapshotter:
    """
    Manages deterministic checkpointing for tasks entering the 'Suspended' state
    under AX Agent Substrate orchestration.
    """

    def __init__(self, storage_dir: Optional[str] = None):
        self.storage_dir = storage_dir or "/tmp/ax_snapshots"
        os.makedirs(self.storage_dir, exist_ok=True)
        self._working_memory: Dict[str, Dict[str, Any]] = {}  # task_id -> {key: value}
        self._checkpoint_counters: Dict[str, int] = {}  # task_id -> index

    def update_working_memory(self, task_id: str, key: str, value: Any):
        """Records an in-memory state mutation during task execution."""
        if task_id not in self._working_memory:
            self._working_memory[task_id] = {}
        self._working_memory[task_id][key] = value

    def get_working_memory(self, task_id: str) -> Dict[str, Any]:
        return self._working_memory.get(task_id, {})

    def capture_checkpoint(
        self,
        task_id: str,
        tenant_id: str,
        agent_id: str,
        parent_context_hash: str,
    ) -> TaskCheckpoint:
        """
        Invoked on AX 'Suspending' signal. Serializes memory deltas,
        generates an SHA-256 digest, and persists checkpoint to disk.
        """
        curr_index = self._checkpoint_counters.get(task_id, 0) + 1
        self._checkpoint_counters[task_id] = curr_index

        deltas = self._working_memory.get(task_id, {}).copy()

        # Compute deterministic state digest
        raw_repr = json.dumps(
            {
                "task_id": task_id,
                "tenant_id": tenant_id,
                "agent_id": agent_id,
                "checkpoint_index": curr_index,
                "parent_context_hash": parent_context_hash,
                "deltas": deltas,
            },
            sort_keys=True,
        )
        checkpoint_hash = hashlib.sha256(raw_repr.encode("utf-8")).hexdigest()

        checkpoint = TaskCheckpoint(
            task_id=task_id,
            tenant_id=tenant_id,
            agent_id=agent_id,
            checkpoint_index=curr_index,
            parent_context_hash=parent_context_hash,
            deltas=deltas,
            checkpoint_hash=checkpoint_hash,
            created_at=time.time(),
        )

        # Write to disk
        out_path = os.path.join(self.storage_dir, f"{task_id}.cp_{curr_index}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)

        return checkpoint

    def rehydrate(self, task_id: str, checkpoint_index: Optional[int] = None) -> TaskCheckpoint:
        """
        Invoked on AX 'Resuming' signal. Verifies cryptographic integrity
        and re-populates active working memory.
        """
        if checkpoint_index is None:
            checkpoint_index = self._checkpoint_counters.get(task_id, 1)

        file_path = os.path.join(self.storage_dir, f"{task_id}.cp_{checkpoint_index}.json")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Checkpoint not found for task {task_id} at index {checkpoint_index}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Verify integrity hash
        raw_repr = json.dumps(
            {
                "task_id": data["task_id"],
                "tenant_id": data["tenant_id"],
                "agent_id": data["agent_id"],
                "checkpoint_index": data["checkpoint_index"],
                "parent_context_hash": data["parent_context_hash"],
                "deltas": data["deltas"],
            },
            sort_keys=True,
        )
        expected_hash = hashlib.sha256(raw_repr.encode("utf-8")).hexdigest()
        if expected_hash != data["checkpoint_hash"]:
            raise ValueError(
                f"Integrity check failed! Expected {expected_hash}, got {data['checkpoint_hash']}"
            )

        # Restore working memory
        self._working_memory[task_id] = data["deltas"].copy()
        self._checkpoint_counters[task_id] = data["checkpoint_index"]

        return TaskCheckpoint(
            task_id=data["task_id"],
            tenant_id=data["tenant_id"],
            agent_id=data["agent_id"],
            checkpoint_index=data["checkpoint_index"],
            parent_context_hash=data["parent_context_hash"],
            deltas=data["deltas"],
            checkpoint_hash=data["checkpoint_hash"],
            created_at=data["created_at"],
        )
