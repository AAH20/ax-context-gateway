"""
Bounded Context Compiler for Google AX Tasks.
Compiles tenant-isolated, token-budgeted memory and GraphRAG context packages.
"""

from __future__ import annotations
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class ContextPolicy:
    max_context_tokens: int = 4096
    max_graph_hops: int = 2
    similarity_threshold: float = 0.65
    enforce_tombstones: bool = True


@dataclass
class MemoryNode:
    node_id: str
    tenant_id: str
    content: str
    token_count: int
    metadata: Dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


@dataclass
class MemoryEdge:
    source_id: str
    target_id: str
    relation: str
    weight: float = 1.0


@dataclass
class CompiledContextPackage:
    context_hash: str
    tenant_id: str
    agent_id: str
    query: str
    nodes: List[Dict[str, any]]
    edges: List[Dict[str, any]]
    total_tokens: int
    budget_exhausted: bool
    compiled_at: float

    def to_dict(self) -> dict:
        return {
            "context_hash": self.context_hash,
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
            "query": self.query,
            "nodes": self.nodes,
            "edges": self.edges,
            "total_tokens": self.total_tokens,
            "budget_exhausted": self.budget_exhausted,
            "compiled_at": self.compiled_at,
        }


class BoundedContextCompiler:
    """
    Compiles bounded, tenant-scoped context graphs into deterministic bundles
    for container injection before an AX Task runs.
    """

    def __init__(self):
        self._tombstones: Dict[str, Set[str]] = {}  # tenant_id -> set of deleted node_ids

    def register_tombstone(self, tenant_id: str, node_id: str):
        """Marks a node as deleted under tenant isolation."""
        if tenant_id not in self._tombstones:
            self._tombstones[tenant_id] = set()
        self._tombstones[tenant_id].add(node_id)

    def is_tombstoned(self, tenant_id: str, node_id: str) -> bool:
        return node_id in self._tombstones.get(tenant_id, set())

    @staticmethod
    def estimate_tokens(text: str) -> int:
        """Approximates token count (avg ~4 chars per token)."""
        return max(1, len(text) // 4)

    def _lexical_similarity(self, query: str, text: str) -> float:
        """Calculates token overlap Jaccard similarity."""
        q_tokens = set(re.findall(r"\w+", query.lower()))
        t_tokens = set(re.findall(r"\w+", text.lower()))
        if not q_tokens or not t_tokens:
            return 0.0
        intersection = len(q_tokens & t_tokens)
        union = len(q_tokens | t_tokens)
        return intersection / union if union > 0 else 0.0

    def compile(
        self,
        query: str,
        tenant_id: str,
        agent_id: str,
        available_nodes: List[MemoryNode],
        available_edges: List[MemoryEdge],
        policy: Optional[ContextPolicy] = None,
    ) -> CompiledContextPackage:
        """
        Selects relevant graph elements adhering strictly to tenant isolation,
        tombstone filters, and token capacity bounds.
        """
        if policy is None:
            policy = ContextPolicy()

        # Step 1: Strict Tenant Scoping & Tombstone Filtering
        valid_nodes: Dict[str, MemoryNode] = {}
        for node in available_nodes:
            # Enforce multi-tenant data boundary
            if node.tenant_id != tenant_id:
                continue
            if policy.enforce_tombstones and self.is_tombstoned(tenant_id, node.node_id):
                continue
            valid_nodes[node.node_id] = node

        # Step 2: Score direct matches (seed nodes)
        scored_nodes: List[Tuple[float, MemoryNode]] = []
        for n in valid_nodes.values():
            sim = self._lexical_similarity(query, n.content)
            if sim >= policy.similarity_threshold or not query.strip():
                scored_nodes.append((sim, n))

        # Sort descending by similarity
        scored_nodes.sort(key=lambda x: x[0], reverse=True)

        # Step 3: Graph Expansion within max_graph_hops
        admitted_node_ids: Set[str] = set()
        admitted_nodes: List[Dict[str, any]] = []
        accumulated_tokens = 0
        budget_exhausted = False

        # Build adjacency list
        adjacency: Dict[str, List[MemoryEdge]] = {}
        for edge in available_edges:
            adjacency.setdefault(edge.source_id, []).append(edge)
            adjacency.setdefault(edge.target_id, []).append(edge)

        # Queue seeds for admission
        frontier: List[Tuple[str, int]] = []
        for _, n in scored_nodes:
            frontier.append((n.node_id, 0))

        # If no seeds met threshold, take candidates in ranked order
        if not frontier and valid_nodes:
            fallback = sorted(
                valid_nodes.values(),
                key=lambda x: self._lexical_similarity(query, x.content),
                reverse=True,
            )
            for n in fallback:
                frontier.append((n.node_id, 0))

        while frontier:
            curr_id, hops = frontier.pop(0)
            if curr_id in admitted_node_ids:
                continue
            node = valid_nodes.get(curr_id)
            if not node:
                continue

            # Check token capacity
            if accumulated_tokens + node.token_count > policy.max_context_tokens:
                budget_exhausted = True
                break

            admitted_node_ids.add(curr_id)
            accumulated_tokens += node.token_count
            admitted_nodes.append(
                {
                    "node_id": node.node_id,
                    "content": node.content,
                    "token_count": node.token_count,
                    "metadata": node.metadata,
                }
            )

            # Expand neighbors if within max_graph_hops
            if hops < policy.max_graph_hops:
                for edge in adjacency.get(curr_id, []):
                    neighbor_id = edge.target_id if edge.source_id == curr_id else edge.source_id
                    if neighbor_id in valid_nodes and neighbor_id not in admitted_node_ids:
                        frontier.append((neighbor_id, hops + 1))

        # Filter edges to only include admitted endpoints
        admitted_edges: List[Dict[str, any]] = []
        for edge in available_edges:
            if edge.source_id in admitted_node_ids and edge.target_id in admitted_node_ids:
                admitted_edges.append(
                    {
                        "source_id": edge.source_id,
                        "target_id": edge.target_id,
                        "relation": edge.relation,
                        "weight": edge.weight,
                    }
                )

        # Deterministic SHA-256 digest of context payload
        canonical_payload = json.dumps(
            {
                "tenant_id": tenant_id,
                "agent_id": agent_id,
                "nodes": sorted(admitted_nodes, key=lambda x: x["node_id"]),
                "edges": sorted(admitted_edges, key=lambda x: (x["source_id"], x["target_id"])),
            },
            sort_keys=True,
        )
        context_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()

        return CompiledContextPackage(
            context_hash=context_hash,
            tenant_id=tenant_id,
            agent_id=agent_id,
            query=query,
            nodes=admitted_nodes,
            edges=admitted_edges,
            total_tokens=accumulated_tokens,
            budget_exhausted=budget_exhausted,
            compiled_at=time.time(),
        )
