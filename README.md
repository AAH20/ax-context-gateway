# ax-context-gateway
> **Kubernetes-native Context & Memory Gateway Sidecar for Google AX (Agent Executor)**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-7%2F7%20Passing-brightgreen.svg)]()
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)]()
[![Conformance](https://img.shields.io/badge/Audit-Zero--Leak%20Verified-blue.svg)]()

`ax-context-gateway` is a production reference controller and sidecar that bridges [Swarm-Context-Commander](https://github.com/AAH20/Swarm-Context-Commander)'s bounded context compilation and token admission with [Google AX](https://github.com/google/ax)'s declarative agent orchestrator on Kubernetes.

---

## Architecture & System Overview

Google AX introduces declarative Kubernetes primitives (`Task`, `Workspace`, `Gateway`, `Model`) and the **Agent Substrate** runtime for high-density, stateful agent multiplexing. Agents run in gVisor (`runsc`) sandboxes under strict zero-trust network policies (`egress: mode: DenyAll`).

`ax-context-gateway` resolves the fundamental friction between zero-trust sandboxing and stateful memory retrieval by serving pre-flight compiled context and state synchronization over an in-pod Unix Domain Socket Model Context Protocol (MCP) stream.

### 1. High-Level System Architecture

```mermaid
flowchart TD
    subgraph ControlPlane ["Kubernetes Control Plane"]
        AX_CTRL["Google AX Controller\n(Task, Workspace, Model)"]
        ACB_CRD["AxContextBinding CRD\n(Tenant, Agent, Token Budget)"]
        GW_CTRL["AxContextController\n(Reconciliation & Admission)"]
        AX_CTRL <--> ACB_CRD
        ACB_CRD --> GW_CTRL
    end

    subgraph Node ["Kubernetes Worker Node (gVisor runsc)"]
        subgraph Pod ["Agent Pod"]
            subgraph Vol ["Shared Memory Volume (emptyDir: Memory)"]
                SOCK["/var/run/ax-context/mcp.sock\n(Zero-Network AF_UNIX IPC)"]
            end

            subgraph Sidecar ["Sidecar Container: ax-context-gateway"]
                COMP["Bounded Context Compiler\n(Token Bounding & Tombstones)"]
                MCP_SRV["UnixSocketMcpSidecar\n(JSON-RPC 2.0 MCP Server)"]
                SNAP["State Snapshotter\n(SHA-256 Checkpointing)"]
                COMP --> MCP_SRV
                SNAP --> MCP_SRV
            end

            subgraph Agent ["Workload Container: Agent Runner"]
                RUNNER["Autonomous Agent Runner\n(Python SDK / LangChain / Swarm)"]
                CLIENT["AxContextClient\n(Zero-Egress MCP Client)"]
                RUNNER --> CLIENT
            end

            MCP_SRV <== "duplex stream" ==> SOCK
            CLIENT <== "duplex stream" ==> SOCK
        end
    end

    GW_CTRL -->|"Pre-flight Context & Volume Prep"| Sidecar
    AX_CTRL -->|"Suspend / Resume Signal"| GW_CTRL
    GW_CTRL -->|"Trigger State Checkpoint"| SNAP
```

---

## Task & Context Lifecycle State Machine

The gateway aligns directly with the **Google AX Agent Substrate** lifecycle states:

```mermaid
stateDiagram-v2
    [*] --> Admitted: AX Task Created
    Admitted --> Bound: AxContextBinding Reconciled
    state Bound {
        [*] --> CompileContext
        CompileContext --> EnforceTenantBoundary
        EnforceTenantBoundary --> FilterTombstones
        FilterTombstones --> BoundTokenCapacity
        BoundTokenCapacity --> StartUnixSocketSidecar
    }
    Bound --> Running: Pod Scheduled & Agent Awakens
    state Running {
        AgentQueryGraph --> InPodMcpSocket
        AgentMutateState --> RecordMemoryDelta
    }
    Running --> Suspending: AX Actor Preempted / Sleep Signal
    state Suspending {
        CaptureWorkingMemory --> ComputeSha256Digest
        ComputeSha256Digest --> SealCheckpointFile
    }
    Suspending --> Suspended: Pod Memory Released
    Suspended --> Resuming: Task Triggered with New Input
    state Resuming {
        LoadCheckpoint --> VerifyIntegrityHash
        VerifyIntegrityHash --> FastRehydrationSub5ms
    }
    Resuming --> Running: Agent Continues Execution
    Running --> Finalized: Task Completed / Terminated
    Finalized --> [*]
```

---

## Context Compilation & Token Bounding Pipeline

Context is compiled deterministically prior to container unfreezing, ensuring zero cross-tenant contamination:

```mermaid
flowchart LR
    Q["Task Initial Query & ContextPolicy"] --> T_GATE{"Tenant Isolation\nBoundary Gate"}
    T_GATE -->|"Mismatch Tenant ID"| DROP["Drop Immediately"]
    T_GATE -->|"Valid Tenant"| TOMB{"Tombstone\nFilter"}
    TOMB -->|"Deleted / Tombstoned"| EXCLUDE["Exclude from Knowledge"]
    TOMB -->|"Active Node"| SIM["Lexical & Seed Scoring\n(Jaccard Similarity)"]
    SIM --> HOP["Multi-Hop Graph Expansion\n(max_graph_hops)"]
    HOP --> CAP{"Token Capacity Gate\n(total <= max_tokens)"}
    CAP -->|"Capacity Exceeded"| TRUNC["Set budget_exhausted = True\nStop Frontier"]
    CAP -->|"Within Budget"| ADMIT["Admit Node & Edges"]
    ADMIT --> HASH["Compute Canonical SHA-256 Digest\n(context_hash)"]
    TRUNC --> HASH
    HASH --> BUNDLE["CompiledContextPackage\nInjected to MCP Sidecar"]
```

---

## Network Topology & Security Boundaries

In contrast to traditional agent architectures that make external network calls to retrieve memory, `ax-context-gateway` maintains an airtight perimeter:

```mermaid
graph TD
    subgraph Internet ["Public Internet / Untrusted Network"]
        EXT_DB[("External Vector DBs")]
        EXT_API["Third-Party Web APIs"]
    end

    subgraph KubernetesCluster ["GKE / Kubernetes Cluster"]
        subgraph NetworkPolicy ["Cilium / Calico Zero-Trust Egress"]
            FW["Egress Rule: DenyAll (Port * Drop)"]
        end

        subgraph Sandbox ["gVisor (runsc) Sandbox Boundary"]
            subgraph AgentApp ["Workload Container"]
                A_EXEC["Agent Code / LLM Loop"]
            end
            
            subgraph IPC ["Kernel AF_UNIX IPC"]
                USOCK["/var/run/ax-context/mcp.sock"]
            end

            subgraph SidecarApp ["Sidecar Container"]
                GATEWAY["ax-context-gateway Sidecar"]
            end
        end
    end

    A_EXEC -.->|"TCP Connect (BLOCKED)"| FW
    FW -.->|"DROPPED"| EXT_DB
    FW -.->|"DROPPED"| EXT_API
    A_EXEC ===|"Zero-Network Unix Domain Socket"| USOCK
    USOCK ===|"Local In-Memory IPC"| GATEWAY
```

---

## Declarative Manifests

### 1. `AxContextBinding` Custom Resource
```yaml
apiVersion: agent.google.com/v1alpha1
kind: AxContextBinding
metadata:
  name: incident-analyst-task-42-binding
  namespace: agent-workloads
spec:
  targetTaskRef:
    name: incident-analyst-task-42
  tenantId: enterprise-acme-corp
  agentId: secops-reconciliation-agent
  contextPolicy:
    maxContextTokens: 4096
    maxGraphHops: 2
    similarityThreshold: 0.75
    enforceTombstones: true
  snapshotConfig:
    autoCheckpointOnSuspend: true
    storageUri: "s3://acme-agent-snapshots/secops-reconciliation-agent/"
```

### 2. Google AX `Task` Manifest
```yaml
apiVersion: agent.google.com/v1alpha1
kind: Task
metadata:
  name: incident-analyst-task-42
  namespace: agent-workloads
spec:
  modelRef:
    name: gemini-2-5-pro
  workspaceRef:
    name: analyst-scratchpad-ws
  runtimeClass: gvisor # runsc sandbox isolation
  networkPolicy:
    egress:
      mode: DenyAll # Strict Zero-Trust
  template:
    containers:
      - name: agent-runner
        image: ghcr.io/aah20/autonomous-analyst:v1.0.0
        env:
          - name: MCP_SOCKET_PATH
            value: /var/run/ax-context/mcp.sock
        volumeMounts:
          - name: ax-context-socket
            mountPath: /var/run/ax-context
    volumes:
      - name: ax-context-socket
        emptyDir:
          medium: Memory
```

---

## Python Client SDK Usage

Agents inside the container use `AxContextClient` with zero boilerplate:

```python
from ax_context_gateway.client import AxContextClient

# Auto-detects $MCP_SOCKET_PATH
client = AxContextClient.from_env()

# 1. Fetch pre-flight compiled context & graph
context = client.get_bounded_context()
print(f"Admitted tokens: {context['total_tokens']} (Hash: {context['context_hash']})")

# 2. Query in-memory graph without network access
nodes = client.query_memory_graph(keyword="database credentials")

# 3. Record state mutations before AX suspension
client.record_memory_delta(key="incident_status", value="mitigation_in_progress")

# 4. Inspect session metrics & budget
metrics = client.get_session_metrics()
print("Session metrics:", metrics)
```

---

## Automated Conformance & Security Drills

The repository includes `AxContextEvaluator` to audit multi-tenant boundaries and benchmark rehydration latency:

```bash
PYTHONPATH=projects python3 -m ax_context_gateway.evaluator
```

```json
{
  "suite": "ax_context_gateway_conformance_eval",
  "all_passed": true,
  "drills": [
    {
      "test_name": "tenant_isolation_leak_drill",
      "passed": true,
      "victim_tenant": "tenant-bank-corp",
      "attacker_tenant": "tenant-adversary",
      "leaked_count": 0,
      "status": "SECURE"
    },
    {
      "test_name": "token_starvation_bounding_drill",
      "passed": true,
      "max_budget": 500,
      "total_admitted_tokens": 500,
      "admitted_node_count": 10,
      "budget_exhausted": true,
      "status": "BOUNDED"
    },
    {
      "test_name": "rehydration_latency_benchmark",
      "passed": true,
      "checkpoint_latency_microseconds": 360.83,
      "avg_rehydration_microseconds": 96.56,
      "min_rehydration_microseconds": 60.58,
      "max_rehydration_microseconds": 401.33,
      "iterations": 50,
      "status": "OPTIMAL (<5ms)"
    }
  ]
}
```

---

## Running the Unit Test Suite

```bash
PYTHONPATH=projects python3 -m unittest discover -s projects/ax_context_gateway/tests -v
```

```text
test_client_duplex_session (test_evaluator.TestEvaluatorAndClient.test_client_duplex_session) ... ok
test_evaluator_drills (test_evaluator.TestEvaluatorAndClient.test_evaluator_drills) ... ok
test_suspension_checkpoint_and_rehydration (test_gateway.TestAxContextGateway.test_suspension_checkpoint_and_rehydration) ... ok
test_tampered_checkpoint_rejection (test_gateway.TestAxContextGateway.test_tampered_checkpoint_rejection) ... ok
test_tenant_isolation_and_tombstones (test_gateway.TestAxContextGateway.test_tenant_isolation_and_tombstones) ... ok
test_token_budget_enforcement (test_gateway.TestAxContextGateway.test_token_budget_enforcement) ... ok
test_unix_socket_mcp_zero_egress (test_gateway.TestAxContextGateway.test_unix_socket_mcp_zero_egress) ... ok

Ran 7 tests in 0.019s (OK)
```

---

## License
Apache-2.0
