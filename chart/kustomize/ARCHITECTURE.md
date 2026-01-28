# Kustomize Architecture Diagram

## Overview Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         HELM TEMPLATE                            │
│                                                                  │
│  Original: 484 lines, 45+ conditionals                          │
│  ❌ Complex   ❌ Hard to test   ❌ Mixed concerns               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ↓ Simplification
┌─────────────────────────────────────────────────────────────────┐
│                    KUSTOMIZE ARCHITECTURE                        │
│                                                                  │
│  Simplified: ~100 lines base + composable overlays              │
│  ✅ Simple   ✅ Easy to test   ✅ Clear separation             │
└─────────────────────────────────────────────────────────────────┘
```

## Layer Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          BASE LAYER                              │
│                                                                  │
│         base/worker-deployment.yaml                              │
│         ├─ Clean Kubernetes Deployment                          │
│         ├─ No conditionals                                       │
│         ├─ ~100 lines                                            │
│         └─ Directly testable                                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ↓ Apply overlays
┌─────────────────────────────────────────────────────────────────┐
│                       FEATURE OVERLAYS                           │
│                                                                  │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│   │  StatefulSet │  │   Kerberos   │  │   GitSync    │         │
│   │              │  │              │  │              │         │
│   │ + Persistence│  │ + Auth       │  │ + DAG Sync   │         │
│   └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                  │
│   ┌──────────────┐  ┌──────────────┐                           │
│   │ Log Groomer  │  │  Deployment  │                           │
│   │              │  │              │                           │
│   │ + Cleanup    │  │ + Standard   │                           │
│   └──────────────┘  └──────────────┘                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ↓ Compose for environments
┌─────────────────────────────────────────────────────────────────┐
│                    ENVIRONMENT OVERLAYS                          │
│                                                                  │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐  │
│  │   Production    │ │     Staging     │ │   Development   │  │
│  │                 │ │                 │ │                 │  │
│  │ + StatefulSet   │ │ + Deployment    │ │ + Deployment    │  │
│  │ + GitSync       │ │ + GitSync       │ │ (Base only)     │  │
│  │ + Log Groomer   │ │ 2 replicas      │ │ 1 replica       │  │
│  │ 5 replicas      │ │ Standard res    │ │ Minimal res     │  │
│  │ High resources  │ │                 │ │                 │  │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Composition Flow

```
Base Deployment
      ↓
      ├─→ [Add GitSync] ──────────────────┐
      ├─→ [Add Kerberos] ────────────────┐│
      ├─→ [Add Log Groomer] ────────────┐││
      └─→ [Change to StatefulSet] ─────┐│││
                                        ││││
                                        ↓↓↓↓
                               Production Environment
                               - StatefulSet
                               - GitSync sidecar
                               - Kerberos sidecar
                               - Log groomer sidecar
                               - 5 replicas
                               - High resources
```

## Dependency Graph

```
                    ┌──────────────┐
                    │     Base     │
                    │  Deployment  │
                    └──────┬───────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ↓               ↓               ↓
    ┌──────────┐    ┌──────────┐   ┌──────────┐
    │StatefulSet│    │ GitSync  │   │ Kerberos │
    └──────────┘    └──────────┘   └──────────┘
           │               │               │
           └───────────────┼───────────────┘
                           │
                           ↓
                  ┌─────────────────┐
                  │   Production    │
                  │   (Composite)   │
                  └─────────────────┘
```

## File Relationship Map

```
kustomize/
│
├── base/
│   ├── worker-deployment.yaml ◄─┐
│   └── kustomization.yaml        │
│                                 │ references
├── overlays/                     │
│   ├── deployment/               │
│   │   └── kustomization.yaml ──┘
│   │
│   ├── statefulset/
│   │   ├── statefulset-patch.yaml ◄─┐
│   │   └── kustomization.yaml ───────┤
│   │                                 │
│   ├── gitsync/                      │ references
│   │   ├── gitsync-patch.yaml ◄─────┤
│   │   └── kustomization.yaml ───────┤
│   │                                 │
│   ├── kerberos/                     │
│   │   ├── kerberos-patch.yaml ◄────┤
│   │   └── kustomization.yaml ───────┤
│   │                                 │
│   ├── log-groomer/                  │
│   │   ├── log-groomer-patch.yaml ◄─┤
│   │   └── kustomization.yaml ───────┤
│   │                                 │
│   └── production/                   │
│       └── kustomization.yaml ───────┘
│           (references all patches)
```

## Before/After Comparison

### Before: Monolithic Template
```
┌─────────────────────────────────────┐
│   worker-deployment.yaml (484 lines)│
│ ┌─────────────────────────────────┐ │
│ │ {{- if persistence }}           │ │
│ │   ┌─────────────────────────┐   │ │
│ │   │ {{- if kerberos }}      │   │ │
│ │   │   ┌─────────────────┐   │   │ │
│ │   │   │ {{- if gitSync}}│   │   │ │
│ │   │   │   ...nested...  │   │   │ │
│ │   │   └─────────────────┘   │   │ │
│ │   └─────────────────────────┘   │ │
│ └─────────────────────────────────┘ │
│                                     │
│  ❌ 45+ conditionals                │
│  ❌ Hard to test                    │
│  ❌ Mixed concerns                  │
└─────────────────────────────────────┘
```

### After: Modular Overlays
```
┌─────────────────────────────────────┐
│  base/worker-deployment.yaml        │
│  (100 lines, no conditionals)       │
└─────────────────────────────────────┘
              +
┌─────────────────────────────────────┐
│  overlays/statefulset/              │
│  (persistence feature)              │
└─────────────────────────────────────┘
              +
┌─────────────────────────────────────┐
│  overlays/gitsync/                  │
│  (DAG sync feature)                 │
└─────────────────────────────────────┐
              +
┌─────────────────────────────────────┐
│  overlays/kerberos/                 │
│  (auth feature)                     │
└─────────────────────────────────────┘
              =
┌─────────────────────────────────────┐
│  Full-featured deployment           │
│  ✅ Composable                      │
│  ✅ Easy to test                    │
│  ✅ Clear separation                │
└─────────────────────────────────────┘
```

## Decision Flow

```
┌─────────────────────┐
│  Need Workers?      │
└──────────┬──────────┘
           │ yes
           ↓
┌─────────────────────┐      no     ┌─────────────────────┐
│ Need Persistence?   │─────────────→│ Use: deployment     │
└──────────┬──────────┘              └─────────────────────┘
           │ yes
           ↓
┌─────────────────────┐      no     ┌─────────────────────┐
│ Need DAG Sync?      │─────────────→│ Use: statefulset    │
└──────────┬──────────┘              └─────────────────────┘
           │ yes
           ↓
┌─────────────────────┐      no     ┌─────────────────────┐
│ Need Kerberos?      │─────────────→│ Use: staging        │
└──────────┬──────────┘              └─────────────────────┘
           │ yes
           ↓
      ┌─────────────────────┐
      │ Use: production     │
      └─────────────────────┘
```

## Patch Application Order

```
1. Base Deployment
   └─ kind: Deployment
      └─ name: airflow-worker
         └─ replicas: 1
            └─ containers: [worker]

2. + StatefulSet Patch
   └─ kind: StatefulSet (changed)
      └─ volumeClaimTemplates: [logs]

3. + GitSync Patch
   └─ initContainers: [git-sync-init]
      └─ containers: [worker, git-sync]
         └─ volumes: [dags]

4. + Kerberos Patch
   └─ initContainers: [git-sync-init, kerberos-init]
      └─ containers: [worker, git-sync, worker-kerberos]
         └─ volumes: [dags, kerberos-keytab, kerberos-ccache]

5. + Log Groomer Patch
   └─ containers: [worker, git-sync, worker-kerberos, worker-log-groomer]

6. + Production Patches
   └─ replicas: 5 (changed)
      └─ resources: high (changed)
```

## Testing Strategy

```
┌─────────────────┐
│  Base           │ ──→ ✓ Test independently
└─────────────────┘

┌─────────────────┐
│  + StatefulSet  │ ──→ ✓ Test with base
└─────────────────┘

┌─────────────────┐
│  + GitSync      │ ──→ ✓ Test with base + StatefulSet
└─────────────────┘

┌─────────────────┐
│  + Kerberos     │ ──→ ✓ Test all combinations
└─────────────────┘

┌─────────────────┐
│  + Log Groomer  │ ──→ ✓ Test full production
└─────────────────┘
```

## Complexity Metrics

```
┌──────────────┬──────────┬───────────┬────────────┐
│   Metric     │  Before  │   After   │ Reduction  │
├──────────────┼──────────┼───────────┼────────────┤
│ Lines        │   484    │   ~130    │    73%     │
│ Conditionals │   45+    │     3     │    93%     │
│ Nesting      │    7     │     0     │   100%     │
│ Files        │    1     │    10+    │    N/A     │
│ Testability  │   Low    │   High    │  ↑↑↑↑↑↑   │
└──────────────┴──────────┴───────────┴────────────┘
```

## GitOps Integration

```
Git Repository
├── overlays/
│   ├── production/      ──→  Flux/ArgoCD  ──→  Production Cluster
│   ├── staging/         ──→  Flux/ArgoCD  ──→  Staging Cluster
│   └── development/     ──→  Flux/ArgoCD  ──→  Dev Cluster
│
│   Each environment:
│   ✓ Explicit configuration
│   ✓ No hidden logic
│   ✓ Easy to diff
│   ✓ Audit trail
```

---

**Visual representation of the kustomize architecture for Airflow worker simplification**
