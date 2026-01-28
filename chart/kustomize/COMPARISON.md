# Helm Template Simplification with Kustomize

## Overview

This document explains how the Airflow worker deployment has been simplified by extracting conditional logic from Helm templates into Kustomize overlays.

## Problem Statement

The original `worker-deployment.yaml` Helm template contained:
- **45+ conditional blocks** (`{{- if ... }}`)
- **Multiple nested conditionals** making it hard to read and maintain
- **Mixed concerns** (persistence, authentication, DAG sync, log management)
- **Complex variable assignments** at the template level

This made the template:
- Difficult to understand and debug
- Hard to test individual features
- Prone to errors when combining features
- Challenging for GitOps workflows

## Solution: Kustomize-Based Architecture

### Before: Complex Helm Template

```yaml
{{- $persistence := .Values.workers.persistence.enabled }}
{{- $keda := .Values.workers.keda.enabled }}
{{- $hpa := and .Values.workers.hpa.enabled (not .Values.workers.keda.enabled) }}
{{- if or (contains "CeleryExecutor" .Values.executor) ... }}
kind: {{ if $persistence }}StatefulSet{{ else }}Deployment{{ end }}
# ... 400+ lines with nested conditionals
{{- if and $persistence .Values.workers.persistence.fixPermissions }}
  # init container
{{- end }}
{{- if and (semverCompare ">=2.8.0" .Values.airflowVersion) .Values.workers.kerberosInitContainer.enabled }}
  # another init container
{{- end }}
{{- if .Values.workers.waitForMigrations.enabled }}
  # yet another init container
{{- end }}
# ... many more conditionals
{{- end }}
```

### After: Clean Separation

**Simplified Helm Template** (worker-deployment-simplified.yaml):
```yaml
# Simple base template with only essential conditionals:
# - Executor type check (CeleryExecutor vs others)
# - StatefulSet vs Deployment (persistence)
# Total: ~130 lines instead of 484
```

**Kustomize Base** (kustomize/base/worker-deployment.yaml):
```yaml
# Pure Kubernetes manifest with no templating
# Can be tested directly with kubectl
# Total: ~100 lines
```

**Kustomize Overlays** (kustomize/overlays/*/):
```
overlays/
├── statefulset/        # Persistent logs configuration
├── kerberos/           # Authentication setup
├── gitsync/            # DAG synchronization
└── log-groomer/        # Log cleanup
```

## Comparison

### Lines of Code

| Component | Original | Simplified | Reduction |
|-----------|----------|------------|-----------|
| Helm Template | 484 lines | 130 lines | **73% reduction** |
| Conditional Blocks | 45+ | 3 | **93% reduction** |

### Conditional Logic Extracted

| Feature | Original Location | New Location |
|---------|------------------|--------------|
| Persistence (StatefulSet vs Deployment) | Helm template | Both (minimal in Helm, full in Kustomize) |
| Kerberos sidecars | Helm template | Kustomize overlay |
| GitSync containers | Helm template | Kustomize overlay |
| Log groomer sidecar | Helm template | Kustomize overlay |
| Init containers | Helm template | Kustomize overlay |
| Volume permissions | Helm template | Kustomize overlay |

### Readability Improvement

**Original template complexity:**
```yaml
{{- if and (.Values.dags.gitSync.enabled) (not .Values.dags.persistence.enabled) }}
  {{- include "git_sync_container" . | nindent 8 }}
{{- end }}
{{- if and $persistence .Values.workers.logGroomerSidecar.enabled }}
  # log groomer sidecar
{{- end }}
{{- if .Values.workers.kerberosSidecar.enabled }}
  # kerberos sidecar
{{- end }}
```

**New approach:**
```bash
# Apply base + specific overlays
kubectl apply -k overlays/statefulset-gitsync-kerberos
```

## Benefits

### 1. **Maintainability**
- Each feature is isolated in its own overlay
- Changes to one feature don't affect others
- Easier to review and test changes

### 2. **Composability**
```bash
# Combine features as needed
kustomize build overlays/statefulset
kustomize build overlays/statefulset-gitsync
kustomize build overlays/statefulset-gitsync-kerberos
```

### 3. **Testing**
- Test each overlay independently
- Validate combinations explicitly
- Use standard Kubernetes tools

### 4. **GitOps Integration**
- Better with Flux/ArgoCD
- Explicit configurations per environment
- No hidden conditional logic

### 5. **Environment-Specific Configs**
```
environments/
├── production/
│   └── kustomization.yaml      # statefulset + all features
├── staging/
│   └── kustomization.yaml      # deployment + gitsync only
└── development/
    └── kustomization.yaml      # minimal base deployment
```

## Migration Path

### Option 1: Full Migration (Recommended for new deployments)
1. Use Kustomize overlays exclusively
2. Remove complex Helm template
3. Keep only essential Helm features (naming, labels)

### Option 2: Hybrid Approach (Recommended for existing deployments)
1. Keep simplified Helm template for basic use cases
2. Document Kustomize option for complex scenarios
3. Gradual migration over time

### Option 3: Helm + Kustomize
1. Use Helm for initial deployment
2. Use Kustomize to customize per environment
3. Apply Helm output through Kustomize

## Decision Matrix: When to Use What?

| Scenario | Use Helm Template | Use Kustomize |
|----------|------------------|---------------|
| Simple worker deployment | ✅ Yes | ✅ Yes (recommended) |
| With persistence only | ✅ Yes | ✅ Yes (recommended) |
| With Kerberos | ⚠️ Complex | ✅ Yes (recommended) |
| With GitSync | ⚠️ Complex | ✅ Yes (recommended) |
| Multiple features combined | ❌ Very complex | ✅ Yes (recommended) |
| Environment-specific configs | ⚠️ Many values files | ✅ Yes (recommended) |
| GitOps workflow | ⚠️ Limited | ✅ Yes (recommended) |

## Example: Complete Migration

### Before (Helm values.yaml):
```yaml
workers:
  persistence:
    enabled: true
    size: 100Gi
  kerberosSidecar:
    enabled: true
  logGroomerSidecar:
    enabled: true
dags:
  gitSync:
    enabled: true
    repo: https://github.com/example/dags
```

### After (Kustomize):

**overlays/production/kustomization.yaml**:
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../../base

patchesStrategicMerge:
  - ../../overlays/statefulset/statefulset-patch.yaml
  - ../../overlays/kerberos/kerberos-patch.yaml
  - ../../overlays/log-groomer/log-groomer-patch.yaml
  - ../../overlays/gitsync/gitsync-patch.yaml

configMapGenerator:
  - name: gitsync-config
    literals:
      - GITSYNC_REPO=https://github.com/example/dags
```

## Recommendations

### For New Deployments
1. **Use Kustomize overlays** for all configurations
2. Keep Helm minimal (just for naming and common labels)
3. Build environment-specific overlays from day one

### For Existing Deployments
1. **Start with simplified Helm template** for immediate benefit
2. **Gradually migrate** complex features to Kustomize overlays
3. **Test each overlay** independently before combining
4. **Document the migration** for your team

### For Teams
1. **Split ownership**: Different teams can own different overlays
2. **Review separately**: Each overlay can be reviewed independently
3. **Test in isolation**: Reduces risk of regressions
4. **Deploy incrementally**: Roll out changes feature by feature

## Conclusion

By extracting conditional logic from Helm templates into Kustomize overlays:
- **Reduced template complexity by 73%**
- **Improved readability and maintainability**
- **Enabled better composition of features**
- **Enhanced GitOps workflows**
- **Simplified testing and validation**

The hybrid approach allows teams to:
- Use simple Helm templates for basic cases
- Use Kustomize for complex configurations
- Migrate gradually without disruption
