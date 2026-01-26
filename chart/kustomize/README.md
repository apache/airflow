# Airflow Worker Kustomize Configuration

This directory contains Kustomize configurations to simplify the Airflow worker deployment by moving conditional logic out of Helm templates.

## Structure

```
kustomize/
├── base/                           # Base worker deployment (minimal config)
│   ├── worker-deployment.yaml      # Simplified worker deployment
│   └── kustomization.yaml          # Base kustomization
└── overlays/                       # Configuration overlays
    ├── deployment/                 # Standard Deployment (ephemeral logs)
    ├── statefulset/                # StatefulSet (persistent logs)
    ├── kerberos/                   # Adds Kerberos authentication
    ├── gitsync/                    # Adds GitSync for DAG synchronization
    └── log-groomer/                # Adds log cleanup sidecar
```

## Usage

### Basic Deployment (Ephemeral Logs)

```bash
kubectl apply -k chart/kustomize/overlays/deployment
```

### StatefulSet with Persistent Logs

```bash
kubectl apply -k chart/kustomize/overlays/statefulset
```

### Deployment with GitSync

```bash
kubectl apply -k chart/kustomize/overlays/gitsync
```

### Deployment with Kerberos

```bash
kubectl apply -k chart/kustomize/overlays/kerberos
```

### StatefulSet with Log Groomer

Create a new overlay that combines statefulset and log-groomer:

```bash
mkdir -p chart/kustomize/overlays/statefulset-log-groomer
```

Create `chart/kustomize/overlays/statefulset-log-groomer/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: airflow

resources:
  - ../../base

patchesStrategicMerge:
  - ../../overlays/statefulset/statefulset-patch.yaml
  - ../../overlays/log-groomer/log-groomer-patch.yaml

patches:
  - target:
      kind: StatefulSet
      name: airflow-worker
    patch: |-
      - op: remove
        path: /spec/template/spec/volumes/1
```

Then apply:

```bash
kubectl apply -k chart/kustomize/overlays/statefulset-log-groomer
```

## Composing Overlays

You can combine multiple overlays by referencing them in a new kustomization. For example, to create a StatefulSet with GitSync and Kerberos:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: airflow

resources:
  - ../../base

patchesStrategicMerge:
  - ../../overlays/statefulset/statefulset-patch.yaml
  - ../../overlays/gitsync/gitsync-patch.yaml
  - ../../overlays/kerberos/kerberos-patch.yaml

patches:
  - target:
      kind: StatefulSet
      name: airflow-worker
    patch: |-
      - op: remove
        path: /spec/template/spec/volumes/1
  - target:
      kind: StatefulSet
      name: airflow-worker
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/volumeMounts/-
        value:
          name: dags
          mountPath: /opt/airflow/dags
          subPath: repo
          readOnly: true
```

## Benefits Over Helm Templates

1. **Simplified Templates**: The base worker deployment is much simpler without complex Jinja conditionals
2. **Composability**: Overlays can be combined to create different configurations
3. **Transparency**: Each configuration variant is explicit and easy to understand
4. **Version Control**: Different teams can maintain different overlays
5. **Testing**: Each overlay can be tested independently
6. **GitOps Friendly**: Better integration with GitOps tools like Flux and ArgoCD

## Migration from Helm

The original Helm template has been simplified. The main conditionals that have been extracted:

1. **StatefulSet vs Deployment** (`$persistence`): Now separate overlays
2. **Kerberos sidecars** (`kerberos.enabled`): Now a separate overlay
3. **GitSync containers** (`dags.gitSync.enabled`): Now a separate overlay
4. **Log groomer sidecar** (`workers.logGroomerSidecar.enabled`): Now a separate overlay

## Customization

To customize any overlay:

1. Copy the overlay directory: `cp -r overlays/deployment overlays/my-custom`
2. Modify the patch files in your custom overlay
3. Apply your custom overlay: `kubectl apply -k overlays/my-custom`

## Environment-Specific Configuration

Create environment-specific overlays:

```
overlays/
├── production/
│   └── kustomization.yaml  # Uses statefulset + log-groomer
├── staging/
│   └── kustomization.yaml  # Uses deployment + gitsync
└── development/
    └── kustomization.yaml  # Uses base deployment only
```

## Building Without Applying

To see the generated manifests without applying:

```bash
kubectl kustomize chart/kustomize/overlays/statefulset
```

Or using kustomize directly:

```bash
kustomize build chart/kustomize/overlays/statefulset
```
