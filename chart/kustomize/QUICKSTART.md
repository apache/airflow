# Quick Start Guide - Kustomize for Airflow Workers

## 🚀 5-Minute Quick Start

### Prerequisites
- `kubectl` or `kustomize` CLI installed
- Access to a Kubernetes cluster (optional, for testing)

### View a Configuration
```bash
cd chart/kustomize

# See what the production config generates
kubectl kustomize overlays/production

# Or with kustomize CLI
kustomize build overlays/production
```

### Apply to Cluster
```bash
# Apply production configuration
kubectl apply -k overlays/production

# Apply staging configuration
kubectl apply -k overlays/staging

# Apply development configuration
kubectl apply -k overlays/development
```

## 📁 What's Available

### Basic Overlays
| Overlay | Description | Use Case |
|---------|-------------|----------|
| `deployment` | Standard Deployment with ephemeral logs | Cost-effective, stateless |
| `statefulset` | StatefulSet with persistent logs | Production, audit requirements |
| `kerberos` | Adds Kerberos authentication | Secure environments |
| `gitsync` | Adds GitSync for DAG sync | Automated DAG deployment |
| `log-groomer` | Adds log cleanup sidecar | Long-running deployments |

### Environment Overlays (Ready to Use)
| Environment | Features | Replicas | Resources |
|-------------|----------|----------|-----------|
| `production` | StatefulSet + GitSync + Log Groomer | 5 | High (2Gi/2CPU) |
| `staging` | Deployment + GitSync | 2 | Standard (1Gi/1CPU) |
| `development` | Minimal Deployment | 1 | Low (512Mi/500m) |

## 🔧 Common Tasks

### 1. Test Configuration Locally
```bash
# Build and inspect
kubectl kustomize overlays/production | less

# Check for errors
kubectl kustomize overlays/production --enable-alpha-plugins

# Save to file
kubectl kustomize overlays/production > /tmp/worker-manifest.yaml
```

### 2. Create Custom Environment
```bash
# Copy production overlay
cp -r overlays/production overlays/my-env

# Edit configuration
vim overlays/my-env/kustomization.yaml

# Change namespace, replicas, resources, etc.

# Test it
kubectl kustomize overlays/my-env
```

### 3. Add a Feature to Existing Overlay
Edit `overlays/YOUR-ENV/kustomization.yaml`:
```yaml
patchesStrategicMerge:
  - ../../overlays/statefulset/statefulset-patch.yaml
  - ../../overlays/gitsync/gitsync-patch.yaml
  - ../../overlays/kerberos/kerberos-patch.yaml  # Add this line
```

### 4. Customize GitSync Repository
Edit `overlays/production/kustomization.yaml`:
```yaml
configMapGenerator:
  - name: gitsync-config
    literals:
      - GITSYNC_REPO=https://github.com/YOUR-ORG/dags.git  # Change this
      - GITSYNC_BRANCH=main                                 # And this
```

### 5. Change Resource Limits
Edit overlay's `kustomization.yaml`:
```yaml
patches:
  - target:
      kind: StatefulSet
      name: airflow-worker
    patch: |-
      - op: replace
        path: /spec/template/spec/containers/0/resources
        value:
          requests:
            cpu: 1000m      # Adjust these
            memory: 4Gi     # values
          limits:
            cpu: 4000m
            memory: 8Gi
```

## 🎯 Decision Tree: Which Overlay Should I Use?

```
Do you need persistent logs?
├─ No → Use "deployment" or "development"
└─ Yes → Use "statefulset" or "production"

Do you need DAG synchronization?
├─ No → Use basic overlays
└─ Yes → Add "gitsync" or use "staging"/"production"

Do you need Kerberos?
├─ No → Use standard overlays
└─ Yes → Add "kerberos" overlay

Do you need log cleanup?
├─ No → Skip log-groomer
└─ Yes → Add "log-groomer" or use "production"
```

## 🔍 Validation

### Quick Validation
```bash
# Validate all overlays
bash examples/validate.sh
```

### Manual Validation
```bash
# Check YAML syntax
kubectl kustomize overlays/production --enable-alpha-plugins

# Dry-run apply
kubectl apply -k overlays/production --dry-run=client

# Server-side dry-run (validates against cluster)
kubectl apply -k overlays/production --dry-run=server
```

## 📊 Compare Configurations

```bash
# Compare two environments
diff <(kubectl kustomize overlays/production) <(kubectl kustomize overlays/staging)

# See what changed
kubectl diff -k overlays/production
```

## 🐛 Troubleshooting

### "Error: unable to find one or more patches"
- Check that referenced patch files exist
- Verify paths are correct (e.g., `../../overlays/statefulset/statefulset-patch.yaml`)

### "Error: no matches for kind 'StatefulSet'"
- You're trying to patch a StatefulSet but the base is a Deployment
- Make sure you include the statefulset patch first to change the kind

### "Error: invalid patch"
- Check your patch syntax (YAML indentation)
- Verify the path exists in the target resource
- Use `kubectl kustomize --enable-alpha-plugins` for better error messages

### Patch Not Applied
- Ensure target selector matches (kind, name)
- Check patch order (some patches depend on others)
- Use strategic merge patches for adding, JSON patches for replacing

## 📚 Learn More

- **README.md** - Complete usage guide with all options
- **COMPARISON.md** - Detailed before/after analysis
- **SUMMARY.md** - Full overview of changes
- **examples/validate.sh** - Validation script to check everything

## 🎓 Examples

### Example 1: Simple Deployment
```bash
kubectl apply -k overlays/deployment
```
Result: Standard Deployment, 1 replica, ephemeral logs

### Example 2: Production-Grade Setup
```bash
kubectl apply -k overlays/production
```
Result: StatefulSet, 5 replicas, persistent logs, GitSync, log groomer, high resources

### Example 3: Custom Staging
```yaml
# overlays/staging-custom/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: airflow-staging
resources:
  - ../../base
patchesStrategicMerge:
  - ../../overlays/gitsync/gitsync-patch.yaml
replicas:
  - name: airflow-worker
    count: 3
```

## 🚦 Getting Help

1. **Validate your setup**: `bash examples/validate.sh`
2. **Check the docs**: Read `README.md` and `COMPARISON.md`
3. **Inspect generated YAML**: `kubectl kustomize overlays/YOUR-OVERLAY | less`
4. **Test locally**: Use `--dry-run=client` before applying

## 💡 Pro Tips

1. **Always validate before applying**: Use `kubectl kustomize` to see what will be created
2. **Use version control**: Commit your custom overlays to git
3. **Document your changes**: Add comments in kustomization.yaml explaining why
4. **Test incrementally**: Start with base, then add one feature at a time
5. **Use strategic merge for additions**: JSON patches for precise replacements

---

**Ready to simplify your Helm charts?** Start with `kubectl kustomize overlays/development` and work your way up! 🎉
