# Kubernetes Executor Support in Breeze - Implementation Plan

## Overview

This plan outlines the implementation of Kubernetes Executor support in Breeze's `start-airflow` command. The implementation will:
1. Auto-create and manage a KinD (Kubernetes in Docker) cluster
2. Build and deploy worker images with DAGs and files from `files/` directory  
3. Configure Airflow to use KubernetesExecutor with the auto-created cluster
4. Reuse existing `breeze k8s` infrastructure for cluster and image management

## Current State (Based on Git History)

- Commit `0524441dc5` added initial WIP support:
  - Added `--kubeconfig-file` and `--force-rebuild-cluster` CLI options to `start-airflow`
  - Updated `START_AIRFLOW_ALLOWED_EXECUTORS` to include `KUBERNETES_EXECUTOR`
  - Updated command help documentation

## Architecture Overview - Simplified Approach

### Single Mode of Operation

**Breeze-Managed KinD Cluster**
- Breeze auto-creates a KinD cluster (reusing `breeze k8s` infrastructure)
- Cluster name: `airflow-python-{python_version}-breeze`
- Breeze builds worker image with DAGs from `files/dags` and includes `files/include`
- Breeze uploads image to KinD cluster using `kind load docker-image`
- Breeze configures Airflow to use KubernetesExecutor with auto-generated kubeconfig
- Namespace: `airflow` (same as used by breeze k8s commands)

### Key Components to Reuse from `breeze k8s`

1. **Cluster Management** (`dev/breeze/src/airflow_breeze/utils/kubernetes_utils.py`):
   - `check_if_kind_cluster_exists()`
   - `create_kind_cluster_with_config()`
   - `get_kind_cluster_name()`
   - `get_kubeconfig_file()`

2. **Image Management** (`dev/breeze/src/airflow_breeze/commands/kubernetes_commands.py`):
   - `_rebuild_k8s_image()` - Build K8s-ready image
   - `_upload_k8s_image()` - Upload to KinD cluster

3. **Configuration**:
   - Use existing K8S_CLUSTERS_PATH for kubeconfig storage
   - Use existing cluster config templates

---

## Implementation Plan - Sequential TODOs

### Phase 1: CLI Infrastructure Cleanup

**TODO 1.1: Remove unnecessary CLI options from `developer_commands.py`**
- [x] Remove `--kubeconfig-file` option (we'll auto-manage this)
- [x] Keep `--force-rebuild-cluster` as is (already used in other parts of breeze)

**TODO 1.2: Update `developer_commands_config.py`**
- [x] Remove `--kubeconfig-file` from parameter groups
- [x] Keep `--force-rebuild-cluster` in "Choosing executor" group (already in right place)

### Phase 2: Integration with breeze k8s Infrastructure

**TODO 2.1: Update ShellParams**
- [x] Add field: `force_recreate_kind_cluster: bool = False`
- [x] Add field: `k8s_namespace: str = "airflow"`

**TODO 2.2: Import and reuse kubernetes utilities**
- [x] In `developer_commands.py`, import functions from `kubernetes_utils.py`
- [x] Import cluster management functions (get_kind_cluster_name, get_kubeconfig_file, make_sure_kubernetes_tools_are_installed, run_command_with_k8s_env)
- [ ] Import image building/upload functions when needed in later phases

### Phase 3: KinD Cluster Management

**TODO 3.1: Create cluster initialization function**
- [x] Function: `initialize_kind_cluster_for_executor(python, force_recreate_cluster)`
- [x] Make kubernetes_version configurable as parameter (defaults to DEFAULT_KUBERNETES_VERSION)
- [x] Create cluster using `_create_cluster` from kubernetes_commands.py
- [x] Use cluster name format: `airflow-python-{python}-{kubernetes_version}` (via get_kind_cluster_name)
- [x] Handle `--force-recreate-kind-cluster` flag (passes through to _create_cluster)
- [x] Refactored `_create_cluster` to return (returncode, message, cluster_name, kubeconfig_path)
- [x] Updated all callers of `_create_cluster` (2 places in kubernetes_commands.py)
- [x] Return tuple: (cluster_name, kubeconfig_path) - now directly from _create_cluster

**TODO 3.2: Generate kubeconfig path**
- [ ] Use `get_kubeconfig_file()` from kubernetes_utils
- [ ] Store path in ShellParams for later use

### Phase 4: Worker Image Building

**TODO 4.1: Create worker image build function**
- [ ] Function: `build_k8s_worker_image(shell_params)`
- [ ] Reuse `_rebuild_k8s_image()` logic
- [ ] Ensure image includes:
  - Base Airflow image
  - Contents of `files/dags/` copied to `/opt/airflow/dags/`
  - Contents of `files/include/` copied to `/opt/airflow/include/`

**TODO 4.2: Upload image to KinD cluster**
- [ ] Reuse `_upload_k8s_image()` function
- [ ] Use `kind load docker-image` command

### Phase 5: Airflow Configuration

**TODO 5.1: Set KubernetesExecutor configuration**
- [ ] Set environment variables:
  - `AIRFLOW__KUBERNETES__KUBE_CONFIG_PATH`: Path to auto-generated kubeconfig
  - `AIRFLOW__KUBERNETES__NAMESPACE`: "airflow"
  - `AIRFLOW__KUBERNETES__WORKER_CONTAINER_REPOSITORY`: Image name used
  - `AIRFLOW__KUBERNETES__WORKER_CONTAINER_TAG`: Image tag
  - `AIRFLOW__KUBERNETES__DELETE_WORKER_PODS`: "True"
  - `AIRFLOW__KUBERNETES__DELETE_WORKER_PODS_ON_FAILURE`: "False" (for debugging)

**TODO 5.2: Create namespace in cluster**
- [ ] Run `kubectl create namespace airflow` if not exists
- [ ] Use kubeconfig from auto-generated path

### Phase 6: Integration with start_airflow Command

**TODO 6.1: Modify start_airflow command flow**
- [ ] In `start_airflow()` function, detect if executor is KubernetesExecutor
- [ ] Call cluster initialization before starting Airflow
- [ ] Call worker image build and upload
- [ ] Set K8s configuration environment variables
- [ ] Start Airflow normally

**TODO 6.2: Add status reporting**
- [ ] Print cluster creation status
- [ ] Print image build/upload progress
- [ ] Print connection details

### Phase 7: Cleanup and Lifecycle Management

**TODO 7.1: Handle breeze stop**
- [ ] When `breeze stop` is called, optionally delete KinD cluster
- [ ] Add `--preserve-kind-cluster` flag to keep cluster running

**TODO 7.2: Handle file changes**
- [ ] Detect changes in `files/dags/` or `files/include/`
- [ ] Automatically rebuild and re-upload worker image
- [ ] Or provide command to manually trigger rebuild

### Phase 8: Testing Infrastructure

**TODO 8.1: Add unit tests**
- [ ] Test cluster name generation
- [ ] Test kubeconfig path generation
- [ ] Test environment variable setting

**TODO 8.2: Add integration tests**
- [ ] Test full flow: cluster creation → image upload → Airflow start
- [ ] Test with sample DAG execution
- [ ] Test cleanup

### Phase 9: Documentation

**TODO 9.1: Update breeze documentation**
- [ ] Add section on KubernetesExecutor support
- [ ] Document auto-cluster management
- [ ] Document how DAGs are packaged

**TODO 9.2: Add examples**
- [ ] Example: Running with KubernetesExecutor
- [ ] Example: Debugging failed pods
- [ ] Example: Viewing worker logs

### Phase 10: Error Handling

**TODO 10.1: Add proper error handling**
- [ ] Handle Docker daemon not running
- [ ] Handle kind not installed
- [ ] Handle kubectl not installed
- [ ] Handle cluster creation failures
- [ ] Handle image build/upload failures

**TODO 10.2: Add recovery mechanisms**
- [ ] Auto-retry cluster creation on failure
- [ ] Provide clear error messages with fix suggestions

### Phase 11: Future Enhancements (Post-MVP)

**TODO 11.1: Add advanced features**
- [ ] Support for custom kubeconfig (user-provided clusters)
- [ ] Support for external container registries
- [ ] Support for multiple namespaces
- [ ] Support for custom worker pod templates
- [ ] Support for GPU nodes
- [ ] Support for different cluster configurations

---

## Key Implementation Notes

1. **Reuse Existing Code**: Maximum reuse of `breeze k8s` infrastructure
2. **Simplicity First**: No custom registries or kubeconfigs in initial version
3. **Namespace**: Default to `airflow` (same as breeze k8s commands for consistency)
4. **Cluster Naming**: Use consistent naming `airflow-python-{version}-breeze`
5. **Image Management**: Use KinD's `kind load docker-image` for simplicity
6. **File Sync**: Package `files/dags` and `files/include` in worker image

## Success Criteria

1. User can run `breeze start-airflow --executor KubernetesExecutor`
2. Breeze automatically creates and configures KinD cluster
3. Worker pods successfully execute tasks
4. DAGs from `files/dags` are available in workers
5. No manual kubernetes configuration required
