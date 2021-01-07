<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Helm Chart for Apache Airflow

[Apache Airflow](https://airflow.apache.org/) is a platform to programmatically author, schedule and monitor workflows.

## Introduction

This chart will bootstrap an [Airflow](https://airflow.apache.org) deployment on a [Kubernetes](http://kubernetes.io)
cluster using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Kubernetes 1.14+ cluster
- Helm 2.11+ or Helm 3.0+
- PV provisioner support in the underlying infrastructure

## Installing the Chart

To install this repository from source (using helm 3)

```bash
kubectl create namespace airflow
helm repo add stable https://charts.helm.sh/stable/
helm dep update
helm install airflow . --namespace airflow
```

The command deploys Airflow on the Kubernetes cluster in the default configuration. The [Parameters](#parameters)
section lists the parameters that can be configured during installation.

> **Tip**: List all releases using `helm list`

## Upgrading the Chart

To upgrade the chart with the release name `airflow`:

```bash
helm upgrade airflow . --namespace airflow
```

## Uninstalling the Chart

To uninstall/delete the `airflow` deployment:

```bash
helm delete airflow --namespace airflow
```

The command removes all the Kubernetes components associated with the chart and deletes the release.

## Updating DAGs

The recommended way to update your DAGs with this chart is to build a new docker image with the latest DAG code (`docker build -t my-company/airflow:8a0da78 .`), push it to an accessible registry (`docker push my-company/airflow:8a0da78`), then update the Airflow pods with that image:

```bash
helm upgrade airflow . \
  --set images.airflow.repository=my-company/airflow \
  --set images.airflow.tag=8a0da78
```

For local development purpose you can also build the image locally and use it via deployment method described by Breeze.

## Mounting DAGS using Git-Sync side car with Persistence enabled

This option will use a Persistent Volume Claim with an accessMode of `ReadWriteMany`. The scheduler pod will sync DAGs from a git repository onto the PVC every configured number of seconds. The other pods will read the synced DAGs. Not all volume  plugins have support for `ReadWriteMany` accessMode. Refer [Persistent Volume Access Modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes) for details

```bash
helm upgrade airflow . \
  --set dags.persistence.enabled=true \
  --set dags.gitSync.enabled=true
  # you can also override the other persistence or gitSync values
  # by setting the  dags.persistence.* and dags.gitSync.* values
  # Please refer to values.yaml for details
```

## Mounting DAGS using Git-Sync side car without Persistence

This option will use an always running Git-Sync side car on every scheduler,webserver and worker pods. The Git-Sync side car containers will sync DAGs from a git repository every configured number of seconds. If you are using the KubernetesExecutor, Git-sync will run as an initContainer on your worker pods.

```bash
helm upgrade airflow . \
  --set dags.persistence.enabled=false \
  --set dags.gitSync.enabled=true
  # you can also override the other gitSync values
  # by setting the  dags.gitSync.* values
  # Refer values.yaml for details
```

## Mounting DAGS from an externally populated PVC

In this approach, Airflow will read the DAGs from a PVC which has `ReadOnlyMany` or `ReadWriteMany` accessMode. You will have to ensure that the PVC is populated/updated with the required DAGs(this won't be handled by the chart). You can pass in the name of the  volume claim to the chart

```bash
helm upgrade airflow . \
  --set dags.persistence.enabled=true \
  --set dags.persistence.existingClaim=my-volume-claim
  --set dags.gitSync.enabled=false
```


## Parameters

The following tables lists the configurable parameters of the Airflow chart and their default values.

| Parameter                                             | Description                                                                                                  | Default                                           |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------- |
| `uid`                                                 | UID to run airflow pods under                                                                                | `50000`                                           |
| `gid`                                                 | GID to run airflow pods under                                                                                | `50000`                                           |
| `nodeSelector`                                        | Node labels for pod assignment                                                                               | `{}`                                              |
| `affinity`                                            | Affinity labels for pod assignment                                                                           | `{}`                                              |
| `tolerations`                                         | Toleration labels for pod assignment                                                                         | `[]`                                              |
| `labels`                                              | Common labels to add to all objects defined in this chart                                                    | `{}`                                              |
| `privateRegistry.enabled`                             | Enable usage of a private registry for Airflow base image                                                    | `false`                                           |
| `privateRegistry.repository`                          | Repository where base image lives (eg: quay.io)                                                              | `~`                                               |
| `ingress.enabled`                                     | Enable Kubernetes Ingress support                                                                            | `false`                                           |
| `ingress.web.*`                                       | Configs for the Ingress of the web Service                                                                   | Please refer to `values.yaml`                     |
| `ingress.flower.*`                                    | Configs for the Ingress of the flower Service                                                                | Please refer to `values.yaml`                     |
| `networkPolicies.enabled`                             | Enable Network Policies to restrict traffic                                                                  | `true`                                            |
| `airflowHome`                                         | Location of airflow home directory                                                                           | `/opt/airflow`                                    |
| `rbacEnabled`                                         | Deploy pods with Kubernetes RBAC enabled                                                                     | `true`                                            |
| `executor`                                            | Airflow executor (eg SequentialExecutor, LocalExecutor, CeleryExecutor, KubernetesExecutor)                  | `KubernetesExecutor`                              |
| `allowPodLaunching`                                   | Allow airflow pods to talk to Kubernetes API to launch more pods                                             | `true`                                            |
| `defaultAirflowRepository`                            | Fallback docker repository to pull airflow image from                                                        | `apache/airflow`                                  |
| `defaultAirflowTag`                                   | Fallback docker image tag to deploy                                                                          | `1.10.10.1-alpha2-python3.6`                      |
| `images.airflow.repository`                           | Docker repository to pull image from. Update this to deploy a custom image                                   | `~`                                               |
| `images.airflow.tag`                                  | Docker image tag to pull image from. Update this to deploy a new custom image tag                            | `~`                                               |
| `images.airflow.pullPolicy`                           | PullPolicy for airflow image                                                                                 | `IfNotPresent`                                    |
| `images.flower.repository`                            | Docker repository to pull image from. Update this to deploy a custom image                                   | `~`                                               |
| `images.flower.tag`                                   | Docker image tag to pull image from. Update this to deploy a new custom image tag                            | `~`                                               |
| `images.flower.pullPolicy`                            | PullPolicy for flower image                                                                                  | `IfNotPresent`                                    |
| `images.statsd.repository`                            | Docker repository to pull image from. Update this to deploy a custom image                                   | `apache/airflow`                                  |
| `images.statsd.tag`                                   | Docker image tag to pull image from. Update this to deploy a new custom image tag                            | `airflow-statsd-exporter-2020.09.05-v0.17.0`      |
| `images.statsd.pullPolicy`                            | PullPolicy for statsd-exporter image                                                                         | `IfNotPresent`                                    |
| `images.redis.repository`                             | Docker repository to pull image from. Update this to deploy a custom image                                   | `redis`                                           |
| `images.redis.tag`                                    | Docker image tag to pull image from. Update this to deploy a new custom image tag                            | `6-buster`                                        |
| `images.redis.pullPolicy`                             | PullPolicy for redis image                                                                                   | `IfNotPresent`                                    |
| `images.pgbouncer.repository`                         | Docker repository to pull image from. Update this to deploy a custom image                                   | `apache/airflow`                                  |
| `images.pgbouncer.tag`                                | Docker image tag to pull image from. Update this to deploy a new custom image tag                            | `airflow-pgbouncer-2020.09.05-1.14.0`             |
| `images.pgbouncer.pullPolicy`                         | PullPolicy for pgbouncer image                                                                               | `IfNotPresent`                                    |
| `images.pgbouncerExporter.repository`                 | Docker repository to pull image from. Update this to deploy a custom image                                   | `apache/airflow`                                  |
| `images.pgbouncerExporter.tag`                        | Docker image tag to pull image from. Update this to deploy a new custom image tag                            | `airflow-pgbouncer-exporter-2020.09.25-0.5.0`     |
| `images.pgbouncerExporter.pullPolicy`                 | PullPolicy for pgbouncer-exporter image                                                                      | `IfNotPresent`                                    |
| `env`                                                 | Environment variables key/values to mount into Airflow pods (deprecated, prefer using extraEnv)              | `[]`                                              |
| `secret`                                              | Secret name/key pairs to mount into Airflow pods                                                             | `[]`                                              |
| `extraEnv`                                            | Extra env 'items' that will be added to the definition of airflow containers                                 | `~`                                               |
| `extraEnvFrom`                                        | Extra envFrom 'items' that will be added to the definition of airflow containers                             | `~`                                               |
| `extraSecrets`                                        | Extra Secrets that will be managed by the chart                                                              | `{}`                                              |
| `extraConfigMaps`                                     | Extra ConfigMaps that will be managed by the chart                                                           | `{}`                                              |
| `data.metadataSecretName`                             | Secret name to mount Airflow connection string from                                                          | `~`                                               |
| `data.resultBackendSecretName`                        | Secret name to mount Celery result backend connection string from                                            | `~`                                               |
| `data.metadataConection`                              | Field separated connection data (alternative to secret name)                                                 | `{}`                                              |
| `data.resultBackendConnection`                        | Field separated connection data (alternative to secret name)                                                 | `{}`                                              |
| `fernetKey`                                           | String representing an Airflow Fernet key                                                                    | `~`                                               |
| `fernetKeySecretName`                                 | Secret name for Airflow Fernet key                                                                           | `~`                                               |
| `kerberos.enabled`                                    | Enable kerberos support for workers                                                                          | `false`                                           |
| `kerberos.ccacheMountPath`                            | Location of the ccache volume                                                                                | `/var/kerberos-ccache`                            |
| `kerberos.ccacheFileName`                             | Name of the ccache file                                                                                      | `ccache`                                          |
| `kerberos.configPath`                                 | Path for the Kerberos config file                                                                            | `/etc/krb5.conf`                                  |
| `kerberos.keytabPath`                                 | Path for the Kerberos keytab file                                                                            | `/etc/airflow.keytab`                             |
| `kerberos.principal`                                  | Name of the Kerberos principal                                                                               | `airflow`                                         |
| `kerberos.reinitFrequency`                            | Frequency of reinitialization of the Kerberos token                                                          | `3600`                                            |
| `kerberos.config`                                      | Content of the configuration file for kerberos (might be templated using Helm templates)                     | `<see values.yaml>`                               |
| `workers.replicas`                                    | Replica count for Celery workers (if applicable)                                                             | `1`                                               |
| `workers.keda.enabled`                                | Enable KEDA autoscaling features                                                                             | `false`                                           |
| `workers.keda.pollingInverval`                        | How often KEDA should poll the backend database for metrics in seconds                                       | `5`                                               |
| `workers.keda.cooldownPeriod`                         | How often KEDA should wait before scaling down in seconds                                                    | `30`                                              |
| `workers.keda.maxReplicaCount`                        | Maximum number of Celery workers KEDA can scale to                                                           | `10`                                              |
| `workers.kerberosSideCar.enabled`                     | Enable Kerberos sidecar for the worker                                                                       | `false`                                           |
| `workers.persistence.enabled`                         | Enable log persistence in workers via StatefulSet                                                            | `false`                                           |
| `workers.persistence.size`                            | Size of worker volumes if enabled                                                                            | `100Gi`                                           |
| `workers.persistence.storageClassName`                | StorageClass worker volumes should use if enabled                                                            | `default`                                         |
| `workers.resources.limits.cpu`                        | CPU Limit of workers                                                                                         | `~`                                               |
| `workers.resources.limits.memory`                     | Memory Limit of workers                                                                                      | `~`                                               |
| `workers.resources.requests.cpu`                      | CPU Request of workers                                                                                       | `~`                                               |
| `workers.resources.requests.memory`                   | Memory Request of workers                                                                                    | `~`                                               |
| `workers.terminationGracePeriodSeconds`               | How long Kubernetes should wait for Celery workers to gracefully drain before force killing                  | `600`                                             |
| `workers.safeToEvict`                                 | Allow Kubernetes to evict worker pods if needed (node downscaling)                                           | `true`                                            |
| `workers.serviceAccountAnnotations`                   | Annotations to add to worker kubernetes service account                                                      | `{}`                                            |
| `workers.extraVolumes`                                | Mount additional volumes into worker                                                                         | `[]`                                            |
| `workers.extraVolumeMounts`                           | Mount additional volumes into worker                                                                         | `[]`                                            |
| `scheduler.podDisruptionBudget.enabled`               | Enable PDB on Airflow scheduler                                                                              | `false`                                           |
| `scheduler.podDisruptionBudget.config.maxUnavailable` | MaxUnavailable pods for scheduler                                                                            | `1`                                               |
| `scheduler.replicas`                                  | # of parallel schedulers (Airflow 2.0 using Mysql 8+ or Postgres only)                                       | `1`                                               |
| `scheduler.resources.limits.cpu`                      | CPU Limit of scheduler                                                                                       | `~`                                               |
| `scheduler.resources.limits.memory`                   | Memory Limit of scheduler                                                                                    | `~`                                               |
| `scheduler.resources.requests.cpu`                    | CPU Request of scheduler                                                                                     | `~`                                               |
| `scheduler.resources.requests.memory`                 | Memory Request of scheduler                                                                                  | `~`                                               |
| `scheduler.airflowLocalSettings`                      | Custom Airflow local settings python file                                                                    | `~`                                               |
| `scheduler.safeToEvict`                               | Allow Kubernetes to evict scheduler pods if needed (node downscaling)                                        | `true`                                            |
| `scheduler.serviceAccountAnnotations`                 | Annotations to add to scheduler kubernetes service account                                                   | `{}`                                            |
| `scheduler.extraVolumes`                              | Mount additional volumes into scheduler                                                                      | `[]`                                            |
| `scheduler.extraVolumeMounts`                         | Mount additional volumes into scheduler                                                                      | `[]`                                            |
| `webserver.livenessProbe.initialDelaySeconds`         | Webserver LivenessProbe initial delay                                                                        | `15`                                              |
| `webserver.livenessProbe.timeoutSeconds`              | Webserver LivenessProbe timeout seconds                                                                      | `30`                                              |
| `webserver.livenessProbe.failureThreshold`            | Webserver LivenessProbe failure threshold                                                                    | `20`                                              |
| `webserver.livenessProbe.periodSeconds`               | Webserver LivenessProbe period seconds                                                                       | `5`                                               |
| `webserver.readinessProbe.initialDelaySeconds`        | Webserver ReadinessProbe initial delay                                                                       | `15`                                              |
| `webserver.readinessProbe.timeoutSeconds`             | Webserver ReadinessProbe timeout seconds                                                                     | `30`                                              |
| `webserver.readinessProbe.failureThreshold`           | Webserver ReadinessProbe failure threshold                                                                   | `20`                                              |
| `webserver.readinessProbe.periodSeconds`              | Webserver ReadinessProbe period seconds                                                                      | `5`                                               |
| `webserver.replicas`                                  | How many Airflow webserver replicas should run                                                               | `1`                                               |
| `webserver.resources.limits.cpu`                      | CPU Limit of webserver                                                                                       | `~`                                               |
| `webserver.resources.limits.memory`                   | Memory Limit of webserver                                                                                    | `~`                                               |
| `webserver.resources.requests.cpu`                    | CPU Request of webserver                                                                                     | `~`                                               |
| `webserver.resources.requests.memory`                 | Memory Request of webserver                                                                                  | `~`                                               |
| `webserver.service.annotations`                       | Annotations to be added to the webserver service                                                             | `{}`                                              |
| `webserver.defaultUser`                               | Optional default airflow user information                                                                    | `{}`                                              |
| `dags.persistence.*`                                  | Dag persistence configuration                                                                                | Please refer to `values.yaml`                     |
| `dags.gitSync.*`                                      | Git sync configuration                                                                                       | Please refer to `values.yaml`                     |
| `multiNamespaceMode`                                  | Whether the KubernetesExecutor can launch pods in multiple namespaces                                        | `False`                                           |
| `serviceAccountAnnottions.*`                          | Map of annotations for worker, webserver, scheduler kubernetes service accounts                              | {}                                                |


Specify each parameter using the `--set key=value[,key=value]` argument to `helm install`. For example,

```bash
helm install --name my-release \
  --set executor=CeleryExecutor \
  --set enablePodLaunching=false .
```

## Autoscaling with KEDA

KEDA stands for Kubernetes Event Driven Autoscaling. [KEDA](https://github.com/kedacore/keda) is a custom controller that allows users to create custom bindings
to the Kubernetes [Horizontal Pod Autoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/).
We've built an experimental scaler that allows users to create scalers based on postgreSQL queries. For the moment this exists
on a separate branch, but will be merged upstream soon. To install our custom version of KEDA on your cluster, please run

```bash
helm repo add kedacore https://kedacore.github.io/charts

helm repo update

helm install \
    --set image.keda=docker.io/kedacore/keda:1.2.0 \
    --set image.metricsAdapter=docker.io/kedacore/keda-metrics-adapter:1.2.0 \
    --namespace keda --name keda kedacore/keda
```

Once KEDA is installed (which should be pretty quick since there is only one pod). You can try out KEDA autoscaling
on this chart by setting `workers.keda.enabled=true` your helm command or in the `values.yaml`.
(Note: KEDA does not support StatefulSets so you need to set `worker.persistence.enabled` to `false`)

```bash
kubectl create namespace airflow

helm install airflow . \
    --namespace airflow \
    --set executor=CeleryExecutor \
    --set workers.keda.enabled=true \
    --set workers.persistence.enabled=false
```

## Walkthrough using kind

**Install kind, and create a cluster:**

We recommend testing with Kubernetes 1.15, as this image doesn't support Kubernetes 1.16+ for CeleryExecutor presently.

```
kind create cluster \
  --image kindest/node:v1.15.7@sha256:e2df133f80ef633c53c0200114fce2ed5e1f6947477dbc83261a6a921169488d
```

Confirm it's up:

```
kubectl cluster-info --context kind-kind
```


**Create namespace + install the chart:**

```
kubectl create namespace airflow
helm install airflow --n airflow .
```

It may take a few minutes. Confirm the pods are up:

```
kubectl get pods --all-namespaces
helm list -n airflow
```

Run `kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow`
to port-forward the Airflow UI to http://localhost:8080/ to confirm Airflow is working.

**Build a Docker image from your DAGs:**

1. Create a project

    ```shell script
    mkdir my-airflow-project && cd my-airflow-project
    mkdir dags  # put dags here
    cat <<EOM > Dockerfile
    FROM apache/airflow
    COPY . .
    EOM
    ```

2. Then build the image:

    ```shell script
    docker build -t my-dags:0.0.1 .
    ```

3. Load the image into kind:

    ```shell script
    kind load docker-image my-dags:0.0.1
    ```

4. Upgrade Helm deployment:

    ```shell script
    # from airflow chart directory
    helm upgrade airflow -n airflow \
        --set images.airflow.repository=my-dags \
        --set images.airflow.tag=0.0.1 \
        .
    ```

## Contributing

Check out [our contributing guide!](../CONTRIBUTING.rst)
