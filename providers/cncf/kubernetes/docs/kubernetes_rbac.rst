 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _kubernetes:rbac:

Kubernetes RBAC permissions
===========================

Airflow components that use Kubernetes need Kubernetes API permissions for the
credentials they use to connect to the cluster. These permissions are separate
from Airflow UI RBAC roles.

Grant the permissions to the Kubernetes ``ServiceAccount`` or kubeconfig user
used by the component that talks to the Kubernetes API:

* the scheduler for ``KubernetesExecutor`` task pod creation and monitoring,
* workers for synchronous ``KubernetesPodOperator`` and related operators,
* the triggerer for deferrable Kubernetes operators that monitor pods or jobs.

Use these rules as a starting point and reduce or extend them for your
deployment. Use a ``Role`` for permissions within a single namespace. Use a
``ClusterRole`` when permissions must cover multiple namespaces or
cluster-scoped resources.

The examples below show the RBAC rules. Bind the ``Role`` or ``ClusterRole`` to
the ``ServiceAccount`` used by the Airflow component that makes the Kubernetes
API calls. For example, a namespace-scoped deployment can bind a pod launcher
role to a worker service account:

.. code-block:: yaml

   apiVersion: rbac.authorization.k8s.io/v1
   kind: RoleBinding
   metadata:
     name: airflow-pod-launcher-binding
     namespace: airflow
   subjects:
     - kind: ServiceAccount
       name: airflow-worker
       namespace: airflow
   roleRef:
     apiGroup: rbac.authorization.k8s.io
     kind: Role
     name: airflow-pod-launcher

Pod launch permissions
----------------------

``KubernetesExecutor``, ``KubernetesPodOperator``, ``@task.kubernetes``, and
``@task.kubernetes_cmd`` launch and monitor Kubernetes Pods. A namespace-scoped
deployment commonly needs these permissions:

.. code-block:: yaml

   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: airflow-pod-launcher
     namespace: airflow
   rules:
     - apiGroups: [""]
       resources: ["pods"]
       verbs: ["create", "get", "list", "watch", "patch", "delete"]
     - apiGroups: [""]
       resources: ["pods/log"]
       verbs: ["get"]
     - apiGroups: [""]
       resources: ["pods/exec"]
       verbs: ["create", "get"]
     - apiGroups: [""]
       resources: ["events"]
       verbs: ["list", "watch"]

``pods/exec`` is needed when an operator uses exec-based functionality, such as
retrieving XCom from the sidecar container. ``events`` access is used to read
Kubernetes events for diagnostics.

Existing Pod exec permissions
-----------------------------

``KubernetesPodExecOperator`` executes a command in an existing Pod without managing its lifecycle.
When the Pod name and namespace are provided directly, it needs only these permissions:

.. code-block:: yaml

   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: airflow-pod-exec
     namespace: airflow
   rules:
     - apiGroups: [""]
       resources: ["pods"]
       verbs: ["get"]
     - apiGroups: [""]
       resources: ["pods/exec"]
       verbs: ["get"]

Job launch permissions
----------------------

``KubernetesJobOperator`` and ``KubernetesStartKueueJobOperator`` create and
monitor Kubernetes Jobs. Job-based operators commonly need these additional
permissions:

.. code-block:: yaml

   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: airflow-job-launcher
     namespace: airflow
   rules:
     - apiGroups: ["batch"]
       resources: ["jobs"]
       verbs: ["create", "get", "list", "watch", "patch", "delete"]
     - apiGroups: ["batch"]
       resources: ["jobs/status"]
       verbs: ["get", "watch"]

When a job operator waits for completion, streams pod logs, reads XCom, or
cleans up discovered pods, it also needs the relevant pod permissions from
the pod launch section.

For deferrable job operators, the worker creates the Job before deferring and
the triggerer polls Job status. If the triggerer uses in-cluster credentials,
bind the triggerer's ``ServiceAccount`` to the Job status permissions. When
XCom is enabled, the triggerer also needs permission to get pods and exec into
the XCom sidecar container. The worker reads pod logs after the task resumes.

``KubernetesPatchJobOperator`` only needs permission to patch jobs.
``KubernetesDeleteJobOperator`` reads Job status before deleting a Job, so it
needs permission to get ``jobs/status`` and delete jobs.

Cleanup permissions
-------------------

If you run a separate cleanup job for old KubernetesExecutor pods, that cleanup
job only needs pod list and delete permissions in the namespace it cleans:

.. code-block:: yaml

   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: airflow-pod-cleanup
     namespace: airflow
   rules:
     - apiGroups: [""]
       resources: ["pods"]
       verbs: ["list", "delete"]

Custom resource permissions
---------------------------

Some operators act on Kubernetes custom resources:

* ``SparkKubernetesOperator`` creates and monitors SparkApplication resources
  from the Spark-on-Kubernetes operator. Grant permissions for the API group
  and plural resource installed in your cluster, commonly
  ``sparkoperator.k8s.io`` and ``sparkapplications``. It also monitors the
  Spark driver pod, so grant the relevant pod permissions from the pod launch
  section. A typical SparkApplication rule includes:

  .. code-block:: yaml

     - apiGroups: ["sparkoperator.k8s.io"]
       resources: ["sparkapplications"]
       verbs: ["create", "get", "delete"]
     - apiGroups: ["sparkoperator.k8s.io"]
       resources: ["sparkapplications/status"]
       verbs: ["get"]

* ``KubernetesCreateResourceOperator`` and ``KubernetesDeleteResourceOperator``
  create or delete the resources from the YAML you provide. Grant permissions
  for every Kubernetes resource kind in that YAML.
* ``KubernetesInstallKueueOperator`` applies the upstream Kueue installation
  manifests and usually needs broad cluster-level permissions because those
  manifests install cluster-scoped resources.

Official Helm chart
-------------------

If you use the :doc:`official Apache Airflow Helm Chart <helm-chart:index>` to
deploy your environment, see its documentation for details about the
permissions and settings that the Chart manages.
