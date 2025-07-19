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

Why RBAC is Required
====================

Kubernetes clusters often have fine-grained access controls for security and
multi-tenancy. Without explicitly granting certain permissions, Airflow will
fail to create or manage pods. Examples include:

* Launching new task pods (via KubernetesExecutor or KubernetesPodOperator)
* Reading pod logs from completed tasks
* Managing temporary configmaps, jobs, or secrets


Essential RBAC Rules
====================

Below is an example of a minimal working Role definition to enable Airflow
components to work properly when launching task pods:

.. code-block:: yaml

   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: airflow-kubernetes-executor
     namespace: airflow  # Replace with your namespace
   rules:
     # Pods - Core permission for creating and managing task pods
     - apiGroups: [""]
       resources: ["pods"]
       verbs: ["create", "get", "list", "watch", "delete", "patch", "update"]
     
     # Pod logs - Required for log retrieval in Airflow UI
     - apiGroups: [""]
       resources: ["pods/log"]
       verbs: ["get", "list"]
     
     # ConfigMaps and Secrets - For passing configuration to pods
     - apiGroups: [""]
       resources: ["configmaps", "secrets"]
       verbs: ["create", "get", "list", "watch", "delete", "patch", "update"]
     
     # Jobs - Required if using Kubernetes Jobs for task execution
     - apiGroups: ["batch"]
       resources: ["jobs"]
       verbs: ["create", "get", "list", "watch", "delete", "patch", "update"]

**Note:** In a production setup, you may prefer using a ``ClusterRole`` if your
Airflow deployment manages pods across multiple namespaces.


Service Account and Role Binding
================================

Complete the RBAC setup with a ServiceAccount and RoleBinding:

.. code-block:: yaml

   ---
   # Service Account for Airflow Kubernetes operations
   apiVersion: v1
   kind: ServiceAccount
   metadata:
     name: airflow-kubernetes-executor
     namespace: airflow

   ---
   # Bind the role to the service account
   apiVersion: rbac.authorization.k8s.io/v1
   kind: RoleBinding
   metadata:
     name: airflow-kubernetes-executor-binding
     namespace: airflow
   subjects:
     - kind: ServiceAccount
       name: airflow-kubernetes-executor
       namespace: airflow
   roleRef:
     kind: Role
     name: airflow-kubernetes-executor
     apiGroup: rbac.authorization.k8s.io


Real-World Example (Helm Chart)
===============================

If you are using the official ``apache/airflow`` Helm chart, RBAC is handled via
Kubernetes manifests in ``chart/templates/rbac/pod-launcher-role.yaml``:

.. code-block:: yaml

   {{- if and .Values.rbac.create .Values.allowPodLaunching }}
   apiVersion: rbac.authorization.k8s.io/v1
   kind: Role
   metadata:
     name: {{ include "airflow.fullname" . }}-pod-launcher-role
     namespace: {{ .Release.Namespace }}
   rules:
     - apiGroups: [""]
       resources: ["pods", "pods/log", "pods/exec"]
       verbs: ["create", "get", "list", "watch", "patch", "delete"]
     - apiGroups: [""]
       resources: ["events"]
       verbs: ["list"]
     - apiGroups: [""]
       resources: ["configmaps", "secrets"]
       verbs: ["create", "get", "list", "watch", "patch", "delete"]
   {{- end }}


Airflow Configuration
=====================

Update your ``airflow.cfg`` to use the service account:

.. code-block:: ini

   [kubernetes]
   # Specify the service account name
   kubernetes_service_account_name = airflow-kubernetes-executor
   
   # Namespace for pod execution
   kubernetes_namespace = airflow


See Also
========

* :doc:`KubernetesExecutor Reference <kubernetes_executor>`
* :doc:`KubernetesPodOperator Documentation <operators>`
* `Helm Chart Permissions <https://github.com/apache/airflow/blob/main/chart/templates/rbac/pod-launcher-role.yaml>`_
* `Kubernetes RBAC Docs <https://kubernetes.io/docs/reference/access-authn-authz/rbac/>`_


Contributions Welcome
=====================

If you find a missing permission or use a different RBAC profile for specific
scenarios, feel free to submit a PR and help improve this page for everyone!