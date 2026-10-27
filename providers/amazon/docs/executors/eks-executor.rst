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

.. |executorName| replace:: EKS

.. _eks_executor:

================
AWS EKS Executor
================

The EKS executor runs each Airflow task in its own pod on an Amazon EKS cluster.

It extends the Kubernetes executor from the ``cncf.kubernetes`` provider, so pod scheduling,
pod templates, per-task pod overrides, log handling and requirements such as a database
backend other than SQLite are all as described in
:doc:`apache-airflow-providers-cncf-kubernetes:kubernetes_executor`. What it adds is
authentication: it builds the Kubernetes client for your EKS cluster from an Airflow AWS
connection, so the scheduler needs no kubeconfig file and you do not have to refresh cluster
credentials yourself.

For a quick start guide please see :ref:`here <eks_setup_guide>`.

Requirements
------------

The executor needs a version of ``apache-airflow-providers-cncf-kubernetes`` that lets an
executor built on the Kubernetes executor bring its own Kubernetes client. The executor checks
this when it is imported and raises a clear error if an older version is installed. Install the
``cncf.kubernetes`` extra of the Amazon provider, which requires that version:

.. code-block:: bash

    pip install 'apache-airflow-providers-amazon[cncf.kubernetes]'

The executor also relies on botocore using the regional STS endpoint by default (botocore 1.40
and newer), and the Amazon provider already requires ``botocore>=1.42.79``, so no extra install
step is needed.

The AWS credentials the executor uses must be allowed to call ``eks:DescribeCluster``
on the cluster, and their IAM principal must also be granted access inside the cluster, as
described in :ref:`eks_grant_access`. Without that in-cluster grant every Kubernetes API call
is rejected.

How authentication works
------------------------

When the executor starts, it plugs its own Kubernetes client into the Kubernetes
executor. Every process that needs a client, including the separate pod watcher process,
builds its own from the :ref:`[aws_eks_executor] <eks_config_options>` settings: it
describes the cluster to find its API endpoint and certificate authority, then gets an
authentication token for it. An EKS token is a presigned STS URL that stays valid for
roughly fifteen minutes, so the executor registers a refresh hook that the Kubernetes client
calls on every authenticated request. The hook reuses the current token and gets a new one
a minute before it expires.

Before it accepts any tasks, the executor checks that the cluster is ``ACTIVE`` or
``UPDATING``, since EKS keeps the Kubernetes API available during an update (see `Update
existing cluster to new Kubernetes version
<https://docs.aws.amazon.com/eks/latest/userguide/update-cluster.html>`__). Unless
:ref:`check_health_on_startup <eks_config_options>` is turned off, it also checks that it
is allowed to list pods in its namespace. It refuses to start if either check fails.

.. _eks_config_options:

Config Options
--------------

The executor reads its own settings from an ``aws_eks_executor`` section in
``airflow.cfg``, or from environment variables of the form
``AIRFLOW__AWS_EKS_EXECUTOR__<OPTION_NAME>``, for example
``AIRFLOW__AWS_EKS_EXECUTOR__CLUSTER_NAME=airflow-eks-cluster``. For more information on
how to set these options, see :doc:`apache-airflow:howto/set-config`.

Required config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  CLUSTER_NAME - The name of the Amazon EKS cluster that tasks run on. The
   executor refuses to start if this is unset.

Optional config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  CONN_ID - The Airflow connection (i.e. credentials) used by the EKS
   executor to make API calls to Amazon EKS. Defaults to ``aws_default``.
-  REGION_NAME - The AWS Region the cluster is in. When this is left empty, it
   falls back to the :ref:`AWS connection's <howto/connection:aws>` region, then
   boto3's.
-  CHECK_HEALTH_ON_STARTUP - Whether to check on startup that the executor can
   list pods in its namespace. Defaults to ``True``.

Pod-level configuration
~~~~~~~~~~~~~~~~~~~~~~~

Settings for the worker pods stay in the
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor`
section, exactly as for the Kubernetes executor. This includes
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__namespace`,
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__pod_template_file`,
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__worker_container_repository`,
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__worker_container_tag`, and
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__delete_worker_pods`.
See :doc:`apache-airflow-providers-cncf-kubernetes:kubernetes_executor` for the full list, and
for how :ref:`pod templates <apache-airflow-providers-cncf-kubernetes:concepts:pod_template_file>` and
:ref:`apache-airflow-providers-cncf-kubernetes:concepts:pod_override` work.

The executor builds its own Kubernetes clients and ignores
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__client_factory` and
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__async_client_factory`,
which only apply to the Kubernetes executor.

Set :ref:`[kubernetes_executor] pod_template_file
<apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__pod_template_file>`
to a YAML file describing the worker pod, since most setups need one. Without one, the worker
pod is built from ``worker_container_repository`` and ``worker_container_tag`` alone, which
only works if that image already has everything the task needs. The template's first container
must be named ``base`` and run your Airflow worker image. The
:ref:`apache-airflow-providers-cncf-kubernetes:concepts:pod_template_file` section of the
Kubernetes executor docs covers the full requirements, and its example templates work here
unchanged.

.. _eks_logging:

.. include:: general.rst
  :start-after: .. BEGIN LOGGING
  :end-before: .. END LOGGING

-  Worker pods are deleted when their task succeeds (:ref:`[kubernetes_executor]
   delete_worker_pods <apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor__delete_worker_pods>`),
   and their logs go with them, so configure remote logging to CloudWatch Logs or S3
   to keep task logs viewable in the Airflow UI.
-  The remote logging configuration must be set on the worker pods as well as on
   the scheduler and API server, and the worker pods need an IAM role, for
   example through EKS Pod Identity, that can write to the log destination.

.. _eks_setup_guide:

Setting up an EKS Executor for Apache Airflow
---------------------------------------------

.. _eks_grant_access:

Grant access to the cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create an `EKS access entry
<https://docs.aws.amazon.com/eks/latest/userguide/access-entries.html>`__ for the IAM
role or user behind the executor's AWS connection (``[aws_eks_executor] conn_id``,
``aws_default`` by default), and associate a policy that allows create, get,
list, watch, patch and delete on pods, and get on pods/log, in the namespace you plan to
use. If you manage cluster access through the `aws-auth ConfigMap
<https://docs.aws.amazon.com/eks/latest/userguide/auth-configmap.html>`__ instead, add
the principal there and map it to a Kubernetes group with the same permissions.

Configure Airflow
~~~~~~~~~~~~~~~~~

Select the executor and name your cluster:

.. code-block:: ini

    [core]
    executor = airflow.providers.amazon.aws.executors.eks.eks_executor.AwsEksExecutor

    [aws_eks_executor]
    cluster_name = airflow-eks-cluster
    region_name = us-east-1
    conn_id = aws_default

    [kubernetes_executor]
    namespace = airflow
    pod_template_file = /opt/airflow/pod_templates/worker_template.yaml
    worker_container_repository = my-account.dkr.ecr.us-east-1.amazonaws.com/airflow
    worker_container_tag = latest

The worker image must contain Airflow and the Amazon provider, and it needs access
to your Dag files and a network path to the Airflow API server, in the same way any
Kubernetes executor worker does. Configure remote logging as described in the
:ref:`logging <eks_logging>` section.

Start the scheduler and trigger a small Dag. A worker pod should appear in your namespace
and the task should finish. If the scheduler logs an authentication or forbidden error from
the Kubernetes API, check the in-cluster access grant for your IAM principal first.

Multi-team deployments
----------------------

The executor supports :doc:`multi-team <apache-airflow:core-concepts/multi-team>` mode
the same way the other AWS executors do. Each team reads its own team-scoped
:ref:`[aws_eks_executor] <eks_config_options>` and
:ref:`apache-airflow-providers-cncf-kubernetes:config:kubernetes_executor` settings, so a
team can use its own cluster, connection, namespace and pod template. Team-scoped settings
don't fall back to the global ones, so set ``cluster_name`` for every team that uses the
executor.

.. code-block:: ini

    [core]
    multi_team = True
    executor = LocalExecutor;team_a=airflow.providers.amazon.aws.executors.eks.eks_executor.AwsEksExecutor;team_b=airflow.providers.amazon.aws.executors.eks.eks_executor.AwsEksExecutor

    [team_a=aws_eks_executor]
    cluster_name = team-a-cluster

    [team_b=aws_eks_executor]
    cluster_name = team-b-cluster
    conn_id = aws_team_b

The executor can also run alongside the Kubernetes executor, in the same team or not, and each
keeps its own cluster. Keep them on different clusters, though: two executors watching the same
namespace of one cluster each act on the other's worker pods.

Fault tolerance
---------------

Fault tolerance, including how worker pod crashes and scheduler restarts are handled, is
the same as for the Kubernetes executor; see
:doc:`apache-airflow-providers-cncf-kubernetes:kubernetes_executor`. Because tokens are
replaced before they expire, repeated authentication failures on a long-running scheduler
usually mean the credentials behind your connection are no longer valid, or the role has
lost its access entry on the cluster.
