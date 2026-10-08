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

This executor extends the Kubernetes executor that ships in the ``cncf.kubernetes``
provider. Pod scheduling, pod templates, per-task pod overrides, and log handling all
behave the same way they do under
:doc:`apache-airflow-providers-cncf-kubernetes:kubernetes_executor`. The part this executor
adds is authentication. It builds the Kubernetes client for your EKS cluster from an
Airflow AWS connection, so the scheduler does not need a kubeconfig file on disk and
you do not have to refresh cluster credentials yourself.

Because the executor inherits its pod behavior, everything written about the
Kubernetes executor still applies, including the requirement for a database backend
other than SQLite.

For a quick start guide please see :ref:`here <eks_setup_guide>`.

Requirements
------------

The executor needs ``apache-airflow-providers-cncf-kubernetes`` version 10.24.0 or
newer, the release that added the ``client_factory`` setting the executor is built on.
Install the ``cncf.kubernetes`` extra of the Amazon provider together with that
version:

.. code-block:: bash

    pip install 'apache-airflow-providers-amazon[cncf.kubernetes]' \
        'apache-airflow-providers-cncf-kubernetes>=10.24.0'

The AWS credentials the executor uses must be allowed to call ``eks:DescribeCluster``
on the cluster, and the IAM principal behind those credentials must be granted
access inside the cluster itself. On modern clusters this means an EKS access entry;
on older clusters it means an entry in the ``aws-auth`` config map. Without that
in-cluster grant every Kubernetes API call is rejected, and the startup check described
below stops the scheduler with an error that points at the access entry.

How authentication works
------------------------

When the executor starts, it plugs its own Kubernetes client into the Kubernetes
executor. Every process that needs a client, including the pod watcher that Airflow runs
as a separate process, builds its own from the ``[aws_eks_executor]`` settings.

To build a client, the executor describes the cluster to find its API endpoint and
certificate authority, then mints an authentication token for it. An EKS token is a presigned
STS URL that stays valid for roughly fifteen minutes, while the scheduler holds a
single client for as long as it runs. To keep the token current, the executor
registers a refresh hook that the Kubernetes client calls on every authenticated
request. Minting a token is a local signing operation with no network call, so
refreshing that often costs very little.

Before it accepts any tasks, the executor checks that the cluster is ``ACTIVE`` or
``UPDATING``. An ``UPDATING`` cluster still serves the Kubernetes API, because EKS
replaces API server instances one at a time during an update (see `Update existing
cluster to new Kubernetes version
<https://docs.aws.amazon.com/eks/latest/userguide/update-cluster.html>`__). Unless
``check_health_on_startup`` is turned off (see the :ref:`[aws_eks_executor]
<config:aws_eks_executor>` configuration reference), the executor also checks that it
is allowed to list pods in its namespace. It refuses to start if either check fails.

.. _eks_config_options:

Config Options
--------------

The executor reads its own settings from an ``aws_eks_executor`` section in
``airflow.cfg``. You can also set any of them with an environment variable using the
``AIRFLOW__AWS_EKS_EXECUTOR__<OPTION_NAME>`` form, for example
``AIRFLOW__AWS_EKS_EXECUTOR__CLUSTER_NAME=airflow-eks-cluster``. For more information on
how to set these options, see :doc:`apache-airflow:howto/set-config`.

Required config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  CLUSTER_NAME - The name of the Amazon EKS cluster that tasks run on. The
   executor refuses to start if this is unset. Required.

Optional config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  CONN_ID - The Airflow connection (i.e. credentials) used by the EKS
   executor to make API calls to Amazon EKS. Defaults to ``aws_default``.
-  REGION_NAME - The AWS Region the cluster is in. When this is left empty, the
   region comes from the standard boto3 resolution order.
-  CHECK_HEALTH_ON_STARTUP - Whether to check on startup that the executor can
   list pods in its namespace. Defaults to ``True``.

Pod-level configuration
~~~~~~~~~~~~~~~~~~~~~~~

Settings that describe the worker pods themselves stay in the
``[kubernetes_executor]`` section, exactly as they are for the Kubernetes executor.
This includes ``namespace``, ``pod_template_file``, ``worker_container_repository``,
``worker_container_tag``, and ``delete_worker_pods``. See
:doc:`apache-airflow-providers-cncf-kubernetes:kubernetes_executor` for the full list
and for how pod templates and ``pod_override`` work.

Leave ``client_factory`` and ``async_client_factory`` in that section unset. The
executor sets both itself, and raises an error at startup if either is set to something
else, so that a stale or conflicting setting cannot silently send tasks to the wrong
cluster.

The worker pod template
~~~~~~~~~~~~~~~~~~~~~~~

Set ``[kubernetes_executor] pod_template_file`` to the path of a YAML file describing
the worker pod. Treat it as required. There is no usable default: when the setting is
empty the Kubernetes executor only logs a warning that the model file does not exist
and then builds a worker pod from an empty template. That pod is missing the container
the executor needs, so the failure arrives later and somewhere else, usually as a
rejected pod or a task that never reports back.

The file must define a container named ``base`` as the first entry in
``spec.containers``, and that container has to run your Airflow worker image.
The ``pod_template_file`` section of
:doc:`apache-airflow-providers-cncf-kubernetes:kubernetes_executor` covers the full set
of requirements and includes templates for Dags baked into the image, Dags on a volume,
and git-sync. Any of those works here unchanged, since this executor only replaces how
the Kubernetes client is authenticated.

.. _eks_logging:

.. include:: general.rst
  :start-after: .. BEGIN LOGGING
  :end-before: .. END LOGGING

-  Worker pods are deleted once their task finishes (``[kubernetes_executor]
   delete_worker_pods``), and their logs go with them, so configure remote
   logging to CloudWatch Logs or S3 to keep task logs viewable in the Airflow UI.
-  The remote logging configuration must be set on the worker pods as well as on
   the scheduler and API server, and the worker pods need an IAM role, for
   example through EKS Pod Identity, that can write to the log destination.

.. _eks_setup_guide:

Setting up an EKS Executor for Apache Airflow
---------------------------------------------

Grant access to the cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create an EKS access entry for the IAM role or user that the scheduler runs as, and
associate a policy that allows create, get, list, watch, patch and delete on pods, and
get on pods/log, in the namespace you plan to use. If you manage cluster access through
the ``aws-auth`` config map instead, add the principal there and map it to a Kubernetes
group with the same permissions.

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
Kubernetes executor worker does.

Task logging
~~~~~~~~~~~~

Configure remote logging as described in the :ref:`logging <eks_logging>` section, so
that task logs stay viewable in the Airflow UI after the worker pods are deleted.

Verify the setup
~~~~~~~~~~~~~~~~

Start the scheduler and trigger a small Dag. A successful run shows a worker pod
appearing in your chosen namespace and the task finishing in the Airflow UI. If the
scheduler logs an authentication or forbidden error from the Kubernetes API, the
in-cluster access grant for your IAM principal is the first thing to check.

Multi-team deployments
----------------------

The executor does not support :doc:`multi-team <apache-airflow:core-concepts/multi-team>`
mode, so it cannot be configured as a team executor. Its Kubernetes client is built from
the global ``[aws_eks_executor]`` section, so every team would share one cluster.

Fault tolerance
---------------

Fault tolerance behaves as it does for the Kubernetes executor, including how worker
pod crashes and scheduler restarts are handled. See
:doc:`apache-airflow-providers-cncf-kubernetes:kubernetes_executor` for details.

The one failure mode specific to this executor is credential expiry. Because tokens
are re-minted per request, an expired token is unusual. If you do see repeated
authentication failures after the scheduler has been running for a long time, check
that the credentials behind your connection are still valid and that the role has not
lost its access entry on the cluster.
