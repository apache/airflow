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

Production Deployment
^^^^^^^^^^^^^^^^^^^^^

It is time to deploy your DAG in production. To do this, first, you need to make sure that the Airflow
is itself production-ready. Let's see what precautions you need to take.

Database backend
================

Airflow comes with an ``SQLite`` backend by default. This allows the user to run Airflow without any external
database. However, such a setup is meant to be used for testing purposes only; running the default setup
in production can lead to data loss in multiple scenarios. If you want to run production-grade Airflow,
make sure you :doc:`configure the backend <../howto/set-up-database>` to be an external database
such as PostgreSQL or MySQL.

You can change the backend using the following config

.. code-block:: ini

    [database]
    sql_alchemy_conn = my_conn_string

Once you have changed the backend, airflow needs to create all the tables required for operation.
Create an empty DB and give Airflow's user permission to ``CREATE/ALTER`` it.
Once that is done, you can run -

.. code-block:: bash

    airflow db migrate

``migrate`` keeps track of migrations already applied, so it's safe to run as often as you need.

.. note::

    Prior to Airflow version 2.7.0, ``airflow db upgrade`` was used to apply migrations,
    however, it has been deprecated in favor of ``airflow db migrate``.


Multi-Node Cluster
==================

Airflow uses :class:`~airflow.executors.sequential_executor.SequentialExecutor` by default. However, by its
nature, the user is limited to executing at most one task at a time. ``Sequential Executor`` also pauses
the scheduler when it runs a task, hence it is not recommended in a production setup. You should use the
:class:`~airflow.executors.local_executor.LocalExecutor` for a single machine.
For a multi-node setup, you should use the :doc:`Kubernetes executor <apache-airflow-providers-cncf-kubernetes:kubernetes_executor>` or
the :doc:`Celery executor <apache-airflow-providers-celery:celery_executor>`.


Once you have configured the executor, it is necessary to make sure that every node in the cluster contains
the same configuration and DAGs. Airflow sends simple instructions such as "execute task X of DAG Y", but
does not send any DAG files or configuration. You can use a simple cronjob or any other mechanism to sync
DAGs and configs across your nodes, e.g., checkout DAGs from git repo every 5 minutes on all nodes.


Logging
========

If you are using disposable nodes in your cluster, configure the log storage to be a distributed file system
(DFS) such as ``S3`` and ``GCS``, or external services such as Stackdriver Logging, Elasticsearch or
Amazon CloudWatch. This way, the logs are available even after the node goes down or gets replaced.
See :doc:`logging-monitoring/logging-tasks` for configurations.

.. note::

    The logs only appear in your DFS after the task has finished. You can view the logs while the task is
    running in UI itself.

Configuration
=============

Airflow comes bundled with a default ``airflow.cfg`` configuration file.
You should use environment variables for configurations that change across deployments
e.g. metadata DB, password, etc. You can accomplish this using the format :envvar:`AIRFLOW__{SECTION}__{KEY}`

.. code-block:: bash

 AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=my_conn_id
 AIRFLOW__WEBSERVER__BASE_URL=http://host:port

Some configurations such as the Airflow Backend connection URI can be derived from bash commands as well:

.. code-block:: ini

 sql_alchemy_conn_cmd = bash_command_to_run


Scheduler Uptime
================

Airflow users occasionally report instances of the scheduler hanging without a trace, for example in these issues:

* `Scheduler gets stuck without a trace <https://github.com/apache/airflow/issues/7935>`_
* `Scheduler stopping frequently <https://github.com/apache/airflow/issues/13243>`_

To mitigate these issues, make sure you have a :doc:`health check <logging-monitoring/check-health>` set up that will detect when your scheduler has not heartbeat in a while.

.. _docker_image:

Production Container Images
===========================

We provide :doc:`a Docker Image (OCI) for Apache Airflow <docker-stack:index>` for use in a containerized environment. Consider using it to guarantee that software will always run the same no matter where it's deployed.

Helm Chart for Kubernetes
=========================

`Helm <https://helm.sh/>`__ provides a simple mechanism to deploy software to a Kubernetes cluster. We maintain
:doc:`an official Helm chart <helm-chart:index>` for Airflow that helps you define, install, and upgrade deployment. The Helm Chart uses :doc:`our official Docker image and Dockerfile <docker-stack:index>` that is also maintained and released by the community.


Live-upgrading Airflow
======================

Airflow is by-design a distributed system and while the
:ref:`basic Airflow deployment <overview-basic-airflow-architecture>` requires usually a complete Airflow
restart to upgrade, it is possible to upgrade Airflow without any downtime when you run Airflow in a
:ref:`distributed deployment <overview-basic-airflow-architecture>`.

Such a live upgrade is possible when there are no changes in Airflow metadata database schema,
so you should aim to do it when you upgrade Airflow patch-level (bugfix) versions of the same minor
Airflow version or when upgrading between adjacent minor versions (feature) of Airflow after reviewing the
:doc:`release notes <../release_notes>` and :doc:`../migrations-ref` and making sure there are no changes
in the database schema between them.

In some cases when database migration is not significant, such live migration could also potentially be
possible with upgrading Airflow database first and between MINOR versions, however, this is not recommended
and you should only do it on your own risk, carefully reviewing the modifications to be applied to the
database schema and assessing the risk of such upgrade - it requires deep knowledge of Airflow
database :doc:`../database-erd-ref` and reviewing the :doc:`../migrations-ref`. You should always thoroughly
test such upgrade in a staging environment first. Usually cost connected with such live upgrade preparation
will be higher than the cost of a short downtime of Airflow, so we strongly discourage such live upgrades.

Make sure to test such live upgrade procedure in a staging environment before you do it in production,
to avoid any surprises and side-effects.

When it comes to live-upgrading the ``Webserver``, ``Triggerer`` components, if you run them in separate
environments and have more than one instances for each of them, you can rolling-restart them one by one,
without any downtime. This should usually be done as the first step in your upgrade procedure.

When you are running a deployment with separate ``DAG processor``, in a
:ref:`Separate DAG processing deployment <overview-separate-dag-processing-airflow-architecture>`
the ``DAG processor`` is not horizontally scaled - even if you have more of them there is usually one
``DAG processor`` running at a time per specific folder, so you can just stop it and start the new one -
but since the ``DAG processor`` is not a critical component, it's ok for it to experience a short downtime.

When it comes to upgrading the schedulers and workers, you can use the live upgrade capabilities
of the executor you use:

* For the :doc:`Local executor <../core-concepts/executor/local>` your tasks are running as subprocesses of
  scheduler and you cannot upgrade the Scheduler without killing the tasks run by it. You can either
  pause all your DAGs and wait for the running tasks to complete or just stop the scheduler and kill all
  the tasks it runs - then you will need to clear and restart those tasks manually after the upgrade
  is completed (or rely on ``retry`` being set for stopped tasks).

* For the :doc:`Celery executor <apache-airflow-providers-celery:celery_executor>`, you have to first put your workers in
  offline mode (usually by setting a single ``TERM`` signal to the workers), wait until the workers
  finish all the running tasks, and then upgrade the code (for example by replacing the image the workers run
  in and restart the workers). You can monitor your workers via ``flower`` monitoring tool and see the number
  of running tasks going down to zero. Once the workers are upgraded, they will be automatically put in online
  mode and start picking up new tasks. You can then upgrade the ``Scheduler`` in a rolling restart mode.

* For the :doc:`Kubernetes executor <apache-airflow-providers-cncf-kubernetes:kubernetes_executor>`, you can upgrade the scheduler
  triggerer, webserver in a rolling restart mode, and generally you should not worry about the workers, as they
  are managed by the Kubernetes cluster and will be automatically adopted by ``Schedulers`` when they are
  upgraded and restarted.

* For the :doc:``CeleryKubernetesExecutor <apache-airflow-providers-celery:celery_kubernetes_executor>``, you follow the
  same procedure as for the ``CeleryExecutor`` - you put the workers in offline mode, wait for the running
  tasks to complete, upgrade the workers, and then upgrade the scheduler, triggerer and webserver in a
  rolling restart mode - which should also adopt tasks run via the ``KubernetesExecutor`` part of the
  executor.

Most of the rolling-restart upgrade scenarios are implemented in the :doc:`helm-chart:index`, so you can
use it to upgrade your Airflow deployment without any downtime - especially in case you do patch-level
upgrades of Airflow.

.. _production-deployment:kerberos:

Kerberos-authenticated workers
==============================

Apache Airflow has a built-in mechanism for authenticating the operation with a KDC (Key Distribution Center).
Airflow has a separate command ``airflow kerberos`` that acts as token refresher. It uses the pre-configured
Kerberos Keytab to authenticate in the KDC to obtain a valid token, and then refreshing valid token
at regular intervals within the current token expiry window.

Each request for refresh uses a configured principal, and only keytab valid for the principal specified
is capable of retrieving the authentication token.

The best practice to implement proper security mechanism in this case is to make sure that worker
workloads have no access to the Keytab but only have access to the periodically refreshed, temporary
authentication tokens. This can be achieved in Docker environment by running the ``airflow kerberos``
command and the worker command in separate containers - where only the ``airflow kerberos`` token has
access to the Keytab file (preferably configured as secret resource). Those two containers should share
a volume where the temporary token should be written by the ``airflow kerberos`` and read by the workers.

In the Kubernetes environment, this can be realized by the concept of sidecar, where both Kerberos
token refresher and worker are part of the same Pod. Only the Kerberos sidecar has access to
Keytab secret and both containers in the same Pod share the volume, where temporary token is written by
the sidecar container and read by the worker container.

This concept is implemented in :doc:`the Helm Chart for Apache Airflow <helm-chart:index>`.


.. spelling:word-list::

   pypirc
   dockerignore


Secured Server and Service Access on Google Cloud
=================================================

This section describes techniques and solutions for securely accessing servers and services when your Airflow
environment is deployed on Google Cloud, or you connect to Google services, or you are connecting
to the Google API.

IAM and Service Accounts
------------------------

You should not rely on internal network segmentation or firewalling as our primary security mechanisms.
To protect your organization's data, every request you make should contain sender identity. In the case of
Google Cloud, the identity is provided by
`the IAM and Service account <https://cloud.google.com/iam/docs/service-accounts>`__. Each Compute Engine
instance has an associated service account identity. It provides cryptographic credentials that your workload
can use to prove its identity when making calls to Google APIs or third-party services. Each instance has
access only to short-lived credentials. If you use Google-managed service account keys, then the private
key is always held in escrow and is never directly accessible.

If you are using Kubernetes Engine, you can use
`Workload Identity <https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity>`__ to assign
an identity to individual pods.

For more information about service accounts in the Airflow, see :ref:`howto/connection:gcp`

Impersonate Service Accounts
----------------------------

If you need access to other service accounts, you can
:ref:`impersonate other service accounts <howto/connection:gcp:impersonation>` to exchange the token with
the default identity to another service account. Thus, the account keys are still managed by Google
and cannot be read by your workload.

It is not recommended to generate service account keys and store them in the metadata database or the
secrets backend. Even with the use of the backend secret, the service account key is available for
your workload.

Access to Compute Engine Instance
---------------------------------

If you want to establish an SSH connection to the Compute Engine instance, you must have the network address
of this instance and credentials to access it. To simplify this task, you can use
:class:`~airflow.providers.google.cloud.hooks.compute.ComputeEngineHook`
instead of :class:`~airflow.providers.ssh.hooks.ssh.SSHHook`

The :class:`~airflow.providers.google.cloud.hooks.compute.ComputeEngineHook` support authorization with
Google OS Login service. It is an extremely robust way to manage Linux access properly as it stores
short-lived ssh keys in the metadata service, offers PAM modules for access and sudo privilege checking
and offers the ``nsswitch`` user lookup into the metadata service as well.

It also solves the discovery problem that arises as your infrastructure grows. You can use the
instance name instead of the network address.

Access to Amazon Web Service
----------------------------

Thanks to the
`Web Identity Federation <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_oidc.html>`__,
you can exchange the Google Cloud Platform identity to the Amazon Web Service identity,
which effectively means access to Amazon Web Service platform.
For more information, see: :ref:`howto/connection:aws:gcp-federation`

.. spelling:word-list::

    nsswitch
    cryptographic
    firewalling
    ComputeEngineHook
