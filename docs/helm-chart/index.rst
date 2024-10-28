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

.. image:: /img/helm-logo.svg
    :width: 100
    :class: no-scaled-link

Helm Chart for Apache Airflow
=============================

.. toctree::
    :hidden:

    Home <self>
    quick-start
    airflow-configuration
    adding-connections-and-variables
    manage-dags-files
    manage-logs
    setting-resources-for-containers
    keda
    using-additional-containers
    customizing-workers
    Installing from sources<installing-helm-chart-from-sources>
    Extending the Chart<extending-the-chart>

.. toctree::
    :hidden:
    :caption: Guides

    production-guide

.. toctree::
    :hidden:
    :caption: References

    Parameters <parameters-ref>
    release_notes


This chart will bootstrap an `Airflow <https://airflow.apache.org>`__
deployment on a `Kubernetes <http://kubernetes.io>`__ cluster using the
`Helm <https://helm.sh>`__ package manager.

Requirements
------------

-  Kubernetes 1.26+ cluster
-  Helm 3.0+
-  PV provisioner support in the underlying infrastructure (optionally)

Features
--------

* Supported executors: ``LocalExecutor``, ``CeleryExecutor``, ``KubernetesExecutor``, ``LocalKubernetesExecutor``, ``CeleryKubernetesExecutor``
* Supported Airflow version: ``1.10+``, ``2.0+``
* Supported database backend: ``PostgreSQL``, ``MySQL``
* Autoscaling for ``CeleryExecutor`` provided by KEDA
* ``PostgreSQL`` and ``PgBouncer`` with a battle-tested configuration
* Monitoring:

   * StatsD/Prometheus metrics for Airflow
   * Prometheus metrics for PgBouncer
   * Flower
* Automatic database migration after a new deployment
* Administrator account creation during deployment
* Kerberos secure configuration
* One-command deployment for any type of executor. You don't need to provide other services e.g. Redis/Database to test the Airflow.

Installing the Chart
--------------------

To install this chart using Helm 3, run the following commands:

.. code-block:: bash

    helm repo add apache-airflow https://airflow.apache.org
    helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace

The command deploys Airflow on the Kubernetes cluster in the default configuration. The :doc:`parameters-ref`
section lists the parameters that can be configured during installation.


.. tip:: List all releases using ``helm list``.

Upgrading the Chart
-------------------

To upgrade the chart with the release name ``airflow``:

.. code-block:: bash

    helm upgrade airflow apache-airflow/airflow --namespace airflow

.. note::
  To upgrade to a new version of the chart, run ``helm repo update`` first.

Uninstalling the Chart
----------------------

To uninstall/delete the ``airflow`` deployment:

.. code-block:: bash

    helm delete airflow --namespace airflow

The command removes all the Kubernetes components associated with the chart and deletes the release.

.. note::
  Some kubernetes resources created by the chart `helm hooks <https://helm.sh/docs/topics/charts_hooks/#hook-resources-are-not-managed-with-corresponding-releases>`__ might be left in the namespace after executing ``helm uninstall``, for example, ``brokerUrlSecret`` or ``fernetKeySecret``.

Installing the Chart with Argo CD, Flux, Rancher or Terraform
-------------------------------------------------------------

When installing the chart using Argo CD, Flux, Rancher or Terraform, you MUST set the four following values, or your application
will not start as the migrations will not be run:

.. code-block:: yaml

    createUserJob:
      useHelmHooks: false
      applyCustomEnv: false
    migrateDatabaseJob:
      useHelmHooks: false
      applyCustomEnv: false

This is so these CI/CD services can perform updates without issues and preserve the immutability of Kubernetes Job manifests.

This also applies if you install the chart using ``--wait`` in your ``helm install`` command.

.. note::
    While deploying this Helm chart with Argo, you might encounter issues with database migrations not running automatically on upgrade.

To run database migrations with Argo CD automatically, you will need to add:

.. code-block:: yaml

    migrateDatabaseJob:
        jobAnnotations:
            "argocd.argoproj.io/hook": Sync

This will run database migrations every time there is a ``Sync`` event in Argo CD. While it is not ideal to run the migrations on every sync, it is a trade-off that allows them to be run automatically.

If you use the Celery(Kubernetes)Executor with the built-in Redis, it is recommended that you set up a static Redis password either by supplying ``redis.passwordSecretName`` and ``data.brokerUrlSecretName`` or ``redis.password``.

By default, Helm hooks are also enabled for ``extraSecrets`` or ``extraConfigMaps``. When using the above CI/CD tools, you might encounter issues due to these default hooks.

To avoid potential problems, it is recommended to disable these hooks by setting ``useHelmHooks=false`` as shown in the following examples:

.. code-block:: yaml

    extraSecrets:
      '{{ .Release.Name }}-example':
        useHelmHooks: false
        data: |
          AIRFLOW_VAR_HELLO_MESSAGE: "Hi!"

    extraConfigMaps:
      '{{ .Release.Name }}-example':
        useHelmHooks: false
        data: |
          AIRFLOW_VAR_HELLO_MESSAGE: "Hi!"

Naming Conventions
------------------

For new installations it is highly recommended to start using standard naming conventions.
It is not enabled by default as this may cause unexpected behaviours on existing installations. However you can enable it using ``useStandardNaming``:

.. code-block:: yaml

    useStandardNaming: true

For existing installations, all your resources will be recreated with a new name and helm will delete previous resources.

This won't delete existing PVCs for logs used by StatefulSets/Deployments, but it will recreate them with brand new PVCs.
If you do want to preserve logs history you'll need to manually copy the data of these volumes into the new volumes after
deployment. Depending on what storage backend/class you're using this procedure may vary. If you don't mind starting
with fresh logs/redis volumes, you can just delete the old persistent volume claims, for example:

.. code-block:: bash

    kubectl delete pvc -n airflow logs-gta-triggerer-0
    kubectl delete pvc -n airflow logs-gta-worker-0
    kubectl delete pvc -n airflow redis-db-gta-redis-0

.. note::

    If you do not change ``useStandardNaming`` or ``fullnameOverride`` after upgrade, you can proceed as usual and no unexpected behaviours will be presented.
