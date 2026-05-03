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

Upgrading the Helm Chart to Airflow 3
=====================================

Before reading this, make sure you have read the Airflow Upgrade Guide for how to prepare for an upgrade:
:doc:`apache-airflow:installation/upgrading_to_airflow3`.

This guide describes the chart-specific tasks for upgrading a deployment of the official Airflow Helm chart from
Airflow 2.x to Airflow 3.x. Upgrade the chart to ``1.16.0`` or later (chart ``1.16.0`` is the first release that
supports Airflow 3) and review your ``values.yaml`` against the items below. Many keys have been renamed, removed,
or replaced and the chart will fail to render or deploy unsupported components silently if old keys remain.

Bump the Airflow image
----------------------

Set ``defaultAirflowTag`` and ``airflowVersion`` to a 3.x release. The chart ships with a 3.x default starting at
chart ``1.17.0``.

Rename ``webserver`` to ``apiServer``
-------------------------------------

The Airflow 3 component that serves the UI and the public REST API is the API server, not the webserver. All
configuration that lived under ``webserver:`` in the chart values must move under ``apiServer:``. The component now
listens on port ``8080`` and is started with ``airflow api-server``. For example:

.. code-block:: yaml

   # Airflow 2.x chart values
   webserver:
     replicas: 2
     service:
       type: ClusterIP
     resources: {}

   # Airflow 3.x chart values
   apiServer:
     replicas: 2
     service:
       type: ClusterIP
     resources: {}

Move the secret key
-------------------

The API server reads ``[api] secret_key`` instead of ``[webserver] secret_key``. If you set this through ``config:``
in your values file or through ``extraEnv``, update the section name.

Add a JWT secret
----------------

Airflow 3 uses short-lived JWT tokens to authenticate workers and triggerers against the API server. The chart
generates a ``jwt-secret`` Secret on install. If you template Secrets out-of-band or pin a custom name through
``jwtSecretName``, make sure the referenced Secret exists with a ``jwt-secret`` key before workers and triggerers
start.

Deploy the standalone Dag processor
-----------------------------------

In Airflow 3 the Dag file processor is no longer embedded in the scheduler. The chart deploys it as a separate
``dag-processor`` Deployment configured under ``dagProcessor:``. Review resources, tolerations, and any RBAC
overrides for this new component, including granting its ServiceAccount access to your Dag bundles (git-sync,
persistent volumes, or custom bundles).

Pick an auth manager
--------------------

The chart defaults to ``FabAuthManager`` (provided by the ``apache-airflow-providers-fab`` package) so that
existing FAB-based users, roles, and SSO configuration keep working. If you migrated to a different auth manager,
set it explicitly under ``config.core.auth_manager`` and ensure the corresponding provider is installed in your
image.

Rework custom plugins
---------------------

If you mount a ``webserver_config.py`` or ship Flask-AppBuilder plugins (``appbuilder_views``,
``appbuilder_menu_items``, ``flask_blueprints``), install the FAB provider and mount the file under the
``apiServer`` section instead of ``webserver``. See the plugin notes in the core
:doc:`apache-airflow:installation/upgrading_to_airflow3` guide and :doc:`apache-airflow-providers-fab:upgrading`
for details.

Check the minimum Kubernetes version
------------------------------------

Chart ``1.17.0`` raised the minimum supported Kubernetes version to ``1.30``. Upgrade your cluster before upgrading
the chart if needed.

Run database migrations as part of the upgrade
----------------------------------------------

The chart's database migration job handles ``airflow db migrate`` automatically when you run ``helm upgrade``. Make
sure the migration job completes successfully before traffic is sent to the new API server. If you disable the
built-in job, run ``airflow db migrate`` yourself before scaling up the scheduler, API server, Dag processor, and
triggerer.

Re-check renamed or removed values
----------------------------------

Many configuration options under ``webserver``/``apiServer``, ``workers``, and ``scheduler`` were renamed or
removed across chart ``1.16.0``..``1.18.0``. Diff your existing ``values.yaml`` against the chart's default
``values.yaml`` and the :doc:`release_notes` for those versions before applying.

After updating ``values.yaml``, render the chart locally with ``helm template`` and inspect the diff against your
current release before running ``helm upgrade``.
