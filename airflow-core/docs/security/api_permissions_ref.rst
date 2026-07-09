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

.. THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY.
   Regenerate with:  python scripts/ci/prek/extract_permissions.py
   Trigger:          prek run generate-api-permissions-doc --all-files

API Endpoint Permission Reference
==================================

This page lists the required permission for every endpoint in the stable
Airflow REST API (``/api/v2``).  It is generated automatically from the
source code so it stays up to date as endpoints are added or changed.

.. seealso::

    :doc:`/security/api` — for authentication instructions (JWT tokens).

.. note::

    Permissions are enforced by the configured **auth manager**.  The
    :class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager`
    interface defines the contract; individual auth manager implementations
    (e.g. the Simple Auth Manager, or the FAB provider) translate these
    resource/method tuples into their own role/permission models.

.. list-table:: Stable REST API endpoint permissions
   :header-rows: 1
   :widths: 7 50 20 13

   * - Method
     - Endpoint path
     - Resource
     - Required permission
   * - ``GET``
     - ``/api/v2/assets``
     - ``Asset``
     - ``GET``
   * - ``GET``
     - ``/api/v2/assets``
     - ``AssetAlias``
     - ``GET``
   * - ``GET``
     - ``/api/v2/assets/aliases``
     - ``AssetAlias``
     - ``GET``
   * - ``GET``
     - ``/api/v2/assets/aliases/{asset_alias_id}``
     - ``AssetAlias``
     - ``GET``
   * - ``GET``
     - ``/api/v2/assets/events``
     - ``Asset``
     - ``GET``
   * - ``POST``
     - ``/api/v2/assets/events``
     - ``Asset``
     - ``POST``
   * - ``GET``
     - ``/api/v2/assets/{asset_id}``
     - ``Asset``
     - ``GET``
   * - ``GET``
     - ``/api/v2/assets/{asset_id}``
     - ``AssetAlias``
     - ``GET``
   * - ``POST``
     - ``/api/v2/assets/{asset_id}/materialize``
     - ``Asset``
     - ``POST``
   * - ``DELETE``
     - ``/api/v2/assets/{asset_id}/queuedEvents``
     - ``Asset``
     - ``DELETE``
   * - ``DELETE``
     - ``/api/v2/assets/{asset_id}/queuedEvents``
     - ``DAG``
     - ``GET``
   * - ``GET``
     - ``/api/v2/assets/{asset_id}/queuedEvents``
     - ``Asset``
     - ``GET``
   * - ``DELETE``
     - ``/api/v2/assets/{asset_id}/state-store``
     - ``Asset``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/assets/{asset_id}/state-store``
     - ``Asset``
     - ``GET``
   * - ``DELETE``
     - ``/api/v2/assets/{asset_id}/state-store/{key:path}``
     - ``Asset``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/assets/{asset_id}/state-store/{key:path}``
     - ``Asset``
     - ``GET``
   * - ``PUT``
     - ``/api/v2/assets/{asset_id}/state-store/{key:path}``
     - ``Asset``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/auth/login``
     - ``Public``
     - ``No Airflow permission required``
   * - ``GET``
     - ``/api/v2/auth/logout``
     - ``Public``
     - ``No Airflow permission required``
   * - ``GET``
     - ``/api/v2/backfills``
     - ``DAG.RUN``
     - ``GET``
   * - ``POST``
     - ``/api/v2/backfills``
     - ``DAG.RUN``
     - ``POST``
   * - ``PUT``
     - ``/api/v2/backfills``
     - ``DAG.RUN``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/config``
     - ``Configuration``
     - ``GET``
   * - ``GET``
     - ``/api/v2/config/section/{section}/option/{option}``
     - ``Configuration``
     - ``GET``
   * - ``GET``
     - ``/api/v2/connections``
     - ``Connection``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/connections``
     - ``Connection``
     - ``multi``
   * - ``POST``
     - ``/api/v2/connections``
     - ``Connection``
     - ``POST``
   * - ``POST``
     - ``/api/v2/connections/defaults``
     - ``Connection``
     - ``POST``
   * - ``GET``
     - ``/api/v2/connections/enqueue-test``
     - ``Public``
     - ``No Airflow permission required``
   * - ``POST``
     - ``/api/v2/connections/enqueue-test``
     - ``Public``
     - ``No Airflow permission required``
   * - ``POST``
     - ``/api/v2/connections/test``
     - ``Connection``
     - ``POST``
   * - ``DELETE``
     - ``/api/v2/connections/{connection_id}``
     - ``Connection``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/connections/{connection_id}``
     - ``Connection``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/connections/{connection_id}``
     - ``Connection``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dagSources/{dag_id}``
     - ``DAG.CODE``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dagStats``
     - ``DAG.RUN``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dagTags``
     - ``DAG``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dagWarnings``
     - ``DAG.WARNING``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags``
     - ``DAG``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags``
     - ``DAG``
     - ``PUT``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}``
     - ``DAG``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}``
     - ``DAG``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}``
     - ``DAG``
     - ``PUT``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/assets/queuedEvents``
     - ``Asset``
     - ``DELETE``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/assets/queuedEvents``
     - ``DAG``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/assets/queuedEvents``
     - ``Asset``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/assets/queuedEvents``
     - ``DAG``
     - ``GET``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents``
     - ``Asset``
     - ``DELETE``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents``
     - ``DAG``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents``
     - ``Asset``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents``
     - ``DAG``
     - ``GET``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/clearDagRuns``
     - ``DAG.RUN``
     - ``multi``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/clearPartitions``
     - ``DAG.RUN``
     - ``PUT``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/clearTaskInstances``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns``
     - ``DAG.RUN``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns``
     - ``DAG.RUN``
     - ``multi``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/dagRuns``
     - ``DAG.RUN``
     - ``POST``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/dagRuns/list``
     - ``DAG.RUN``
     - ``GET``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}``
     - ``DAG.RUN``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}``
     - ``DAG.RUN``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}``
     - ``DAG.RUN``
     - ``PUT``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/clear``
     - ``DAG.RUN``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/hitlDetails``
     - ``DAG.HITL_DETAIL``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskGroupInstances/{group_id}``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskGroupInstances/{group_id}/dry_run``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/list``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}``
     - ``DAG.TASK_INSTANCE``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/dependencies``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/dry_run``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/externalLogUrl/{try_number}``
     - ``DAG.TASK_LOGS``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/listMapped``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}``
     - ``DAG.TASK_LOGS``
     - ``GET``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store``
     - ``DAG.TASK_INSTANCE``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store/{key:path}``
     - ``DAG.TASK_INSTANCE``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store/{key:path}``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store/{key:path}``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``PUT``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store/{key:path}``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries/{task_try_number}``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries``
     - ``DAG.XCOM``
     - ``GET``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries``
     - ``DAG.XCOM``
     - ``POST``
   * - ``DELETE``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key:path}``
     - ``DAG.XCOM``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key:path}``
     - ``DAG.XCOM``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key:path}``
     - ``DAG.XCOM``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/dependencies``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/dry_run``
     - ``DAG.TASK_INSTANCE``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/hitlDetails``
     - ``DAG.HITL_DETAIL``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/hitlDetails``
     - ``DAG.HITL_DETAIL``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/hitlDetails/tries/{try_number}``
     - ``DAG.HITL_DETAIL``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/tries``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/tries/{task_try_number}``
     - ``DAG.TASK_INSTANCE``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/upstreamAssetEvents``
     - ``Asset``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/upstreamAssetEvents``
     - ``DAG.RUN``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/wait``
     - ``DAG.RUN``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagVersions``
     - ``DAG.VERSION``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/dagVersions/{version_number}``
     - ``DAG.VERSION``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/details``
     - ``DAG``
     - ``GET``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/favorite``
     - ``DAG``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/tasks``
     - ``DAG.TASK``
     - ``GET``
   * - ``GET``
     - ``/api/v2/dags/{dag_id}/tasks/{task_id}``
     - ``DAG.TASK``
     - ``GET``
   * - ``POST``
     - ``/api/v2/dags/{dag_id}/unfavorite``
     - ``DAG``
     - ``GET``
   * - ``GET``
     - ``/api/v2/eventLogs``
     - ``DAG.AUDIT_LOG``
     - ``GET``
   * - ``GET``
     - ``/api/v2/eventLogs/{event_log_id}``
     - ``DAG.AUDIT_LOG``
     - ``GET``
   * - ``GET``
     - ``/api/v2/importErrors``
     - ``View.IMPORT_ERRORS``
     - ``IMPORT_ERRORS``
   * - ``GET``
     - ``/api/v2/importErrors/{import_error_id}``
     - ``View.IMPORT_ERRORS``
     - ``IMPORT_ERRORS``
   * - ``GET``
     - ``/api/v2/jobs``
     - ``View.JOBS``
     - ``JOBS``
   * - ``GET``
     - ``/api/v2/monitor/health``
     - ``Public``
     - ``No Airflow permission required``
   * - ``PUT``
     - ``/api/v2/parseDagFile/{file_token}``
     - ``DAG``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/plugins``
     - ``View.PLUGINS``
     - ``PLUGINS``
   * - ``GET``
     - ``/api/v2/plugins/importErrors``
     - ``View.PLUGINS``
     - ``PLUGINS``
   * - ``GET``
     - ``/api/v2/pools``
     - ``Pool``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/pools``
     - ``Pool``
     - ``multi``
   * - ``POST``
     - ``/api/v2/pools``
     - ``Pool``
     - ``POST``
   * - ``DELETE``
     - ``/api/v2/pools/{pool_name:path}``
     - ``Pool``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/pools/{pool_name:path}``
     - ``Pool``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/pools/{pool_name:path}``
     - ``Pool``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/providers``
     - ``View.PROVIDERS``
     - ``PROVIDERS``
   * - ``GET``
     - ``/api/v2/variables``
     - ``Variable``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/variables``
     - ``Variable``
     - ``multi``
   * - ``POST``
     - ``/api/v2/variables``
     - ``Variable``
     - ``POST``
   * - ``DELETE``
     - ``/api/v2/variables/{variable_key:path}``
     - ``Variable``
     - ``DELETE``
   * - ``GET``
     - ``/api/v2/variables/{variable_key:path}``
     - ``Variable``
     - ``GET``
   * - ``PATCH``
     - ``/api/v2/variables/{variable_key:path}``
     - ``Variable``
     - ``PUT``
   * - ``GET``
     - ``/api/v2/version``
     - ``Public``
     - ``No Airflow permission required``
