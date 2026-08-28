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
   Regenerate with:  python scripts/ci/prek/fab_permissions_doc.py
   Trigger:          prek run generate-fab-permissions-doc --all-files

.. list-table:: Stable REST API permissions (FAB auth manager)
   :header-rows: 1
   :widths: 45 8 32 15

   * - Endpoint
     - Method
     - Permissions
     - Minimum role
   * - ``/api/v2/assets``
     - GET
     - Asset Aliases.can_read
     - Viewer
   * - ``/api/v2/assets``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/assets/aliases``
     - GET
     - Asset Aliases.can_read
     - Viewer
   * - ``/api/v2/assets/aliases/{asset_alias_id}``
     - GET
     - Asset Aliases.can_read
     - Viewer
   * - ``/api/v2/assets/events``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/assets/events``
     - POST
     - Assets.can_create
     - User
   * - ``/api/v2/assets/{asset_id}``
     - GET
     - Asset Aliases.can_read
     - Viewer
   * - ``/api/v2/assets/{asset_id}``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/assets/{asset_id}/materialize``
     - POST
     - Assets.can_create
     - User
   * - ``/api/v2/assets/{asset_id}/queuedEvents``
     - DELETE
     - Assets.can_delete
     - Op
   * - ``/api/v2/assets/{asset_id}/queuedEvents``
     - DELETE
     - DAGs.can_edit
     - User
   * - ``/api/v2/assets/{asset_id}/queuedEvents``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/assets/{asset_id}/state-store``
     - DELETE
     - Assets.can_delete
     - Op
   * - ``/api/v2/assets/{asset_id}/state-store``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/assets/{asset_id}/state-store/{key:path}``
     - DELETE
     - Assets.can_delete
     - Op
   * - ``/api/v2/assets/{asset_id}/state-store/{key:path}``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/assets/{asset_id}/state-store/{key:path}``
     - PUT
     - Assets.can_edit
     - Op
   * - ``/api/v2/auth/login``
     - GET
     - None
     - Public
   * - ``/api/v2/auth/logout``
     - GET
     - None
     - Public
   * - ``/api/v2/backfills``
     - GET
     - DAGs.can_read, DAG Runs.can_read
     - Viewer
   * - ``/api/v2/backfills``
     - POST
     - DAGs.can_edit, DAG Runs.can_create
     - User
   * - ``/api/v2/backfills``
     - PUT
     - DAGs.can_edit, DAG Runs.can_edit
     - User
   * - ``/api/v2/config``
     - GET
     - Configurations.can_read
     - Op
   * - ``/api/v2/config/section/{section}/option/{option}``
     - GET
     - Configurations.can_read
     - Op
   * - ``/api/v2/connections``
     - GET
     - Connections.can_read
     - Op
   * - ``/api/v2/connections``
     - PATCH
     - Connections.can_read
     - Op
   * - ``/api/v2/connections``
     - POST
     - Connections.can_create
     - Op
   * - ``/api/v2/connections/defaults``
     - POST
     - Connections.can_create
     - Op
   * - ``/api/v2/connections/enqueue-test``
     - GET
     - None
     - Public
   * - ``/api/v2/connections/enqueue-test``
     - POST
     - None
     - Public
   * - ``/api/v2/connections/test``
     - POST
     - Connections.can_create
     - Op
   * - ``/api/v2/connections/{connection_id}``
     - DELETE
     - Connections.can_delete
     - Op
   * - ``/api/v2/connections/{connection_id}``
     - GET
     - Connections.can_read
     - Op
   * - ``/api/v2/connections/{connection_id}``
     - PATCH
     - Connections.can_edit
     - Op
   * - ``/api/v2/dagSources/{dag_id}``
     - GET
     - DAGs.can_read, DAG Code.can_read
     - Viewer
   * - ``/api/v2/dagStats``
     - GET
     - DAGs.can_read, DAG Runs.can_read
     - Viewer
   * - ``/api/v2/dagTags``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dagWarnings``
     - GET
     - DAGs.can_read, DAG Warnings.can_read
     - Viewer
   * - ``/api/v2/dags``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags``
     - PATCH
     - DAGs.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}``
     - DELETE
     - DAGs.can_delete
     - User
   * - ``/api/v2/dags/{dag_id}``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}``
     - PATCH
     - DAGs.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/assets/queuedEvents``
     - DELETE
     - Assets.can_delete
     - Op
   * - ``/api/v2/dags/{dag_id}/assets/queuedEvents``
     - DELETE
     - DAGs.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/assets/queuedEvents``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/assets/queuedEvents``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents``
     - DELETE
     - Assets.can_delete
     - Op
   * - ``/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents``
     - DELETE
     - DAGs.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/clearDagRuns``
     - POST
     - DAGs.can_edit, DAG Runs.can_read
     - User
   * - ``/api/v2/dags/{dag_id}/clearPartitions``
     - POST
     - DAGs.can_edit, DAG Runs.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/clearTaskInstances``
     - POST
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns``
     - GET
     - DAGs.can_read, DAG Runs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_read
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns``
     - POST
     - DAGs.can_edit, DAG Runs.can_create
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/list``
     - POST
     - DAGs.can_read, DAG Runs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}``
     - DELETE
     - DAGs.can_edit, DAG Runs.can_delete
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}``
     - GET
     - DAGs.can_read, DAG Runs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/clear``
     - POST
     - DAGs.can_edit, DAG Runs.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/hitlDetails``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskGroupInstances/{group_id}``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskGroupInstances/{group_id}/dry_run``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/list``
     - POST
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}``
     - DELETE
     - DAGs.can_edit, DAG Runs.can_delete, Task Instances.can_delete
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/dependencies``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/dry_run``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/externalLogUrl/{try_number}``
     - GET
     - DAGs.can_read, Task Logs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/listMapped``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}``
     - GET
     - DAGs.can_read, Task Logs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store``
     - DELETE
     - DAGs.can_edit, DAG Runs.can_delete, Task Instances.can_delete
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store/{key:path}``
     - DELETE
     - DAGs.can_edit, DAG Runs.can_delete, Task Instances.can_delete
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store/{key:path}``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store/{key:path}``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/state-store/{key:path}``
     - PUT
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries/{task_try_number}``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries``
     - GET
     - DAGs.can_read, XComs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries``
     - POST
     - DAGs.can_edit, XComs.can_create
     - Op
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key:path}``
     - DELETE
     - DAGs.can_edit, XComs.can_delete
     - Op
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key:path}``
     - GET
     - DAGs.can_read, XComs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key:path}``
     - PATCH
     - DAGs.can_edit, XComs.can_edit
     - Op
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/dependencies``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/dry_run``
     - PATCH
     - DAGs.can_edit, DAG Runs.can_edit, Task Instances.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/hitlDetails``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/hitlDetails``
     - PATCH
     - DAGs.can_edit
     - User
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/hitlDetails/tries/{try_number}``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/tries``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/tries/{task_try_number}``
     - GET
     - DAGs.can_read, DAG Runs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/upstreamAssetEvents``
     - GET
     - Assets.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/upstreamAssetEvents``
     - GET
     - DAGs.can_read, DAG Runs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/wait``
     - GET
     - DAGs.can_read, DAG Runs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagVersions``
     - GET
     - DAGs.can_read, DAG Versions.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/dagVersions/{version_number}``
     - GET
     - DAGs.can_read, DAG Versions.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/details``
     - GET
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/favorite``
     - POST
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/tasks``
     - GET
     - DAGs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/tasks/{task_id}``
     - GET
     - DAGs.can_read, Task Instances.can_read
     - Viewer
   * - ``/api/v2/dags/{dag_id}/unfavorite``
     - POST
     - DAGs.can_read
     - Viewer
   * - ``/api/v2/eventLogs``
     - GET
     - DAGs.can_read, Audit Logs.can_read
     - Admin
   * - ``/api/v2/eventLogs/{event_log_id}``
     - GET
     - DAGs.can_read, Audit Logs.can_read
     - Admin
   * - ``/api/v2/importErrors``
     - GET
     - ImportError.can_read
     - Viewer
   * - ``/api/v2/importErrors/{import_error_id}``
     - GET
     - ImportError.can_read
     - Viewer
   * - ``/api/v2/jobs``
     - GET
     - Jobs.can_read
     - Viewer
   * - ``/api/v2/monitor/health``
     - GET
     - None
     - Public
   * - ``/api/v2/parseDagFile/{file_token}``
     - PUT
     - DAGs.can_edit
     - User
   * - ``/api/v2/plugins``
     - GET
     - Plugins.can_read
     - Op
   * - ``/api/v2/plugins/importErrors``
     - GET
     - Plugins.can_read
     - Op
   * - ``/api/v2/pools``
     - GET
     - Pools.can_read
     - Viewer
   * - ``/api/v2/pools``
     - PATCH
     - Pools.can_read
     - Viewer
   * - ``/api/v2/pools``
     - POST
     - Pools.can_create
     - Op
   * - ``/api/v2/pools/{pool_name:path}``
     - DELETE
     - Pools.can_delete
     - Op
   * - ``/api/v2/pools/{pool_name:path}``
     - GET
     - Pools.can_read
     - Viewer
   * - ``/api/v2/pools/{pool_name:path}``
     - PATCH
     - Pools.can_edit
     - Op
   * - ``/api/v2/providers``
     - GET
     - Providers.can_read
     - Op
   * - ``/api/v2/variables``
     - GET
     - Variables.can_read
     - Op
   * - ``/api/v2/variables``
     - PATCH
     - Variables.can_read
     - Op
   * - ``/api/v2/variables``
     - POST
     - Variables.can_create
     - Op
   * - ``/api/v2/variables/{variable_key:path}``
     - DELETE
     - Variables.can_delete
     - Op
   * - ``/api/v2/variables/{variable_key:path}``
     - GET
     - Variables.can_read
     - Op
   * - ``/api/v2/variables/{variable_key:path}``
     - PATCH
     - Variables.can_edit
     - Op
   * - ``/api/v2/version``
     - GET
     - None
     - Public
   * - ``/fab/v1/permissions``
     - GET
     - Roles.can_read
     - Admin
   * - ``/fab/v1/roles``
     - GET
     - Roles.can_read
     - Admin
   * - ``/fab/v1/roles``
     - POST
     - Roles.can_create
     - Admin
   * - ``/fab/v1/roles/{name}``
     - DELETE
     - Roles.can_delete
     - Admin
   * - ``/fab/v1/roles/{name}``
     - GET
     - Roles.can_read
     - Admin
   * - ``/fab/v1/roles/{name}``
     - PATCH
     - Roles.can_edit
     - Admin
   * - ``/fab/v1/users``
     - GET
     - Users.can_read
     - Admin
   * - ``/fab/v1/users``
     - POST
     - Users.can_create
     - Admin
   * - ``/fab/v1/users/{username}``
     - DELETE
     - Users.can_delete
     - Admin
   * - ``/fab/v1/users/{username}``
     - GET
     - Users.can_read
     - Admin
   * - ``/fab/v1/users/{username}``
     - PATCH
     - Users.can_edit
     - Admin
