# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


@pytest.fixture
def all_access_test_client():
    with conf_vars(
        {
            ("core", "simple_auth_manager_all_admins"): "true",
            ("webserver", "expose_config"): "true",
        }
    ):
        app = create_app()
        yield TestClient(app)


@pytest.mark.parametrize(
    "method, path",
    [
        ("GET", "/api/v1/{_:path}"),
        ("GET", "/api/v2/assets"),
        ("GET", "/api/v2/assets/aliases"),
        ("GET", "/api/v2/assets/aliases/{asset_alias_id}"),
        ("GET", "/api/v2/assets/events"),
        ("POST", "/api/v2/assets/events"),
        ("GET", "/api/v2/assets/{asset_id}"),
        ("POST", "/api/v2/assets/{asset_id}/materialize"),
        ("GET", "/api/v2/assets/{asset_id}/queuedEvents"),
        ("DELETE", "/api/v2/assets/{asset_id}/queuedEvents"),
        ("GET", "/api/v2/backfills"),
        ("POST", "/api/v2/backfills"),
        ("POST", "/api/v2/backfills/dry_run"),
        ("GET", "/api/v2/backfills/{backfill_id}"),
        ("PUT", "/api/v2/backfills/{backfill_id}/cancel"),
        ("PUT", "/api/v2/backfills/{backfill_id}/pause"),
        ("PUT", "/api/v2/backfills/{backfill_id}/unpause"),
        ("GET", "/api/v2/config"),
        ("GET", "/api/v2/config/section/{section}/option/{option}"),
        ("GET", "/api/v2/connections"),
        ("POST", "/api/v2/connections"),
        ("PATCH", "/api/v2/connections"),
        ("POST", "/api/v2/connections/defaults"),
        ("POST", "/api/v2/connections/test"),
        ("DELETE", "/api/v2/connections/{connection_id}"),
        ("GET", "/api/v2/connections/{connection_id}"),
        ("PATCH", "/api/v2/connections/{connection_id}"),
        ("GET", "/api/v2/dagReports"),
        ("GET", "/api/v2/dagSources/{dag_id}"),
        ("GET", "/api/v2/dagStats"),
        ("GET", "/api/v2/dagTags"),
        ("GET", "/api/v2/dagWarnings"),
        ("GET", "/api/v2/dags"),
        ("PATCH", "/api/v2/dags"),
        ("GET", "/api/v2/dags/{dag_id}"),
        ("PATCH", "/api/v2/dags/{dag_id}"),
        ("DELETE", "/api/v2/dags/{dag_id}"),
        ("GET", "/api/v2/dags/{dag_id}/assets/queuedEvents"),
        ("DELETE", "/api/v2/dags/{dag_id}/assets/queuedEvents"),
        ("GET", "/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents"),
        ("DELETE", "/api/v2/dags/{dag_id}/assets/{asset_id}/queuedEvents"),
        ("POST", "/api/v2/dags/{dag_id}/clearTaskInstances"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns"),
        ("POST", "/api/v2/dags/{dag_id}/dagRuns"),
        ("POST", "/api/v2/dags/{dag_id}/dagRuns/list"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"),
        ("DELETE", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"),
        ("PATCH", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"),
        ("POST", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/clear"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"),
        ("POST", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/list"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"),
        ("PATCH", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/dependencies"),
        ("PATCH", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/dry_run"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/listMapped"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries/{task_try_number}"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries"),
        ("POST", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}"),
        (
            "PATCH",
            "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}",
        ),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}"),
        ("PATCH", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}"),
        (
            "GET",
            "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/dependencies",
        ),
        ("PATCH", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/dry_run"),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/tries"),
        (
            "GET",
            "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/tries/{task_try_number}",
        ),
        ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/upstreamAssetEvents"),
        ("GET", "/api/v2/dags/{dag_id}/dagVersions"),
        ("GET", "/api/v2/dags/{dag_id}/dagVersions/{version_number}"),
        ("GET", "/api/v2/dags/{dag_id}/details"),
        ("GET", "/api/v2/dags/{dag_id}/tasks"),
        ("GET", "/api/v2/dags/{dag_id}/tasks/{task_id}"),
        ("GET", "/api/v2/eventLogs"),
        ("GET", "/api/v2/eventLogs/{event_log_id}"),
        ("GET", "/api/v2/importErrors"),
        ("GET", "/api/v2/importErrors/{import_error_id}"),
        ("GET", "/api/v2/jobs"),
        ("GET", "/api/v2/monitor/health"),
        ("PUT", "/api/v2/parseDagFile/{file_token}"),
        ("GET", "/api/v2/plugins"),
        ("GET", "/api/v2/plugins/importErrors"),
        ("GET", "/api/v2/pools"),
        ("POST", "/api/v2/pools"),
        ("PATCH", "/api/v2/pools"),
        ("DELETE", "/api/v2/pools/{pool_name}"),
        ("GET", "/api/v2/pools/{pool_name}"),
        ("PATCH", "/api/v2/pools/{pool_name}"),
        ("GET", "/api/v2/providers"),
        ("GET", "/api/v2/variables"),
        ("POST", "/api/v2/variables"),
        ("PATCH", "/api/v2/variables"),
        ("DELETE", "/api/v2/variables/{variable_key}"),
        ("GET", "/api/v2/variables/{variable_key}"),
        ("PATCH", "/api/v2/variables/{variable_key}"),
        ("GET", "/api/v2/version"),
        ("GET", "/api/{_:path}"),
        ("HEAD", "/docs/oauth2-redirect"),
        ("GET", "/docs/oauth2-redirect"),
        ("GET", "/ui/auth/menus"),
        ("GET", "/ui/backfills"),
        ("GET", "/ui/config"),
        ("GET", "/ui/connections/hook_meta"),
        ("GET", "/ui/dags/recent_dag_runs"),
        ("GET", "/ui/dashboard/historical_metrics_data"),
        ("GET", "/ui/dependencies"),
        ("GET", "/ui/grid/{dag_id}"),
        ("GET", "/ui/next_run_assets/{dag_id}"),
        ("GET", "/ui/structure/structure_data"),
    ],
)
def test_all_endpoints_without_auth_header(all_access_test_client, method, path):
    response = all_access_test_client.request(method, path)
    assert response.status_code not in {401, 403}, (
        f"Unexpected status code {response.status_code} for {method} {path}"
    )
