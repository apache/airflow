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
"""Contract test: every REST endpoint this MCP server calls must exist in Airflow's
generated OpenAPI spec, with the HTTP method the server uses.

The server is a thin proxy over Airflow's public REST API (``/api/v2/...``). If Airflow
renames, removes, or changes the method of an endpoint we depend on, our hard-coded paths
would break silently at runtime. This test turns that into a CI failure against the
committed OpenAPI contract instead.

``SERVER_ENDPOINTS`` is maintained by hand alongside the ``call_api(...)`` sites in
``server.py`` -- keep the two in sync when adding or changing a tool. Path-parameter names
are normalized away, so only the URL structure and method are compared (the contract we
actually depend on), not the placeholder spelling.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

# Repo layout: dev/mcp_server/tests/test_openapi_contract.py -> up 3 -> repo root.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_OPENAPI_SPEC = (
    _REPO_ROOT
    / "airflow-core"
    / "src"
    / "airflow"
    / "api_fastapi"
    / "core_api"
    / "openapi"
    / "v2-rest-api-generated.yaml"
)

# (method, path template) for every endpoint server.py drives through call_api().
# Keep in sync with the call_api(...) calls in src/airflow_mcp_server/server.py.
SERVER_ENDPOINTS = [
    ("GET", "/api/v2/version"),
    ("GET", "/api/v2/dags"),
    ("GET", "/api/v2/dags/{dag_id}/details"),
    ("GET", "/api/v2/dagSources/{dag_id}"),
    ("GET", "/api/v2/dags/{dag_id}/dagRuns"),
    ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"),
    ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"),
    ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"),
    ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}"),
    ("GET", "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"),
    ("GET", "/api/v2/importErrors"),
    ("GET", "/api/v2/dagWarnings"),
    ("GET", "/api/v2/variables"),
    ("GET", "/api/v2/variables/{key}"),
    ("GET", "/api/v2/connections"),
    ("GET", "/api/v2/pools"),
    ("POST", "/api/v2/dags/{dag_id}/dagRuns"),
    ("PATCH", "/api/v2/dags/{dag_id}"),
    ("POST", "/api/v2/dags/{dag_id}/clearTaskInstances"),
]

_HTTP_METHODS = {"get", "post", "patch", "put", "delete"}


def _normalize(path: str) -> str:
    """Collapse every ``{placeholder}`` to ``{}`` so only structure and literals matter."""
    return re.sub(r"\{[^}]+\}", "{}", path)


@pytest.fixture(scope="module")
def spec_operations() -> set[tuple[str, str]]:
    if not _OPENAPI_SPEC.is_file():
        pytest.skip(f"OpenAPI spec not found at {_OPENAPI_SPEC} (needs the airflow monorepo checkout)")
    spec = yaml.safe_load(_OPENAPI_SPEC.read_text())
    return {
        (method.upper(), _normalize(path))
        for path, item in spec.get("paths", {}).items()
        for method in item
        if method.lower() in _HTTP_METHODS
    }


@pytest.mark.parametrize(("method", "path"), SERVER_ENDPOINTS, ids=[f"{m} {p}" for m, p in SERVER_ENDPOINTS])
def test_server_endpoint_exists_in_openapi_contract(
    method: str, path: str, spec_operations: set[tuple[str, str]]
):
    assert (method, _normalize(path)) in spec_operations, (
        f"{method} {path} is not in Airflow's OpenAPI contract "
        f"({_OPENAPI_SPEC.name}). The Airflow REST API may have changed; update the MCP "
        f"server (and this list) to match."
    )


def test_server_endpoints_list_covers_all_paths_in_server_source():
    """Guard the hand-maintained SERVER_ENDPOINTS list against drift from server.py.

    Extracts every ``/api/v2/...`` string literal from the server source and asserts each
    (normalized) path template appears in SERVER_ENDPOINTS -- so adding a tool without
    extending the list fails here instead of silently escaping contract coverage.
    """
    import airflow_mcp_server.server as server_module

    source = Path(server_module.__file__).read_text()
    # Only double-quoted literals: real call paths are ruff-formatted to double quotes, while
    # path-shaped examples in docstrings/Field descriptions are single-quoted prose.
    source_paths = {_normalize(match) for match in re.findall(r'"(/api/v2/[^"]*)"', source)}
    covered_paths = {_normalize(path) for _, path in SERVER_ENDPOINTS}
    uncovered = source_paths - covered_paths
    assert not uncovered, (
        f"server.py calls paths missing from SERVER_ENDPOINTS (add them with their method): "
        f"{sorted(uncovered)}"
    )
