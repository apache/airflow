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

import ast
import contextlib
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from airflow.models.dag import clear_team_name_cache

from tests_common.test_utils.api_fastapi import iter_api_routes
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_backfills,
    clear_db_connections,
    clear_db_logs,
    clear_db_pools,
    clear_db_variables,
)
from tests_common.test_utils.paths import AIRFLOW_CORE_ROOT_PATH, AIRFLOW_ROOT_PATH

KNOWN_SYNC_DB_FILE = AIRFLOW_ROOT_PATH / "generated" / "known_sync_db_on_event_loop.txt"
KNOWN_OFFLOADS_FILE = AIRFLOW_ROOT_PATH / "generated" / "known_api_server_threadpool_offloads.txt"
API_FASTAPI_PATH = AIRFLOW_CORE_ROOT_PATH / "src" / "airflow" / "api_fastapi"

MODES = ("default", "multi_team")

# The auth manager interface is sync, so these calls cannot be awaited directly.
ALLOWED_OFFLOADS = frozenset(
    {
        "airflow-core/src/airflow/api_fastapi/auth/middlewares/refresh_token.py::JWTRefreshMiddleware.refresh_user",
        "airflow-core/src/airflow/api_fastapi/auth/middlewares/team_authorization.py::TeamAuthorizationMiddleware.dispatch",
    }
)
OFFLOAD_NAMES = frozenset({"run_in_threadpool", "run_in_executor", "to_thread"})
OFFLOAD_IMPORTS = frozenset(
    {
        ("starlette.concurrency", "run_in_threadpool"),
        ("fastapi.concurrency", "run_in_threadpool"),
        ("anyio.to_thread", "run_sync"),
        ("asyncio", "to_thread"),
    }
)


def _read_known(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


KNOWN_SYNC_DB = _read_known(KNOWN_SYNC_DB_FILE)
KNOWN_OFFLOADS = _read_known(KNOWN_OFFLOADS_FILE)


def _build_xfail_params(entries: list[str], reason: str) -> list:
    """Make each known entry a strict xfail, so fixing it fails the test until the line is removed."""
    return [
        pytest.param(
            entry, marks=pytest.mark.xfail(strict=True, raises=AssertionError, reason=reason), id=entry
        )
        for entry in entries
    ]


def _list_routes(app) -> list[tuple[str, str]]:
    return sorted(
        (method, route.path)
        for route in iter_api_routes(app.routes)
        for method in (route.methods or set()) - {"HEAD", "OPTIONS"}
    )


def _send_request(client: TestClient, queries: list, method: str, path: str, mode: str) -> list[str]:
    """Send one request and return the sorted, unique sites that ran sync SQL on the event loop."""
    # DagModel.get_team_name is cached, which would hide its query from every route after the first.
    clear_team_name_cache()
    queries.clear()
    url = re.sub(r"\{[^}]+\}", "1", path)
    body: dict | None = {} if method in {"POST", "PUT", "PATCH"} else None
    with conf_vars({("core", "multi_team"): "True" if mode == "multi_team" else "False"}):
        # The client re-raises server errors (anyio may group them). The queries recorded before the
        # error are all we need.
        with contextlib.suppress(BaseException):
            client.request(method, url, json=body)
    return sorted({query.site for query in queries})


@pytest.mark.db_test
class TestNoSyncDbOnEventLoop:
    """
    No route may run sync DB statements on the event loop.

    A blocked loop cannot release the connections held by sync endpoints, which stalled the whole API
    server in https://github.com/apache/airflow/issues/76417. Each root-app route is requested in both
    modes with ``"1"`` for every path parameter and ``{}`` as the body, so a route that rejects that
    input before its DB call is not covered. Mounted sub-apps such as ``/execution`` reject the UI
    token before any dependency runs, so they are skipped.

    Known offenders are strict xfails listed in ``generated/known_sync_db_on_event_loop.txt``.
    """

    def teardown_method(self):
        clear_db_logs()
        clear_db_connections()
        clear_db_variables()
        clear_db_pools()
        clear_db_backfills()

    @pytest.mark.parametrize("mode", MODES)
    def test_unlisted_routes_do_not_block_the_event_loop(self, test_client, event_loop_queries, mode):
        routes = _list_routes(test_client.app)
        assert routes, "found no routes, so this would pass vacuously"
        known = set(KNOWN_SYNC_DB)

        offenders = []
        for method, path in routes:
            entry = f"{method} {path}::{mode}"
            if entry in known:
                continue
            if sites := _send_request(test_client, event_loop_queries, method, path, mode):
                offenders.append("\n    ".join([entry, *sites]))

        if offenders:
            pytest.fail(
                "These routes ran sync DB statements on the event loop. Fix them, or paste the unindented "
                "lines into generated/known_sync_db_on_event_loop.txt:\n" + "\n".join(offenders),
                pytrace=False,
            )

    @pytest.mark.parametrize(
        "entry",
        _build_xfail_params(
            KNOWN_SYNC_DB,
            "known to run sync DB statements on the event loop; delete the line from "
            "generated/known_sync_db_on_event_loop.txt once fixed or the route is gone",
        ),
    )
    def test_known_route_no_longer_blocks(self, test_client, event_loop_queries, entry):
        route, mode = entry.rsplit("::", 1)
        method, path = route.split(" ", 1)
        assert not _send_request(test_client, event_loop_queries, method, path, mode)


class _OffloadFinder(ast.NodeVisitor):
    """Collect ``<file>::<Qualified.function>`` of every call that hands work to a thread."""

    def __init__(self, relative_path: str):
        self.relative_path = relative_path
        self.scope: list[str] = []
        self.sites: set[str] = set()
        self.offload_names = set(OFFLOAD_NAMES)

    def _visit_scope(self, node):
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    visit_ClassDef = visit_FunctionDef = visit_AsyncFunctionDef = _visit_scope

    def visit_ImportFrom(self, node: ast.ImportFrom):
        for alias in node.names:
            if (node.module, alias.name) in OFFLOAD_IMPORTS:
                self.offload_names.add(alias.asname or alias.name)

    def visit_Call(self, node: ast.Call):
        func = node.func
        if isinstance(func, ast.Name):
            is_offload = func.id in self.offload_names
        elif isinstance(func, ast.Attribute):
            is_offload = func.attr in OFFLOAD_NAMES or ast.unparse(func).endswith("to_thread.run_sync")
        else:
            is_offload = False
        if is_offload:
            self.sites.add(f"{self.relative_path}::{'.'.join(self.scope) or '<module>'}")
        self.generic_visit(node)


def _find_offloads() -> set[str]:
    sites: set[str] = set()
    for path in sorted(API_FASTAPI_PATH.rglob("*.py")):
        finder = _OffloadFinder(path.relative_to(AIRFLOW_ROOT_PATH).as_posix())
        finder.visit(ast.parse(path.read_text()))
        sites |= finder.sites
    return sites


class TestNoThreadpoolOffload:
    """
    The API server must not hide sync DB work behind an explicit threadpool offload.

    Offloading keeps the event loop free, but each call needs a threadpool worker that the sync
    endpoints also need, so under load it waits behind them. Prefer an async DB call. Known sites are
    listed in ``generated/known_api_server_threadpool_offloads.txt``.
    """

    def test_no_unlisted_offload(self):
        unexpected = sorted(_find_offloads() - ALLOWED_OFFLOADS - set(KNOWN_OFFLOADS))
        assert not unexpected, (
            "These functions offload work to a thread. Use an async call instead, or paste the lines "
            "into generated/known_api_server_threadpool_offloads.txt:\n" + "\n".join(unexpected)
        )

    def test_allowed_offloads_still_exist(self):
        stale = sorted(ALLOWED_OFFLOADS - _find_offloads())
        assert not stale, "These ALLOWED_OFFLOADS no longer offload, remove them:\n" + "\n".join(stale)

    @pytest.mark.parametrize(
        "site",
        _build_xfail_params(
            KNOWN_OFFLOADS,
            "known to offload to a thread; delete the line from "
            "generated/known_api_server_threadpool_offloads.txt once fixed or the function is gone",
        ),
    )
    def test_known_site_no_longer_offloads(self, site):
        assert site not in _find_offloads()
