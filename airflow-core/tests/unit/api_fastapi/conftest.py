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

import datetime
import os
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import time_machine
from fastapi import FastAPI
from fastapi.routing import Mount
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.models import Connection
from airflow.providers.git.bundles.git import GitDagBundle
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections, parse_and_sync_to_db

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager

API_PATHS = {
    "public": "/api/v2",
    "ui": "/ui",
}

BASE_URL = "http://testserver"


def get_api_path(request):
    """Determine the API path based on the test's subdirectory."""
    test_dir = os.path.dirname(request.path)
    subdirectory_name = test_dir.split("/")[-1]

    return API_PATHS.get(subdirectory_name, "/")


@pytest.fixture(scope="session")
def _shared_api_app():
    """
    Build the FastAPI app once per test session.

    ``create_app()`` rebuilds two full FastAPI apps (core + execution), registers every route and
    builds the OpenAPI schema -- ~0.5s. The default ``test_client`` always uses the same config
    (SimpleAuthManager), so the app structure is identical across tests; only per-test DB state and
    request data differ. Building it once and reusing it removes that per-test rebuild cost.
    """
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
        }
    ):
        return create_app()


def _mounted_fastapi_apps(app: FastAPI) -> list[FastAPI]:
    """Return ``app`` and every FastAPI app mounted under it, recursively (``/execution``, ``/auth``, ...)."""
    apps = [app]
    for route in app.routes:
        if isinstance(route, Mount) and isinstance(route.app, FastAPI):
            apps.extend(_mounted_fastapi_apps(route.app))
    return apps


@pytest.fixture
def _isolated_shared_app(_shared_api_app):
    """
    Yield the session-shared app with its mutable state snapshotted and restored around each test.

    The app is built once per session, so a test that rebinds something on ``app.state`` (the auth
    endpoint tests swap ``auth_manager`` for a mock) or installs a ``dependency_overrides`` entry
    (extra-links/tasks/logs install a ``dag_bag_from_app`` override) would leak into later tests.
    Snapshotting ``state`` and ``dependency_overrides`` on the root app and every mounted sub-app on
    entry and restoring them on exit keeps the reset resilient to future mutations without having to
    enumerate them.

    ``app.state.dag_bag`` is the exception: tests mutate the DagBag object *in place* (its cache of
    deserialized Dags fills as requests resolve them), which a state snapshot can't undo, so its
    cache is cleared explicitly. A leaked warm entry would otherwise let a later test skip a
    serialized-Dag DB read and break query-count assertions (e.g. the grid ``ti_summaries`` stream
    tests) depending on execution order.
    """
    apps = _mounted_fastapi_apps(_shared_api_app)
    # ``app.state._state`` is Starlette's backing dict for ``State`` -- the only way to enumerate it.
    saved = [(app, dict(app.state._state), dict(app.dependency_overrides)) for app in apps]
    _shared_api_app.state.dag_bag.clear_cache()
    try:
        yield _shared_api_app
    finally:
        for app, state, overrides in saved:
            app.state._state.clear()
            app.state._state.update(state)
            app.dependency_overrides.clear()
            app.dependency_overrides.update(overrides)


def _authed_test_client(app: FastAPI, request):
    auth_manager: SimpleAuthManager = app.state.auth_manager
    # set time_very_before to 2014-01-01 00:00:00 and time_very_after to tomorrow
    # to make the JWT token always valid for all test cases with time_machine
    time_very_before = datetime.datetime(2014, 1, 1, 0, 0, 0)
    time_after = datetime.datetime.now() + datetime.timedelta(days=1)
    with time_machine.travel(time_very_before, tick=False):
        token = auth_manager._get_token_signer(
            expiration_time_in_seconds=(time_after - time_very_before).total_seconds()
        ).generate(
            auth_manager.serialize_user(
                SimpleAuthManagerUser(username="test", role="admin", teams=["team1"])
            ),
        )
    with mock.patch("airflow.models.revoked_token.RevokedToken.is_revoked", return_value=False):
        yield TestClient(
            app,
            headers={"Authorization": f"Bearer {token}"},
            base_url=f"{BASE_URL}{get_api_path(request)}",
        )


@pytest.fixture
def test_client(request, _isolated_shared_app):
    yield from _authed_test_client(_isolated_shared_app, request)


@pytest.fixture
def fresh_test_client(request):
    """
    Like ``test_client`` but backed by a freshly built app instead of the session-shared one.

    For the rare tests that patch app construction (e.g. counting ``DBDagBag`` instantiation) and
    so need the app built *after* their patch is applied.
    """
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
        }
    ):
        app = create_app()
    yield from _authed_test_client(app, request)


@pytest.fixture
def unauthenticated_test_client(request, _isolated_shared_app):
    return TestClient(_isolated_shared_app, base_url=f"{BASE_URL}{get_api_path(request)}")


@pytest.fixture
def unauthorized_test_client(request, _isolated_shared_app):
    app = _isolated_shared_app
    auth_manager: SimpleAuthManager = app.state.auth_manager
    token = auth_manager._get_token_signer().generate(
        auth_manager.serialize_user(SimpleAuthManagerUser(username="dummy", role=None))
    )
    with mock.patch("airflow.models.revoked_token.RevokedToken.is_revoked", return_value=False):
        yield TestClient(
            app,
            headers={"Authorization": f"Bearer {token}"},
            base_url=f"{BASE_URL}{get_api_path(request)}",
        )


@pytest.fixture
def client(request):
    """This fixture is more flexible than test_client, as it allows to specify which apps to include."""

    def create_test_client(apps="all"):
        app = create_app(apps=apps)
        return TestClient(app, base_url=f"{BASE_URL}{get_api_path(request)}")

    return create_test_client


@pytest.fixture
def configure_git_connection_for_dag_bundle(session):
    clear_db_connections(False)
    # Git connection is required for the bundles to have a url.
    connection = Connection(
        conn_id="git_default",
        conn_type="git",
        description="default git connection",
        host="http://test_host.github.com",
        port=8081,
        login="",
    )
    session.add(connection)
    with (
        conf_vars(
            {
                (
                    "dag_processor",
                    "dag_bundle_config_list",
                ): '[{ "name": "dag_maker", "classpath": "airflow.providers.git.bundles.git.GitDagBundle", "kwargs": {"subdir": "dags", "tracking_ref": "main", "refresh_interval": 0}}, { "name": "another_bundle_name", "classpath": "airflow.providers.git.bundles.git.GitDagBundle", "kwargs": {"subdir": "dags", "tracking_ref": "main", "refresh_interval": 0}}]'
            }
        ),
        mock.patch("airflow.providers.git.bundles.git.GitHook") as mock_git_hook,
        mock.patch.object(GitDagBundle, "get_current_version") as mock_get_current_version,
    ):
        mock_get_current_version.return_value = "some_commit_hash"
        mock_git_hook.return_value.repo_url = connection.host
        DagBundlesManager().sync_bundles_to_db()
        yield
    # in case no flush or commit was executed after the "session.add" above, we need to flush the session
    # manually here to make sure that the added connection will be deleted by query(Connection).delete()
    # in the`clear_db_connections` function below
    session.flush()
    clear_db_connections(False)


@pytest.fixture
def make_dag_with_multiple_versions(dag_maker, configure_git_connection_for_dag_bundle, session):
    """
    Create DAG with multiple versions

    Version 1 will have 1 task, version 2 will have 2 tasks, and version 3 will have 3 tasks.

    Configure the associated dag_bundles.
    """
    dag_id = "dag_with_multiple_versions"
    for version_number in range(1, 4):
        with dag_maker(dag_id, session=session, bundle_version=f"some_commit_hash{version_number}"):
            for task_number in range(version_number):
                EmptyOperator(task_id=f"task{task_number + 1}")
        dag_maker.create_dagrun(
            run_id=f"run{version_number}",
            logical_date=datetime.datetime(2020, 1, version_number, tzinfo=datetime.timezone.utc),
            session=session,
        )
        session.commit()


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models.dagbag import DBDagBag

    with conf_vars({("core", "load_examples"): "True"}):
        parse_and_sync_to_db(os.devnull)
    return DBDagBag()


@pytest.fixture
def get_execution_app():
    def _get_execution_app(test_client):
        test_app = test_client.app
        for route in test_app.router.routes:
            if route.path == "/execution":
                return route.app
        raise RuntimeError("Execution app not found at /execution")

    return _get_execution_app
