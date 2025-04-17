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

import pytest
import time_machine
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.models import Connection
from airflow.models.serialized_dag import SerializedDagModel
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


@pytest.fixture
def test_client(request):
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
        }
    ):
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager
        # set time_very_before to 2014-01-01 00:00:00 and time_very_after to tomorrow
        # to make the JWT token always valid for all test cases with time_machine
        time_very_before = datetime.datetime(2014, 1, 1, 0, 0, 0)
        time_after = datetime.datetime.now() + datetime.timedelta(days=1)
        with time_machine.travel(time_very_before, tick=False):
            token = auth_manager._get_token_signer(
                expiration_time_in_seconds=(time_after - time_very_before).total_seconds()
            ).generate(
                auth_manager.serialize_user(SimpleAuthManagerUser(username="test", role="admin")),
            )
        yield TestClient(
            app, headers={"Authorization": f"Bearer {token}"}, base_url=f"{BASE_URL}{get_api_path(request)}"
        )


@pytest.fixture
def unauthenticated_test_client(request):
    return TestClient(create_app(), base_url=f"{BASE_URL}{get_api_path(request)}")


@pytest.fixture
def unauthorized_test_client(request):
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
        }
    ):
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager
        token = auth_manager._get_token_signer().generate(
            auth_manager.serialize_user(SimpleAuthManagerUser(username="dummy", role=None))
        )
        yield TestClient(
            app, headers={"Authorization": f"Bearer {token}"}, base_url=f"{BASE_URL}{get_api_path(request)}"
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
    # Git connection is required for the bundles to have a url.
    connection = Connection(
        conn_id="git_default",
        conn_type="git",
        description="default git connection",
        host="fakeprotocol://test_host.github.com",
        port=8081,
        login="",
    )
    session.add(connection)
    with conf_vars(
        {
            (
                "dag_processor",
                "dag_bundle_config_list",
            ): '[{ "name": "dag_maker", "classpath": "airflow.providers.git.bundles.git.GitDagBundle", "kwargs": {"subdir": "dags", "tracking_ref": "main", "refresh_interval": 0}}, { "name": "another_bundle_name", "classpath": "airflow.providers.git.bundles.git.GitDagBundle", "kwargs": {"subdir": "dags", "tracking_ref": "main", "refresh_interval": 0}}]'
        }
    ):
        yield

    clear_db_connections(False)


@pytest.fixture
def make_dag_with_multiple_versions(dag_maker, configure_git_connection_for_dag_bundle):
    """
    Create DAG with multiple versions

    Version 1 will have 1 task, version 2 will have 2 tasks, and version 3 will have 3 tasks.

    Configure the associated dag_bundles.
    """
    dag_id = "dag_with_multiple_versions"
    for version_number in range(1, 4):
        with dag_maker(dag_id) as dag:
            for task_number in range(version_number):
                EmptyOperator(task_id=f"task{task_number + 1}")
        SerializedDagModel.write_dag(
            dag, bundle_name="dag_maker", bundle_version=f"some_commit_hash{version_number}"
        )
        dag_maker.create_dagrun(
            run_id=f"run{version_number}",
            logical_date=datetime.datetime(2020, 1, version_number, tzinfo=datetime.timezone.utc),
        )
        dag.sync_to_db()


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    parse_and_sync_to_db(os.devnull, include_examples=True)
    return DagBag(read_dags_from_db=True)
