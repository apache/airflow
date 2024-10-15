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
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS, ParseImportError
from tests_common.test_utils.db import clear_db_dags, clear_db_import_errors
from tests_common.test_utils.permissions import _resource_name

from airflow.models.dag import DagModel
from airflow.security import permissions
from airflow.utils import timezone

from providers.tests.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skip_if_database_isolation_mode,
    pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+"),
]

TEST_DAG_IDS = ["test_dag", "test_dag2"]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api
    create_user(
        app,
        username="test_single_dag",
        role_name="TestSingleDAG",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR)],
    )
    # For some reason, DAG level permissions are not synced when in the above list of perms,
    # so do it manually here:
    app.appbuilder.sm.bulk_sync_roles(
        [
            {
                "role": "TestSingleDAG",
                "perms": [
                    (
                        permissions.ACTION_CAN_READ,
                        _resource_name(TEST_DAG_IDS[0], permissions.RESOURCE_DAG),
                    )
                ],
            }
        ]
    )

    yield app

    delete_user(app, username="test_single_dag")


class TestBaseImportError:
    timestamp = "2020-06-10T12:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore

        clear_db_import_errors()
        clear_db_dags()

    def teardown_method(self) -> None:
        clear_db_import_errors()
        clear_db_dags()

    @staticmethod
    def _normalize_import_errors(import_errors):
        for i, import_error in enumerate(import_errors, 1):
            import_error["import_error_id"] = i


class TestGetImportErrorEndpoint(TestBaseImportError):
    def test_should_raise_403_forbidden_without_dag_read(self, session):
        import_error = ParseImportError(
            filename="Lorem_ipsum.py",
            stacktrace="Lorem ipsum",
            timestamp=timezone.parse(self.timestamp, timezone="UTC"),
        )
        session.add(import_error)
        session.commit()

        response = self.client.get(
            f"/api/v1/importErrors/{import_error.id}", environ_overrides={"REMOTE_USER": "test_single_dag"}
        )

        assert response.status_code == 403

    def test_should_return_200_with_single_dag_read(self, session):
        dag_model = DagModel(dag_id=TEST_DAG_IDS[0], fileloc="Lorem_ipsum.py")
        session.add(dag_model)
        import_error = ParseImportError(
            filename="Lorem_ipsum.py",
            stacktrace="Lorem ipsum",
            timestamp=timezone.parse(self.timestamp, timezone="UTC"),
        )
        session.add(import_error)
        session.commit()

        response = self.client.get(
            f"/api/v1/importErrors/{import_error.id}", environ_overrides={"REMOTE_USER": "test_single_dag"}
        )

        assert response.status_code == 200
        response_data = response.json
        response_data["import_error_id"] = 1
        assert {
            "filename": "Lorem_ipsum.py",
            "import_error_id": 1,
            "stack_trace": "Lorem ipsum",
            "timestamp": "2020-06-10T12:00:00+00:00",
        } == response_data

    def test_should_return_200_redacted_with_single_dag_read_in_dagfile(self, session):
        for dag_id in TEST_DAG_IDS:
            dag_model = DagModel(dag_id=dag_id, fileloc="Lorem_ipsum.py")
            session.add(dag_model)
        import_error = ParseImportError(
            filename="Lorem_ipsum.py",
            stacktrace="Lorem ipsum",
            timestamp=timezone.parse(self.timestamp, timezone="UTC"),
        )
        session.add(import_error)
        session.commit()

        response = self.client.get(
            f"/api/v1/importErrors/{import_error.id}", environ_overrides={"REMOTE_USER": "test_single_dag"}
        )

        assert response.status_code == 200
        response_data = response.json
        response_data["import_error_id"] = 1
        assert {
            "filename": "Lorem_ipsum.py",
            "import_error_id": 1,
            "stack_trace": "REDACTED - you do not have read permission on all DAGs in the file",
            "timestamp": "2020-06-10T12:00:00+00:00",
        } == response_data


class TestGetImportErrorsEndpoint(TestBaseImportError):
    def test_get_import_errors_single_dag(self, session):
        for dag_id in TEST_DAG_IDS:
            fake_filename = f"/tmp/{dag_id}.py"
            dag_model = DagModel(dag_id=dag_id, fileloc=fake_filename)
            session.add(dag_model)
            importerror = ParseImportError(
                filename=fake_filename,
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC"),
            )
            session.add(importerror)
        session.commit()

        response = self.client.get(
            "/api/v1/importErrors", environ_overrides={"REMOTE_USER": "test_single_dag"}
        )

        assert response.status_code == 200
        response_data = response.json
        self._normalize_import_errors(response_data["import_errors"])
        assert {
            "import_errors": [
                {
                    "filename": "/tmp/test_dag.py",
                    "import_error_id": 1,
                    "stack_trace": "Lorem ipsum",
                    "timestamp": "2020-06-10T12:00:00+00:00",
                },
            ],
            "total_entries": 1,
        } == response_data

    def test_get_import_errors_single_dag_in_dagfile(self, session):
        for dag_id in TEST_DAG_IDS:
            fake_filename = "/tmp/all_in_one.py"
            dag_model = DagModel(dag_id=dag_id, fileloc=fake_filename)
            session.add(dag_model)

        importerror = ParseImportError(
            filename="/tmp/all_in_one.py",
            stacktrace="Lorem ipsum",
            timestamp=timezone.parse(self.timestamp, timezone="UTC"),
        )
        session.add(importerror)
        session.commit()

        response = self.client.get(
            "/api/v1/importErrors", environ_overrides={"REMOTE_USER": "test_single_dag"}
        )

        assert response.status_code == 200
        response_data = response.json
        self._normalize_import_errors(response_data["import_errors"])
        assert {
            "import_errors": [
                {
                    "filename": "/tmp/all_in_one.py",
                    "import_error_id": 1,
                    "stack_trace": "REDACTED - you do not have read permission on all DAGs in the file",
                    "timestamp": "2020-06-10T12:00:00+00:00",
                },
            ],
            "total_entries": 1,
        } == response_data
