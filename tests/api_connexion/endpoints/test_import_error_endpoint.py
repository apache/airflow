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

from datetime import timedelta

import pytest

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models.dag import DagModel
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import provide_session
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.compat import ParseImportError
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_import_errors
from tests.test_utils.permissions import _resource_name

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

TEST_DAG_IDS = ["test_dag", "test_dag2"]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type:ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR),
        ],  # type: ignore
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore
    create_user(
        app,  # type:ignore
        username="test_single_dag",
        role_name="TestSingleDAG",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR)],  # type: ignore
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

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore
    delete_user(app, username="test_single_dag")  # type: ignore


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
    def test_response_200(self, session):
        import_error = ParseImportError(
            filename="Lorem_ipsum.py",
            stacktrace="Lorem ipsum",
            timestamp=timezone.parse(self.timestamp, timezone="UTC"),
        )
        session.add(import_error)
        session.commit()

        response = self.client.get(
            f"/api/v1/importErrors/{import_error.id}", environ_overrides={"REMOTE_USER": "test"}
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

    def test_response_404(self):
        response = self.client.get("/api/v1/importErrors/2", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 404
        assert {
            "detail": "The ImportError with import_error_id: `2` was not found",
            "status": 404,
            "title": "Import error not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self, session):
        import_error = ParseImportError(
            filename="Lorem_ipsum.py",
            stacktrace="Lorem ipsum",
            timestamp=timezone.parse(self.timestamp, timezone="UTC"),
        )
        session.add(import_error)
        session.commit()

        response = self.client.get(f"/api/v1/importErrors/{import_error.id}")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/importErrors", environ_overrides={"REMOTE_USER": "test_no_permissions"}
        )
        assert response.status_code == 403

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
    def test_get_import_errors(self, session):
        import_error = [
            ParseImportError(
                filename="Lorem_ipsum.py",
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC"),
            )
            for _ in range(2)
        ]
        session.add_all(import_error)
        session.commit()

        response = self.client.get("/api/v1/importErrors", environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        response_data = response.json
        self._normalize_import_errors(response_data["import_errors"])
        assert {
            "import_errors": [
                {
                    "filename": "Lorem_ipsum.py",
                    "import_error_id": 1,
                    "stack_trace": "Lorem ipsum",
                    "timestamp": "2020-06-10T12:00:00+00:00",
                },
                {
                    "filename": "Lorem_ipsum.py",
                    "import_error_id": 2,
                    "stack_trace": "Lorem ipsum",
                    "timestamp": "2020-06-10T12:00:00+00:00",
                },
            ],
            "total_entries": 2,
        } == response_data

    def test_get_import_errors_order_by(self, session):
        import_error = [
            ParseImportError(
                filename=f"Lorem_ipsum{i}.py",
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC") + timedelta(days=-i),
            )
            for i in range(1, 3)
        ]
        session.add_all(import_error)
        session.commit()

        response = self.client.get(
            "/api/v1/importErrors?order_by=-timestamp", environ_overrides={"REMOTE_USER": "test"}
        )

        assert response.status_code == 200
        response_data = response.json
        self._normalize_import_errors(response_data["import_errors"])
        assert {
            "import_errors": [
                {
                    "filename": "Lorem_ipsum1.py",
                    "import_error_id": 1,  # id normalized with self._normalize_import_errors
                    "stack_trace": "Lorem ipsum",
                    "timestamp": "2020-06-09T12:00:00+00:00",
                },
                {
                    "filename": "Lorem_ipsum2.py",
                    "import_error_id": 2,
                    "stack_trace": "Lorem ipsum",
                    "timestamp": "2020-06-08T12:00:00+00:00",
                },
            ],
            "total_entries": 2,
        } == response_data

    def test_order_by_raises_400_for_invalid_attr(self, session):
        import_error = [
            ParseImportError(
                filename="Lorem_ipsum.py",
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC"),
            )
            for _ in range(2)
        ]
        session.add_all(import_error)
        session.commit()

        response = self.client.get(
            "/api/v1/importErrors?order_by=timest", environ_overrides={"REMOTE_USER": "test"}
        )

        assert response.status_code == 400
        msg = "Ordering with 'timest' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    def test_should_raises_401_unauthenticated(self, session):
        import_error = [
            ParseImportError(
                filename="Lorem_ipsum.py",
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC"),
            )
            for _ in range(2)
        ]
        session.add_all(import_error)
        session.commit()

        response = self.client.get("/api/v1/importErrors")

        assert_401(response)

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


class TestGetImportErrorsEndpointPagination(TestBaseImportError):
    @pytest.mark.parametrize(
        "url, expected_import_error_ids",
        [
            # Limit test data
            ("/api/v1/importErrors?limit=1", ["/tmp/file_1.py"]),
            ("/api/v1/importErrors?limit=100", [f"/tmp/file_{i}.py" for i in range(1, 101)]),
            # Offset test data
            ("/api/v1/importErrors?offset=1", [f"/tmp/file_{i}.py" for i in range(2, 102)]),
            ("/api/v1/importErrors?offset=3", [f"/tmp/file_{i}.py" for i in range(4, 104)]),
            # Limit and offset test data
            ("/api/v1/importErrors?offset=3&limit=3", [f"/tmp/file_{i}.py" for i in [4, 5, 6]]),
        ],
    )
    @provide_session
    def test_limit_and_offset(self, url, expected_import_error_ids, session):
        import_errors = [
            ParseImportError(
                filename=f"/tmp/file_{i}.py",
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC"),
            )
            for i in range(1, 110)
        ]
        session.add_all(import_errors)
        session.commit()

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})

        assert response.status_code == 200
        import_ids = [pool["filename"] for pool in response.json["import_errors"]]
        assert import_ids == expected_import_error_ids

    def test_should_respect_page_size_limit_default(self, session):
        import_errors = [
            ParseImportError(
                filename=f"/tmp/file_{i}.py",
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC"),
            )
            for i in range(1, 110)
        ]
        session.add_all(import_errors)
        session.commit()
        response = self.client.get("/api/v1/importErrors", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert len(response.json["import_errors"]) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        import_errors = [
            ParseImportError(
                filename=f"/tmp/file_{i}.py",
                stacktrace="Lorem ipsum",
                timestamp=timezone.parse(self.timestamp, timezone="UTC"),
            )
            for i in range(200)
        ]
        session.add_all(import_errors)
        session.commit()
        response = self.client.get(
            "/api/v1/importErrors?limit=180", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        assert len(response.json["import_errors"]) == 150
