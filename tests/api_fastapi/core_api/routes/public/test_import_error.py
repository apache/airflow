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

from datetime import datetime, timezone
from unittest import mock

import pytest

from airflow.models import DagModel
from airflow.models.errors import ParseImportError
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_dags, clear_db_import_errors
from tests_common.test_utils.format_datetime import from_datetime_to_zulu_without_ms

pytestmark = pytest.mark.db_test

FILENAME1 = "test_filename1.py"
FILENAME2 = "test_filename2.py"
FILENAME3 = "Lorem_ipsum.py"
STACKTRACE1 = "test_stacktrace1"
STACKTRACE2 = "test_stacktrace2"
STACKTRACE3 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
TIMESTAMP1 = datetime(2024, 6, 15, 1, 0, tzinfo=timezone.utc)
TIMESTAMP2 = datetime(2024, 6, 15, 5, 0, tzinfo=timezone.utc)
TIMESTAMP3 = datetime(2024, 6, 15, 3, 0, tzinfo=timezone.utc)
IMPORT_ERROR_NON_EXISTED_ID = 9999
IMPORT_ERROR_NON_EXISTED_KEY = "non_existed_key"
BUNDLE_NAME = "dag_maker"


class TestImportErrorEndpoint:
    """Common class for /public/importErrors related unit tests."""

    @staticmethod
    def _clear_db():
        clear_db_import_errors()
        clear_db_dags()

    @provide_session
    def prepare_dag_model(self, session=None):
        dag_model = DagModel(fileloc=FILENAME1, dag_id="dag_id1", is_paused=False)
        not_permitted_dag_model = DagModel(fileloc=FILENAME1, dag_id="dag_id4", is_paused=False)
        session.add_all([dag_model, not_permitted_dag_model])
        self.dag_id = str(dag_model.dag_id)
        self.not_permitted_dag_id = str(not_permitted_dag_model.dag_id)

    @pytest.fixture(autouse=True)
    @provide_session
    def setup(self, session=None) -> dict[str, ParseImportError]:
        """
        Setup method which is run before every test.
        """
        self._clear_db()
        import_error1 = ParseImportError(
            bundle_name=BUNDLE_NAME,
            filename=FILENAME1,
            stacktrace=STACKTRACE1,
            timestamp=TIMESTAMP1,
        )
        import_error2 = ParseImportError(
            bundle_name=BUNDLE_NAME,
            filename=FILENAME2,
            stacktrace=STACKTRACE2,
            timestamp=TIMESTAMP2,
        )
        import_error3 = ParseImportError(
            bundle_name=BUNDLE_NAME,
            filename=FILENAME3,
            stacktrace=STACKTRACE3,
            timestamp=TIMESTAMP3,
        )
        session.add_all([import_error1, import_error2, import_error3])
        session.commit()
        return {FILENAME1: import_error1, FILENAME2: import_error2, FILENAME3: import_error3}

    def teardown_method(self) -> None:
        self._clear_db()


class TestGetImportError(TestImportErrorEndpoint):
    @pytest.mark.parametrize(
        "import_error_key, expected_status_code, expected_body",
        [
            (
                FILENAME1,
                200,
                {
                    "import_error_id": 1,
                    "timestamp": TIMESTAMP1,
                    "filename": FILENAME1,
                    "stack_trace": STACKTRACE1,
                    "bundle_name": BUNDLE_NAME,
                },
            ),
            (
                FILENAME2,
                200,
                {
                    "import_error_id": 2,
                    "timestamp": TIMESTAMP2,
                    "filename": FILENAME2,
                    "stack_trace": STACKTRACE2,
                    "bundle_name": BUNDLE_NAME,
                },
            ),
            (IMPORT_ERROR_NON_EXISTED_KEY, 404, {}),
        ],
    )
    def test_get_import_error(
        self, test_client, setup, import_error_key, expected_status_code, expected_body
    ):
        import_error: ParseImportError | None = setup.get(import_error_key)
        import_error_id = import_error.id if import_error else IMPORT_ERROR_NON_EXISTED_ID
        response = test_client.get(f"/public/importErrors/{import_error_id}")
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return
        expected_json = {
            "import_error_id": import_error_id,
            "timestamp": from_datetime_to_zulu_without_ms(expected_body["timestamp"]),
            "filename": expected_body["filename"],
            "stack_trace": expected_body["stack_trace"],
            "bundle_name": BUNDLE_NAME,
        }
        assert response.json() == expected_json

    def test_should_raises_401_unauthenticated(self, unauthenticated_test_client, setup):
        import_error_id = setup[FILENAME1].id
        response = unauthenticated_test_client.get(f"/public/importErrors/{import_error_id}")
        assert response.status_code == 401

    def test_should_raises_403_unauthorized(self, unauthorized_test_client, setup):
        import_error_id = setup[FILENAME1].id
        response = unauthorized_test_client.get(f"/public/importErrors/{import_error_id}")
        assert response.status_code == 403

    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.get_auth_manager")
    def test_should_raises_403_unauthorized__user_can_not_read_any_dags_in_file(
        self, mock_get_auth_manager, test_client, setup
    ):
        import_error_id = setup[FILENAME1].id
        mock_is_authorized_dag = mock_get_auth_manager.return_value.is_authorized_dag
        mock_is_authorized_dag.return_value = False
        mock_get_permitted_dag_ids = mock_get_auth_manager.return_value.get_permitted_dag_ids
        mock_get_permitted_dag_ids.return_value = set()
        response = test_client.get(f"/public/importErrors/{import_error_id}")
        mock_is_authorized_dag.assert_called_once_with(method="GET", user=mock.ANY)
        mock_get_permitted_dag_ids.assert_called_once_with(user=mock.ANY)
        assert response.status_code == 403
        assert response.json() == {"detail": "You do not have read permission on any of the DAGs in the file"}

    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.requires_access_dag")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.get_auth_manager")
    def test_get_import_error__user_dont_have_read_permission_to_read_all_dags_in_file(
        self, mock_get_auth_manager, mock_requires_access_dag, test_client, setup
    ):
        self.prepare_dag_model()
        import_error_id = setup[FILENAME1].id
        mock_is_authorized_dag = mock_get_auth_manager.return_value.is_authorized_dag
        mock_is_authorized_dag.return_value = False
        mock_get_permitted_dag_ids = mock_get_auth_manager.return_value.get_permitted_dag_ids
        mock_get_permitted_dag_ids.return_value = {self.dag_id}
        mock_requires_access_dag.return_value = lambda method: lambda: None
        response = test_client.get(f"/public/importErrors/{import_error_id}")
        assert response.status_code == 200
        assert response.json() == {
            "import_error_id": import_error_id,
            "timestamp": from_datetime_to_zulu_without_ms(TIMESTAMP1),
            "filename": FILENAME1,
            "stack_trace": "REDACTED - you do not have read permission on all DAGs in the file",
            "bundle_name": BUNDLE_NAME,
        }


class TestGetImportErrors(TestImportErrorEndpoint):
    @pytest.mark.parametrize(
        "query_params, expected_status_code, expected_total_entries, expected_filenames",
        [
            (
                {},
                200,
                3,
                [FILENAME1, FILENAME2, FILENAME3],
            ),
            # offset, limit
            (
                {"limit": 1, "offset": 1},
                200,
                3,
                [FILENAME2],
            ),
            (
                {"limit": 1, "offset": 2},
                200,
                3,
                [FILENAME3],
            ),
            # order_by
            (
                {"order_by": "-filename"},
                200,
                3,
                [FILENAME2, FILENAME1, FILENAME3],
            ),
            (
                {"order_by": "timestamp"},
                200,
                3,
                [FILENAME1, FILENAME3, FILENAME2],
            ),
            (
                {"order_by": "import_error_id"},
                200,
                3,
                [FILENAME1, FILENAME2, FILENAME3],
            ),
            (
                {"order_by": "-import_error_id"},
                200,
                3,
                [FILENAME3, FILENAME2, FILENAME1],
            ),
            # invalid order_by
            (
                {"order_by": "invalid_order_by"},
                400,
                0,
                [],
            ),
            # combination of query parameters
            (
                {"limit": 2, "offset": 1, "order_by": "-filename"},
                200,
                3,
                [FILENAME1, FILENAME3],
            ),
            (
                {"limit": 1, "offset": 2, "order_by": "-filename"},
                200,
                3,
                [FILENAME3],
            ),
            (
                {"limit": 5, "offset": 1, "order_by": "timestamp"},
                200,
                3,
                [FILENAME3, FILENAME2],
            ),
        ],
    )
    def test_get_import_errors(
        self,
        test_client,
        query_params,
        expected_status_code,
        expected_total_entries,
        expected_filenames,
    ):
        response = test_client.get("/public/importErrors", params=query_params)

        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        response_json = response.json()
        assert response_json["total_entries"] == expected_total_entries
        assert [
            import_error["filename"] for import_error in response_json["import_errors"]
        ] == expected_filenames

    def test_should_raises_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/public/importErrors")
        assert response.status_code == 401

    def test_should_raises_403_unauthorized(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/public/importErrors")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "batch_is_authorized_dag_return_value, expected_stack_trace",
        [
            pytest.param(True, STACKTRACE1, id="user_has_read_access_to_all_dags_in_current_file"),
            pytest.param(
                False,
                "REDACTED - you do not have read permission on all DAGs in the file",
                id="user_does_not_have_read_access_to_all_dags_in_current_file",
            ),
        ],
    )
    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.requires_access_dag")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.get_auth_manager")
    def test_user_can_not_read_all_dags_in_file(
        self,
        mock_get_auth_manager,
        mock_requires_access_dag,
        test_client,
        setup,
        batch_is_authorized_dag_return_value,
        expected_stack_trace,
    ):
        self.prepare_dag_model()
        mock_is_authorized_dag = mock_get_auth_manager.return_value.is_authorized_dag
        mock_is_authorized_dag.return_value = False
        mock_get_permitted_dag_ids = mock_get_auth_manager.return_value.get_permitted_dag_ids
        mock_get_permitted_dag_ids.return_value = {self.dag_id}
        mock_batch_is_authorized_dag = mock_get_auth_manager.return_value.batch_is_authorized_dag
        mock_batch_is_authorized_dag.return_value = batch_is_authorized_dag_return_value
        mock_requires_access_dag.return_value = lambda method: lambda: None
        response = test_client.get("/public/importErrors")
        mock_get_permitted_dag_ids.assert_called_once_with(method="GET", user=mock.ANY)
        assert response.status_code == 200
        response_json = response.json()
        assert response_json == {
            "total_entries": 1,
            "import_errors": [
                {
                    "import_error_id": setup[FILENAME1].id,
                    "timestamp": from_datetime_to_zulu_without_ms(TIMESTAMP1),
                    "filename": FILENAME1,
                    "stack_trace": expected_stack_trace,
                    "bundle_name": BUNDLE_NAME,
                }
            ],
        }
