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
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.api_fastapi.auth.managers.models.resource_details import DagDetails
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError
from airflow.utils.session import NEW_SESSION, provide_session

from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_import_errors
from tests_common.test_utils.format_datetime import from_datetime_to_zulu_without_ms

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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
BUNDLE_NAME = "testing"


@pytest.fixture
@provide_session
def permitted_dag_model(testing_dag_bundle, session: Session = NEW_SESSION) -> DagModel:
    dag_model = DagModel(
        fileloc=FILENAME1,
        relative_fileloc=FILENAME1,
        dag_id="dag_id1",
        is_paused=False,
        bundle_name=BUNDLE_NAME,
    )
    session.add(dag_model)
    session.commit()
    return dag_model


@pytest.fixture
@provide_session
def not_permitted_dag_model(testing_dag_bundle, session: Session = NEW_SESSION) -> DagModel:
    dag_model = DagModel(
        fileloc=FILENAME1,
        bundle_name=BUNDLE_NAME,
        relative_fileloc=FILENAME1,
        dag_id="dag_id4",
        is_paused=False,
    )
    session.add(dag_model)
    session.commit()
    return dag_model


@pytest.fixture(autouse=True)
def clear_db():
    clear_db_import_errors()
    clear_db_dags()
    clear_db_dag_bundles()

    yield

    clear_db_import_errors()
    clear_db_dags()
    clear_db_dag_bundles()


@pytest.fixture(autouse=True)
@provide_session
def import_errors(session: Session = NEW_SESSION) -> list[ParseImportError]:
    _import_errors = [
        ParseImportError(
            bundle_name=bundle,
            filename=filename,
            stacktrace=stacktrace,
            timestamp=timestamp,
        )
        for bundle, filename, stacktrace, timestamp in zip(
            (BUNDLE_NAME, BUNDLE_NAME, None),
            (FILENAME1, FILENAME2, FILENAME3),
            (STACKTRACE1, STACKTRACE2, STACKTRACE3),
            (TIMESTAMP1, TIMESTAMP2, TIMESTAMP3),
        )
    ]

    session.add_all(_import_errors)
    return _import_errors


def set_mock_auth_manager__is_authorized_dag(
    mock_auth_manager: mock.Mock, is_authorized_dag_return_value: bool = False
) -> mock.Mock:
    mock_is_authorized_dag = mock_auth_manager.return_value.is_authorized_dag
    mock_is_authorized_dag.return_value = is_authorized_dag_return_value
    return mock_is_authorized_dag


def set_mock_auth_manager__get_authorized_dag_ids(
    mock_auth_manager: mock.Mock, get_authorized_dag_ids_return_value: set[str] | None = None
) -> mock.Mock:
    if get_authorized_dag_ids_return_value is None:
        get_authorized_dag_ids_return_value = set()
    mock_get_authorized_dag_ids = mock_auth_manager.return_value.get_authorized_dag_ids
    mock_get_authorized_dag_ids.return_value = get_authorized_dag_ids_return_value
    return mock_get_authorized_dag_ids


def set_mock_auth_manager__batch_is_authorized_dag(
    mock_auth_manager: mock.Mock, batch_is_authorized_dag_return_value: bool = False
) -> mock.Mock:
    mock_batch_is_authorized_dag = mock_auth_manager.return_value.batch_is_authorized_dag
    mock_batch_is_authorized_dag.return_value = batch_is_authorized_dag_return_value
    return mock_batch_is_authorized_dag


class TestGetImportError:
    @pytest.mark.parametrize(
        "prepared_import_error_idx, expected_status_code, expected_body",
        [
            (
                0,
                200,
                {
                    "timestamp": from_datetime_to_zulu_without_ms(TIMESTAMP1),
                    "filename": FILENAME1,
                    "stack_trace": STACKTRACE1,
                    "bundle_name": BUNDLE_NAME,
                },
            ),
            (
                1,
                200,
                {
                    "timestamp": from_datetime_to_zulu_without_ms(TIMESTAMP2),
                    "filename": FILENAME2,
                    "stack_trace": STACKTRACE2,
                    "bundle_name": BUNDLE_NAME,
                },
            ),
            (
                2,
                200,
                {
                    "timestamp": from_datetime_to_zulu_without_ms(TIMESTAMP3),
                    "filename": FILENAME3,
                    "stack_trace": STACKTRACE3,
                    "bundle_name": None,
                },
            ),
            (None, 404, {}),
        ],
    )
    def test_get_import_error(
        self, prepared_import_error_idx, expected_status_code, expected_body, test_client, import_errors
    ):
        import_error: ParseImportError | None = (
            import_errors[prepared_import_error_idx] if prepared_import_error_idx is not None else None
        )
        import_error_id = import_error.id if import_error else IMPORT_ERROR_NON_EXISTED_ID
        response = test_client.get(f"/importErrors/{import_error_id}")
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        expected_body.update({"import_error_id": import_error_id})
        assert response.json() == expected_body

    def test_should_raises_401_unauthenticated(self, unauthenticated_test_client, import_errors):
        import_error_id = import_errors[0].id
        response = unauthenticated_test_client.get(f"/importErrors/{import_error_id}")
        assert response.status_code == 401

    def test_should_raises_403_unauthorized(self, unauthorized_test_client, import_errors):
        import_error_id = import_errors[0].id
        response = unauthorized_test_client.get(f"/importErrors/{import_error_id}")
        assert response.status_code == 403

    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.get_auth_manager")
    def test_should_raises_403_unauthorized__user_can_not_read_any_dags_in_file(
        self, mock_get_auth_manager, test_client, import_errors
    ):
        import_error_id = import_errors[0].id
        # Mock auth_manager
        mock_is_authorized_dag = set_mock_auth_manager__is_authorized_dag(mock_get_auth_manager)
        mock_get_authorized_dag_ids = set_mock_auth_manager__get_authorized_dag_ids(mock_get_auth_manager)
        # Act
        response = test_client.get(f"/importErrors/{import_error_id}")
        # Assert
        mock_is_authorized_dag.assert_called_once_with(method="GET", user=mock.ANY)
        mock_get_authorized_dag_ids.assert_called_once_with(user=mock.ANY)
        assert response.status_code == 403
        assert response.json() == {"detail": "You do not have read permission on any of the DAGs in the file"}

    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.get_auth_manager")
    def test_get_import_error__user_dont_have_read_permission_to_read_all_dags_in_file(
        self, mock_get_auth_manager, test_client, permitted_dag_model, not_permitted_dag_model, import_errors
    ):
        import_error_id = import_errors[0].id
        set_mock_auth_manager__is_authorized_dag(mock_get_auth_manager)
        set_mock_auth_manager__get_authorized_dag_ids(mock_get_auth_manager, {permitted_dag_model.dag_id})
        # Act
        response = test_client.get(f"/importErrors/{import_error_id}")
        # Assert
        assert response.status_code == 200
        assert response.json() == {
            "import_error_id": import_error_id,
            "timestamp": from_datetime_to_zulu_without_ms(TIMESTAMP1),
            "filename": FILENAME1,
            "stack_trace": "REDACTED - you do not have read permission on all DAGs in the file",
            "bundle_name": BUNDLE_NAME,
        }


class TestGetImportErrors:
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
        response = test_client.get("/importErrors", params=query_params)

        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        response_json = response.json()
        assert response_json["total_entries"] == expected_total_entries
        assert [
            import_error["filename"] for import_error in response_json["import_errors"]
        ] == expected_filenames

    def test_should_raises_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/importErrors")
        assert response.status_code == 401

    def test_should_raises_403_unauthorized(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/importErrors")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "team, batch_is_authorized_dag_return_value, expected_stack_trace",
        [
            pytest.param(
                "test_team",
                True,
                STACKTRACE1,
                id="user_has_read_access_to_all_dags_in_current_file_with_team",
            ),
            pytest.param(
                None, True, STACKTRACE1, id="user_has_read_access_to_all_dags_in_current_file_without_team"
            ),
            pytest.param(
                None,
                False,
                "REDACTED - you do not have read permission on all DAGs in the file",
                id="user_does_not_have_read_access_to_all_dags_in_current_file",
            ),
        ],
    )
    @pytest.mark.usefixtures("permitted_dag_model")
    @mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.get_auth_manager")
    def test_user_can_not_read_all_dags_in_file(
        self,
        mock_get_auth_manager,
        mock_get_dag_id_to_team_name_mapping,
        test_client,
        team,
        batch_is_authorized_dag_return_value,
        expected_stack_trace,
        permitted_dag_model,
        import_errors,
    ):
        mock_get_dag_id_to_team_name_mapping.return_value = {permitted_dag_model.dag_id: team}
        set_mock_auth_manager__is_authorized_dag(mock_get_auth_manager)
        mock_get_authorized_dag_ids = set_mock_auth_manager__get_authorized_dag_ids(
            mock_get_auth_manager, {permitted_dag_model.dag_id}
        )
        mock_batch_is_authorized_dag = set_mock_auth_manager__batch_is_authorized_dag(
            mock_get_auth_manager, batch_is_authorized_dag_return_value
        )
        # Act
        response = test_client.get("/importErrors")
        # Assert
        mock_get_authorized_dag_ids.assert_called_once_with(method="GET", user=mock.ANY)
        assert response.status_code == 200
        response_json = response.json()
        assert response_json == {
            "total_entries": 1,
            "import_errors": [
                {
                    "import_error_id": import_errors[0].id,
                    "timestamp": from_datetime_to_zulu_without_ms(TIMESTAMP1),
                    "filename": FILENAME1,
                    "stack_trace": expected_stack_trace,
                    "bundle_name": BUNDLE_NAME,
                }
            ],
        }
        mock_batch_is_authorized_dag.assert_called_once_with(
            [
                {
                    "method": "GET",
                    "details": DagDetails(id=permitted_dag_model.dag_id, team_name=team),
                }
            ],
            user=mock.ANY,
        )

    @pytest.mark.usefixtures("permitted_dag_model")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.import_error.get_auth_manager")
    def test_bundle_name_join_condition_for_import_errors(
        self, mock_get_auth_manager, test_client, permitted_dag_model, import_errors, session
    ):
        """Test that the bundle_name join condition works correctly."""
        set_mock_auth_manager__is_authorized_dag(mock_get_auth_manager)
        mock_get_authorized_dag_ids = set_mock_auth_manager__get_authorized_dag_ids(
            mock_get_auth_manager, {permitted_dag_model.dag_id}
        )
        set_mock_auth_manager__batch_is_authorized_dag(mock_get_auth_manager, True)

        response = test_client.get("/importErrors")

        # Assert
        mock_get_authorized_dag_ids.assert_called_once_with(method="GET", user=mock.ANY)
        assert response.status_code == 200
        response_json = response.json()

        # Should return the import error with matching bundle_name and filename
        assert response_json["total_entries"] == 1
        assert response_json["import_errors"][0]["bundle_name"] == BUNDLE_NAME
        assert response_json["import_errors"][0]["filename"] == FILENAME1

        # Now test that removing the bundle_name from the DagModel causes the import error to not be returned
        permitted_dag_model.bundle_name = "another_bundle_name"
        session.add(DagBundleModel(name="another_bundle_name"))
        session.flush()
        session.merge(permitted_dag_model)
        session.commit()

        response2 = test_client.get("/importErrors")

        # Assert - should return 0 entries because bundle_name no longer matches
        assert response2.status_code == 200
        response_json2 = response2.json()
        assert response_json2["total_entries"] == 0
        assert response_json2["import_errors"] == []
