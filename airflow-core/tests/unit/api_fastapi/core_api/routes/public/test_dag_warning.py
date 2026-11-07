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

from unittest import mock

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.core_api.routes.public.dag_warning import PER_DAG_WARNING_TYPES
from airflow.api_fastapi.core_api.security import REDACTED_FILE_CONTENT
from airflow.models.dag import DagModel
from airflow.models.dagwarning import DagWarning
from airflow.utils.session import NEW_SESSION, provide_session

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_dag_warnings, clear_db_dags

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
DAG1_MESSAGE = "test message 1"
DAG1_DISPLAY_NAME = "dag_display_name1"
DAG2_ID = "test_dag2"
DAG2_MESSAGE = "test message 2"
DAG2_DISPLAY_NAME = "dag_display_name2"
DAG3_ID = "test_dag3"
DAG3_MESSAGE = "test message 3"
DAG3_DISPLAY_NAME = "dag_display_name3"
DAG_WARNING_TYPE = "non-existent pool"

expected_display_names = {
    DAG1_ID: DAG1_DISPLAY_NAME,
    DAG2_ID: DAG2_DISPLAY_NAME,
    DAG3_ID: DAG3_DISPLAY_NAME,
}


@pytest.fixture(autouse=True)
@provide_session
def setup(dag_maker, testing_dag_bundle, *, session: Session = NEW_SESSION) -> None:
    clear_db_dags()
    clear_db_dag_warnings()

    bundle_name = "testing"

    session.add(
        DagModel(dag_id=DAG1_ID, bundle_name=bundle_name, _dag_display_property_value=DAG1_DISPLAY_NAME)
    )
    session.add(
        DagModel(dag_id=DAG2_ID, bundle_name=bundle_name, _dag_display_property_value=DAG2_DISPLAY_NAME)
    )
    session.add(
        DagModel(dag_id=DAG3_ID, bundle_name=bundle_name, _dag_display_property_value=DAG3_DISPLAY_NAME)
    )
    session.add(DagWarning(DAG1_ID, DAG_WARNING_TYPE, DAG1_MESSAGE))
    session.add(DagWarning(DAG2_ID, DAG_WARNING_TYPE, DAG2_MESSAGE))
    session.add(DagWarning(DAG3_ID, DAG_WARNING_TYPE, DAG3_MESSAGE))
    session.commit()


class TestGetDagWarnings:
    @pytest.mark.parametrize(
        ("query_params", "expected_total_entries", "expected_messages"),
        [
            ({}, 3, [DAG1_MESSAGE, DAG2_MESSAGE, DAG3_MESSAGE]),
            ({"dag_id": DAG1_ID}, 1, [DAG1_MESSAGE]),
            ({"warning_type": DAG_WARNING_TYPE}, 3, [DAG1_MESSAGE, DAG2_MESSAGE, DAG3_MESSAGE]),
            ({"limit": 1, "order_by": "message"}, 3, [DAG1_MESSAGE]),
            ({"limit": 1, "offset": 1, "order_by": "message"}, 3, [DAG2_MESSAGE]),
            ({"limit": 1, "offset": 2, "order_by": "dag_id"}, 3, [DAG3_MESSAGE]),
            ({"limit": 1, "offset": 2, "order_by": "-dag_id"}, 3, [DAG1_MESSAGE]),
            ({"limit": 1, "order_by": "timestamp"}, 3, [DAG1_MESSAGE]),
            ({"limit": 1, "order_by": "-timestamp"}, 3, [DAG3_MESSAGE]),
            ({"order_by": "timestamp"}, 3, [DAG1_MESSAGE, DAG2_MESSAGE, DAG3_MESSAGE]),
            ({"order_by": "-timestamp"}, 3, [DAG3_MESSAGE, DAG2_MESSAGE, DAG1_MESSAGE]),
            ({"order_by": "dag_id"}, 3, [DAG1_MESSAGE, DAG2_MESSAGE, DAG3_MESSAGE]),
            ({"order_by": "-dag_id"}, 3, [DAG3_MESSAGE, DAG2_MESSAGE, DAG1_MESSAGE]),
        ],
    )
    def test_get_dag_warnings(self, test_client, query_params, expected_total_entries, expected_messages):
        with assert_queries_count(3 if query_params.get("dag_id") is None else 4):
            response = test_client.get("/dagWarnings", params=query_params)
        assert response.status_code == 200
        response_json = response.json()
        assert response_json["total_entries"] == expected_total_entries
        assert len(response_json["dag_warnings"]) == len(expected_messages)
        assert [dag_warning["message"] for dag_warning in response_json["dag_warnings"]] == expected_messages

        for dag_warning in response_json["dag_warnings"]:
            assert "dag_display_name" in dag_warning
            dag_id = dag_warning["dag_id"]
            assert dag_warning["dag_display_name"] == expected_display_names[dag_id]

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dagWarnings", params={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dagWarnings", params={})
        assert response.status_code == 403

    def test_get_dag_warnings_bad_request(self, test_client):
        response = test_client.get("/dagWarnings", params={"warning_type": "invalid"})
        response_json = response.json()
        assert response.status_code == 422
        assert response_json["detail"][0]["msg"] == (
            "Input should be 'asset conflict', 'duplicate dag id', 'non-existent pool', "
            "'runtime varying value' or 'task group cycle'"
        )

    def test_get_dag_warnings_non_matching_type(self, test_client):
        response = test_client.get("/dagWarnings", params={"warning_type": "yaml:unused_type"})
        response_json = response.json()
        assert response.status_code == 200
        assert response_json["total_entries"] == 0
        assert response_json["dag_warnings"] == []

    def test_get_dag_warnings_importer_defined_type(self, test_client, session):
        session.add(DagWarning(DAG1_ID, "yaml:schema_violation", "importer message"))
        session.commit()

        response = test_client.get("/dagWarnings", params={"warning_type": "yaml:schema_violation"})

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["total_entries"] == 1
        assert response_json["dag_warnings"][0]["warning_type"] == "yaml:schema_violation"
        assert response_json["dag_warnings"][0]["message"] == "importer message"


class TestGetDagWarningsOfMultiDagFile:
    FILE_LEVEL_MESSAGE = "Line 12, Col 4\nCode: BashOperator(task_id='colocated_task')"

    @pytest.fixture(autouse=True)
    def colocate_dags(self, session):
        """Put DAG1 and DAG2 in one file and DAG3 in a file of its own."""
        session.get(DagModel, DAG1_ID).relative_fileloc = "multi_dag_file.py"
        session.get(DagModel, DAG2_ID).relative_fileloc = "multi_dag_file.py"
        session.get(DagModel, DAG3_ID).relative_fileloc = "single_dag_file.py"
        session.commit()

    @pytest.mark.parametrize("warning_type", ["runtime varying value", "yaml:schema_violation"])
    @pytest.mark.parametrize(
        ("readable_dag_ids", "expected_messages"),
        [
            pytest.param({DAG1_ID}, {DAG1_ID: REDACTED_FILE_CONTENT}, id="colocated-dag-unreadable"),
            pytest.param(
                {DAG1_ID, DAG3_ID},
                {DAG1_ID: REDACTED_FILE_CONTENT, DAG3_ID: FILE_LEVEL_MESSAGE},
                id="only-file-with-unreadable-dag-redacted",
            ),
            pytest.param(
                {DAG1_ID, DAG2_ID},
                {DAG1_ID: FILE_LEVEL_MESSAGE, DAG2_ID: FILE_LEVEL_MESSAGE},
                id="all-dags-in-file-readable",
            ),
        ],
    )
    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_file_level_message_is_redacted_unless_caller_can_read_all_dags_in_file(
        self,
        mock_get_authorized_dag_ids,
        test_client,
        session,
        readable_dag_ids,
        expected_messages,
        warning_type,
    ):
        mock_get_authorized_dag_ids.return_value = readable_dag_ids
        for dag_id in (DAG1_ID, DAG2_ID, DAG3_ID):
            session.add(DagWarning(dag_id, warning_type, self.FILE_LEVEL_MESSAGE))
        session.commit()

        with assert_queries_count(4):
            response = test_client.get("/dagWarnings", params={"warning_type": warning_type})

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["total_entries"] == len(expected_messages)
        assert {w["dag_id"]: w["message"] for w in response_json["dag_warnings"]} == expected_messages
        session.expire_all()
        assert set(
            session.scalars(select(DagWarning.message).where(DagWarning.warning_type == warning_type))
        ) == {self.FILE_LEVEL_MESSAGE}

    @pytest.mark.parametrize("warning_type", sorted(PER_DAG_WARNING_TYPES))
    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_per_dag_message_is_returned_when_caller_cannot_read_all_dags_in_file(
        self, mock_get_authorized_dag_ids, test_client, session, warning_type
    ):
        mock_get_authorized_dag_ids.return_value = {DAG1_ID}
        clear_db_dag_warnings()
        session.add(DagWarning(DAG1_ID, warning_type, DAG1_MESSAGE))
        session.add(DagWarning(DAG2_ID, warning_type, DAG2_MESSAGE))
        session.commit()

        with assert_queries_count(3):
            response = test_client.get("/dagWarnings", params={"warning_type": warning_type})

        assert response.status_code == 200
        assert [(w["dag_id"], w["message"]) for w in response.json()["dag_warnings"]] == [
            (DAG1_ID, DAG1_MESSAGE)
        ]
