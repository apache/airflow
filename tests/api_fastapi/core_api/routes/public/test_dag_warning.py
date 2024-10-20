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

from airflow.models.dag import DagModel
from airflow.models.dagwarning import DagWarning
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_dag_warnings, clear_db_dags

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
DAG1_MESSAGE = "test message 1"
DAG2_ID = "test_dag2"
DAG2_MESSAGE = "test message 2"
DAG3_ID = "test_dag3"
DAG3_MESSAGE = "test message 3"
DAG_WARNING_TYPE = "non-existent pool"


@pytest.fixture(autouse=True)
@provide_session
def setup(dag_maker, session=None) -> None:
    clear_db_dags()
    clear_db_dag_warnings()

    session.add(DagModel(dag_id=DAG1_ID))
    session.add(DagModel(dag_id=DAG2_ID))
    session.add(DagModel(dag_id=DAG3_ID))
    session.add(DagWarning(DAG1_ID, DAG_WARNING_TYPE, DAG1_MESSAGE))
    # sleep(0.01)
    session.add(DagWarning(DAG2_ID, DAG_WARNING_TYPE, DAG2_MESSAGE))
    # sleep(0.01)
    session.add(DagWarning(DAG3_ID, DAG_WARNING_TYPE, DAG3_MESSAGE))
    session.commit()


class TestGetDagWarnings:
    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_messages",
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
        response = test_client.get("/public/dagWarnings", params=query_params)
        assert response.status_code == 200
        response_json = response.json()
        # breakpoint()
        assert response_json["total_entries"] == expected_total_entries
        assert len(response_json["dag_warnings"]) == len(expected_messages)
        assert [dag_warning["message"] for dag_warning in response_json["dag_warnings"]] == expected_messages

    def test_get_dag_warnings_bad_request(self, test_client):
        response = test_client.get("/public/dagWarnings", params={"warning_type": "invalid"})
        response_json = response.json()
        assert response.status_code == 422
        assert response_json["detail"][0]["msg"] == "Input should be 'non-existent pool'"
