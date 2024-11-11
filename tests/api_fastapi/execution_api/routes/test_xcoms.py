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

from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCom
from airflow.utils.session import create_session

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def reset_db():
    """Reset XCom entries."""
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(XCom).delete()


class TestXComsGetEndpoint:
    @pytest.mark.parametrize(
        ("value", "expected_value"),
        [
            ("value1", '"value1"'),
            ({"key2": "value2"}, '{"key2": "value2"}'),
            ({"key2": "value2", "key3": ["value3"]}, '{"key2": "value2", "key3": ["value3"]}'),
        ],
    )
    def test_xcom_get_from_db_serialized(self, client, create_task_instance, session, value, expected_value):
        ti = create_task_instance()
        ti.xcom_push(key="xcom_1", value=value, session=session)

        session.commit()

        response = client.get(
            f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/xcom_1",
            params={"deserialize": False},
        )

        assert response.status_code == 200
        assert response.json() == {"key": "xcom_1", "value": expected_value, "is_serialized": True}

    @pytest.mark.parametrize(
        ("value", "expected_value"),
        [
            ("value1", "value1"),
            ({"key2": "value2"}, {"key2": "value2"}),
            ({"key2": "value2", "key3": ["value3"]}, {"key2": "value2", "key3": ["value3"]}),
        ],
    )
    def test_xcom_get_from_db_deserialized(
        self, client, create_task_instance, session, value, expected_value
    ):
        ti = create_task_instance()
        ti.xcom_push(key="xcom_1", value=value, session=session)

        session.commit()

        response = client.get(f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/xcom_1")

        assert response.status_code == 200
        assert response.json() == {"key": "xcom_1", "value": expected_value, "is_serialized": False}

    def test_xcom_not_found(self, client, create_task_instance):
        response = client.get("/execution/xcoms/dag/runid/task/xcom_non_existent")

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "XCom with key 'xcom_non_existent' not found for task 'task' in DAG 'dag'",
                "reason": "not_found",
            }
        }

    def test_xcom_access_denied(self, client):
        with mock.patch("airflow.api_fastapi.execution_api.routes.xcoms.has_xcom_access", return_value=False):
            response = client.get("/execution/xcoms/dag/runid/task/xcom_perms")

        # Assert response status code and detail for access denied
        assert response.status_code == 403
        assert response.json() == {
            "detail": {
                "reason": "access_denied",
                "message": "Task does not have access to XCom key 'xcom_perms'",
            }
        }
