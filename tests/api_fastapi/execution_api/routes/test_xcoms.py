# Licensed to the Apache Software Foundation (ASF) under one
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

import contextlib
import logging

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Path, Request, status

from airflow.api_fastapi.execution_api.datamodels.xcom import XComResponse
from airflow.models.dagrun import DagRun
from airflow.models.taskmap import TaskMap
from airflow.models.xcom import XComModel
from airflow.serialization.serde import deserialize, serialize
from airflow.utils.session import create_session

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def reset_db():
    """Reset XCom entries."""
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(XComModel).delete()


@pytest.fixture
def access_denied(client):
    from airflow.api_fastapi.execution_api.deps import JWTBearerDep
    from airflow.api_fastapi.execution_api.routes.xcoms import has_xcom_access

    last_route = client.app.routes[-1]
    assert isinstance(last_route.app, FastAPI)
    exec_app = last_route.app

    async def _(
        request: Request,
        dag_id: str = Path(),
        run_id: str = Path(),
        task_id: str = Path(),
        xcom_key: str = Path(alias="key"),
        token=JWTBearerDep,
    ):
        await has_xcom_access(dag_id, run_id, task_id, xcom_key, request, token)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
            },
        )

    exec_app.dependency_overrides[has_xcom_access] = _

    yield

    exec_app.dependency_overrides = {}


class TestXComsGetEndpoint:
    @pytest.mark.parametrize(
        ("db_value"),
        [
            ("value1"),
            ({"key2": "value2"}),
            ({"key2": "value2", "key3": ["value3"]}),
            (["value1"]),
        ],
    )
    def test_xcom_get_from_db(self, client, create_task_instance, session, db_value):
        """Test that XCom value is returned from the database in JSON-compatible format."""
        # The tests expect serialised strings because v2 serialised and stored in the DB
        ti = create_task_instance()

        x = XComModel(
            key="xcom_1",
            value=db_value,
            dag_run_id=ti.dag_run.id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            dag_id=ti.dag_id,
        )
        session.add(x)
        session.commit()

        response = client.get(f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/xcom_1")

        assert response.status_code == 200
        assert response.json() == {"key": "xcom_1", "value": db_value}

    def test_xcom_not_found(self, client, create_task_instance):
        response = client.get("/execution/xcoms/dag/runid/task/xcom_non_existent")

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "XCom with key='xcom_non_existent' map_index=-1 not found for task 'task' in DAG run 'runid' of 'dag'",
                "reason": "not_found",
            }
        }

    @pytest.mark.usefixtures("access_denied")
    def test_xcom_access_denied(self, client, caplog):
        with caplog.at_level(logging.DEBUG):
            response = client.get("/execution/xcoms/dag/runid/task/xcom_perms")

        assert response.status_code == 403, response.json()
        assert response.json() == {
            "detail": {
                "reason": "access_denied",
            }
        }
        assert any(msg.startswith("Checking read XCom access") for msg in caplog.messages)


class TestXComsSetEndpoint:
    @pytest.mark.parametrize(
        ("value", "expected_value"),
        [
            ('"value1"', '"value1"'),
            ('{"key2": "value2"}', '{"key2": "value2"}'),
            ('{"key2": "value2", "key3": ["value3"]}', '{"key2": "value2", "key3": ["value3"]}'),
            ('["value1"]', '["value1"]'),
        ],
    )
    def test_xcom_set(self, client, create_task_instance, session, value, expected_value):
        """
        Test that XCom value is set correctly. The value is passed as a JSON string in the request body.
        XCom.set then uses json.dumps to serialize it and store the value in the database.
        This is done so that Task SDK in multiple languages can use the same API to set XCom values.
        """
        ti = create_task_instance()
        session.commit()
        value = serialize(value)
        response = client.post(
            f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/xcom_1",
            json=value,
        )

        assert response.status_code == 201
        assert response.json() == {"message": "XCom successfully set"}

        xcom = session.query(XComModel).filter_by(task_id=ti.task_id, dag_id=ti.dag_id, key="xcom_1").first()
        assert xcom.value == expected_value
        task_map = session.query(TaskMap).filter_by(task_id=ti.task_id, dag_id=ti.dag_id).one_or_none()
        assert task_map is None, "Should not be mapped"

    @pytest.mark.parametrize(
        "orig_value, ser_value, deser_value",
        [
            pytest.param(1, 1, 1, id="int"),
            pytest.param(1.0, 1.0, 1.0, id="float"),
            pytest.param("string", "string", "string", id="str"),
            pytest.param(True, True, True, id="bool"),
            pytest.param({"key": "value"}, {"key": "value"}, {"key": "value"}, id="dict"),
            pytest.param([1, 2], [1, 2], [1, 2], id="list"),
            pytest.param(
                (1, 2),
                # Client serializes tuple as encoded list, send the encoded list to the API
                {"__classname__": "builtins.tuple", "__data__": [1, 2], "__version__": 1},
                # The API will send the encoded list to the DB and sends the same encoded list back
                # during the response to the client as it is the clients responsibility to
                # serialize it into a JSON object & deserialize value into a native object.
                {"__classname__": "builtins.tuple", "__data__": [1, 2], "__version__": 1},
                id="tuple",
            ),
        ],
    )
    def test_xcom_round_trip(self, client, create_task_instance, session, orig_value, ser_value, deser_value):
        """
        Test that deserialization works when XCom values are stored directly in the DB with API Server.

        This tests the case where the XCom value is stored from the Task API where the value is serialized
        via Client SDK into JSON object and passed via the API Server to the DB. It by-passes
        the XComModel.serialize_value and stores valid Python JSON compatible objects to DB.

        This test is to ensure that the deserialization works correctly in this case as well as
        checks that the value is stored correctly before it hits the API.
        """

        ti = create_task_instance()
        session.commit()

        # Serialize the value to simulate the client SDK
        value = serialize(orig_value)

        # Test that the value is serialized correctly
        assert value == ser_value

        response = client.post(
            f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/xcom_1",
            json=value,
        )

        assert response.status_code == 201

        stored_value = XComModel.get_many(
            key="xcom_1",
            dag_ids=ti.dag_id,
            task_ids=ti.task_id,
            run_id=ti.run_id,
            session=session,
        ).first()
        deserialized_value = XComModel.deserialize_value(stored_value)

        assert deserialized_value == deser_value

        # Ensure that the deserialized value on the client side is the same as the original value
        assert deserialize(deserialized_value) == orig_value

    def test_xcom_set_mapped(self, client, create_task_instance, session):
        ti = create_task_instance()
        session.commit()

        value = serialize("value1")

        response = client.post(
            f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/xcom_1",
            params={"map_index": -1, "mapped_length": 3},
            json=value,
        )

        assert response.status_code == 201
        assert response.json() == {"message": "XCom successfully set"}

        xcom = (
            session.query(XComModel)
            .filter_by(task_id=ti.task_id, dag_id=ti.dag_id, key="xcom_1", map_index=-1)
            .first()
        )
        assert xcom.value == "value1"
        task_map = session.query(TaskMap).filter_by(task_id=ti.task_id, dag_id=ti.dag_id).one_or_none()
        assert task_map is not None, "Should be mapped"
        assert task_map.dag_id == "dag"
        assert task_map.run_id == "test"
        assert task_map.task_id == "op1"
        assert task_map.map_index == -1
        assert task_map.length == 3

    @pytest.mark.parametrize(
        ("length", "err_context"),
        [
            pytest.param(
                20,
                contextlib.nullcontext(),
                id="20-success",
            ),
            pytest.param(
                2000,
                pytest.raises(httpx.HTTPStatusError),
                id="2000-too-long",
            ),
        ],
    )
    def test_xcom_set_downstream_of_mapped(self, client, create_task_instance, session, length, err_context):
        """
        Test that XCom value is set correctly. The value is passed as a JSON string in the request body.
        XCom.set then uses json.dumps to serialize it and store the value in the database.
        This is done so that Task SDK in multiple languages can use the same API to set XCom values.
        """
        ti = create_task_instance()
        session.commit()

        with err_context:
            response = client.post(
                f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/xcom_1",
                json='"valid json"',
                params={"mapped_length": length},
            )
            response.raise_for_status()

            task_map = session.query(TaskMap).filter_by(task_id=ti.task_id, dag_id=ti.dag_id).one_or_none()
            assert task_map.length == length

    @pytest.mark.usefixtures("access_denied")
    def test_xcom_access_denied(self, client, caplog):
        with caplog.at_level(logging.DEBUG):
            response = client.post(
                "/execution/xcoms/dag/runid/task/xcom_perms",
                json='"value1"',
            )

        assert response.status_code == 403
        assert response.json() == {
            "detail": {
                "reason": "access_denied",
            }
        }
        assert any(msg.startswith("Checking write XCom access") for msg in caplog.messages)

    @pytest.mark.parametrize(
        ("value", "expected_value"),
        [
            ('"value1"', '"value1"'),
            ('{"key2": "value2"}', '{"key2": "value2"}'),
            ('{"key2": "value2", "key3": ["value3"]}', '{"key2": "value2", "key3": ["value3"]}'),
            ('["value1"]', '["value1"]'),
        ],
    )
    def test_xcom_roundtrip(self, client, create_task_instance, session, value, expected_value):
        """
        Test that XCom value is set and retrieved correctly using API.

        This test sets an XCom value using the API and then retrieves it using the API so we can
        ensure client and server are working correctly together. The server expects a JSON string
        and it will also return a JSON string. It is the client's responsibility to parse the JSON
        string into a native object. This is useful for Task SDKs in other languages.
        """
        ti = create_task_instance()

        value = serialize(value)
        session.commit()
        client.post(
            f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/test_xcom_roundtrip",
            json=value,
        )

        xcom = (
            session.query(XComModel)
            .filter_by(task_id=ti.task_id, dag_id=ti.dag_id, key="test_xcom_roundtrip")
            .first()
        )
        assert xcom.value == expected_value

        response = client.get(f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/test_xcom_roundtrip")

        assert response.status_code == 200
        assert XComResponse.model_validate_json(response.read()).value == expected_value


class TestXComsDeleteEndpoint:
    def test_xcom_delete_endpoint(self, client, create_task_instance, session):
        """Test that XCom value is deleted when Delete API is called."""
        ti = create_task_instance()
        ti.xcom_push(key="xcom_1", value='"value1"', session=session)

        ti1 = create_task_instance(dag_id="my_dag_1", task_id="task_1")
        ti1.xcom_push(key="xcom_1", value='"value2"', session=session)
        session.commit()

        xcoms = session.query(XComModel).filter_by(key="xcom_1").all()
        assert xcoms is not None
        assert len(xcoms) == 2

        response = client.delete(f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/xcom_1")

        assert response.status_code == 200
        assert response.json() == {"message": "XCom with key: xcom_1 successfully deleted."}

        xcom_ti = (
            session.query(XComModel).filter_by(task_id=ti.task_id, dag_id=ti.dag_id, key="xcom_1").first()
        )
        assert xcom_ti is None

        xcom_ti = (
            session.query(XComModel).filter_by(task_id=ti1.task_id, dag_id=ti1.dag_id, key="xcom_1").first()
        )
        assert xcom_ti is not None
