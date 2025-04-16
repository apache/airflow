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

import logging
from unittest import mock

import pytest
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.routing import Mount

from airflow.models.variable import Variable

from tests_common.test_utils.db import clear_db_variables

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def setup_method():
    clear_db_variables()
    yield
    clear_db_variables()


@pytest.fixture
def access_denied(client):
    from airflow.api_fastapi.execution_api.deps import JWTBearerDep
    from airflow.api_fastapi.execution_api.routes.variables import has_variable_access

    last_route = client.app.routes[-1]
    assert isinstance(last_route, Mount)
    assert isinstance(last_route.app, FastAPI)
    exec_app = last_route.app

    async def _(request: Request, variable_key: str, token=JWTBearerDep):
        await has_variable_access(request, variable_key, token)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
            },
        )

    exec_app.dependency_overrides[has_variable_access] = _

    yield

    exec_app.dependency_overrides = {}


class TestGetVariable:
    def test_variable_get_from_db(self, client, session):
        Variable.set(key="var1", value="value", session=session)
        session.commit()

        response = client.get("/execution/variables/var1")

        assert response.status_code == 200
        assert response.json() == {"key": "var1", "value": "value"}

        # Remove connection
        Variable.delete(key="var1", session=session)
        session.commit()

    @mock.patch.dict(
        "os.environ",
        {"AIRFLOW_VAR_KEY1": "VALUE"},
    )
    def test_variable_get_from_env_var(self, client, session):
        response = client.get("/execution/variables/key1")

        assert response.status_code == 200
        assert response.json() == {"key": "key1", "value": "VALUE"}

    def test_variable_get_not_found(self, client):
        response = client.get("/execution/variables/non_existent_var")

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Variable with key 'non_existent_var' not found",
                "reason": "not_found",
            }
        }

    @pytest.mark.usefixtures("access_denied")
    def test_variable_get_access_denied(self, client, caplog):
        with caplog.at_level(logging.DEBUG):
            response = client.get("/execution/variables/key1")

        # Assert response status code and detail for access denied
        assert response.status_code == 403
        assert response.json() == {
            "detail": {
                "reason": "access_denied",
            }
        }

        assert any(msg.startswith("Checking read access for task instance") for msg in caplog.messages)


class TestPutVariable:
    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param({"value": "{}", "description": "description"}, id="valid-payload"),
            pytest.param({"value": "{}"}, id="missing-description"),
        ],
    )
    def test_should_create_variable(self, client, payload, session):
        key = "var_create"
        response = client.put(
            f"/execution/variables/{key}",
            json=payload,
        )
        assert response.status_code == 201, response.json()
        assert response.json()["message"] == "Variable successfully set"

        var_from_db = session.query(Variable).where(Variable.key == "var_create").first()
        assert var_from_db is not None
        assert var_from_db.key == key
        assert var_from_db.val == payload["value"]
        if "description" in payload:
            assert var_from_db.description == payload["description"]

    @pytest.mark.parametrize(
        "key, status_code, payload",
        [
            pytest.param("", 404, {"value": "{}", "description": "description"}, id="missing-key"),
            pytest.param("var_create", 422, {"description": "description"}, id="missing-value"),
        ],
    )
    def test_variable_missing_mandatory_fields(self, client, key, status_code, payload, session):
        response = client.put(
            f"/execution/variables/{key}",
            json=payload,
        )
        assert response.status_code == status_code
        if response.status_code == 422:
            assert response.json()["detail"][0]["type"] == "missing"
            assert response.json()["detail"][0]["msg"] == "Field required"

    @pytest.mark.parametrize(
        "key, payload",
        [
            pytest.param("key", {"key": "key", "value": "{}", "description": "description"}, id="adding-key"),
            pytest.param(
                "key", {"type": "PutVariable", "value": "{}", "description": "description"}, id="adding-type"
            ),
            pytest.param(
                "key",
                {"value": "{}", "description": "description", "lorem": "ipsum", "foo": "bar"},
                id="adding-extra-fields",
            ),
        ],
    )
    def test_variable_adding_extra_fields(self, client, key, payload, session):
        response = client.put(
            f"/execution/variables/{key}",
            json=payload,
        )
        assert response.status_code == 422
        assert response.json()["detail"][0]["type"] == "extra_forbidden"
        assert response.json()["detail"][0]["msg"] == "Extra inputs are not permitted"

    def test_overwriting_existing_variable(self, client, session):
        key = "var_create"
        Variable.set(key=key, value="value", session=session)
        session.commit()

        payload = {"value": "new_value"}
        response = client.put(
            f"/execution/variables/{key}",
            json=payload,
        )
        assert response.status_code == 201
        assert response.json()["message"] == "Variable successfully set"
        # variable should have been updated to the new value
        var_from_db = session.query(Variable).where(Variable.key == key).first()
        assert var_from_db is not None
        assert var_from_db.key == key
        assert var_from_db.val == payload["value"]

    @pytest.mark.usefixtures("access_denied")
    def test_post_variable_access_denied(self, client, caplog):
        with caplog.at_level(logging.DEBUG):
            key = "var_create"
            payload = {"value": "{}"}
            response = client.put(
                f"/execution/variables/{key}",
                json=payload,
            )

        # Assert response status code and detail for access denied
        assert response.status_code == 403
        assert response.json() == {
            "detail": {
                "reason": "access_denied",
            }
        }
        assert any(msg.startswith("Checking write access for task instance") for msg in caplog.messages)


class TestDeleteVariable:
    def test_should_delete_variable(self, client, session):
        for i in range(1, 3):
            Variable.set(key=f"key{i}", value=i)

        vars = session.query(Variable).all()
        assert len(vars) == 2

        response = client.delete("/execution/variables/key1")

        assert response.status_code == 204

        vars = session.query(Variable).all()
        assert len(vars) == 1

    def test_should_not_delete_variable(self, client, session):
        Variable.set(key="key", value="value")

        vars = session.query(Variable).all()
        assert len(vars) == 1

        response = client.delete("/execution/variables/non_existent_key")

        assert response.status_code == 204

        vars = session.query(Variable).all()
        assert len(vars) == 1
