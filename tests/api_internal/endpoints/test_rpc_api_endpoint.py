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

import json
from typing import Generator
from unittest import mock

import pytest
from flask import Flask

from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.settings import _ENABLE_AIP_44
from airflow.utils.state import State
from airflow.www import app
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules

TEST_METHOD_NAME = "test_method"
TEST_METHOD_WITH_LOG_NAME = "test_method_with_log"

mock_test_method = mock.MagicMock()


@pytest.fixture(scope="session")
def minimal_app_for_internal_api() -> Flask:
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_appbuilder",
            "init_api_internal",
        ]
    )
    def factory() -> Flask:
        with conf_vars({("webserver", "run_internal_api"): "true"}):
            return app.create_app(testing=True, config={"WTF_CSRF_ENABLED": False})  # type:ignore

    return factory()


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
class TestRpcApiEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, minimal_app_for_internal_api: Flask) -> Generator:
        self.app = minimal_app_for_internal_api
        self.client = self.app.test_client()  # type:ignore
        mock_test_method.reset_mock()
        mock_test_method.side_effect = None
        with mock.patch(
            "airflow.api_internal.endpoints.rpc_api_endpoint._initialize_map"
        ) as mock_initialize_map:
            mock_initialize_map.return_value = {
                TEST_METHOD_NAME: mock_test_method,
            }
            yield mock_initialize_map

    @pytest.mark.parametrize(
        "input_data, method_result, method_params, expected_mock, expected_code",
        [
            (
                {"jsonrpc": "2.0", "method": TEST_METHOD_NAME, "params": ""},
                "test_me",
                {},
                mock_test_method,
                200,
            ),
            (
                {"jsonrpc": "2.0", "method": TEST_METHOD_NAME, "params": ""},
                None,
                {},
                mock_test_method,
                200,
            ),
            (
                {
                    "jsonrpc": "2.0",
                    "method": TEST_METHOD_NAME,
                    "params": json.dumps(BaseSerialization.serialize({"dag_id": 15, "task_id": "fake-task"})),
                },
                ("dag_id_15", "fake-task", 1),
                {"dag_id": 15, "task_id": "fake-task"},
                mock_test_method,
                200,
            ),
        ],
    )
    def test_method(self, input_data, method_result, method_params, expected_mock, expected_code):
        if method_result:
            expected_mock.return_value = method_result

        response = self.client.post(
            "/internal_api/v1/rpcapi",
            headers={"Content-Type": "application/json"},
            data=json.dumps(input_data),
        )
        assert response.status_code == expected_code
        if method_result:
            response_data = BaseSerialization.deserialize(json.loads(response.data))
            assert response_data == method_result

        expected_mock.assert_called_once_with(**method_params)

    def test_method_with_pydantic_serialized_object(self):
        ti = TaskInstance(task=EmptyOperator(task_id="task"), run_id="run_id", state=State.RUNNING)
        mock_test_method.return_value = ti

        response = self.client.post(
            "/internal_api/v1/rpcapi",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"jsonrpc": "2.0", "method": TEST_METHOD_NAME, "params": ""}),
        )
        assert response.status_code == 200
        print(response.data)
        response_data = BaseSerialization.deserialize(json.loads(response.data), use_pydantic_models=True)
        expected_data = TaskInstancePydantic.from_orm(ti)
        assert response_data == expected_data

    def test_method_with_exception(self):
        mock_test_method.side_effect = ValueError("Error!!!")
        data = {"jsonrpc": "2.0", "method": TEST_METHOD_NAME, "params": ""}

        response = self.client.post(
            "/internal_api/v1/rpcapi", headers={"Content-Type": "application/json"}, data=json.dumps(data)
        )
        assert response.status_code == 500
        assert response.data, b"Error executing method: test_method."
        mock_test_method.assert_called_once()

    def test_unknown_method(self):
        data = {"jsonrpc": "2.0", "method": "i-bet-it-does-not-exist", "params": ""}

        response = self.client.post(
            "/internal_api/v1/rpcapi", headers={"Content-Type": "application/json"}, data=json.dumps(data)
        )
        assert response.status_code == 400
        assert response.data == b"Unrecognized method: i-bet-it-does-not-exist."
        mock_test_method.assert_not_called()

    def test_invalid_jsonrpc(self):
        data = {"jsonrpc": "1.0", "method": TEST_METHOD_NAME, "params": ""}

        response = self.client.post(
            "/internal_api/v1/rpcapi", headers={"Content-Type": "application/json"}, data=json.dumps(data)
        )
        assert response.status_code == 400
        assert response.data == b"Expected jsonrpc 2.0 request."
        mock_test_method.assert_not_called()
