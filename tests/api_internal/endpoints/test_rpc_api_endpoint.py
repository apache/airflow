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
from unittest import mock

import pytest
from flask import Flask

from airflow.api_internal.endpoints import rpc_api_endpoint
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.www import app
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules

TEST_METHOD_NAME = "test_method"

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


class TestRpcApiEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, minimal_app_for_internal_api: Flask) -> None:
        rpc_api_endpoint.METHODS_MAP[TEST_METHOD_NAME] = mock_test_method
        self.app = minimal_app_for_internal_api
        self.client = self.app.test_client()  # type:ignore
        mock_test_method.reset_mock()
        mock_test_method.side_effect = None

    @pytest.mark.parametrize(
        "input_data, method_result, method_params, expected_code",
        [
            ({"jsonrpc": "2.0", "method": TEST_METHOD_NAME, "params": ""}, "test_me", None, 200),
            ({"jsonrpc": "2.0", "method": TEST_METHOD_NAME, "params": ""}, None, None, 200),
            (
                {
                    "jsonrpc": "2.0",
                    "method": TEST_METHOD_NAME,
                    "params": json.dumps(BaseSerialization.serialize({"dag_id": 15, "task_id": "fake-task"})),
                },
                ("dag_id_15", "fake-task", 1),
                {"dag_id": 15, "task_id": "fake-task"},
                200,
            ),
        ],
    )
    def test_method(self, input_data, method_result, method_params, expected_code):
        if method_result:
            mock_test_method.return_value = method_result

        response = self.client.post(
            "/internal_api/v1/rpcapi",
            headers={"Content-Type": "application/json"},
            data=json.dumps(input_data),
        )
        assert response.status_code == expected_code
        if method_result:
            response_data = BaseSerialization.deserialize(json.loads(response.data))
            assert response_data == method_result
        if method_params:
            mock_test_method.assert_called_once_with(**method_params)
        else:
            mock_test_method.assert_called_once()

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
