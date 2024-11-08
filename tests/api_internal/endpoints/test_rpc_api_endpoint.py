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
from typing import TYPE_CHECKING, Generator
from unittest import mock

import pytest

from airflow.api_connexion.exceptions import PermissionDenied
from airflow.configuration import conf
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.settings import _ENABLE_AIP_44
from airflow.utils.jwt_signer import JWTSigner
from airflow.utils.state import State
from airflow.www import app

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.decorators import dont_initialize_flask_app_submodules

# Note: Sounds a bit strange to disable internal API tests in isolation mode but...
# As long as the test is modelled to run its own internal API endpoints, it is conflicting
# to the test setup with a dedicated internal API server.
pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

if TYPE_CHECKING:
    from flask import Flask

TEST_METHOD_NAME = "test_method"
TEST_METHOD_WITH_LOG_NAME = "test_method_with_log"

mock_test_method = mock.MagicMock()

pytest.importorskip("pydantic", minversion="2.0.0")


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


def equals(a, b) -> bool:
    return a == b


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
class TestRpcApiEndpoint:
    @pytest.fixture
    def setup_attrs(self, minimal_app_for_internal_api: Flask) -> Generator:
        self.app = minimal_app_for_internal_api
        self.client = self.app.test_client()  # type:ignore
        mock_test_method.reset_mock()
        mock_test_method.side_effect = None
        with mock.patch(
            "airflow.api_internal.endpoints.rpc_api_endpoint.initialize_method_map"
        ) as mock_initialize_method_map:
            mock_initialize_method_map.return_value = {
                TEST_METHOD_NAME: mock_test_method,
            }
            yield mock_initialize_method_map

    @pytest.fixture
    def signer(self) -> JWTSigner:
        return JWTSigner(
            secret_key=conf.get("core", "internal_api_secret_key"),
            expiration_time_in_seconds=conf.getint("core", "internal_api_clock_grace", fallback=30),
            audience="api",
        )

    def test_initialize_method_map(self):
        from airflow.api_internal.endpoints.rpc_api_endpoint import initialize_method_map

        method_map = initialize_method_map()
        assert len(method_map) > 70

    @pytest.mark.parametrize(
        "input_params, method_result, result_cmp_func, method_params",
        [
            ({}, None, lambda got, _: got == b"", {}),
            ({}, "test_me", equals, {}),
            (
                BaseSerialization.serialize({"dag_id": 15, "task_id": "fake-task"}),
                ("dag_id_15", "fake-task", 1),
                equals,
                {"dag_id": 15, "task_id": "fake-task"},
            ),
            (
                {},
                TaskInstance(task=EmptyOperator(task_id="task"), run_id="run_id", state=State.RUNNING),
                lambda a, b: a.model_dump() == TaskInstancePydantic.model_validate(b).model_dump()
                and isinstance(a.task, BaseOperator),
                {},
            ),
            (
                {},
                Connection(conn_id="test_conn", conn_type="http", host="", password=""),
                lambda a, b: a.get_uri() == b.get_uri() and a.conn_id == b.conn_id,
                {},
            ),
        ],
    )
    def test_method(
        self, input_params, method_result, result_cmp_func, method_params, setup_attrs, signer: JWTSigner
    ):
        mock_test_method.return_value = method_result
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": signer.generate_signed_token({"method": TEST_METHOD_NAME}),
        }
        input_data = {
            "jsonrpc": "2.0",
            "method": TEST_METHOD_NAME,
            "params": input_params,
        }
        response = self.client.post(
            "/internal_api/v1/rpcapi",
            headers=headers,
            data=json.dumps(input_data),
        )
        assert response.status_code == 200
        if method_result:
            response_data = BaseSerialization.deserialize(json.loads(response.data), use_pydantic_models=True)
        else:
            response_data = response.data

        assert result_cmp_func(response_data, method_result)

        mock_test_method.assert_called_once_with(**method_params, session=mock.ANY)

    def test_method_with_exception(self, setup_attrs, signer: JWTSigner):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": signer.generate_signed_token({"method": TEST_METHOD_NAME}),
        }
        mock_test_method.side_effect = ValueError("Error!!!")
        data = {"jsonrpc": "2.0", "method": TEST_METHOD_NAME, "params": {}}

        response = self.client.post("/internal_api/v1/rpcapi", headers=headers, data=json.dumps(data))
        assert response.status_code == 500
        assert response.data, b"Error executing method: test_method."
        mock_test_method.assert_called_once()

    def test_unknown_method(self, setup_attrs, signer: JWTSigner):
        UNKNOWN_METHOD = "i-bet-it-does-not-exist"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": signer.generate_signed_token({"method": UNKNOWN_METHOD}),
        }
        data = {"jsonrpc": "2.0", "method": UNKNOWN_METHOD, "params": {}}

        response = self.client.post("/internal_api/v1/rpcapi", headers=headers, data=json.dumps(data))
        assert response.status_code == 400
        assert response.data.startswith(b"Unrecognized method: i-bet-it-does-not-exist.")
        mock_test_method.assert_not_called()

    def test_invalid_jsonrpc(self, setup_attrs, signer: JWTSigner):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": signer.generate_signed_token({"method": TEST_METHOD_NAME}),
        }
        data = {"jsonrpc": "1.0", "method": TEST_METHOD_NAME, "params": {}}

        response = self.client.post("/internal_api/v1/rpcapi", headers=headers, data=json.dumps(data))
        assert response.status_code == 400
        assert response.data.startswith(b"Expected jsonrpc 2.0 request.")
        mock_test_method.assert_not_called()

    def test_missing_token(self, setup_attrs):
        mock_test_method.return_value = None

        input_data = {
            "jsonrpc": "2.0",
            "method": TEST_METHOD_NAME,
            "params": {},
        }
        with pytest.raises(PermissionDenied, match="Unable to authenticate API via token."):
            self.client.post(
                "/internal_api/v1/rpcapi",
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                data=json.dumps(input_data),
            )

    def test_invalid_token(self, setup_attrs, signer: JWTSigner):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": signer.generate_signed_token({"method": "WRONG_METHOD_NAME"}),
        }
        data = {"jsonrpc": "1.0", "method": TEST_METHOD_NAME, "params": {}}

        with pytest.raises(
            PermissionDenied, match="Bad Signature. Please use only the tokens provided by the API."
        ):
            self.client.post("/internal_api/v1/rpcapi", headers=headers, data=json.dumps(data))

    def test_missing_accept(self, setup_attrs, signer: JWTSigner):
        headers = {
            "Content-Type": "application/json",
            "Authorization": signer.generate_signed_token({"method": "WRONG_METHOD_NAME"}),
        }
        data = {"jsonrpc": "1.0", "method": TEST_METHOD_NAME, "params": {}}

        with pytest.raises(PermissionDenied, match="Expected Accept: application/json"):
            self.client.post("/internal_api/v1/rpcapi", headers=headers, data=json.dumps(data))

    def test_wrong_accept(self, setup_attrs, signer: JWTSigner):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/html",
            "Authorization": signer.generate_signed_token({"method": "WRONG_METHOD_NAME"}),
        }
        data = {"jsonrpc": "1.0", "method": TEST_METHOD_NAME, "params": {}}

        with pytest.raises(PermissionDenied, match="Expected Accept: application/json"):
            self.client.post("/internal_api/v1/rpcapi", headers=headers, data=json.dumps(data))
