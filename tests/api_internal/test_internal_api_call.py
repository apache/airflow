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
import requests

from airflow.api_internal.internal_api_call import InternalApiConfig, internal_api_call
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.settings import _ENABLE_AIP_44
from airflow.utils.state import State
from tests.test_utils.config import conf_vars


@pytest.fixture(autouse=True)
def reset_init_api_config():
    InternalApiConfig._initialized = False


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
class TestInternalApiConfig:
    @conf_vars(
        {
            ("core", "database_access_isolation"): "false",
            ("core", "internal_api_url"): "http://localhost:8888",
        }
    )
    def test_get_use_internal_api_disabled(self):
        assert InternalApiConfig.get_use_internal_api() is False

    @conf_vars(
        {
            ("core", "database_access_isolation"): "true",
            ("core", "internal_api_url"): "http://localhost:8888",
        }
    )
    def test_get_use_internal_api_enabled(self):
        assert InternalApiConfig.get_use_internal_api() is True
        assert InternalApiConfig.get_internal_api_endpoint() == "http://localhost:8888/internal_api/v1/rpcapi"

    @conf_vars(
        {
            ("core", "database_access_isolation"): "true",
            ("core", "internal_api_url"): "http://localhost:8888",
        }
    )
    def test_force_database_direct_access(self):
        InternalApiConfig.force_database_direct_access()
        assert InternalApiConfig.get_use_internal_api() is False


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
class TestInternalApiCall:
    @staticmethod
    @internal_api_call
    def fake_method() -> str:
        return "local-call"

    @staticmethod
    @internal_api_call
    def fake_method_with_params(dag_id: str, task_id: int, session) -> str:
        return f"local-call-with-params-{dag_id}-{task_id}"

    @classmethod
    @internal_api_call
    def fake_class_method_with_params(cls, dag_id: str, session) -> str:
        return f"local-classmethod-call-with-params-{dag_id}"

    @staticmethod
    @internal_api_call
    def fake_class_method_with_serialized_params(
        ti: TaskInstance | TaskInstancePydantic,
        session,
    ) -> str:
        return f"local-classmethod-call-with-serialized-{ti.task_id}"

    @conf_vars(
        {
            ("core", "database_access_isolation"): "false",
            ("core", "internal_api_url"): "http://localhost:8888",
        }
    )
    @mock.patch("airflow.api_internal.internal_api_call.requests")
    def test_local_call(self, mock_requests):
        result = TestInternalApiCall.fake_method()

        assert result == "local-call"
        mock_requests.post.assert_not_called()

    @conf_vars(
        {
            ("core", "database_access_isolation"): "true",
            ("core", "internal_api_url"): "http://localhost:8888",
        }
    )
    @mock.patch("airflow.api_internal.internal_api_call.requests")
    def test_remote_call(self, mock_requests):
        response = requests.Response()
        response.status_code = 200

        response._content = json.dumps(BaseSerialization.serialize("remote-call"))

        mock_requests.post.return_value = response

        result = TestInternalApiCall.fake_method()
        assert result == "remote-call"
        expected_data = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "tests.api_internal.test_internal_api_call.TestInternalApiCall.fake_method",
                "params": json.dumps(BaseSerialization.serialize({})),
            }
        )
        mock_requests.post.assert_called_once_with(
            url="http://localhost:8888/internal_api/v1/rpcapi",
            data=expected_data,
            headers={"Content-Type": "application/json"},
        )

    @conf_vars(
        {
            ("core", "database_access_isolation"): "true",
            ("core", "internal_api_url"): "http://localhost:8888",
        }
    )
    @mock.patch("airflow.api_internal.internal_api_call.requests")
    def test_remote_call_with_params(self, mock_requests):
        response = requests.Response()
        response.status_code = 200

        response._content = json.dumps(BaseSerialization.serialize("remote-call"))

        mock_requests.post.return_value = response

        result = TestInternalApiCall.fake_method_with_params("fake-dag", task_id=123, session="session")

        assert result == "remote-call"
        expected_data = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "tests.api_internal.test_internal_api_call.TestInternalApiCall."
                "fake_method_with_params",
                "params": json.dumps(
                    BaseSerialization.serialize(
                        {
                            "dag_id": "fake-dag",
                            "task_id": 123,
                        }
                    )
                ),
            }
        )
        mock_requests.post.assert_called_once_with(
            url="http://localhost:8888/internal_api/v1/rpcapi",
            data=expected_data,
            headers={"Content-Type": "application/json"},
        )

    @conf_vars(
        {
            ("core", "database_access_isolation"): "true",
            ("core", "internal_api_url"): "http://localhost:8888",
        }
    )
    @mock.patch("airflow.api_internal.internal_api_call.requests")
    def test_remote_classmethod_call_with_params(self, mock_requests):
        response = requests.Response()
        response.status_code = 200

        response._content = json.dumps(BaseSerialization.serialize("remote-call"))

        mock_requests.post.return_value = response

        result = TestInternalApiCall.fake_class_method_with_params("fake-dag", session="session")

        assert result == "remote-call"
        expected_data = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "tests.api_internal.test_internal_api_call.TestInternalApiCall."
                "fake_class_method_with_params",
                "params": json.dumps(
                    BaseSerialization.serialize(
                        {
                            "dag_id": "fake-dag",
                        }
                    )
                ),
            }
        )
        mock_requests.post.assert_called_once_with(
            url="http://localhost:8888/internal_api/v1/rpcapi",
            data=expected_data,
            headers={"Content-Type": "application/json"},
        )

    @conf_vars(
        {
            ("core", "database_access_isolation"): "true",
            ("core", "internal_api_url"): "http://localhost:8888",
        }
    )
    @mock.patch("airflow.api_internal.internal_api_call.requests")
    def test_remote_call_with_serialized_model(self, mock_requests):
        response = requests.Response()
        response.status_code = 200

        response._content = json.dumps(BaseSerialization.serialize("remote-call"))

        mock_requests.post.return_value = response
        ti = TaskInstance(task=EmptyOperator(task_id="task"), run_id="run_id", state=State.RUNNING)

        result = TestInternalApiCall.fake_class_method_with_serialized_params(ti, session="session")

        assert result == "remote-call"
        expected_data = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "tests.api_internal.test_internal_api_call.TestInternalApiCall."
                "fake_class_method_with_serialized_params",
                "params": json.dumps(BaseSerialization.serialize({"ti": ti}, use_pydantic_models=True)),
            }
        )
        mock_requests.post.assert_called_once_with(
            url="http://localhost:8888/internal_api/v1/rpcapi",
            data=expected_data,
            headers={"Content-Type": "application/json"},
        )
