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

from airflow.providers.grpc.operators.grpc import GrpcOperator


class StubClass:
    def __init__(self, channel):
        pass

    def stream_call(self, data):
        pass


class TestGrpcOperator:
    def custom_conn_func(self, connection):
        pass

    @mock.patch("airflow.providers.grpc.operators.grpc.GrpcHook")
    def test_with_interceptors(self, mock_hook):
        operator = GrpcOperator(
            stub_class=StubClass,
            call_func="stream_call",
            interceptors=[],
            task_id="test_grpc",
        )

        operator.execute({})
        mock_hook.assert_called_once_with("grpc_default", interceptors=[], custom_connection_func=None)

    @mock.patch("airflow.providers.grpc.operators.grpc.GrpcHook")
    def test_with_custom_connection_func(self, mock_hook):
        operator = GrpcOperator(
            stub_class=StubClass,
            call_func="stream_call",
            custom_connection_func=self.custom_conn_func,
            task_id="test_grpc",
        )

        operator.execute({})
        mock_hook.assert_called_once_with(
            "grpc_default", interceptors=None, custom_connection_func=self.custom_conn_func
        )

    @mock.patch("airflow.providers.grpc.operators.grpc.GrpcHook")
    def test_execute_with_log(self, mock_hook):
        mocked_hook = mock.Mock()
        mock_hook.return_value = mocked_hook
        mocked_hook.configure_mock(**{"run.return_value": ["value1", "value2"]})
        operator = GrpcOperator(
            stub_class=StubClass,
            call_func="stream_call",
            log_response=True,
            task_id="test_grpc",
        )

        with mock.patch.object(operator.log, "info") as mock_info:
            operator.execute({})

            mock_hook.assert_called_once_with("grpc_default", interceptors=None, custom_connection_func=None)
            mocked_hook.run.assert_called_once_with(StubClass, "stream_call", data={}, streaming=False)
            mock_info.assert_any_call("Calling gRPC service")
            mock_info.assert_any_call("%r", "value1")
            mock_info.assert_any_call("%r", "value2")

    @mock.patch("airflow.providers.grpc.operators.grpc.GrpcHook")
    def test_execute_with_callback(self, mock_hook):
        mocked_hook = mock.Mock()
        callback = mock.Mock()
        mock_hook.return_value = mocked_hook
        mocked_hook.configure_mock(**{"run.return_value": ["value1", "value2"]})
        operator = GrpcOperator(
            stub_class=StubClass, call_func="stream_call", task_id="test_grpc", response_callback=callback
        )

        with mock.patch.object(operator.log, "info") as mock_info:
            operator.execute({})
            mock_hook.assert_called_once_with("grpc_default", interceptors=None, custom_connection_func=None)
            mocked_hook.run.assert_called_once_with(StubClass, "stream_call", data={}, streaming=False)
            assert ("'value1'", "'value2'") not in mock_info.call_args_list
            mock_info.assert_any_call("Calling gRPC service")
            callback.assert_any_call("value1", {})
            callback.assert_any_call("value2", {})
