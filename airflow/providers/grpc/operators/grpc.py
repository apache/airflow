#
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

from typing import TYPE_CHECKING, Any, Callable, Sequence

from airflow.models import BaseOperator
from airflow.providers.grpc.hooks.grpc import GrpcHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GrpcOperator(BaseOperator):
    """
    Calls a gRPC endpoint to execute an action.

    :param stub_class: The stub client to use for this gRPC call
    :param call_func: The client function name to call the gRPC endpoint
    :param grpc_conn_id: The connection to run the operator against
    :param data: The data to pass to the rpc call
    :param interceptors: A list of gRPC interceptor objects to be used on the channel
    :param custom_connection_func: The customized connection function to return channel object.
        A callable that accepts the connection as its only arg.
    :param streaming: A flag to indicate if the call is a streaming call
    :param response_callback: The callback function to process the response from gRPC call,
        takes in response object and context object, context object can be used to perform
        push xcom or other after task actions
    :param log_response: A flag to indicate if we need to log the response
    """

    template_fields: Sequence[str] = ("stub_class", "call_func", "data")
    template_fields_renderers = {"data": "py"}

    def __init__(
        self,
        *,
        stub_class: Callable,
        call_func: str,
        grpc_conn_id: str = "grpc_default",
        data: dict | None = None,
        interceptors: list[Callable] | None = None,
        custom_connection_func: Callable | None = None,
        streaming: bool = False,
        response_callback: Callable | None = None,
        log_response: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.stub_class = stub_class
        self.call_func = call_func
        self.grpc_conn_id = grpc_conn_id
        self.data = data or {}
        self.interceptors = interceptors
        self.custom_connection_func = custom_connection_func
        self.streaming = streaming
        self.log_response = log_response
        self.response_callback = response_callback

    def _get_grpc_hook(self) -> GrpcHook:
        return GrpcHook(
            self.grpc_conn_id,
            interceptors=self.interceptors,
            custom_connection_func=self.custom_connection_func,
        )

    def execute(self, context: Context) -> None:
        hook = self._get_grpc_hook()
        self.log.info("Calling gRPC service")

        # grpc hook always yield
        responses = hook.run(self.stub_class, self.call_func, streaming=self.streaming, data=self.data)

        for response in responses:
            self._handle_response(response, context)

    def _handle_response(self, response: Any, context: Context) -> None:
        if self.log_response:
            self.log.info("%r", response)
        if self.response_callback:
            self.response_callback(response, context)
