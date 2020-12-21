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

"""GRPC Hook"""
from typing import Any, Callable, Dict, Generator, List, Optional

import grpc
from google import auth as google_auth
from google.auth import jwt as google_auth_jwt
from google.auth.transport import (
    grpc as google_auth_transport_grpc,
    requests as google_auth_transport_requests,
)

from airflow.exceptions import AirflowConfigException
from airflow.hooks.base import BaseHook


class GrpcHook(BaseHook):
    """
    General interaction with gRPC servers.

    :param grpc_conn_id: The connection ID to use when fetching connection info.
    :type grpc_conn_id: str
    :param interceptors: a list of gRPC interceptor objects which would be applied
        to the connected gRPC channel. None by default.
    :type interceptors: a list of gRPC interceptors based on or extends the four
        official gRPC interceptors, eg, UnaryUnaryClientInterceptor,
        UnaryStreamClientInterceptor, StreamUnaryClientInterceptor,
        StreamStreamClientInterceptor.
    :param custom_connection_func: The customized connection function to return gRPC channel.
    :type custom_connection_func: python callable objects that accept the connection as
        its only arg. Could be partial or lambda.
    """

    conn_name_attr = 'grpc_conn_id'
    default_conn_name = 'grpc_default'
    conn_type = 'grpc'
    hook_name = 'GRPC Connection'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__grpc__auth_type": StringField(
                lazy_gettext('Grpc Auth Type'), widget=BS3TextFieldWidget()
            ),
            "extra__grpc__credential_pem_file": StringField(
                lazy_gettext('Credential Keyfile Path'), widget=BS3TextFieldWidget()
            ),
            "extra__grpc__scopes": StringField(
                lazy_gettext('Scopes (comma separated)'), widget=BS3TextFieldWidget()
            ),
        }

    def __init__(
        self,
        grpc_conn_id: str = default_conn_name,
        interceptors: Optional[List[Callable]] = None,
        custom_connection_func: Optional[Callable] = None,
    ) -> None:
        super().__init__()
        self.grpc_conn_id = grpc_conn_id
        self.conn = self.get_connection(self.grpc_conn_id)
        self.extras = self.conn.extra_dejson
        self.interceptors = interceptors if interceptors else []
        self.custom_connection_func = custom_connection_func

    def get_conn(self) -> grpc.Channel:
        base_url = self.conn.host

        if self.conn.port:
            base_url = base_url + ":" + str(self.conn.port)

        auth_type = self._get_field("auth_type")

        if auth_type == "NO_AUTH":
            channel = grpc.insecure_channel(base_url)
        elif auth_type in {"SSL", "TLS"}:
            credential_file_name = self._get_field("credential_pem_file")
            with open(credential_file_name, "rb") as credential_file:
                creds = grpc.ssl_channel_credentials(credential_file.read())
            channel = grpc.secure_channel(base_url, creds)
        elif auth_type == "JWT_GOOGLE":
            credentials, _ = google_auth.default()
            jwt_creds = google_auth_jwt.OnDemandCredentials.from_signing_credentials(credentials)
            channel = google_auth_transport_grpc.secure_authorized_channel(jwt_creds, None, base_url)
        elif auth_type == "OATH_GOOGLE":
            scopes = self._get_field("scopes").split(",")
            credentials, _ = google_auth.default(scopes=scopes)
            request = google_auth_transport_requests.Request()
            channel = google_auth_transport_grpc.secure_authorized_channel(credentials, request, base_url)
        elif auth_type == "CUSTOM":
            if not self.custom_connection_func:
                raise AirflowConfigException(
                    "Customized connection function not set, not able to establish a channel"
                )
            channel = self.custom_connection_func(self.conn)
        else:
            raise AirflowConfigException(
                "auth_type not supported or not provided, channel cannot be established,\
                given value: %s"
                % str(auth_type)
            )

        if self.interceptors:
            for interceptor in self.interceptors:
                channel = grpc.intercept_channel(channel, interceptor)

        return channel

    def run(
        self, stub_class: Callable, call_func: str, streaming: bool = False, data: Optional[dict] = None
    ) -> Generator:
        """Call gRPC function and yield response to caller"""
        if data is None:
            data = {}
        with self.get_conn() as channel:
            stub = stub_class(channel)
            try:
                rpc_func = getattr(stub, call_func)
                response = rpc_func(**data)
                if not streaming:
                    yield response
                else:
                    yield from response
            except grpc.RpcError as ex:
                self.log.exception(
                    "Error occurred when calling the grpc service: %s, method: %s \
                    status code: %s, error details: %s",
                    stub.__class__.__name__,
                    call_func,
                    ex.code(),  # pylint: disable=no-member
                    ex.details(),  # pylint: disable=no-member
                )
                raise ex

    def _get_field(self, field_name: str) -> str:
        """
        Fetches a field from extras, and returns it. This is some Airflow
        magic. The grpc hook type adds custom UI elements
        to the hook page, which allow admins to specify scopes, credential pem files, etc.
        They get formatted as shown below.
        """
        full_field_name = f'extra__grpc__{field_name}'
        return self.extras[full_field_name]
