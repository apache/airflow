# -*- coding: utf-8 -*-

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

import grpc
from google import auth as google_auth
from google.auth import jwt as google_auth_jwt
from google.auth.transport import grpc as google_auth_transport_grpc
from google.auth.transport import requests as google_auth_transport_requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowConfigException


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

    def __init__(self, grpc_conn_id, interceptors=None, custom_connection_func=None):
        self.grpc_conn_id = grpc_conn_id
        self.conn = self.get_connection(self.grpc_conn_id)
        self.extras = self.conn.extra_dejson
        self.interceptors = interceptors if interceptors else []
        self.custom_connection_func = custom_connection_func

    def get_conn(self):
        base_url = self.conn.host

        if self.conn.port:
            base_url = base_url + ":" + str(self.conn.port)

        auth_type = self._get_field("auth_type")

        if auth_type == "NO_AUTH":
            channel = grpc.insecure_channel(base_url)
        elif auth_type in {"SSL", "TLS"}:
            credential_file_name = self._get_field("credential_pem_file")
            creds = grpc.ssl_channel_credentials(open(credential_file_name).read())
            channel = grpc.secure_channel(base_url, creds)
        elif auth_type == "JWT_GOOGLE":
            credentials, _ = google_auth.default()
            jwt_creds = google_auth_jwt.OnDemandCredentials.from_signing_credentials(
                credentials)
            channel = google_auth_transport_grpc.secure_authorized_channel(
                jwt_creds, None, base_url)
        elif auth_type == "OATH_GOOGLE":
            scopes = self._get_field("scopes").split(",")
            credentials, _ = google_auth.default(scopes=scopes)
            request = google_auth_transport_requests.Request()
            channel = google_auth_transport_grpc.secure_authorized_channel(
                credentials, request, base_url)
        elif auth_type == "CUSTOM":
            if not self.custom_connection_func:
                raise AirflowConfigException(
                    "Customized connection function not set, not able to establish a channel")
            channel = self.custom_connection_func(self.conn)
        else:
            raise AirflowConfigException(
                "auth_type not supported or not provided, channel cannot be established,\
                given value: %s" % str(auth_type))

        if self.interceptors:
            for interceptor in self.interceptors:
                channel = grpc.intercept_channel(channel,
                                                 interceptor)

        return channel

    def run(self, stub_class, call_func, streaming=False, data=None):
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
                    for single_response in response:
                        yield single_response
            except grpc.RpcError as ex:
                # noinspection PyUnresolvedReferences
                self.log.exception(
                    "Error occurred when calling the grpc service: {0}, method: {1} \
                    status code: {2}, error details: {3}"
                    .format(stub.__class__.__name__,
                            call_func,
                            ex.code(),  # pylint: disable=no-member
                            ex.details()))  # pylint: disable=no-member
                raise ex

    def _get_field(self, field_name, default=None):
        """
        Fetches a field from extras, and returns it. This is some Airflow
        magic. The grpc hook type adds custom UI elements
        to the hook page, which allow admins to specify scopes, credential pem files, etc.
        They get formatted as shown below.
        """
        full_field_name = 'extra__grpc__{}'.format(field_name)
        if full_field_name in self.extras:
            return self.extras[full_field_name]
        else:
            return default
