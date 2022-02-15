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

from enum import Enum

import pulsar

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AuthProvider(Enum):
    TLS: str = "tls"
    JWT: str = "jwt"
    ATHENZ: str = "athenz"
    OAUTH: str = "oauth"


class PulsarHook(BaseHook):
    """
    Interact with Apache Pulsar instance using `pulsar-client`.
    Hook attribute `conn` returns the client

    .. seealso::
        - https://pulsar.apache.org/docs/en/client-libraries-python/

    .. seealso::
        :class:`~airflow.providers.apache.pulsar.hooks.pulsar.PulsarHook`

    :param pulsar_conn_id: The connection id to the Pulsar instance
    """

    conn_name_attr = 'pulsar_conn_id'
    default_conn_name = 'pulsar_default'
    conn_type = 'pulsar'
    hook_name = 'Apache Pulsar'

    def __init__(self, pulsar_conn_id: str = 'pulsar_default') -> None:

        super().__init__()
        self.pulsar_conn_id = pulsar_conn_id
        self.client = self._create_client()

    def get_conn(self) -> pulsar.Client:
        """Returns a connection object"""
        return self.client

    def get_conn_url(self) -> str:
        """Get Pulsar connection url"""
        conn = self.get_connection(self.pulsar_conn_id)

        host = 'localhost' if not conn.host else conn.host
        port = 6650 if not conn.port else conn.port
        conn_type = 'pulsar' if not conn.conn_type else conn.conn_type

        use_tls = conn.extra_dejson.get("use_tls", False)
        if use_tls and 'ssl' not in conn.conn_type:
            conn_type += "+ssl"

        servers = map(lambda h: h if ':' in h else f'{h}:{port}', host.split(','))

        return f"{conn_type}://{','.join(servers)}"

    def _create_client(self) -> pulsar.Client:
        """Create and return Pulsar client"""
        conn = self.get_connection(self.pulsar_conn_id)
        extras = conn.extra_dejson
        auth_type = AuthProvider(conn.schema)

        if auth_type.value == 'tls':
            if not conn.login or not conn.password:
                raise AirflowException(
                    "For TLS Authentication 'certificate_path' should be passed to login field "
                    "and 'private_key_path' to password field."
                )
            auth = pulsar.AuthenticationTLS(certificate_path=conn.login, private_key_path=conn.password)
        elif auth_type.value == 'jwt':
            if not conn.password:
                raise AirflowException(
                    "For JWT (json webt token) Authentication token should be passed to password field."
                )
            auth = pulsar.AuthenticationToken(token=conn.password)
        elif auth_type.value == 'athenz':
            if not conn.login:
                raise AirflowException(
                    "For Athenz Authentication 'auth_params' should be passed to login field."
                )
            auth = pulsar.AuthenticationAthenz(auth_params_string=conn.login)
        elif auth_type.value == 'oauth':
            if not conn.login:
                raise AirflowException(
                    "For Oauth2 Authentication 'auth_params' should be passed to login field."
                )
            auth = pulsar.AuthenticationOauth2(auth_params_string=conn.login)
        else:
            if conn.login or conn.password:
                raise AirflowException("For absent Authentication login password fields should be empty.")
            auth = None

        supported_extras = {
            'operation_timeout_seconds',
            'io_threads',
            'message_listener_threads',
            'concurrent_lookup_requests',
            'use_tls',
            'tls_trust_certs_file_path',
            'tls_allow_insecure_connection',
            'tls_validate_hostname',
            'connection_timeout_ms',
        }
        not_supported_extras = set(extras.keys()).difference(supported_extras)
        if not_supported_extras:
            AirflowException(f"Parameters {not_supported_extras} from 'Extra' field are not supported")
        extras.pop("use_tls", None)

        pulsar_client = pulsar.Client(service_url=self.get_conn_url(), authentication=auth, **extras)

        return pulsar_client


__all__ = ['PulsarHook']
