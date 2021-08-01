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
from typing import Dict, Optional

from docker import APIClient, tls
from docker.errors import APIError

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


def get_client(
    docker_conn_id=None,
    docker_url=None,
    api_version=None,
    tls_ca_cert=None,
    tls_client_cert=None,
    tls_client_key=None,
    tls_ssl_version=None,
    tls_hostname=None,
):
    """
    Build an Docker client for use from network or connection configuration to use with a DockerClientHook

    :param docker_conn_id: The :ref:`Docker connection id <howto/connection:docker>`
    :type docker_conn_id: str
    :param docker_url: URL of the host running the docker daemon.
    :type docker_url: str
    :param api_version: Remote API version. Set to ``auto`` to automatically
        detect the server's version.
    :type api_version: str
    :param tls_ca_cert: Path to a PEM-encoded certificate authority
        to secure the docker connection.
    :type tls_ca_cert: str
    :param tls_client_cert: Path to the PEM-encoded certificate
        used to authenticate docker client.
    :type tls_client_cert: str
    :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
    :type tls_client_key: str
    :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
    :type tls_ssl_version: str
    :param tls_hostname: Hostname to match against
        the docker server certificate or False to disable the check.
    :type tls_hostname: str or bool
    """
    tls_config = None
    if tls_ca_cert and tls_client_cert and tls_client_key:
        # Ignore type error on SSL version here - it is deprecated and type annotation is wrong
        # it should be string
        tls_config = tls.TLSConfig(
            ca_cert=tls_ca_cert,
            client_cert=(tls_client_cert, tls_client_key),
            verify=True,
            ssl_version=tls_ssl_version,
            assert_hostname=tls_hostname,
        )
        docker_url = docker_url.replace('tcp://', 'https://')
    if docker_conn_id:
        hook = DockerHook(
            docker_conn_id=docker_conn_id,
            base_url=docker_url,
            version=api_version,
            tls=tls_config,
        )
        return hook.get_conn()
    else:
        return APIClient(base_url=docker_url, version=api_version, tls=tls_config)


class DockerHook(BaseHook, LoggingMixin):
    """
    Interact with a Docker Daemon or Registry.

    :param docker_conn_id: The :ref:`Docker connection id <howto/connection:docker>`
        where credentials and extra configuration are stored
    :type docker_conn_id: str
    """

    conn_name_attr = 'docker_conn_id'
    default_conn_name = 'docker_default'
    conn_type = 'docker'
    hook_name = 'Docker'

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema'],
            "relabeling": {
                'host': 'Registry URL',
                'login': 'Username',
            },
        }

    def __init__(
        self,
        docker_conn_id: str = default_conn_name,
        base_url: Optional[str] = None,
        version: Optional[str] = None,
        tls: Optional[str] = None,
    ) -> None:
        super().__init__()
        if not base_url:
            raise AirflowException('No Docker base URL provided')
        if not version:
            raise AirflowException('No Docker API version provided')

        conn = self.get_connection(docker_conn_id)
        if not conn.host:
            raise AirflowException('No Docker URL provided')
        if not conn.login:
            raise AirflowException('No username provided')
        extra_options = conn.extra_dejson

        self.__base_url = base_url
        self.__version = version
        self.__tls = tls
        if conn.port:
            self.__registry = f"{conn.host}:{conn.port}"
        else:
            self.__registry = conn.host
        self.__username = conn.login
        self.__password = conn.password
        self.__email = extra_options.get('email')
        self.__reauth = extra_options.get('reauth') != 'no'

    def get_conn(self) -> APIClient:
        client = APIClient(base_url=self.__base_url, version=self.__version, tls=self.__tls)
        self.__login(client)
        return client

    def __login(self, client) -> int:
        self.log.debug('Logging into Docker')
        try:
            client.login(
                username=self.__username,
                password=self.__password,
                registry=self.__registry,
                email=self.__email,
                reauth=self.__reauth,
            )
            self.log.debug('Login successful')
        except APIError as docker_error:
            self.log.error('Docker login failed: %s', str(docker_error))
            raise AirflowException(f'Docker login failed: {docker_error}')
