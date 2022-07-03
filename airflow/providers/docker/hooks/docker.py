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

from typing import Any

from docker import APIClient  # type: ignore[attr-defined]
from docker.constants import DEFAULT_TIMEOUT_SECONDS  # type: ignore[attr-defined]
from docker.errors import APIError  # type: ignore[attr-defined]

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class DockerHook(BaseHook, LoggingMixin):
    """
    Interact with a Docker Daemon or Registry.

    :param docker_conn_id: The :ref:`Docker connection id <howto/connection:docker>`
        where credentials and extra configuration are stored
    """

    conn_name_attr = 'docker_conn_id'
    default_conn_name = 'docker_default'
    conn_type = 'docker'
    hook_name = 'Docker'

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
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
        docker_conn_id: str | None = default_conn_name,
        base_url: str | None = None,
        version: str | None = None,
        tls: str | None = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__()
        if not base_url:
            raise AirflowException('No Docker base URL provided')
        if not version:
            raise AirflowException('No Docker API version provided')

        if not docker_conn_id:
            raise AirflowException('No Docker connection id provided')

        conn = self.get_connection(docker_conn_id)

        if not conn.host:
            raise AirflowException('No Docker URL provided')
        if not conn.login:
            raise AirflowException('No username provided')
        extra_options = conn.extra_dejson

        self.__base_url = base_url
        self.__version = version
        self.__tls = tls
        self.__timeout = timeout
        if conn.port:
            self.__registry = f"{conn.host}:{conn.port}"
        else:
            self.__registry = conn.host
        self.__username = conn.login
        self.__password = conn.password
        self.__email = extra_options.get('email')
        self.__reauth = extra_options.get('reauth') != 'no'

    def get_conn(self) -> APIClient:
        client = APIClient(
            base_url=self.__base_url, version=self.__version, tls=self.__tls, timeout=self.__timeout
        )
        self.__login(client)
        return client

    def __login(self, client) -> None:
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
