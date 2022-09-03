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

import json
from typing import Any, Dict, Optional

from docker import APIClient
from docker.constants import DEFAULT_TIMEOUT_SECONDS
from docker.errors import APIError

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.docker.credentials.base import BaseDockerCredentialHelper
from airflow.providers.docker.credentials.connection import AirflowConnectionDockerCredentialHelper
from airflow.utils.module_loading import import_string


class DockerHook(BaseHook):
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
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema'],
            "relabeling": {
                'host': 'Registry URL',
                'login': 'Username',
            },
            "placeholders": {
                'extra': json.dumps(
                    {
                        'reauth': False,
                        'email': 'Jane.Doe@example.org',
                        'credential_helper': 'dotted.path.to.credential.Helper',
                        'credential_helper_kwargs': {"foo": "bar"},
                    }
                )
            },
        }

    def __init__(
        self,
        docker_conn_id: Optional[str] = default_conn_name,
        base_url: Optional[str] = None,
        version: Optional[str] = None,
        tls: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__()
        if not base_url:
            raise AirflowException('No Docker base URL provided')
        if not version:
            raise AirflowException('No Docker API version provided')

        if not docker_conn_id:
            raise AirflowException('No Docker connection id provided')
        self.docker_conn_id = docker_conn_id
        self.__base_url = base_url
        self.__version = version
        self.__tls = tls
        self.__timeout = timeout

    @cached_property
    def api_client(self) -> APIClient:
        """Create connection to docker host and login to the docker registries. (cached)"""
        conn = self.get_connection(self.docker_conn_id)
        client = APIClient(
            base_url=self.__base_url, version=self.__version, tls=self.__tls, timeout=self.__timeout
        )

        credential_helper = conn.extra_dejson.get("credential_helper")
        if not credential_helper:
            # If not specified credential helper than retrieve information from Connection.
            credential_helper = AirflowConnectionDockerCredentialHelper
            credential_helper_kwargs = {}
        else:
            credential_helper = import_string(credential_helper)
            credential_helper_kwargs = conn.extra_dejson.get("credential_helper_kwargs", {})

            if not issubclass(credential_helper, BaseDockerCredentialHelper):
                raise TypeError(
                    f"Your credential_helper `{credential_helper.__name__}` is not a subclass "
                    f"of `{BaseDockerCredentialHelper.__name__}`."
                )

        for creds in credential_helper(conn=conn, **credential_helper_kwargs).get_credentials() or []:
            try:
                self.log.info('Login into Docker Registry: %s', creds.registry)
                client.login(
                    username=creds.username,
                    password=creds.password,
                    registry=creds.registry,
                    email=creds.email,
                    reauth=creds.reauth,
                )
                self.log.debug('Login successful')
            except APIError as docker_error:
                self.log.error('Docker login failed: %s', str(docker_error))
                raise AirflowException(f'Docker login failed: {docker_error}')
        return client

    def get_conn(self) -> APIClient:
        """Create connection to docker host and login to the docker registries. (cached)"""
        return self.api_client
