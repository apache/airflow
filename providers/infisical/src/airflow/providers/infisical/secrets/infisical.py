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
"""Objects relating to sourcing connections, variables, and configs from Infisical."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.infisical._internal_client.infisical_client import _InfisicalClient
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class InfisicalBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connections, Variables, and Configs from Infisical.

    Configurable via ``airflow.cfg`` as follows:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.infisical.secrets.infisical.InfisicalBackend
        backend_kwargs = {
          "connections_path": "/connections",
          "variables_path": "/variables",
          "configs_path": "/configs",
          "url": "https://app.infisical.com",
          "auth_type": "universal-auth",
          "universal_auth_client_id": "1982a5d0-e5ce-4512-a92d-f56442d01f99",
          "universal_auth_client_secret": "158b424b24b256f366620e40937c326d70aa5a24f0757d12e108803532c66055",
          "project_id": "abd70853-ecdf-4a79-b465-cc0a380b820b",
          "environment_slug": "dev"
        }
    """

    def __init__(
        self,
        # Secret paths
        connections_path: str | None = None,  # /connections
        variables_path: str | None = None,  # /variables
        config_path: str | None = None,  # /configs
        # Infisical project config
        url: str | None = None,
        project_id: str | None = None,
        environment_slug: str | None = None,
        # Infisical auth config
        auth_type: str = "universal-auth",
        universal_auth_client_id: str | None = None,
        universal_auth_client_secret: str | None = None,
        **kwargs,
    ):
        super().__init__()
        if project_id is None:
            raise AirflowException("project_id is required")
        if environment_slug is None:
            raise AirflowException("environment_slug is required")

        self.infisical_client = _InfisicalClient(
            url=url,
            auth_type=auth_type,
            universal_auth_client_id=universal_auth_client_id,
            universal_auth_client_secret=universal_auth_client_secret,
            **kwargs,
        )

        self.project_id = project_id
        self.environment_slug = environment_slug
        self.connections_path = connections_path
        self.variables_path = variables_path
        self.config_path = config_path

    # Make sure connection is imported this way for type checking, otherwise when importing
    # the backend it will get a circular dependency and fail
    if TYPE_CHECKING:
        from airflow.models.connection import Connection

    def get_connection(self, conn_id: str) -> Connection | None:
        from airflow.models.connection import Connection

        if self.connections_path is None or conn_id is None:
            return None

        try:
            secret_value = self.infisical_client.get_secret_value(
                secret_path=self.connections_path,
                project_id=self.project_id,
                environment_slug=self.environment_slug,
                secret_name=conn_id,
            )

            if secret_value is None:
                return None

            conn_params = json.loads(secret_value)
            if not isinstance(conn_params, dict):
                raise NotADirectoryError(f"Connection data is JSON but not a dictionary: {type(conn_params)}")

            uri = conn_params.get("conn_uri")
            if uri:
                return Connection(conn_id, uri=uri)

            return Connection(conn_id=conn_id, **conn_params)

        except Exception as e:
            if isinstance(e, json.JSONDecodeError):
                raise AirflowException(
                    f"The connection {conn_id} is not a valid JSON object. Please check if the value within Infisical is a valid JSON object."
                )
            elif isinstance(e, NotADirectoryError):
                raise e

            raise AirflowException(f"Connection {conn_id} not found: {str(e)}")

    def get_variable(self, key: str) -> str | None:
        """
        Get Airflow Variable.

        :param key: Variable Key
        :return: Variable Value retrieved from Infisical
        """
        if self.variables_path is None or key is None:
            return None

        secretValue = self.infisical_client.get_secret_value(
            secret_path=self.variables_path,
            project_id=self.project_id,
            environment_slug=self.environment_slug,
            secret_name=key,
        )

        return secretValue if secretValue else None

    def get_config(self, key: str) -> str | None:
        """
        Get Airflow Configuration.

        :param key: Configuration Option Key
        :return: Configuration Option Value retrieved from Infisical
        """
        if self.config_path is None or key is None:
            return None
        else:
            secretValue = self.infisical_client.get_secret_value(
                secret_path=self.config_path,
                project_id=self.project_id,
                environment_slug=self.environment_slug,
                secret_name=key,
            )
        return secretValue if secretValue else None
