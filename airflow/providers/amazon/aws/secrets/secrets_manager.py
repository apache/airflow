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
"""
Objects relating to sourcing secrets from AWS Secrets Manager
"""
import ast
from typing import Optional

import boto3
from cached_property import cached_property

from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class SecretsManagerBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection or Variables from AWS Secrets Manager

    Configurable via ``airflow.cfg`` like so:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
        backend_kwargs = {"connections_prefix": "airflow/connections"}

    For example, if secrets prefix is ``airflow/connections/smtp_default``, this would be accessible
    if you provide ``{"connections_prefix": "airflow/connections"}`` and request conn_id ``smtp_default``.
    And if variables prefix is ``airflow/variables/hello``, this would be accessible
    if you provide ``{"variables_prefix": "airflow/variables"}`` and request variable key ``hello``.

    You can also pass additional keyword arguments like ``aws_secret_access_key``, ``aws_access_key_id``
    or ``region_name`` to this class and they would be passed on to Boto3 client.

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
    :type connections_prefix: str
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
    :type variables_prefix: str
    :param profile_name: The name of a profile to use. If not given, then the default profile is used.
    :type profile_name: str
    :param sep: separator used to concatenate secret_prefix and secret_id. Default: "/"
    :type sep: str
    """

    def __init__(
        self,
        connections_prefix: Optional[str] = None,
        variables_prefix: Optional[str] = None,
        profile_name: Optional[str] = None,
        sep: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        if connections_prefix:
            self.connections_prefix = connections_prefix.rstrip('/')
        if variables_prefix:
            self.variables_prefix = variables_prefix.rstrip('/')
        self.profile_name = profile_name
        self.sep = sep
        self.kwargs = kwargs

    @cached_property
    def client(self):
        """
        Create a Secrets Manager client
        """
        session = boto3.session.Session(
            profile_name=self.profile_name,
        )
        return session.client(service_name="secretsmanager", **self.kwargs)

    def get_conn_uri(self, conn_id: str) -> Optional[str]:
        """
        Get Connection Value

        :param conn_id: connection id
        :type conn_id: str
        """
        if self.connections_prefix and self.sep:
            conn_id = self.build_path(self.connections_prefix, conn_id, self.sep)

        try:
            secret_string = self._get_secret(conn_id)
            if secret_string is not None:
                secret = ast.literal_eval(secret_string)
                user = secret['user']
                password = secret['pass']
                host = secret['host']
                port = secret['port']
                database = secret['database']
                engine = secret['engine']

                if engine in ('redshift', 'postgresql'):
                    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
                else:
                    conn_string = f'mysql://{user}:{password}@{host}:{port}/{database}'
                return conn_string
        except KeyError:
            return self._get_secret(conn_id)

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get Airflow Variable from Environment Variable

        :param key: Variable Key
        :return: Variable Value
        """
        if self.variables_prefix and self.sep:
            key = self.build_path(self.variables_prefix, key, self.sep)
        return self._get_secret(key)

    def _get_secret(self, secret_id: str) -> Optional[str]:
        """
        Get secret value from Secrets Manager

        :param secret_id: Secret Key, including prefix if exists
        :type secret_id: str
        """

        try:
            response = self.client.get_secret_value(
                SecretId=secret_id
            )
            return response.get('SecretString')
        except self.client.exceptions.ResourceNotFoundException:
            self.log.debug(
                "An error occurred (ResourceNotFoundException) when calling the "
                "get_secret_value operation: "
                "Secret %s not found.", secret_id
            )
            return None
