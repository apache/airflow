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
        connections_prefix=None,
        variables_prefix=None,
        profile_name=None,
        sep=None,
        **kwargs
    ):
        super().__init__(**kwargs)

        if connections_prefix is not None:
            self.connections_prefix = connections_prefix.rstrip('/')
        if variables_prefix is not None:
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

    def _get_user_and_password(self, secret):
        for user_denomination in ['user', 'username', 'login']:
            if user_denomination in secret:
                user = secret[user_denomination]

        for password_denomination in ['pass', 'password', 'key']:
            if password_denomination in secret:
                password = secret[password_denomination]

        return user, password

    def _get_host(self, secret):
        for host_denomination in ['host', 'remote_host', 'server']:
            if host_denomination in secret:
                host = secret[host_denomination]

        return host

    def _get_conn_type(self, secret):
        for type_word in ['conn_type', 'conn_id', 'connection_type', 'engine']:
            if type_word in secret:
                conn_type = secret[type_word]
                conn_type = 'postgresql' if conn_type == 'redshift' else conn_type
            else:
                conn_type = 'connection'
        return conn_type

    def _get_port_and_database(self, secret):
        if 'port' in secret:
            port = secret['port']
        else:
            port = 5432
        if 'database' in secret:
            database = secret['database']
        else:
            database = ''

        return port, database

    def _get_extra(self, secret, conn_string):
        if 'extra' in secret:
            extra_dict = ast.literal_eval(secret['extra'])
            counter = 0
            for key, value in extra_dict.values():
                if counter == 0:
                    conn_string += f'{key}={value}'
                else:
                    conn_string += f'&{key}={value}'

                counter += 1

        return conn_string

    def get_conn_uri(self, conn_id: str):
        """
        Get Connection Value

        :param conn_id: connection id
        :type conn_id: str
        """
        if self.connections_prefix and self.sep:
            conn_id = self.build_path(self.connections_prefix, conn_id, self.sep)

        try:
            secret_string = self._get_secret(conn_id)
            secret = ast.literal_eval(secret_string)
        except ValueError:  # 'malformed node or string: ' error, for empty conns
            connection = None
            secret = None

        # These lines will check if we have with some denomination stored an username, password and host
        if secret:
            try:
                user, password = self._get_user_and_password(secret)
                host = self._get_host(secret)
                if user and password and host:
                    conn_type = self._get_conn_type(secret)
                    port, database = self._get_port_and_database(secret)

                    conn_string = f'{conn_type}://{user}:{password}@{host}:{port}/{database}'

                    connection = self._get_extra(secret, conn_string)
            except UnboundLocalError:
                conn_string = self._get_secret(conn_id)
                connection = f'{{{conn_string[:-1]}}}'  # without this line, the secret_string
                # will end with a bracket } # and the literal_eval won't work

        return connection

    def get_variable(self, key: str):
        """
        Get Airflow Variable from Environment Variable

        :param key: Variable Key
        :return: Variable Value
        """
        if self.variables_prefix and self.sep:
            key = self.build_path(self.variables_prefix, key, self.sep)
        return self._get_secret(key)

    def _get_secret(self, secret_id: str):
        """
        Get secret value from Secrets Manager
        :param secret_id: Secret Key
        :type secret_id: str
        """
        try:
            response = self.client.get_secret_value(
                SecretId=secret_id,
            )
            return response.get('SecretString')
        except self.client.exceptions.ResourceNotFoundException:
            self.log.debug(
                "An error occurred (ResourceNotFoundException) when calling the "
                "get_secret_value operation: "
                "Secret %s not found.", secret_id
            )
            return None
