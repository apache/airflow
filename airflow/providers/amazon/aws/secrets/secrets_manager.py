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
"""Objects relating to sourcing secrets from AWS Secrets Manager"""

import ast
from typing import Optional
from urllib.parse import urlencode

import boto3

try:
    from functools import cached_property
except ImportError:
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
    If variables prefix is ``airflow/variables/hello``, this would be accessible
    if you provide ``{"variables_prefix": "airflow/variables"}`` and request variable key ``hello``.
    And if config_prefix is ``airflow/config/sql_alchemy_conn``, this would be accessible
    if you provide ``{"config_prefix": "airflow/config"}`` and request config
    key ``sql_alchemy_conn``.

    You can also pass additional keyword arguments like ``aws_secret_access_key``, ``aws_access_key_id``
    or ``region_name`` to this class and they would be passed on to Boto3 client.

    There are two ways of storing secrets in Secret Manager for using them with this operator:
    storing them as a conn URI in one field, or taking advantage of native approach of Secrets Manager
    and storing them in multiple fields. There are certain words that will be searched in the name
    of fields for trying to retrieve a connection part. Those words are:

    .. code-block:: python

        possible_words_for_conn_fields = {
            "user": ["user", "username", "login", "user_name"],
            "password": ["password", "pass", "key"],
            "host": ["host", "remote_host", "server"],
            "port": ["port"],
            "schema": ["database", "schema"],
            "conn_type": ["conn_type", "conn_id", "connection_type", "engine"],
        }

    However, these lists can be extended using the configuration parameter ``extra_conn_words``.

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
        If set to None (null), requests for connections will not be sent to AWS Secrets Manager
    :type connections_prefix: str
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null), requests for variables will not be sent to AWS Secrets Manager
    :type variables_prefix: str
    :param config_prefix: Specifies the prefix of the secret to read to get Configurations.
        If set to None (null), requests for configurations will not be sent to AWS Secrets Manager
    :type config_prefix: str
    :param profile_name: The name of a profile to use. If not given, then the default profile is used.
    :type profile_name: str
    :param sep: separator used to concatenate secret_prefix and secret_id. Default: "/"
    :type sep: str
    :param full_url_mode: if True, the secrets must be stored as one conn URI in just one field per secret.
        Otherwise, you can store the secret using different fields (password, user...)
    :type full_url_mode: bool
    :param extra_conn_words: for using just when you set full_url_mode as False and store
        the secrets in different fields of secrets manager. You can add more words for each connection
        part beyond the default ones. The extra words to be searched should be passed as a dict of lists,
        each list corresponding to a connection part. The optional keys of the dict must be: user,
        password, host, schema, conn_type.
    :type extra_conn_words: dict
    """

    def __init__(
        self,
        connections_prefix: str = 'airflow/connections',
        variables_prefix: str = 'airflow/variables',
        config_prefix: str = 'airflow/config',
        profile_name: Optional[str] = None,
        sep: str = "/",
        full_url_mode: bool = True,
        extra_conn_words: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__()
        if connections_prefix is not None:
            self.connections_prefix = connections_prefix.rstrip(sep)
        else:
            self.connections_prefix = connections_prefix
        if variables_prefix is not None:
            self.variables_prefix = variables_prefix.rstrip(sep)
        else:
            self.variables_prefix = variables_prefix
        if config_prefix is not None:
            self.config_prefix = config_prefix.rstrip(sep)
        else:
            self.config_prefix = config_prefix
        self.profile_name = profile_name
        self.sep = sep
        self.full_url_mode = full_url_mode
        self.extra_conn_words = extra_conn_words if extra_conn_words else {}
        self.kwargs = kwargs

    @cached_property
    def client(self):
        """Create a Secrets Manager client"""
        session = boto3.session.Session(profile_name=self.profile_name)

        return session.client(service_name="secretsmanager", **self.kwargs)

    def _format_uri_with_extra(self, secret, conn_string):
        try:
            extra_dict = secret['extra']
        except KeyError:
            return conn_string
        conn_string = f"{conn_string}?{urlencode(extra_dict)}"

        return conn_string

    def get_uri_from_secret(self, secret):
        possible_words_for_conn_fields = {
            'user': ['user', 'username', 'login', 'user_name'],
            'password': ['password', 'pass', 'key'],
            'host': ['host', 'remote_host', 'server'],
            'port': ['port'],
            'schema': ['database', 'schema'],
            'conn_type': ['conn_type', 'conn_id', 'connection_type', 'engine'],
        }

        for conn_field, extra_words in self.extra_conn_words.items():
            possible_words_for_conn_fields[conn_field].extend(extra_words)

        conn_d = {}
        for conn_field, possible_words in possible_words_for_conn_fields.items():
            try:
                conn_d[conn_field] = [v for k, v in secret.items() if k in possible_words][0]
            except IndexError:
                conn_d[conn_field] = ''

        conn_string = "{conn_type}://{user}:{password}@{host}:{port}/{schema}".format(**conn_d)

        connection = self._format_uri_with_extra(secret, conn_string)

        return connection

    def get_conn_uri(self, conn_id: str):
        """
        Get Connection Value

        :param conn_id: connection id
        :type conn_id: str
        """
        if self.full_url_mode:
            if self.connections_prefix is None:
                return None

            return self._get_secret(self.connections_prefix, conn_id)
        else:
            try:
                secret_string = self._get_secret(self.connections_prefix, conn_id)
                secret = ast.literal_eval(secret_string)  # json.loads gives error
            except ValueError:  # 'malformed node or string: ' error, for empty conns
                connection = None
                secret = None

            # These lines will check if we have with some denomination stored an username, password and host
            if secret:
                connection = self.get_uri_from_secret(secret)

            return connection

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get Airflow Variable from Environment Variable
        :param key: Variable Key
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None

        return self._get_secret(self.variables_prefix, key)

    def get_config(self, key: str) -> Optional[str]:
        """
        Get Airflow Configuration
        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        if self.config_prefix is None:
            return None

        return self._get_secret(self.config_prefix, key)

    def _get_secret(self, path_prefix, secret_id: str) -> Optional[str]:
        """
        Get secret value from Secrets Manager
        :param path_prefix: Prefix for the Path to get Secret
        :type path_prefix: str
        :param secret_id: Secret Key
        :type secret_id: str
        """
        secrets_path = self.build_path(path_prefix, secret_id, self.sep)

        try:
            response = self.client.get_secret_value(
                SecretId=secrets_path,
            )
            return response.get('SecretString')
        except self.client.exceptions.ResourceNotFoundException:
            self.log.debug(
                "An error occurred (ResourceNotFoundException) when calling the "
                "get_secret_value operation: "
                "Secret %s not found.",
                secret_id,
            )
            return None
