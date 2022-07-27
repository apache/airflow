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
import json
import re
import warnings
from typing import Optional
from urllib.parse import urlencode

import boto3

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.utils import get_airflow_version
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


def _parse_version(val):
    val = re.sub(r'(\d+\.\d+\.\d+).*', lambda x: x.group(1), val)
    return tuple(int(x) for x in val.split('.'))


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

    However, these lists can be extended using the configuration parameter ``extra_conn_words``. Also,
    you can have a field named extra for extra parameters for the conn. Please note that this extra field
    must be a valid JSON.

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
        If set to None (null value in the configuration), requests for connections will not be
        sent to AWS Secrets Manager. If you don't want a connections_prefix, set it as an empty string
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null value in the configuration), requests for variables will not be sent to
        AWS Secrets Manager. If you don't want a variables_prefix, set it as an empty string
    :param config_prefix: Specifies the prefix of the secret to read to get Configurations.
        If set to None (null value in the configuration), requests for configurations will not be sent to
        AWS Secrets Manager. If you don't want a config_prefix, set it as an empty string
    :param profile_name: The name of a profile to use. If not given, then the default profile is used.
    :param sep: separator used to concatenate secret_prefix and secret_id. Default: "/"
    :param full_url_mode: if True, the secrets must be stored as one conn URI in just one field per secret.
        If False (set it as false in backend_kwargs), you can store the secret using different
        fields (password, user...).
    :param extra_conn_words: for using just when you set full_url_mode as false and store
        the secrets in different fields of secrets manager. You can add more words for each connection
        part beyond the default ones. The extra words to be searched should be passed as a dict of lists,
        each list corresponding to a connection part. The optional keys of the dict must be: user,
        password, host, schema, conn_type.
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
        if connections_prefix:
            self.connections_prefix = connections_prefix.rstrip(sep)
        else:
            self.connections_prefix = connections_prefix
        if variables_prefix:
            self.variables_prefix = variables_prefix.rstrip(sep)
        else:
            self.variables_prefix = variables_prefix
        if config_prefix:
            self.config_prefix = config_prefix.rstrip(sep)
        else:
            self.config_prefix = config_prefix
        self.profile_name = profile_name
        self.sep = sep
        self.full_url_mode = full_url_mode
        self.extra_conn_words = extra_conn_words or {}
        self.kwargs = kwargs

    @cached_property
    def client(self):
        """Create a Secrets Manager client"""
        session = boto3.session.Session(profile_name=self.profile_name)

        return session.client(service_name="secretsmanager", **self.kwargs)

    @staticmethod
    def _format_uri_with_extra(secret, conn_string):
        try:
            extra_dict = secret['extra']
        except KeyError:
            return conn_string

        extra = json.loads(extra_dict)  # this is needed because extra_dict is a string and we need a dict
        conn_string = f"{conn_string}?{urlencode(extra)}"

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

        return self._format_uri_with_extra(secret, conn_string)

    def get_conn_value(self, conn_id: str):
        """
        Get serialized representation of Connection

        :param conn_id: connection id
        """
        if self.connections_prefix is None:
            return None

        if self.full_url_mode:
            return self._get_secret(self.connections_prefix, conn_id)
        try:
            secret_string = self._get_secret(self.connections_prefix, conn_id)
            # json.loads gives error
            secret = ast.literal_eval(secret_string) if secret_string else None
        except ValueError:  # 'malformed node or string: ' error, for empty conns
            connection = None
            secret = None

        # These lines will check if we have with some denomination stored an username, password and host
        if secret:
            connection = self.get_uri_from_secret(secret)

        return connection

    def get_conn_uri(self, conn_id: str) -> Optional[str]:
        """
        Return URI representation of Connection conn_id.

        As of Airflow version 2.3.0 this method is deprecated.

        :param conn_id: the connection id
        :return: deserialized Connection
        """
        if get_airflow_version() >= (2, 3):
            warnings.warn(
                f"Method `{self.__class__.__name__}.get_conn_uri` is deprecated and will be removed "
                "in a future release.  Please use method `get_conn_value` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        return self.get_conn_value(conn_id)

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
        :param secret_id: Secret Key
        """
        error_msg = "An error occurred when calling the get_secret_value operation"
        if path_prefix:
            secrets_path = self.build_path(path_prefix, secret_id, self.sep)
        else:
            secrets_path = secret_id

        try:
            response = self.client.get_secret_value(
                SecretId=secrets_path,
            )
            return response.get('SecretString')
        except self.client.exceptions.ResourceNotFoundException:
            self.log.debug(
                "ResourceNotFoundException: %s. Secret %s not found.",
                error_msg,
                secret_id,
            )
            return None
        except self.client.exceptions.InvalidParameterException:
            self.log.debug(
                "InvalidParameterException: %s",
                error_msg,
                exc_info=True,
            )
            return None
        except self.client.exceptions.InvalidRequestException:
            self.log.debug(
                "InvalidRequestException: %s",
                error_msg,
                exc_info=True,
            )
            return None
        except self.client.exceptions.DecryptionFailure:
            self.log.debug(
                "DecryptionFailure: %s",
                error_msg,
                exc_info=True,
            )
            return None
        except self.client.exceptions.InternalServiceError:
            self.log.debug(
                "InternalServiceError: %s",
                error_msg,
                exc_info=True,
            )
            return None
