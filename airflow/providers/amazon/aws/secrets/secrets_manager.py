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
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlencode

import boto3

from airflow.compat.functools import cached_property
from airflow.models.connection import Connection
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
            "login": ["user", "username", "login", "user_name"],
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
    :param are_secret_values_urlencoded: If True, and full_url_mode is False, then the values are assumed to
        be URL-encoded and will be decoded before being passed into a Connection object. This option is
        ignored when full_url_mode is True.
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
        are_secret_values_urlencoded: Optional[bool] = None,
        extra_conn_words: Optional[Dict[str, List[str]]] = None,
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

        if are_secret_values_urlencoded is None:
            self.are_secret_values_urlencoded = True
        else:
            warnings.warn(
                "The `secret_values_are_urlencoded` kwarg only exists to assist in migrating away from"
                " URL-encoding secret values when `full_url_mode` is False. It will be considered deprecated"
                " when values are not required to be URL-encoded by default.",
                PendingDeprecationWarning,
                stacklevel=2,
            )
            if full_url_mode and not are_secret_values_urlencoded:
                warnings.warn(
                    "The `secret_values_are_urlencoded` kwarg for the SecretsManagerBackend is only used"
                    " when `full_url_mode` is False. When `full_url_mode` is True, the secret needs to be"
                    " URL-encoded.",
                    UserWarning,
                    stacklevel=2,
                )
            self.are_secret_values_urlencoded = are_secret_values_urlencoded
        self.extra_conn_words = extra_conn_words or {}
        self.kwargs = kwargs

    @cached_property
    def client(self):
        """Create a Secrets Manager client"""
        session = boto3.session.Session(profile_name=self.profile_name)

        return session.client(service_name="secretsmanager", **self.kwargs)

    @staticmethod
    def _format_uri_with_extra(secret, conn_string: str) -> str:
        try:
            extra_dict = secret['extra']
        except KeyError:
            return conn_string

        extra = json.loads(extra_dict)  # this is needed because extra_dict is a string and we need a dict
        conn_string = f"{conn_string}?{urlencode(extra)}"

        return conn_string

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        if not self.full_url_mode:
            secret_string = self._get_secret(self.connections_prefix, conn_id)
            secret_dict = self._deserialize_json_string(secret_string)

            if not secret_dict:
                return None

            if 'extra' in secret_dict and isinstance(secret_dict['extra'], str):
                secret_dict['extra'] = self._deserialize_json_string(secret_dict['extra'])

            data = self._standardize_secret_keys(secret_dict)

            if self.are_secret_values_urlencoded:
                data = self._remove_escaping_in_secret_dict(secret=data, conn_id=conn_id)

            port: Optional[int] = None

            if data['port'] is not None:
                port = int(data['port'])

            return Connection(
                conn_id=conn_id,
                login=data['user'],
                password=data['password'],
                host=data['host'],
                port=port,
                schema=data['schema'],
                conn_type=data['conn_type'],
                extra=data['extra'],
            )

        return super().get_connection(conn_id=conn_id)

    def _standardize_secret_keys(self, secret: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize the names of the keys in the dict. These keys align with"""
        possible_words_for_conn_fields = {
            'user': ['user', 'username', 'login', 'user_name'],
            'password': ['password', 'pass', 'key'],
            'host': ['host', 'remote_host', 'server'],
            'port': ['port'],
            'schema': ['database', 'schema'],
            'conn_type': ['conn_type', 'conn_id', 'connection_type', 'engine'],
            'extra': ['extra'],
        }

        for conn_field, extra_words in self.extra_conn_words.items():
            possible_words_for_conn_fields[conn_field].extend(extra_words)

        conn_d: Dict[str, Any] = {}
        for conn_field, possible_words in possible_words_for_conn_fields.items():
            try:
                conn_d[conn_field] = [v for k, v in secret.items() if k in possible_words][0]
            except IndexError:
                conn_d[conn_field] = None

        return conn_d

    def get_uri_from_secret(self, secret: Dict[str, str]) -> str:
        conn_d: Dict[str, str] = {k: v if v else '' for k, v in self._standardize_secret_keys(secret).items()}
        conn_string = "{conn_type}://{user}:{password}@{host}:{port}/{schema}".format(**conn_d)
        return self._format_uri_with_extra(secret, conn_string)

    def _deserialize_json_string(self, value: Optional[str]) -> Optional[Dict[Any, Any]]:
        if not value:
            return None
        try:
            # Use ast.literal_eval for backwards compatibility.
            # Previous version of this code had a comment saying that using json.loads caused errors.
            # This likely means people were using dict reprs instead of valid JSONs.
            res: Dict[str, Any] = json.loads(value)
        except json.JSONDecodeError:
            try:
                res = ast.literal_eval(value) if value else None
                warnings.warn(
                    f'In future versions, `{type(self).__name__}` will only support valid JSONs, not dict'
                    ' reprs. Please make sure your secret is a valid JSON.'
                )
            except ValueError:  # 'malformed node or string: ' error, for empty conns
                return None

        return res

    def _remove_escaping_in_secret_dict(self, secret: Dict[str, Any], conn_id: str) -> Dict[str, Any]:
        # When ``unquote(v) == v``, then removing unquote won't affect the user, regardless of
        # whether or not ``v`` is URL-encoded. For example, "foo bar" is not URL-encoded. But
        # because decoding it doesn't affect the value, then it will migrate safely when
        # ``unquote`` gets removed.
        #
        # When parameters are URL-encoded, but decoding is idempotent, we need to warn the user
        # to un-escape their secrets. For example, if "foo%20bar" is a URL-encoded string, then
        # decoding is idempotent because ``unquote(unquote("foo%20bar")) == unquote("foo%20bar")``.
        #
        # In the rare situation that value is URL-encoded but the decoding is _not_ idempotent,
        # this causes a major issue. For example, if "foo%2520bar" is URL-encoded, then decoding is
        # _not_ idempotent because ``unquote(unquote("foo%2520bar")) != unquote("foo%2520bar")``
        #
        # This causes a problem for migration because if the user decodes their value, we cannot
        # infer that is the case by looking at the decoded value (from our vantage point, it will
        # look to be URL-encoded.)
        #
        # So when this uncommon situation occurs, the user _must_ adjust the configuration and set
        # ``parameters_are_urlencoded`` to False to migrate safely. In all other cases, we do not
        # need the user to adjust this object to migrate; they can transition their secrets with
        # the default configuration.

        warn_user = False
        idempotent = True

        for k, v in secret.copy().items():

            if k == "extra" and isinstance(v, dict):
                # The old behavior was that extras were _not_ urlencoded inside the secret.
                # If they were urlencoded (e.g. "foo%20bar"), then they would be re-urlencoded
                # (e.g. "foo%20bar" becomes "foo%2520bar") and then unquoted once when parsed.
                # So we should just allow the extra dict to remain as-is.
                continue

            elif v is not None:
                v_unquoted = unquote(v)
                if v != v_unquoted:
                    secret[k] = unquote(v)
                    warn_user = True

                    # Check to see if decoding is idempotent.
                    if v_unquoted == unquote(v_unquoted):
                        idempotent = False

        if warn_user:
            msg = (
                "When ``full_url_mode=True``, URL-encoding secret values is deprecated. In future versions, "
                f" this value will not be un-escaped. For the conn_id {conn_id!r}, please remove the"
                " URL-encoding."
                "\n\nThis warning was raised because the SecretsManagerBackend detected that this connection"
                " was URL-encoded."
            )
            if idempotent:
                msg = f" Once the values for conn_id {conn_id!r} are decoded, this warning will go away."
            if not idempotent:
                msg += (
                    " In addition to decoding the values for your connection, you must also set"
                    " ``secret_values_are_urlencoded=False`` for your config variable"
                    " ``secrets.backend_kwargs`` because this connection's URL encoding is not idempotent."
                    " For more information, see:"
                    " https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/secrets-backends"
                    "/aws-secrets-manager.html#url-encoding-of-secrets-when-full-url-mode-is-false"
                )
            warnings.warn(msg, DeprecationWarning, stacklevel=2)

        return secret

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """
        Get serialized representation of Connection

        :param conn_id: connection id
        """
        if self.connections_prefix is None:
            return None

        if self.full_url_mode:
            return self._get_secret(self.connections_prefix, conn_id)
        else:
            warnings.warn(
                f'In future versions, `{type(self).__name__}.get_conn_value` will return a JSON string when'
                ' full_url_mode is False, not a URI.',
                DeprecationWarning,
            )

        # It is very rare for user code to get to this point, since:
        #
        # - When full_url_mode is True, the previous statement returns.
        # - When full_url_mode is False, get_connection() does not call
        #   `get_conn_value`. Additionally, full_url_mode defaults to True.
        #
        # So the code would have to be calling `get_conn_value` directly, and
        # the user would be using a non-default setting.
        #
        # As of Airflow 2.3.0, get_conn_value() is allowed to return a JSON
        # string in the base implementation. This is a way to deprecate this
        # behavior gracefully.

        secret_string = self._get_secret(self.connections_prefix, conn_id)

        secret = self._deserialize_json_string(secret_string)
        connection = None

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
