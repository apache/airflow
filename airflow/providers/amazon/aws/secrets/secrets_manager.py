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
from __future__ import annotations

import json
import warnings
from typing import Any
from urllib.parse import unquote

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.utils import trim_none_values
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

    You can also pass additional keyword arguments listed in AWS Connection Extra config
    to this class, and they would be used for establishing a connection and passed on to Boto3 client.

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
        backend_kwargs = {"connections_prefix": "airflow/connections", "region_name": "eu-west-1"}

    .. seealso::
        :ref:`howto/connection:aws:configuring-the-connection`

    There are two ways of storing secrets in Secret Manager for using them with this operator:
    storing them as a conn URI in one field, or taking advantage of native approach of Secrets Manager
    and storing them in multiple fields. There are certain words that will be searched in the name
    of fields for trying to retrieve a connection part. Those words are:

    .. code-block:: python

        possible_words_for_conn_fields = {
            "login": ["login", "user", "username", "user_name"],
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
    :param sep: separator used to concatenate secret_prefix and secret_id. Default: "/"
    :param extra_conn_words: for using just when you set full_url_mode as false and store
        the secrets in different fields of secrets manager. You can add more words for each connection
        part beyond the default ones. The extra words to be searched should be passed as a dict of lists,
        each list corresponding to a connection part. The optional keys of the dict must be: user,
        password, host, schema, conn_type.
    """

    def __init__(
        self,
        connections_prefix: str = "airflow/connections",
        variables_prefix: str = "airflow/variables",
        config_prefix: str = "airflow/config",
        sep: str = "/",
        extra_conn_words: dict[str, list[str]] | None = None,
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
        self.sep = sep

        if kwargs.pop("full_url_mode", None) is not None:
            warnings.warn(
                "The `full_url_mode` kwarg is deprecated. Going forward, the `SecretsManagerBackend`"
                " will support both URL-encoded and JSON-encoded secrets at the same time. The encoding"
                " of the secret will be determined automatically.",
                DeprecationWarning,
                stacklevel=2,
            )

        if kwargs.get("are_secret_values_urlencoded") is not None:
            warnings.warn(
                "The `secret_values_are_urlencoded` is deprecated. This kwarg only exists to assist in"
                " migrating away from URL-encoding secret values for JSON secrets."
                " To remove this warning, make sure your JSON secrets are *NOT* URL-encoded, and then"
                " remove this kwarg from backend_kwargs.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.are_secret_values_urlencoded = kwargs.pop("are_secret_values_urlencoded", None)
        else:
            self.are_secret_values_urlencoded = False

        self.extra_conn_words = extra_conn_words or {}

        self.profile_name = kwargs.get("profile_name", None)
        # Remove client specific arguments from kwargs
        self.api_version = kwargs.pop("api_version", None)
        self.use_ssl = kwargs.pop("use_ssl", None)

        self.kwargs = kwargs

    @cached_property
    def client(self):
        """Create a Secrets Manager client"""
        from airflow.providers.amazon.aws.hooks.base_aws import SessionFactory
        from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper

        conn_id = f"{self.__class__.__name__}__connection"
        conn_config = AwsConnectionWrapper.from_connection_metadata(conn_id=conn_id, extra=self.kwargs)
        client_kwargs = trim_none_values(
            {
                "region_name": conn_config.region_name,
                "verify": conn_config.verify,
                "endpoint_url": conn_config.endpoint_url,
                "api_version": self.api_version,
                "use_ssl": self.use_ssl,
            }
        )

        session = SessionFactory(conn=conn_config).create_session()
        return session.client(service_name="secretsmanager", **client_kwargs)

    def _standardize_secret_keys(self, secret: dict[str, Any]) -> dict[str, Any]:
        """Standardize the names of the keys in the dict. These keys align with"""
        possible_words_for_conn_fields = {
            "login": ["login", "user", "username", "user_name"],
            "password": ["password", "pass", "key"],
            "host": ["host", "remote_host", "server"],
            "port": ["port"],
            "schema": ["database", "schema"],
            "conn_type": ["conn_type", "conn_id", "connection_type", "engine"],
            "extra": ["extra"],
        }

        for conn_field, extra_words in self.extra_conn_words.items():
            if conn_field == "user":
                # Support `user` for backwards compatibility.
                conn_field = "login"
            possible_words_for_conn_fields[conn_field].extend(extra_words)

        conn_d: dict[str, Any] = {}
        for conn_field, possible_words in possible_words_for_conn_fields.items():
            try:
                conn_d[conn_field] = [v for k, v in secret.items() if k in possible_words][0]
            except IndexError:
                conn_d[conn_field] = None

        return conn_d

    def _remove_escaping_in_secret_dict(self, secret: dict[str, Any]) -> dict[str, Any]:
        """Un-escape secret values that are URL-encoded"""
        for k, v in secret.copy().items():
            if k == "extra" and isinstance(v, dict):
                # The old behavior was that extras were _not_ urlencoded inside the secret.
                # So we should just allow the extra dict to remain as-is.
                continue

            elif v is not None:
                secret[k] = unquote(v)

        return secret

    def get_conn_value(self, conn_id: str) -> str | None:
        """
        Get serialized representation of Connection

        :param conn_id: connection id
        """
        if self.connections_prefix is None:
            return None

        secret = self._get_secret(self.connections_prefix, conn_id)

        if secret is not None and secret.strip().startswith("{"):
            # Before Airflow 2.3, the AWS SecretsManagerBackend added support for JSON secrets.
            #
            # The way this was implemented differs a little from how Airflow's core API handle JSON secrets.
            #
            # The most notable difference is that SecretsManagerBackend supports extra aliases for the
            # Connection parts, e.g. "users" is allowed instead of "login".
            #
            # This means we need to deserialize then re-serialize the secret if it's a JSON, potentially
            # renaming some keys in the process.

            secret_dict = json.loads(secret)
            standardized_secret_dict = self._standardize_secret_keys(secret_dict)
            if self.are_secret_values_urlencoded:
                standardized_secret_dict = self._remove_escaping_in_secret_dict(standardized_secret_dict)
            standardized_secret = json.dumps(standardized_secret_dict)
            return standardized_secret
        else:
            return secret

    def get_conn_uri(self, conn_id: str) -> str | None:
        """
        Return URI representation of Connection conn_id.

        As of Airflow version 2.3.0 this method is deprecated.

        :param conn_id: the connection id
        :return: deserialized Connection
        """
        warnings.warn(
            f"Method `{self.__class__.__name__}.get_conn_uri` is deprecated and will be removed "
            "in a future release. Please use method `get_conn_value` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_conn_value(conn_id)

    def get_variable(self, key: str) -> str | None:
        """
        Get Airflow Variable from Environment Variable
        :param key: Variable Key
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None

        return self._get_secret(self.variables_prefix, key)

    def get_config(self, key: str) -> str | None:
        """
        Get Airflow Configuration
        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        if self.config_prefix is None:
            return None

        return self._get_secret(self.config_prefix, key)

    def _get_secret(self, path_prefix, secret_id: str) -> str | None:
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
            return response.get("SecretString")
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
