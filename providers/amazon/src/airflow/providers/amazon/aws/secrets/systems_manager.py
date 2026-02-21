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
"""Objects relating to sourcing connections from AWS SSM Parameter Store."""

from __future__ import annotations

import re
from functools import cached_property

from airflow.providers.amazon.aws.utils import trim_none_values
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class SystemsManagerParameterStoreBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection or Variables from AWS SSM Parameter Store.

    Configurable via ``airflow.cfg`` like so:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
        backend_kwargs = {"connections_prefix": "/airflow/connections", "profile_name": null}

    For example, if ssm path is ``/airflow/connections/smtp_default``, this would be accessible
    if you provide ``{"connections_prefix": "/airflow/connections"}`` and request conn_id ``smtp_default``.
    And if ssm path is ``/airflow/variables/hello``, this would be accessible
    if you provide ``{"variables_prefix": "/airflow/variables"}`` and variable key ``hello``.

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
        If set to None (null), requests for connections will not be sent to AWS SSM Parameter Store.
    :param connections_lookup_pattern: Specifies a pattern the connection ID needs to match to be looked up in
        AWS Parameter Store. Applies only if `connections_prefix` is not None.
        If set to None (null value in the configuration), all connections will be looked up first in
        AWS Parameter Store.
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null), requests for variables will not be sent to AWS SSM Parameter Store.
    :param variables_lookup_pattern: Specifies a pattern the variable key needs to match to be looked up in
        AWS Parameter Store. Applies only if `variables_prefix` is not None.
        If set to None (null value in the configuration), all variables will be looked up first in
        AWS Parameter Store.
    :param config_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null), requests for configurations will not be sent to AWS SSM Parameter Store.
    :param config_lookup_pattern: Specifies a pattern the config key needs to match to be looked up in
        AWS Parameter Store. Applies only if `config_prefix` is not None.
        If set to None (null value in the configuration), all config keys will be looked up first in
        AWS Parameter Store.

    You can also pass additional keyword arguments listed in AWS Connection Extra config
    to this class, and they would be used for establish connection and passed on to Boto3 client.

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
        backend_kwargs = {"connections_prefix": "airflow/connections", "region_name": "eu-west-1"}

    .. seealso::
        :ref:`howto/connection:aws:configuring-the-connection`

    """

    def __init__(
        self,
        connections_prefix: str = "/airflow/connections",
        connections_lookup_pattern: str | None = None,
        variables_prefix: str = "/airflow/variables",
        variables_lookup_pattern: str | None = None,
        config_prefix: str = "/airflow/config",
        config_lookup_pattern: str | None = None,
        **kwargs,
    ):
        super().__init__()
        if connections_prefix is not None:
            self.connections_prefix = connections_prefix.rstrip("/")
        else:
            self.connections_prefix = connections_prefix
        if variables_prefix is not None:
            self.variables_prefix = variables_prefix.rstrip("/")
        else:
            self.variables_prefix = variables_prefix
        if config_prefix is not None:
            self.config_prefix = config_prefix.rstrip("/")
        else:
            self.config_prefix = config_prefix

        self.connections_lookup_pattern = connections_lookup_pattern
        self.variables_lookup_pattern = variables_lookup_pattern
        self.config_lookup_pattern = config_lookup_pattern
        self.profile_name = kwargs.get("profile_name", None)
        # Remove client specific arguments from kwargs
        self.api_version = kwargs.pop("api_version", None)
        self.use_ssl = kwargs.pop("use_ssl", None)

        self.kwargs = kwargs

    @cached_property
    def client(self):
        """Create a SSM client."""
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
        return session.client(service_name="ssm", **client_kwargs)

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        """
        Get param value.

        :param conn_id: connection id
        :param team_name: Team name associated to the task trying to access the connection (if any)
        """
        if self.connections_prefix is None:
            return None

        return self._get_secret(self.connections_prefix, conn_id, self.connections_lookup_pattern)

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None

        return self._get_secret(self.variables_prefix, key, self.variables_lookup_pattern)

    def get_config(self, key: str) -> str | None:
        """
        Get Airflow Configuration.

        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        if self.config_prefix is None:
            return None

        return self._get_secret(self.config_prefix, key, self.config_lookup_pattern)

    def _get_secret(self, path_prefix: str, secret_id: str, lookup_pattern: str | None) -> str | None:
        """
        Get secret value from Parameter Store.

        :param path_prefix: Prefix for the Path to get Secret
        :param secret_id: Secret Key
        :param lookup_pattern: If provided, `secret_id` must match this pattern to look up the secret in
            Systems Manager
        """
        if lookup_pattern and not re.match(lookup_pattern, secret_id, re.IGNORECASE):
            return None

        ssm_path = self.build_path(path_prefix, secret_id)
        ssm_path = self._ensure_leading_slash(ssm_path)

        try:
            response = self.client.get_parameter(Name=ssm_path, WithDecryption=True)
            return response["Parameter"]["Value"]
        except self.client.exceptions.ParameterNotFound:
            self.log.debug("Parameter %s not found.", ssm_path)
            return None

    def _ensure_leading_slash(self, ssm_path: str):
        """
        AWS Systems Manager mandate to have a leading "/". Adding it dynamically if not there to the SSM path.

        :param ssm_path: SSM parameter path
        """
        if not ssm_path.startswith("/"):
            ssm_path = f"/{ssm_path}"

        return ssm_path
