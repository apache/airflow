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
"""Objects relating to sourcing secrets from Yandex Cloud Lockbox."""

from __future__ import annotations

from functools import cached_property
from typing import Any

import yandex.cloud.lockbox.v1.payload_pb2 as payload_pb
import yandex.cloud.lockbox.v1.payload_service_pb2 as payload_service_pb
import yandex.cloud.lockbox.v1.payload_service_pb2_grpc as payload_service_pb_grpc
import yandex.cloud.lockbox.v1.secret_pb2 as secret_pb
import yandex.cloud.lockbox.v1.secret_service_pb2 as secret_service_pb
import yandex.cloud.lockbox.v1.secret_service_pb2_grpc as secret_service_pb_grpc
import yandexcloud

from airflow.models import Connection
from airflow.providers.common.compat.sdk import conf
from airflow.providers.yandex.utils.credentials import get_credentials
from airflow.providers.yandex.utils.defaults import default_conn_name
from airflow.providers.yandex.utils.fields import get_field_from_extras
from airflow.providers.yandex.utils.user_agent import provider_user_agent
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin

TEAM_SEP_MULTIPLIER = 2


class LockboxSecretBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves connections or variables or configs from Yandex Lockbox.

    Configurable via ``airflow.cfg`` like so:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend
        backend_kwargs = {"connections_prefix": "airflow/connections"}

    For example, when ``{"connections_prefix": "airflow/connections"}`` is set, if a secret is defined with
    the path ``airflow/connections/smtp_default``, the connection with conn_id ``smtp_default`` would be
    accessible.

    When ``{"variables_prefix": "airflow/variables"}`` is set, if a secret is defined with
    the path ``airflow/variables/hello``, the variable with the name ``hello`` would be accessible.

    When ``{"config_prefix": "airflow/config"}`` is set, if a secret is defined with
    the path ``airflow/config/sql_alchemy_conn``, the config with key ``sql_alchemy_conn`` would be
    accessible.

    If the prefix is empty, the requests will not be sent to Yandex Lockbox.

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend
        backend_kwargs = {"yc_connection_id": "<connection_ID>", "folder_id": "<folder_ID>"}

    You need to specify credentials or the ID of the ``yandexcloud`` connection to connect to Yandex Lockbox.
    The credentials will be used with the following priority:

    * OAuth token
    * Service account key in JSON from file
    * Service account key in JSON
    * Yandex Cloud connection

    If you do not specify any credentials,
    the system will use the default connection ID:``yandexcloud_default``.

    Also, you need to specify the Yandex Cloud folder ID to search for Yandex Lockbox secrets in.
    If you do not specify folder ID, the requests will use the connection ``folder_id`` if specified.

    :param yc_oauth_token: Specifies the user account OAuth token to connect to Yandex Lockbox.
        The parameter value should look like ``y3_xx123``.
    :param yc_sa_key_json: Specifies the service account key in JSON.
        The parameter value should look like
        ``{"id": "...", "service_account_id": "...", "private_key": "..."}``.
    :param yc_sa_key_json_path: Specifies the service account key in JSON file path.
        The parameter value should look like ``/home/airflow/authorized_key.json``,
        while the file content should have the following format:
        ``{"id": "...", "service_account_id": "...", "private_key": "..."}``.
    :param yc_connection_id: Specifies the connection ID to connect to Yandex Lockbox.
        The default value is ``yandexcloud_default``.
    :param folder_id: Specifies the folder ID to search for Yandex Lockbox secrets in.
        If set to ``None`` (``null`` in JSON),
        the requests will use the connection ``folder_id``, if specified.
    :param connections_prefix: Specifies the prefix of the secret to read to get connections.
        If set to ``None`` (``null`` in JSON),
        the requests for connections will not be sent to Yandex Lockbox.
        The default value is ``airflow/connections``.
    :param variables_prefix: Specifies the prefix of the secret to read to get variables.
        If set to ``None`` (``null`` in JSON), the requests for variables will not be sent to Yandex Lockbox.
        The default value is ``airflow/variables``.
    :param config_prefix: Specifies the prefix of the secret to read to get configurations.
        If set to ``None`` (``null`` in JSON), the requests for variables will not be sent to Yandex Lockbox.
        The default value is ``airflow/config``.
    :param sep: Specifies the separator to concatenate ``secret_prefix`` and ``secret_id``.
        The default value is ``/``.
    :param endpoint: Specifies the API endpoint.
        If set to ``None`` (``null`` in JSON), the requests will use the connection endpoint, if specified;
        otherwise, they will use the default endpoint.
    """

    def __init__(
        self,
        yc_oauth_token: str | None = None,
        yc_sa_key_json: dict | str | None = None,
        yc_sa_key_json_path: str | None = None,
        yc_connection_id: str | None = None,
        folder_id: str = "",
        connections_prefix: str | None = "airflow/connections",
        variables_prefix: str | None = "airflow/variables",
        config_prefix: str | None = "airflow/config",
        sep: str = "/",
        endpoint: str | None = None,
    ):
        super().__init__()

        self.yc_oauth_token = yc_oauth_token
        self.yc_sa_key_json = yc_sa_key_json
        self.yc_sa_key_json_path = yc_sa_key_json_path
        self.yc_connection_id = None
        if not any([yc_oauth_token, yc_sa_key_json, yc_sa_key_json_path]):
            self.yc_connection_id = yc_connection_id or default_conn_name
        elif yc_connection_id is not None:
            raise ValueError("`yc_connection_id` should not be used if other credentials are specified")

        self.folder_id = folder_id
        self.connections_prefix = connections_prefix.rstrip(sep) if connections_prefix is not None else None
        self.variables_prefix = variables_prefix.rstrip(sep) if variables_prefix is not None else None
        self.config_prefix = config_prefix.rstrip(sep) if config_prefix is not None else None
        self.sep = sep
        self.endpoint = endpoint

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        """
        Retrieve from Secrets Backend a string value representing the Connection object.

        :param conn_id: Connection ID
        :param team_name: Team name associated to the task trying to access the connection (if any)
        :return: Connection Value
        """
        if self.connections_prefix is None:
            return None

        if conn_id == self.yc_connection_id:
            return None

        if self._names_a_team_namespace(conn_id):
            self._log_refusal("connection", conn_id)
            return None

        return self._get_secret_value(self.connections_prefix, conn_id, team_name=team_name)

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Return value for Airflow Variable.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None

        if self._names_a_team_namespace(key):
            self._log_refusal("variable", key)
            return None

        return self._get_secret_value(self.variables_prefix, key, team_name=team_name)

    def get_config(self, key: str) -> str | None:
        """
        Return value for Airflow Config Key.

        :param key: Config Key
        :return: Config Value
        """
        if self.config_prefix is None:
            return None

        return self._get_secret_value(self.config_prefix, key)

    @cached_property
    def _client(self):
        """
        Create a Yandex Cloud SDK client.

        Lazy loading is used here
        because we can't establish a Connection until all secrets backends have been initialized.
        """
        if self.yc_connection_id:
            self.yc_oauth_token = self._get_field("oauth")
            self.yc_sa_key_json = self._get_field("service_account_json")
            self.yc_sa_key_json_path = self._get_field("service_account_json_path")
            self.folder_id = self.folder_id or self._get_field("folder_id")
            self.endpoint = self.endpoint or self._get_field("endpoint")

        credentials = get_credentials(
            oauth_token=self.yc_oauth_token,
            service_account_json=self.yc_sa_key_json,
            service_account_json_path=self.yc_sa_key_json_path,
        )
        sdk_config = self._get_endpoint()
        return yandexcloud.SDK(user_agent=provider_user_agent(), **credentials, **sdk_config).client

    def _get_endpoint(self) -> dict[str, str]:
        sdk_config = {}

        if self.endpoint:
            sdk_config["endpoint"] = self.endpoint

        return sdk_config

    @cached_property
    def _connection(self) -> Connection | None:
        if not self.yc_connection_id:
            return None

        conn = Connection.get_connection_from_secrets(self.yc_connection_id)
        self.log.info("Using connection ID '%s' for task execution.", conn.conn_id)

        return conn

    def _get_field(self, field_name: str, default: Any = None) -> Any:
        conn = self._connection
        if not conn:
            return None

        return get_field_from_extras(
            extras=conn.extra_dejson,
            field_name=field_name,
            default=default,
        )

    def _build_secret_name(self, prefix: str, key: str):
        if len(prefix) == 0:
            return key
        return f"{prefix}{self.sep}{key}"

    def _build_team_secret_name(self, prefix: str, team_name: str, key: str) -> str:
        team_prefix = self._build_secret_name(prefix, team_name)
        return f"{team_prefix}{self.sep * TEAM_SEP_MULTIPLIER}{key}"

    def _names_a_team_namespace(self, secret_id: str) -> bool:
        """
        Whether ``secret_id`` spells out a team scoped secret name.

        A team scoped secret is named ``<team><team sep><key>``, so an id that itself contains
        the team separator makes the built name ambiguous: team ``a`` with key ``b//c`` and team
        ``a//b`` with key ``c`` produce the same string. Such an id is refused for *every*
        lookup -- team scoped as well as team agnostic -- because the ambiguity exists in both
        directions and the caller's own namespace is not a safe harbour for it.

        The id is never parsed to work out *which* team it names, because it cannot be: nothing
        in the string distinguishes the two readings above. Comparing the id against the prefix
        the caller's own team builds looks equivalent and is not -- a caller in team ``a`` would
        match ``a//b``'s namespace on the prefix and read its secrets. Only the caller's own
        namespace is ever constructed, never parsed.

        Only checked in multi-team mode: ``team_name`` is never non-``None`` otherwise, so no
        team scoped secret can exist to collide with.
        """
        if not conf.getboolean("core", "multi_team", fallback=False):
            return False
        return self.sep * TEAM_SEP_MULTIPLIER in secret_id

    def _log_refusal(self, kind: str, secret_id: str) -> None:
        self.log.warning(
            "%s id %r contains %r, which separates the team name from the key in a team scoped "
            "secret name. Such an id is ambiguous and is not looked up. Returning None.",
            kind.capitalize(),
            secret_id,
            self.sep * TEAM_SEP_MULTIPLIER,
        )

    def _get_secret_value(self, prefix: str, key: str, team_name: str | None = None) -> str | None:
        secrets = self._get_secrets()
        secret: secret_pb.Secret | None = None
        # The team scoped name is tried first. Keys that would make it name a namespace other
        # than the caller's own are refused by the callers before reaching here.
        if team_name:
            secret = self._find_secret(secrets, prefix, self._build_team_secret_name("", team_name, key))

        if not secret:
            secret = self._find_secret(secrets, prefix, key)

        if not secret:
            return None

        payload = self._get_payload(secret.id, secret.current_version.id)
        entries = {entry.key: entry.text_value for entry in payload.entries if entry.text_value}

        if len(entries) == 0:
            return None
        return sorted(entries.values())[0]

    def _find_secret(self, secrets: list[secret_pb.Secret], prefix: str, key: str) -> secret_pb.Secret | None:
        for s in secrets:
            if s.name == self._build_secret_name(prefix=prefix, key=key):
                return s
        return None

    def _get_secrets(self) -> list[secret_pb.Secret]:
        # generate client if not exists, to load folder_id from connections
        _ = self._client

        response = self._list_secrets(folder_id=self.folder_id)

        secrets: list[secret_pb.Secret] = response.secrets[:]
        next_page_token = response.next_page_token
        while next_page_token != "":
            response = self._list_secrets(
                folder_id=self.folder_id,
                page_token=next_page_token,
            )
            secrets.extend(response.secrets)
            next_page_token = response.next_page_token

        return secrets

    def _get_payload(self, secret_id: str, version_id: str) -> payload_pb.Payload:
        request = payload_service_pb.GetPayloadRequest(
            secret_id=secret_id,
            version_id=version_id,
        )
        return self._client(payload_service_pb_grpc.PayloadServiceStub).Get(request)

    def _list_secrets(self, folder_id: str, page_token: str = "") -> secret_service_pb.ListSecretsResponse:
        request = secret_service_pb.ListSecretsRequest(
            folder_id=folder_id,
            page_token=page_token,
        )
        return self._client(secret_service_pb_grpc.SecretServiceStub).List(request)
