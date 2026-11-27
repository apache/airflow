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
from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit

from airflow.providers.common.compat.sdk import (
    AirflowNotFoundException,
    AirflowOptionalProviderFeatureException,
)
from airflow.providers.microsoft.azure.utils import get_field

try:
    from airflow.providers.duckdb.hooks.duckdb import DuckDBHook
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'duckdb' provider to be installed. Install it with: "
        "pip install 'apache-airflow-providers-microsoft-azure[duckdb]'"
    )

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

    from airflow.providers.common.compat.sdk import Connection

CredentialStrategy = Literal["config", "credential_chain", "none"]

#: Used when neither an argument nor the connection extra names an ADLS connection.
DEFAULT_AZURE_CONN_ID = "adls_default"


class AzureDuckDBHook(DuckDBHook):
    """
    Interact with DuckDB using Azure credentials brokered by Airflow.

    Extends :class:`~airflow.providers.duckdb.hooks.duckdb.DuckDBHook` with an Azure secret built
    from an Airflow :ref:`ADLS connection <howto/connection:adls>`, so ``read_parquet('abfss://...')``,
    ``delta_scan('abfss://...')`` and ``COPY ... TO 'abfss://...'`` work with no credential wiring in
    the Dag. It is the Azure counterpart of
    :class:`~airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook`.

    Two credential strategies are supported:

    ``config`` (the default)
        Builds the secret from the credentials of the ADLS connection, read in the order
        :class:`~airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook` reads
        them: its ``connection_string`` when the extra has one (``PROVIDER config``), else an Active
        Directory service principal when the extra has a ``tenant_id`` (``PROVIDER
        service_principal``, with the connection's login as client id and its password as client
        secret), else the password as the key of the storage account (``PROVIDER config``, with a
        connection string built from the account and the key). A connection without credentials is
        rejected: use ``credential_chain`` for it. The extra's ``proxies["https"]`` becomes the
        secret's proxy. The credentials become part of the SQL statement.

    ``credential_chain``
        Issues ``CREATE SECRET (TYPE azure, PROVIDER credential_chain)``, which resolves credentials
        through the Azure SDK inside DuckDB's ``azure`` extension (Azure CLI, managed identity,
        workload identity, environment). Nothing secret is written into SQL text. The default ADLS
        connection is optional then: when it exists, it supplies the storage account and proxy. An
        ``azure_conn_id`` other than the default has to exist. The chain does not read the
        connection's ``managed_identity_client_id`` or ``workload_identity_tenant_id``.

    The storage account comes from ``account_name``, else from the ``account_url`` of the ADLS
    connection's extra (``onelake`` for ``https://onelake.dfs.fabric.microsoft.com``), else from its
    host.

    This hook needs the ``azure`` extension and installs it if it is missing, as ``AwsDuckDBHook``
    does with ``httpfs`` and ``aws``. Set ``autoinstall_extensions=False`` to forbid the download.

    :param azure_conn_id: the ADLS connection used to reach the storage. Defaults to ``adls_default``.
    :param account_name: the storage account of the secret. Defaults to the ADLS connection's.
    :param credential_strategy: ``config``, ``credential_chain``, or ``none`` to create no secret at
        all (for a DuckDB database that never touches Azure).
    :param credential_chain: providers DuckDB's credential chain consults, for example
        ``"managed_identity;env"``. Only used with the ``credential_chain`` strategy; DuckDB's own
        default applies when it is not set.
    :param transport_option_type: the HTTP transport of the Azure SDK in DuckDB (the
        ``azure_transport_option_type`` setting): ``default`` or ``curl``. ``curl`` searches the
        usual locations for the CA bundle, which resolves TLS certificate errors on Linux systems
        whose bundle is not where the default transport looks. Left unset, DuckDB's default applies.
    :param secret_name: name of the DuckDB secret to create.

    As with :class:`~airflow.providers.duckdb.hooks.duckdb.DuckDBHook`, every parameter may also be set
    in the DuckDB connection ``extra``, and an explicit argument wins.
    """

    #: ``azure`` provides the ``az://``/``abfss://`` filesystems and the Azure secret type.
    required_extensions = ("azure",)

    # A connection type is what lets DuckDBExecuteQueryOperator reach this hook: the operator
    # resolves the hook from the connection a Dag names.
    conn_type = "duckdb_azure"
    hook_name = "DuckDB on Azure"
    default_conn_name = "duckdb_azure_default"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for the DuckDB on Azure connection."""
        return {
            "hidden_fields": ["login", "password", "port", "schema"],
            "relabeling": {"host": "Database path"},
            "placeholders": {
                "host": "/tmp/analytics.duckdb (leave empty for an in-memory database)",
                "extra": '{"azure_conn_id": "adls_default", "credential_strategy": "config", '
                '"memory_limit": "2GB", "threads": 4}',
            },
        }

    def __init__(
        self,
        *args,
        azure_conn_id: str | None = None,
        account_name: str | None = None,
        credential_strategy: CredentialStrategy | None = None,
        credential_chain: str | None = None,
        transport_option_type: Literal["default", "curl"] | None = None,
        secret_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._azure_conn_id = azure_conn_id
        self._account_name = account_name
        self._credential_strategy = credential_strategy
        self._credential_chain = credential_chain
        self._transport_option_type = transport_option_type
        self._secret_name = secret_name

    @property
    def azure_conn_id(self) -> str:
        return self.resolve_parameter("azure_conn_id", self._azure_conn_id, DEFAULT_AZURE_CONN_ID)

    @cached_property
    def azure_connection(self) -> Connection:
        """Return the ADLS connection the secret is built from."""
        return self.get_connection(self.azure_conn_id)

    @cached_property
    def optional_azure_connection(self) -> Connection | None:
        """
        Return the ADLS connection, or ``None`` when the default one does not exist.

        With the ``credential_chain`` strategy the ADLS connection only supplies the storage account
        and proxy, so the default one may be missing. A connection named explicitly has to exist, as
        for the DuckDB connection itself: leaving it out would create a secret without the account
        and proxy the Dag author asked for.
        """
        try:
            return self.azure_connection
        except AirflowNotFoundException:
            if self.azure_conn_id != DEFAULT_AZURE_CONN_ID:
                raise
            return None

    @property
    def credential_strategy(self) -> CredentialStrategy:
        return self.resolve_parameter("credential_strategy", self._credential_strategy, "config")

    @property
    def credential_chain(self) -> str | None:
        return self.resolve_parameter("credential_chain", self._credential_chain)

    @property
    def transport_option_type(self) -> str | None:
        return self.resolve_parameter("transport_option_type", self._transport_option_type)

    @property
    def secret_name(self) -> str:
        return self.resolve_parameter("secret_name", self._secret_name, "airflow_azure")

    @property
    def autoinstall_extensions(self) -> bool:
        """
        Default to installing missing extensions, unlike the generic hook.

        Reaching for the Azure hook is a statement that the task runs against Azure, so ``azure``
        should just work. The generic hook cannot assume that and leaves downloads off.
        """
        return bool(self.resolve_parameter("autoinstall_extensions", self._autoinstall_extensions, True))

    def get_account_name(self) -> str | None:
        """Return the storage account of the secret."""
        explicit = self.resolve_parameter("account_name", self._account_name)
        if explicit:
            return explicit
        connection = self.optional_azure_connection
        if connection is None:
            return None
        account_url = self._get_azure_field(connection, "account_url")
        if account_url:
            # The Azure SDK accepts an account_url without a scheme, which urlsplit reads as a path.
            hostname = urlsplit(account_url if "//" in account_url else f"//{account_url}").hostname
            if hostname:
                return hostname.split(".", 1)[0]
        return connection.host or None

    def configure_secrets(self, conn: DuckDBPyConnection) -> None:
        """Set the Azure transport, if one is configured, and create the DuckDB Azure secret."""
        transport = self.transport_option_type
        if transport:
            if transport not in ("default", "curl"):
                raise ValueError(
                    f"Unknown transport_option_type {transport!r}. Expected 'default' or 'curl'."
                )
            # GLOBAL, not the session: DbApiHook runs statements on conn.cursor(), a connection of
            # its own that does not see a session setting of conn, while the secret is
            # database-wide already.
            conn.execute(f"SET GLOBAL azure_transport_option_type = '{transport}';")

        strategy = self.credential_strategy
        if strategy == "none":
            self.log.debug("credential_strategy is 'none'; not creating a DuckDB Azure secret.")
            return

        self._validate_identifier(self.secret_name, "secret")
        parts = ["TYPE azure", *self._provider_clauses(strategy)]
        self.log.info("Creating DuckDB Azure secret %r using the %r strategy.", self.secret_name, strategy)
        conn.execute(f"CREATE OR REPLACE SECRET {self.secret_name} ({', '.join(parts)});")

    def _provider_clauses(self, strategy: CredentialStrategy) -> list[str]:
        if strategy == "config":
            return [*self._config_clauses(), *self._proxy_clauses()]
        if strategy == "credential_chain":
            clauses = ["PROVIDER credential_chain"]
            if self.credential_chain:
                clauses.append(f"CHAIN '{self._quote(self.credential_chain)}'")
            return [*clauses, *self._account_clauses(), *self._proxy_clauses()]
        raise ValueError(
            f"Unknown credential_strategy {strategy!r}. Expected 'config', 'credential_chain' or 'none'."
        )

    def _config_clauses(self) -> list[str]:
        """Translate the credentials of the ADLS connection, in the order the ADLS hook reads them."""
        connection = self.azure_connection
        connection_string = self._get_azure_field(connection, "connection_string")
        if connection_string:
            return self._connection_string_clauses(connection_string)
        tenant_id = self._get_azure_field(connection, "tenant_id")
        if tenant_id:
            return self._service_principal_clauses(tenant_id)
        if connection.password:
            return self._account_key_clauses(connection.password)
        raise ValueError(
            f"The ADLS connection {self.azure_conn_id!r} has no credentials for the 'config' credential "
            "strategy: it needs a connection_string, a tenant_id with a login (client id) and a "
            "password (client secret), or a password (account key). Use 'credential_chain' to let "
            "the Azure SDK resolve credentials."
        )

    def _connection_string_clauses(self, connection_string: str) -> list[str]:
        return ["PROVIDER config", f"CONNECTION_STRING '{self._quote(connection_string)}'"]

    def _account_key_clauses(self, account_key: str) -> list[str]:
        account_name = self.get_account_name()
        if not account_name:
            raise ValueError(
                f"The ADLS connection {self.azure_conn_id!r} has an account key but no storage account. "
                "Set its host or account_url, or account_name on the DuckDB connection."
            )
        return self._connection_string_clauses(f"AccountName={account_name};AccountKey={account_key}")

    def _service_principal_clauses(self, tenant_id: str) -> list[str]:
        connection = self.azure_connection
        if not (connection.login and connection.password):
            raise ValueError(
                f"The ADLS connection {self.azure_conn_id!r} has a tenant_id but no login (client id) "
                "or password (client secret) for the 'config' credential strategy."
            )
        return [
            "PROVIDER service_principal",
            f"TENANT_ID '{self._quote(tenant_id)}'",
            f"CLIENT_ID '{self._quote(connection.login)}'",
            f"CLIENT_SECRET '{self._quote(connection.password)}'",
            *self._account_clauses(),
        ]

    def _account_clauses(self) -> list[str]:
        account_name = self.get_account_name()
        return [f"ACCOUNT_NAME '{self._quote(account_name)}'"] if account_name else []

    def _proxy_clauses(self) -> list[str]:
        connection = self.optional_azure_connection
        if connection is None:
            return []
        proxy = (connection.extra_dejson.get("proxies") or {}).get("https")
        return [f"HTTP_PROXY '{self._quote(proxy)}'"] if proxy else []

    @staticmethod
    def _get_azure_field(connection: Connection, field_name: str) -> Any:
        return get_field(
            conn_id=connection.conn_id,
            conn_type="adls",
            extras=connection.extra_dejson,
            field_name=field_name,
        )

    @staticmethod
    def _quote(value: str) -> str:
        """Escape single quotes so a value cannot break out of its SQL literal."""
        return value.replace("'", "''")

    def get_openlineage_database_info(self, connection: Any) -> Any:
        """Return no lineage metadata; a DuckDB database is task-local and has no stable namespace."""
        return None
