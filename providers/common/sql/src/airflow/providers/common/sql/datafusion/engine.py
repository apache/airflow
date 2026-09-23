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

import os
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlsplit

from datafusion import SessionContext

from airflow.providers.common.compat.sdk import BaseHook, Connection
from airflow.providers.common.sql.config import ConnectionConfig, DataSourceConfig, StorageType
from airflow.providers.common.sql.datafusion.exceptions import (
    ObjectStoreCreationException,
    QueryExecutionException,
)
from airflow.providers.common.sql.datafusion.format_handlers import get_format_handler
from airflow.providers.common.sql.datafusion.object_storage_provider import get_object_storage_provider
from airflow.utils.log.logging_mixin import LoggingMixin


class DataFusionEngine(LoggingMixin):
    """Apache DataFusion engine."""

    def __init__(self):
        super().__init__()
        # TODO: session context has additional parameters via SessionConfig see what's possible we can use Possible via DataFusionHook ?
        self.df_ctx = SessionContext()
        self.registered_tables: dict[str, str] = {}

    @property
    def session_context(self) -> SessionContext:
        """Return the session context."""
        return self.df_ctx

    def register_datasource(self, datasource_config: DataSourceConfig):
        """Register a datasource with the datafusion engine."""
        if not isinstance(datasource_config, DataSourceConfig):
            raise ValueError("datasource_config must be of type DataSourceConfig")

        if not datasource_config.is_table_provider and datasource_config.storage_type is None:
            raise ValueError(
                f"DataSourceConfig for table {datasource_config.table_name!r} has no uri or format; "
                "DataFusionEngine only registers object-store or catalog-managed sources."
            )

        if not datasource_config.is_table_provider:
            if datasource_config.storage_type == StorageType.LOCAL:
                connection_config = None
            else:
                connection_config = self._get_connection_config(datasource_config.conn_id)

            self._register_object_store(datasource_config, connection_config)

        self._register_data_source_format(datasource_config)

    def _register_object_store(
        self, datasource_config: DataSourceConfig, connection_config: ConnectionConfig | None
    ):
        """Register object stores."""
        if TYPE_CHECKING:
            assert datasource_config.storage_type is not None

        try:
            storage_provider = get_object_storage_provider(datasource_config.storage_type)
            object_store = storage_provider.create_object_store(
                datasource_config.uri, connection_config=connection_config
            )
            schema = storage_provider.get_scheme()
            self.session_context.register_object_store(schema=schema, store=object_store)
            self.log.info("Registered object store for schema: %s", schema)
        except Exception as e:
            raise ObjectStoreCreationException(
                f"Error while creating object store for {datasource_config.storage_type}: {e}"
            )

    def _register_data_source_format(self, datasource_config: DataSourceConfig):
        """Register data source format."""
        if TYPE_CHECKING:
            assert datasource_config.table_name is not None
            assert datasource_config.format is not None

        if datasource_config.table_name in self.registered_tables:
            raise ValueError(
                f"Table {datasource_config.table_name} already registered for {self.registered_tables[datasource_config.table_name]}, please choose different name"
            )

        format_cls = get_format_handler(datasource_config)

        format_cls.register_data_source_format(self.session_context)
        self.registered_tables[datasource_config.table_name] = datasource_config.uri
        self.log.info(
            "Registered data source format %s for table: %s",
            datasource_config.format,
            datasource_config.table_name,
        )

    def execute_query(self, query: str, max_rows: int | None = None) -> dict[str, list[Any]]:
        """Execute a query and return the result as a dictionary."""
        try:
            self.log.info("Executing query: %s", query)
            df = self.session_context.sql(query)

            if max_rows is not None:
                result = df.limit(max_rows + 1).to_pydict()
                if result and len(next(iter(result.values()))) > max_rows:
                    self.log.warning(
                        "Query returned more than %s rows. Returning first %s rows.",
                        max_rows,
                        max_rows,
                    )
                    return {column: values[:max_rows] for column, values in result.items()}
                return result
            return df.to_pydict()
        except Exception as e:
            raise QueryExecutionException(f"Error while executing query: {e}")

    def _get_connection_config(self, conn_id: str) -> ConnectionConfig:

        airflow_conn = BaseHook.get_connection(conn_id)

        credentials, extra_config = self._get_credentials(airflow_conn)

        return ConnectionConfig(
            conn_id=airflow_conn.conn_id,
            credentials=credentials,
            extra_config=extra_config,
        )

    def _get_credentials(self, conn: Connection) -> tuple[dict[str, Any], dict[str, Any]]:

        credentials = {}
        extra_config = {}

        def _fetch_extra_configs(keys: list[str]) -> dict[str, Any]:
            conf = {}
            extra_dejson = conn.extra_dejson
            for key in keys:
                if key in extra_dejson:
                    conf[key] = conn.extra_dejson[key]
            return conf

        def _get_gcp_extra_field(extra_dejson: dict[str, Any], field_name: str) -> Any:
            # Older Airflow connection UIs wrote custom extra fields as
            # extra__google_cloud_platform__<field_name> instead of the bare key; GoogleBaseHook
            # still reads that legacy spelling as a fallback, so this must too.
            if field_name in extra_dejson:
                return extra_dejson[field_name]
            return extra_dejson.get(f"extra__google_cloud_platform__{field_name}")

        match conn.conn_type:
            case "aws":
                try:
                    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
                except ImportError:
                    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

                    raise AirflowOptionalProviderFeatureException(
                        "Failed to import AwsGenericHook. To use the S3 storage functionality, please install the "
                        "apache-airflow-providers-amazon package."
                    )
                aws_hook: AwsGenericHook = AwsGenericHook(aws_conn_id=conn.conn_id, client_type="s3")
                creds = aws_hook.get_credentials()
                credentials.update(
                    {
                        "access_key_id": conn.login or creds.access_key,
                        "secret_access_key": conn.password or creds.secret_key,
                        "session_token": creds.token if creds.token else None,
                    }
                )
                credentials = self._remove_none_values(credentials)
                extra_config = _fetch_extra_configs(["region", "endpoint"])

            case "google_cloud_platform":
                extra_dejson = conn.extra_dejson
                for unsupported_field in ("key_secret_name", "credential_config_file", "impersonation_chain"):
                    if _get_gcp_extra_field(extra_dejson, unsupported_field):
                        raise ValueError(
                            f"Connection field {unsupported_field!r} is not supported for DataFusion "
                            "GCS access; only key_path, keyfile_dict, GOOGLE_APPLICATION_CREDENTIALS, or "
                            "ambient credentials (gcloud ADC file / metadata server) are used."
                        )
                key_path = _get_gcp_extra_field(extra_dejson, "key_path") or None
                keyfile_dict = _get_gcp_extra_field(extra_dejson, "keyfile_dict") or None
                if key_path and keyfile_dict:
                    raise ValueError(
                        "The `keyfile_dict` and `key_path` fields are mutually exclusive. "
                        "Please provide only one value."
                    )
                if not key_path and not keyfile_dict:
                    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
                credentials = self._remove_none_values({"key_path": key_path, "keyfile_dict": keyfile_dict})

            case "wasb":
                extra_dejson = conn.extra_dejson
                for unsupported_field in (
                    "connection_string",
                    "managed_identity_client_id",
                    "workload_identity_tenant_id",
                ):
                    if extra_dejson.get(unsupported_field):
                        raise ValueError(
                            f"Connection field {unsupported_field!r} is not supported for DataFusion "
                            "Azure Blob Storage access; only tenant_id+login+password (service "
                            "principal), sas_token, shared_access_key/account_key/password, or ambient "
                            "credentials (AZURE_* environment variables, managed identity, workload "
                            "identity, or az login) are used."
                        )
                credentials = {"account": self._resolve_wasb_account(conn.host, conn.login)}
                tenant_id = extra_dejson.get("tenant_id")
                sas_token = extra_dejson.get("sas_token")
                explicit_credential = False
                if tenant_id:
                    if not conn.login or not conn.password:
                        # Falling through instead of raising would silently authenticate with a
                        # different identity than the one requested (ambient auth, or the client
                        # secret sent as a shared key) -- DataFusion's binding also panics on a
                        # partial client_id/client_secret/tenant_id combination.
                        missing = "login (client_id)" if not conn.login else "password (client_secret)"
                        raise ValueError(
                            f"Connection extra 'tenant_id' is set for DataFusion Azure Blob Storage "
                            f"service-principal auth, but {missing} is not."
                        )
                    credentials.update(
                        {"client_id": conn.login, "client_secret": conn.password, "tenant_id": tenant_id}
                    )
                    explicit_credential = True
                elif sas_token:
                    if sas_token.startswith("http"):
                        raise ValueError(
                            "A URL-form `sas_token` is not supported for DataFusion Azure Blob Storage "
                            "access; provide the SAS token as a query string instead."
                        )
                    credentials["sas_query_pairs"] = parse_qsl(sas_token.lstrip("?"))
                    explicit_credential = True
                else:
                    access_key = (
                        conn.password
                        or extra_dejson.get("shared_access_key")
                        or extra_dejson.get("account_key")
                    )
                    if access_key:
                        credentials["access_key"] = access_key
                        explicit_credential = True

                if explicit_credential:
                    # DataFusion's binding always calls MicrosoftAzureBuilder::from_env() before
                    # overlaying these credentials, and object_store checks an environment-derived
                    # access key or workload-identity triple before the client secret or SAS query
                    # pairs set here -- so any of these worker env vars would silently win over the
                    # connection's credential. The binding has no way to skip from_env(), so this can
                    # only be caught, not fixed, on the Python side.
                    conflicting_env_vars = [
                        var
                        for var in (
                            "AZURE_FEDERATED_TOKEN_FILE",
                            "AZURE_STORAGE_ACCOUNT_KEY",
                            "AZURE_STORAGE_ACCESS_KEY",
                            "AZURE_STORAGE_SAS_KEY",
                            "AZURE_STORAGE_TOKEN",
                        )
                        if os.environ.get(var)
                    ]
                    if conflicting_env_vars:
                        raise ValueError(
                            f"Worker environment variable(s) {', '.join(conflicting_env_vars)} would "
                            "silently take precedence over this connection's explicit credential in "
                            "DataFusion's Azure Blob Storage binding. Unset them on the worker, or "
                            "remove the explicit credential from this connection to rely on the "
                            "environment instead."
                        )
                credentials = self._remove_none_values(credentials)

            case _:
                raise ValueError(f"Unknown connection type {conn.conn_type}")
        return credentials, extra_config

    @staticmethod
    def _remove_none_values(params: dict[str, Any]) -> dict[str, Any]:
        """Filter out None values from the dictionary."""
        return {k: v for k, v in params.items() if v is not None}

    _AZURE_PUBLIC_SUFFIX = ".blob.core.windows.net"

    @classmethod
    def _resolve_wasb_account(cls, host: str | None, login: str | None) -> str | None:
        """
        Return the storage account name the way WasbHook resolves it.

        From ``host`` when set (its netloc's first label), falling back to ``login`` only when
        ``host`` is empty -- login holds the service-principal client_id in that auth mode, not
        the account name. Returns ``None`` when neither is set, so the binding falls back to
        ``AZURE_STORAGE_ACCOUNT_NAME`` instead of targeting the literal string ``"None"``.
        Reimplemented locally rather than importing
        ``airflow.providers.microsoft.azure.utils.parse_blob_account_url``, to avoid pulling the
        microsoft-azure provider's full Azure SDK dependency stack into common-sql for one string
        operation that only needs the stdlib.

        Only the public ``*.blob.core.windows.net`` cloud is supported: DataFusion's Azure binding
        takes no endpoint override, so a sovereign-cloud or emulator host would otherwise be
        silently misrouted to the public account of the same name.
        """
        if not host and not login:
            return None
        netloc = urlsplit(host if host else f"https://{login}.blob.core.windows.net/").netloc
        if not netloc:
            # No scheme was given (e.g. a bare DNS name); urlsplit put it all in the path instead.
            netloc = urlsplit(f"https://{host}").netloc
        if "." not in netloc:
            # Only an Active Directory ID was given, not a full URL or DNS name.
            netloc = f"{login}.blob.core.windows.net"
        if not netloc.endswith(cls._AZURE_PUBLIC_SUFFIX):
            raise ValueError(
                f"Connection host {host!r} does not resolve to the public {cls._AZURE_PUBLIC_SUFFIX} "
                "cloud, which is the only one DataFusion's Azure Blob Storage binding can target (it "
                "has no endpoint override). Sovereign clouds and the Azurite emulator are not "
                "supported; set the AZURE_STORAGE_ENDPOINT environment variable instead."
            )
        # Azure storage account names are capped at 24 characters.
        return netloc.split(".", 1)[0][:24]

    def get_schema(self, table_name: str):
        """Get the schema of a table."""
        schema = str(self.session_context.table(table_name).schema())
        return schema
