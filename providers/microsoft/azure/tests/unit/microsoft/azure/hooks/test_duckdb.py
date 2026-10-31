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

import json
from unittest import mock

import pytest
from duckdb import DuckDBPyConnection

from airflow.models.connection import Connection
from airflow.providers.duckdb.hooks.duckdb import DuckDBHook
from airflow.providers.duckdb.operators.duckdb import DuckDBExecuteQueryOperator
from airflow.providers.microsoft.azure.hooks.duckdb import AzureDuckDBHook

try:
    from airflow.sdk.exceptions import AirflowNotFoundException
except ImportError:
    from airflow.exceptions import AirflowNotFoundException  # type: ignore[no-redef]

GET_CONNECTION = "airflow.providers.microsoft.azure.hooks.duckdb.AzureDuckDBHook.get_connection"

SERVICE_PRINCIPAL = Connection(
    conn_id="adls_default",
    conn_type="adls",
    login="client-id",
    password="client-secret",
    extra=json.dumps(
        {
            "tenant_id": "tenant-id",
            "account_url": "https://onelake.dfs.fabric.microsoft.com",
            "proxies": {"https": "http://proxy:8080"},
        }
    ),
)


def connections(*available: Connection):
    """Patch get_connection to return ``available`` by id; any other id (the DuckDB one) is missing."""
    by_id = {connection.conn_id: connection for connection in available}

    def get_connection(conn_id):
        if conn_id not in by_id:
            raise AirflowNotFoundException(conn_id)
        return by_id[conn_id]

    return mock.patch(GET_CONNECTION, side_effect=get_connection)


def statements(hook: AzureDuckDBHook) -> list[str]:
    """Run configure_secrets against a fake DuckDB connection; return the statements it issued."""
    conn = mock.create_autospec(DuckDBPyConnection, instance=True)
    hook.configure_secrets(conn)
    return [call.args[0] for call in conn.execute.call_args_list]


def secret_statement(hook: AzureDuckDBHook) -> str:
    issued = statements(hook)
    return issued[-1] if issued else ""


class TestAzureDuckDBHookConnectionType:
    """The connection type is the integration point: it is how the generic operator reaches this hook."""

    def test_declares_its_own_connection_type(self):
        assert AzureDuckDBHook.conn_type == "duckdb_azure"
        assert AzureDuckDBHook.conn_type != DuckDBHook.conn_type
        assert AzureDuckDBHook.default_conn_name == "duckdb_azure_default"

    def test_reuses_the_duckdb_connection_id_attribute(self):
        """``Connection.get_hook`` passes the conn id under this name, so it has to keep matching."""
        assert AzureDuckDBHook.conn_name_attr == DuckDBHook.conn_name_attr

    def test_the_generic_operator_resolves_this_hook_from_the_connection(self):
        connection = Connection(conn_id="azure_duck", conn_type=AzureDuckDBHook.conn_type)
        operator = DuckDBExecuteQueryOperator(task_id="t", sql="SELECT 1", conn_id="azure_duck")
        with (
            mock.patch(GET_CONNECTION, return_value=connection),
            mock.patch(
                "airflow.providers.duckdb.hooks.duckdb.DuckDBHook.get_connection", return_value=connection
            ),
        ):
            hook = operator.get_db_hook()

        assert isinstance(hook, AzureDuckDBHook)
        assert hook.get_conn_id() == "azure_duck"

    def test_ui_hides_the_fields_it_does_not_use(self):
        behaviour = AzureDuckDBHook.get_ui_field_behaviour()
        assert set(behaviour["hidden_fields"]) == {"login", "password", "port", "schema"}
        assert behaviour["relabeling"] == {"host": "Database path"}


class TestAzureDuckDBHookExtensions:
    def test_azure_extension_is_required(self):
        with connections():
            assert AzureDuckDBHook().get_extensions() == ["azure"]

    def test_required_extension_is_kept_when_more_are_requested(self):
        with connections():
            assert AzureDuckDBHook(extensions=["delta"]).get_extensions() == ["azure", "delta"]

    def test_extension_downloads_are_enabled_for_azure(self):
        with connections():
            assert AzureDuckDBHook().autoinstall_extensions is True

    def test_extension_downloads_can_still_be_forbidden(self):
        with connections():
            assert AzureDuckDBHook(autoinstall_extensions=False).autoinstall_extensions is False


class TestAzureDuckDBHookConfigStrategy:
    def test_config_is_the_default_and_uses_the_service_principal(self):
        with connections(SERVICE_PRINCIPAL):
            statement = secret_statement(AzureDuckDBHook())

        assert statement == (
            "CREATE OR REPLACE SECRET airflow_azure (TYPE azure, PROVIDER service_principal, "
            "TENANT_ID 'tenant-id', CLIENT_ID 'client-id', CLIENT_SECRET 'client-secret', "
            "ACCOUNT_NAME 'onelake', HTTP_PROXY 'http://proxy:8080');"
        )

    def test_connection_string_wins_over_the_service_principal(self):
        connection = Connection(
            conn_id="adls_default",
            conn_type="adls",
            login="client-id",
            password="client-secret",
            extra=json.dumps({"connection_string": "AccountName=acc;AccountKey=key", "tenant_id": "t"}),
        )
        with connections(connection):
            statement = secret_statement(AzureDuckDBHook())

        assert "PROVIDER config, CONNECTION_STRING 'AccountName=acc;AccountKey=key'" in statement
        assert "CLIENT_SECRET" not in statement

    def test_account_falls_back_to_the_host(self):
        connection = Connection(
            conn_id="adls_default",
            conn_type="adls",
            host="storageaccount",
            login="client-id",
            password="client-secret",
            extra=json.dumps({"tenant_id": "tenant-id"}),
        )
        with connections(connection):
            statement = secret_statement(AzureDuckDBHook())

        assert "ACCOUNT_NAME 'storageaccount'" in statement
        assert "HTTP_PROXY" not in statement

    def test_account_name_argument_wins(self):
        with connections(SERVICE_PRINCIPAL):
            statement = secret_statement(AzureDuckDBHook(account_name="other"))

        assert "ACCOUNT_NAME 'other'" in statement

    def test_missing_credentials_are_rejected(self):
        connection = Connection(conn_id="adls_default", conn_type="adls", host="storageaccount")
        with connections(connection), pytest.raises(ValueError, match="needs a connection_string"):
            secret_statement(AzureDuckDBHook())

    def test_quotes_in_credentials_cannot_break_out_of_the_literal(self):
        connection = Connection(
            conn_id="adls_default",
            conn_type="adls",
            login="client-id",
            password="it's",
            extra=json.dumps({"tenant_id": "tenant-id"}),
        )
        with connections(connection):
            statement = secret_statement(AzureDuckDBHook())

        assert "CLIENT_SECRET 'it''s'" in statement


class TestAzureDuckDBHookOtherStrategies:
    def test_credential_chain_does_not_write_credentials(self):
        with connections(SERVICE_PRINCIPAL):
            statement = secret_statement(
                AzureDuckDBHook(
                    credential_strategy="credential_chain", credential_chain="managed_identity;env"
                )
            )

        assert statement == (
            "CREATE OR REPLACE SECRET airflow_azure (TYPE azure, PROVIDER credential_chain, "
            "CHAIN 'managed_identity;env', ACCOUNT_NAME 'onelake', HTTP_PROXY 'http://proxy:8080');"
        )

    def test_credential_chain_needs_no_adls_connection(self):
        with connections():
            statement = secret_statement(
                AzureDuckDBHook(credential_strategy="credential_chain", account_name="myaccount")
            )

        assert statement == (
            "CREATE OR REPLACE SECRET airflow_azure (TYPE azure, PROVIDER credential_chain, "
            "ACCOUNT_NAME 'myaccount');"
        )

    def test_config_strategy_needs_the_adls_connection(self):
        with connections(), pytest.raises(AirflowNotFoundException):
            secret_statement(AzureDuckDBHook())

    def test_strategy_none_creates_no_secret(self):
        with connections():
            assert statements(AzureDuckDBHook(credential_strategy="none")) == []

    def test_unknown_strategy_is_rejected(self):
        with connections(SERVICE_PRINCIPAL), pytest.raises(ValueError, match="Unknown credential_strategy"):
            secret_statement(AzureDuckDBHook(credential_strategy="bogus"))  # type: ignore[arg-type]

    def test_secret_name_is_validated(self):
        with connections(SERVICE_PRINCIPAL), pytest.raises(ValueError, match="Invalid DuckDB secret name"):
            secret_statement(AzureDuckDBHook(secret_name="bad name; DROP"))

    def test_secret_name_is_used(self):
        with connections(SERVICE_PRINCIPAL):
            statement = secret_statement(AzureDuckDBHook(secret_name="lake"))

        assert statement.startswith("CREATE OR REPLACE SECRET lake (TYPE azure,")


class TestAzureDuckDBHookTransport:
    def test_no_transport_setting_by_default(self):
        with connections(SERVICE_PRINCIPAL):
            issued = statements(AzureDuckDBHook())

        assert len(issued) == 1
        assert issued[0].startswith("CREATE OR REPLACE SECRET")

    def test_transport_is_set_globally_before_the_secret(self):
        with connections(SERVICE_PRINCIPAL):
            issued = statements(AzureDuckDBHook(transport_option_type="curl"))

        assert issued[0] == "SET GLOBAL azure_transport_option_type = 'curl';"
        assert issued[1].startswith("CREATE OR REPLACE SECRET")

    def test_unknown_transport_is_rejected(self):
        with connections(SERVICE_PRINCIPAL), pytest.raises(ValueError, match="Unknown transport_option_type"):
            statements(AzureDuckDBHook(transport_option_type="winhttp"))  # type: ignore[arg-type]


class TestAzureDuckDBHookConnectionOverrides:
    """Every parameter may be set in the extra of the DuckDB connection; an explicit argument wins."""

    duckdb_connection = Connection(
        conn_id="duckdb_azure_default",
        conn_type="duckdb_azure",
        extra=json.dumps(
            {
                "azure_conn_id": "lake",
                "credential_strategy": "credential_chain",
                "account_name": "fromextra",
                "transport_option_type": "curl",
                "secret_name": "from_extra",
            }
        ),
    )
    lake = Connection(conn_id="lake", conn_type="adls", host="lakeaccount")

    def test_parameters_come_from_the_connection_extra(self):
        with connections(self.duckdb_connection, self.lake):
            hook = AzureDuckDBHook()
            issued = statements(hook)

        assert hook.azure_conn_id == "lake"
        assert issued == [
            "SET GLOBAL azure_transport_option_type = 'curl';",
            "CREATE OR REPLACE SECRET from_extra (TYPE azure, PROVIDER credential_chain, "
            "ACCOUNT_NAME 'fromextra');",
        ]

    def test_explicit_argument_wins_over_the_connection_extra(self):
        with connections(self.duckdb_connection, self.lake):
            hook = AzureDuckDBHook(account_name="explicit", secret_name="explicit")
            statement = secret_statement(hook)

        assert statement.startswith("CREATE OR REPLACE SECRET explicit (")
        assert "ACCOUNT_NAME 'explicit'" in statement

    def test_azure_conn_id_defaults_to_adls_default(self):
        with connections():
            assert AzureDuckDBHook().azure_conn_id == "adls_default"
