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

import duckdb
import pytest

from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.duckdb import AwsDuckDBHook
from airflow.providers.duckdb.hooks.duckdb import DuckDBHook
from airflow.providers.duckdb.operators.duckdb import DuckDBExecuteQueryOperator

GET_CONNECTION = "airflow.providers.amazon.aws.hooks.duckdb.AwsDuckDBHook.get_connection"
AWS_BASE_HOOK = "airflow.providers.amazon.aws.hooks.duckdb.AwsBaseHook"

try:
    from airflow.sdk.exceptions import AirflowNotFoundException
except ImportError:
    from airflow.exceptions import AirflowNotFoundException  # type: ignore[no-redef]


@pytest.fixture
def aws_base_hook():
    """Patch AwsBaseHook so no AWS calls or connections are needed."""
    with mock.patch(AWS_BASE_HOOK) as patched:
        instance = patched.return_value
        instance.region_name = "us-west-2"
        instance.conn_config.endpoint_url = None
        credentials = instance.get_credentials.return_value
        credentials.access_key = "AKIAEXAMPLE"
        credentials.secret_key = "secret"
        credentials.token = None
        yield instance


@pytest.fixture
def no_duckdb_connection():
    with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")) as patched:
        yield patched


def secret_statement(hook) -> str:
    """Run configure_secrets against a fake connection and return the statement it issued."""
    conn = mock.MagicMock()
    hook.configure_secrets(conn)
    if not conn.execute.call_args_list:
        return ""
    return conn.execute.call_args_list[-1].args[0]


class TestAwsDuckDBHookConnectionType:
    """The connection type is the integration point: it is how the generic operator reaches this hook."""

    def test_declares_its_own_connection_type(self):
        assert AwsDuckDBHook.conn_type == "duckdb_aws"
        assert AwsDuckDBHook.conn_type != DuckDBHook.conn_type
        assert AwsDuckDBHook.default_conn_name == "duckdb_aws_default"

    def test_reuses_the_duckdb_connection_id_attribute(self):
        """``Connection.get_hook`` passes the conn id under this name, so it has to keep matching."""
        assert AwsDuckDBHook.conn_name_attr == DuckDBHook.conn_name_attr

    def test_the_generic_operator_resolves_this_hook_from_the_connection(self):
        """
        The whole point of the connection type: no AWS-specific operator.

        A Dag author swaps from local DuckDB to DuckDB on S3 by pointing the same operator at a
        ``duckdb_aws`` connection.
        """
        connection = Connection(conn_id="aws_duck", conn_type=AwsDuckDBHook.conn_type)
        operator = DuckDBExecuteQueryOperator(task_id="t", sql="SELECT 1", conn_id="aws_duck")
        with (
            mock.patch(GET_CONNECTION, return_value=connection),
            mock.patch(
                "airflow.providers.duckdb.hooks.duckdb.DuckDBHook.get_connection", return_value=connection
            ),
        ):
            hook = operator.get_db_hook()

        assert isinstance(hook, AwsDuckDBHook)
        assert hook.get_conn_id() == "aws_duck"

    def test_ui_hides_the_fields_it_does_not_use(self):
        behaviour = AwsDuckDBHook.get_ui_field_behaviour()
        assert set(behaviour["hidden_fields"]) == {"login", "password", "port", "schema"}
        assert behaviour["relabeling"] == {"host": "Database path"}


@pytest.mark.usefixtures("no_duckdb_connection")
class TestAwsDuckDBHookExtensions:
    def test_httpfs_and_aws_extensions_are_required(self, aws_base_hook):
        assert AwsDuckDBHook().get_extensions() == ["httpfs", "aws"]

    def test_required_extensions_are_kept_when_more_are_requested(self, aws_base_hook):
        assert AwsDuckDBHook(extensions=["iceberg"]).get_extensions() == ["httpfs", "aws", "iceberg"]

    def test_extension_downloads_are_enabled_for_aws(self, aws_base_hook):
        """
        The AWS hook installs its required extensions, diverging from the generic hook's default.

        Reaching for this hook implies an AWS environment where httpfs and aws should just work.
        """
        config = AwsDuckDBHook().get_connect_config()
        assert config["autoinstall_known_extensions"] is True
        assert config["autoload_known_extensions"] is True

    def test_extension_downloads_can_still_be_forbidden(self, aws_base_hook):
        config = AwsDuckDBHook(autoinstall_extensions=False).get_connect_config()
        assert config["autoinstall_known_extensions"] is False


@pytest.mark.usefixtures("no_duckdb_connection")
class TestAwsDuckDBHookCredentials:
    def test_credential_chain_is_the_default(self, aws_base_hook):
        statement = secret_statement(AwsDuckDBHook())
        assert "PROVIDER credential_chain" in statement
        assert "KEY_ID" not in statement

    def test_credential_chain_does_not_resolve_credentials_itself(self, aws_base_hook):
        """The point of credential_chain: nothing secret is materialised into the SQL statement."""
        secret_statement(AwsDuckDBHook())
        aws_base_hook.get_credentials.assert_not_called()

    def test_explicit_chain_is_passed_through(self, aws_base_hook):
        statement = secret_statement(AwsDuckDBHook(credential_chain="env;instance"))
        assert "CHAIN 'env;instance'" in statement

    def test_config_strategy_writes_the_credentials_into_the_secret(self, aws_base_hook):
        statement = secret_statement(AwsDuckDBHook(credential_strategy="config"))
        assert "PROVIDER config" in statement
        assert "KEY_ID 'AKIAEXAMPLE'" in statement
        assert "SECRET 'secret'" in statement
        assert "SESSION_TOKEN" not in statement

    def test_config_strategy_includes_a_session_token_when_present(self, aws_base_hook):
        aws_base_hook.get_credentials.return_value.token = "token123"
        statement = secret_statement(AwsDuckDBHook(credential_strategy="config"))
        assert "SESSION_TOKEN 'token123'" in statement

    def test_quotes_in_credentials_cannot_break_out_of_the_literal(self, aws_base_hook):
        aws_base_hook.get_credentials.return_value.secret_key = "abc' OR '1'='1"
        statement = secret_statement(AwsDuckDBHook(credential_strategy="config"))
        assert "SECRET 'abc'' OR ''1''=''1'" in statement

    def test_strategy_none_creates_no_secret(self, aws_base_hook):
        assert secret_statement(AwsDuckDBHook(credential_strategy="none")) == ""

    def test_unknown_strategy_is_rejected(self, aws_base_hook):
        with pytest.raises(ValueError, match="Unknown credential_strategy"):
            secret_statement(AwsDuckDBHook(credential_strategy="magic"))

    def test_secret_name_is_validated(self, aws_base_hook):
        with pytest.raises(ValueError, match="Invalid DuckDB secret name"):
            secret_statement(AwsDuckDBHook(secret_name="a); DROP TABLE t; --"))

    def test_secret_name_is_used(self, aws_base_hook):
        statement = secret_statement(AwsDuckDBHook(secret_name="my_secret"))
        assert statement.startswith("CREATE OR REPLACE SECRET my_secret (")

    @pytest.mark.parametrize(
        ("strategy", "tolerated"),
        [
            pytest.param("credential_chain", True, id="chain-warns"),
            pytest.param("config", False, id="config-raises"),
        ],
    )
    def test_secret_creation_failure_depends_on_the_strategy(self, aws_base_hook, strategy, tolerated):
        """
        DuckDB 1.4+ resolves the credential chain when the secret is created, not when S3 is read.

        A chain that finds nothing is not a misconfiguration, so it must not stop the connection
        opening. Explicit keys failing is a misconfiguration and must.
        """
        conn = mock.MagicMock()
        conn.execute.side_effect = duckdb.Error("Secret Validation Failure")
        hook = AwsDuckDBHook(credential_strategy=strategy)
        with mock.patch.object(AwsDuckDBHook, "log") as mock_log:
            if tolerated:
                hook.configure_secrets(conn)
                mock_log.warning.assert_called_once()
            else:
                with pytest.raises(duckdb.Error):
                    hook.configure_secrets(conn)
                mock_log.warning.assert_not_called()


@pytest.mark.usefixtures("no_duckdb_connection")
class TestAwsDuckDBHookRegion:
    def test_region_comes_from_the_aws_connection_by_default(self, aws_base_hook):
        assert AwsDuckDBHook().get_region_name() == "us-west-2"
        assert "REGION 'us-west-2'" in secret_statement(AwsDuckDBHook())

    def test_region_argument_wins(self, aws_base_hook):
        assert AwsDuckDBHook(region_name="eu-central-1").get_region_name() == "eu-central-1"

    def test_no_region_clause_when_no_region_is_resolvable(self, aws_base_hook):
        aws_base_hook.region_name = None
        assert "REGION" not in secret_statement(AwsDuckDBHook())


class TestAwsDuckDBHookEndpoint:
    @pytest.mark.parametrize(
        ("endpoint_url", "expected_endpoint", "expects_plaintext"),
        [
            pytest.param("http://localhost:4566", "localhost:4566", True, id="http-with-port"),
            pytest.param(
                "https://s3.us-west-2.amazonaws.com", "s3.us-west-2.amazonaws.com", False, id="https"
            ),
            pytest.param("localhost:9000", "localhost:9000", False, id="bare-host-port"),
        ],
    )
    def test_endpoint_is_translated_into_secret_clauses(
        self, aws_base_hook, no_duckdb_connection, endpoint_url, expected_endpoint, expects_plaintext
    ):
        statement = secret_statement(AwsDuckDBHook(s3_endpoint_url=endpoint_url))
        assert f"ENDPOINT '{expected_endpoint}'" in statement
        assert "URL_STYLE 'path'" in statement
        assert ("USE_SSL false" in statement) is expects_plaintext

    def test_endpoint_falls_back_to_the_aws_connection(self, aws_base_hook, no_duckdb_connection):
        aws_base_hook.conn_config.endpoint_url = "http://localstack:4566"
        assert "ENDPOINT 'localstack:4566'" in secret_statement(AwsDuckDBHook())

    def test_no_endpoint_clauses_by_default(self, aws_base_hook, no_duckdb_connection):
        statement = secret_statement(AwsDuckDBHook())
        assert "ENDPOINT" not in statement
        assert "URL_STYLE" not in statement


class TestAwsDuckDBHookConnectionOverrides:
    """The DuckDB connection extra may override how AWS access is configured."""

    @staticmethod
    def duckdb_connection(**extra) -> Connection:
        return Connection(conn_id="duckdb_default", conn_type="duckdb", extra=json.dumps(extra))

    def test_credential_strategy_can_be_set_on_the_connection(self, aws_base_hook):
        with mock.patch(GET_CONNECTION, return_value=self.duckdb_connection(credential_strategy="none")):
            assert secret_statement(AwsDuckDBHook()) == ""

    def test_region_can_be_set_on_the_connection(self, aws_base_hook):
        with mock.patch(GET_CONNECTION, return_value=self.duckdb_connection(region_name="ap-south-1")):
            assert AwsDuckDBHook().get_region_name() == "ap-south-1"

    def test_endpoint_can_be_set_on_the_connection(self, aws_base_hook):
        with mock.patch(
            GET_CONNECTION, return_value=self.duckdb_connection(endpoint_url="http://minio:9000")
        ):
            assert "ENDPOINT 'minio:9000'" in secret_statement(AwsDuckDBHook())

    def test_secret_name_can_be_set_on_the_connection(self, aws_base_hook):
        with mock.patch(GET_CONNECTION, return_value=self.duckdb_connection(secret_name="from_extra")):
            assert secret_statement(AwsDuckDBHook()).startswith("CREATE OR REPLACE SECRET from_extra (")

    def test_credential_chain_can_be_set_on_the_connection(self, aws_base_hook):
        with mock.patch(GET_CONNECTION, return_value=self.duckdb_connection(credential_chain="env;instance")):
            assert "CHAIN 'env;instance'" in secret_statement(AwsDuckDBHook())

    def test_aws_conn_id_defaults_to_aws_default(self, aws_base_hook, no_duckdb_connection):
        assert AwsDuckDBHook().aws_conn_id == "aws_default"

    def test_aws_conn_id_can_be_set_on_the_connection(self, aws_base_hook):
        with mock.patch(GET_CONNECTION, return_value=self.duckdb_connection(aws_conn_id="from_extra")):
            assert AwsDuckDBHook().aws_conn_id == "from_extra"

    def test_aws_conn_id_null_on_the_connection_selects_the_ambient_environment(self, aws_base_hook):
        """
        ``None`` is a valid choice rather than the absence of one, so a null in the extra has to stick.

        Resolving it like the other parameters would read it as "not set" and quietly fall back to
        ``aws_default``, which is the opposite of what was asked for.
        """
        with mock.patch(GET_CONNECTION, return_value=self.duckdb_connection(aws_conn_id=None)):
            assert AwsDuckDBHook().aws_conn_id is None

    @pytest.mark.parametrize("explicit", ["explicit_conn", None], ids=["a-name", "none"])
    def test_explicit_aws_conn_id_wins_over_the_connection(self, aws_base_hook, explicit):
        with mock.patch(GET_CONNECTION, return_value=self.duckdb_connection(aws_conn_id="from_extra")):
            assert AwsDuckDBHook(aws_conn_id=explicit).aws_conn_id == explicit

    def test_s3_endpoint_url_can_be_set_on_the_connection(self, aws_base_hook):
        with mock.patch(
            GET_CONNECTION, return_value=self.duckdb_connection(s3_endpoint_url="http://minio:9000")
        ):
            assert "ENDPOINT 'minio:9000'" in secret_statement(AwsDuckDBHook())

    def test_explicit_argument_wins_over_the_connection_extra(self, aws_base_hook):
        with mock.patch(GET_CONNECTION, return_value=self.duckdb_connection(secret_name="from_extra")):
            statement = secret_statement(AwsDuckDBHook(secret_name="explicit"))
        assert statement.startswith("CREATE OR REPLACE SECRET explicit (")
