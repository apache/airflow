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
from airflow.providers.duckdb.hooks.duckdb import IN_MEMORY_DATABASE, DuckDBHook
from airflow.providers.duckdb.version_compat import AirflowNotFoundException

GET_CONNECTION = "airflow.providers.duckdb.hooks.duckdb.DuckDBHook.get_connection"


def make_connection(**kwargs) -> Connection:
    extra = kwargs.pop("extra", None)
    return Connection(
        conn_id="duckdb_default",
        conn_type="duckdb",
        extra=json.dumps(extra) if extra is not None else None,
        **kwargs,
    )


@mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope"))
class TestDuckDBHookWithoutConnection:
    """The Airflow connection is optional; a missing one must degrade to an in-memory database."""

    def test_missing_connection_resolves_to_in_memory(self, mock_get_connection):
        assert DuckDBHook().get_database() == IN_MEMORY_DATABASE

    def test_missing_connection_yields_empty_extra(self, mock_get_connection):
        assert DuckDBHook().connection_extra == {}

    def test_database_argument_is_used_without_a_connection(self, mock_get_connection):
        assert DuckDBHook(database="/tmp/x.duckdb").get_database() == "/tmp/x.duckdb"

    def test_get_conn_works_without_a_connection(self, mock_get_connection):
        with DuckDBHook().get_conn() as conn:
            assert conn.execute("SELECT 42").fetchone() == (42,)

    def test_read_only_in_memory_is_rejected(self, mock_get_connection):
        with pytest.raises(ValueError, match="read_only is not supported"):
            DuckDBHook(read_only=True).get_conn()


class TestDuckDBHookDatabaseResolution:
    @pytest.mark.parametrize(
        ("connection_kwargs", "expected"),
        [
            pytest.param({"host": "/tmp/from_host.duckdb"}, "/tmp/from_host.duckdb", id="host"),
            pytest.param(
                {"extra": {"database": "/tmp/from_extra.duckdb"}},
                "/tmp/from_extra.duckdb",
                id="extra-wins-over-empty-host",
            ),
            pytest.param(
                {"host": "/tmp/from_host.duckdb", "extra": {"database": "/tmp/from_extra.duckdb"}},
                "/tmp/from_extra.duckdb",
                id="extra-wins-over-host",
            ),
            pytest.param({}, IN_MEMORY_DATABASE, id="empty-connection-is-in-memory"),
        ],
    )
    def test_database_precedence(self, connection_kwargs, expected):
        with mock.patch(GET_CONNECTION, return_value=make_connection(**connection_kwargs)):
            assert DuckDBHook().get_database() == expected

    def test_database_argument_overrides_the_connection(self):
        with mock.patch(GET_CONNECTION, return_value=make_connection(host="/tmp/ignored.duckdb")):
            assert DuckDBHook(database=IN_MEMORY_DATABASE).get_database() == IN_MEMORY_DATABASE

    @pytest.mark.parametrize(
        "connection_kwargs",
        [
            pytest.param({"schema": "analytics", "password": "tok"}, id="token-in-password"),
            pytest.param({"schema": "analytics", "extra": {"motherduck_token": "tok"}}, id="token-in-extra"),
        ],
    )
    def test_motherduck_database(self, connection_kwargs):
        with mock.patch(GET_CONNECTION, return_value=make_connection(**connection_kwargs)):
            assert DuckDBHook().get_database() == "md:analytics?motherduck_token=tok"

    def test_motherduck_token_is_redacted_for_logging(self):
        redacted = DuckDBHook._redact_database("md:db?motherduck_token=supersecret")
        assert "supersecret" not in redacted
        assert redacted == "md:db?motherduck_token=***"

    def test_get_uri(self):
        with mock.patch(GET_CONNECTION, return_value=make_connection(host="/tmp/a.duckdb")):
            assert DuckDBHook().get_uri() == "duckdb:////tmp/a.duckdb"


@mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope"))
class TestDuckDBHookExtensions:
    def test_no_extensions_by_default(self, mock_get_connection):
        assert DuckDBHook().get_extensions() == []

    def test_required_extensions_come_first_and_duplicates_collapse(self, mock_get_connection):
        class RequiringHook(DuckDBHook):
            required_extensions = ("httpfs", "aws")

        assert RequiringHook(extensions=["aws", "iceberg"]).get_extensions() == [
            "httpfs",
            "aws",
            "iceberg",
        ]

    def test_load_is_attempted_before_install(self, mock_get_connection):
        conn = mock.MagicMock()
        DuckDBHook(extensions=["httpfs"]).load_extensions(conn)
        assert [call.args[0] for call in conn.execute.call_args_list] == ["LOAD httpfs;"]

    def test_install_only_happens_when_load_fails(self, mock_get_connection):
        conn = mock.MagicMock()
        conn.execute.side_effect = [duckdb.Error("not installed"), None, None]
        DuckDBHook(extensions=["httpfs"]).load_extensions(conn)
        assert [call.args[0] for call in conn.execute.call_args_list] == [
            "LOAD httpfs;",
            "INSTALL httpfs;",
            "LOAD httpfs;",
        ]

    def test_autoinstall_disabled_raises_an_actionable_error(self, mock_get_connection):
        conn = mock.MagicMock()
        conn.execute.side_effect = duckdb.Error("not installed")
        hook = DuckDBHook(extensions=["httpfs"], autoinstall_extensions=False)
        with pytest.raises(ValueError, match="autoinstall_extensions is disabled"):
            hook.load_extensions(conn)
        assert conn.execute.call_count == 1

    @pytest.mark.parametrize(
        "extension", ["httpfs; DROP TABLE t", "http-fs", "", "1httpfs", "httpfs FROM community"]
    )
    def test_extension_names_are_validated(self, mock_get_connection, extension):
        with pytest.raises(ValueError, match="Invalid DuckDB extension name"):
            DuckDBHook(extensions=[extension]).load_extensions(mock.MagicMock())

    def test_extensions_can_come_from_the_connection_extra(self, mock_get_connection):
        with mock.patch(GET_CONNECTION, return_value=make_connection(extra={"extensions": ["json"]})):
            assert DuckDBHook().get_extensions() == ["json"]

    def test_extensions_argument_overrides_the_connection_extra(self, mock_get_connection):
        with mock.patch(GET_CONNECTION, return_value=make_connection(extra={"extensions": ["json"]})):
            assert DuckDBHook(extensions=["icu"]).get_extensions() == ["icu"]


@mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope"))
class TestDuckDBHookConnectConfig:
    def test_extension_installation_is_on_by_default(self, mock_get_connection):
        config = DuckDBHook().get_connect_config()
        assert config["autoinstall_known_extensions"] is True
        assert config["autoload_known_extensions"] is True

    def test_community_extensions_are_disabled_by_default(self, mock_get_connection):
        assert DuckDBHook().get_connect_config()["allow_community_extensions"] is False

    def test_disabling_autoinstall_turns_off_network_extension_loading(self, mock_get_connection):
        config = DuckDBHook(autoinstall_extensions=False).get_connect_config()
        assert config["autoinstall_known_extensions"] is False
        assert config["autoload_known_extensions"] is False

    def test_resource_limits_are_passed_through(self, mock_get_connection):
        config = DuckDBHook(
            memory_limit="2GB", threads=3, temp_directory="/tmp/spill", extension_directory="/opt/ext"
        ).get_connect_config()
        assert config["memory_limit"] == "2GB"
        assert config["threads"] == 3
        assert config["temp_directory"] == "/tmp/spill"
        assert config["extension_directory"] == "/opt/ext"

    def test_resource_limits_are_absent_when_unset(self, mock_get_connection):
        config = DuckDBHook().get_connect_config()
        assert "memory_limit" not in config
        assert "threads" not in config
        assert "temp_directory" not in config

    def test_settings_argument_wins_over_the_connection_extra(self, mock_get_connection):
        with mock.patch(
            GET_CONNECTION,
            return_value=make_connection(extra={"memory_limit": "1GB", "settings": {"threads": 1}}),
        ):
            hook = DuckDBHook(settings={"threads": 8})
            config = hook.get_connect_config()
            assert config["memory_limit"] == "1GB"
            assert config["threads"] == 8

    def test_resource_limits_are_actually_applied_to_the_database(self, mock_get_connection):
        # DuckDB reports memory_limit back in binary units, so ask for binary units to compare.
        with DuckDBHook(memory_limit="512MiB", threads=2).get_conn() as conn:
            assert conn.execute("SELECT current_setting('threads')").fetchone() == (2,)
            assert conn.execute("SELECT current_setting('memory_limit')").fetchone() == ("512.0 MiB",)


@mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope"))
class TestDuckDBHookSecrets:
    def test_configure_secrets_is_a_no_op_by_default(self, mock_get_connection):
        conn = mock.MagicMock()
        DuckDBHook().configure_secrets(conn)
        conn.execute.assert_not_called()

    def test_get_conn_closes_the_database_when_setup_fails(self, mock_get_connection):
        class FailingHook(DuckDBHook):
            def configure_secrets(self, conn):
                raise RuntimeError("boom")

        with mock.patch.object(duckdb, "connect") as mock_connect:
            with pytest.raises(RuntimeError, match="boom"):
                FailingHook().get_conn()
        mock_connect.return_value.close.assert_called_once()


class TestDuckDBHookConnectionExtraResolution:
    """
    Every hook parameter must be settable from the connection ``extra``.

    ``BaseSQLOperator.get_hook()`` normally merges connection extras into the hook's keyword
    arguments, but the connection-optional operator bypasses that path, so the hook resolves them
    itself. Without this, connection-level configuration is silently ignored — and for
    ``autoinstall_extensions`` that fails unsafe, leaving runtime extension downloads enabled.
    """

    @pytest.mark.parametrize(
        ("extra_key", "extra_value", "probe", "expected"),
        [
            pytest.param(
                "database", "/tmp/e.duckdb", lambda h: h.get_database(), "/tmp/e.duckdb", id="database"
            ),
            pytest.param("extensions", ["json"], lambda h: h.get_extensions(), ["json"], id="extensions"),
            pytest.param(
                "extension_directory",
                "/opt/ext",
                lambda h: h.extension_directory,
                "/opt/ext",
                id="extension_directory",
            ),
            pytest.param(
                "autoinstall_extensions",
                False,
                lambda h: h.autoinstall_extensions,
                False,
                id="autoinstall_extensions",
            ),
            pytest.param(
                "allow_community_extensions",
                True,
                lambda h: h.allow_community_extensions,
                True,
                id="allow_community_extensions",
            ),
            pytest.param("memory_limit", "3GB", lambda h: h.memory_limit, "3GB", id="memory_limit"),
            pytest.param("threads", 7, lambda h: h.threads, 7, id="threads"),
            pytest.param(
                "temp_directory", "/tmp/spill", lambda h: h.temp_directory, "/tmp/spill", id="temp_directory"
            ),
            pytest.param("read_only", True, lambda h: h.read_only, True, id="read_only"),
            pytest.param("settings", {"threads": 9}, lambda h: h.settings, {"threads": 9}, id="settings"),
        ],
    )
    def test_parameter_can_come_from_the_connection_extra(self, extra_key, extra_value, probe, expected):
        with mock.patch(GET_CONNECTION, return_value=make_connection(extra={extra_key: extra_value})):
            assert probe(DuckDBHook()) == expected

    @pytest.mark.parametrize(
        ("kwarg", "extra_value", "explicit_value", "probe"),
        [
            pytest.param(
                "extension_directory",
                "/from/extra",
                "/explicit",
                lambda h: h.extension_directory,
                id="extension_directory",
            ),
            pytest.param(
                "autoinstall_extensions",
                True,
                False,
                lambda h: h.autoinstall_extensions,
                id="autoinstall_extensions",
            ),
            pytest.param(
                "allow_community_extensions",
                False,
                True,
                lambda h: h.allow_community_extensions,
                id="allow_community_extensions",
            ),
            pytest.param("memory_limit", "1GB", "8GB", lambda h: h.memory_limit, id="memory_limit"),
            pytest.param("threads", 1, 16, lambda h: h.threads, id="threads"),
            pytest.param("read_only", False, True, lambda h: h.read_only, id="read_only"),
        ],
    )
    def test_explicit_argument_wins_over_the_connection_extra(
        self, kwarg, extra_value, explicit_value, probe
    ):
        with mock.patch(GET_CONNECTION, return_value=make_connection(extra={kwarg: extra_value})):
            assert probe(DuckDBHook(**{kwarg: explicit_value})) == explicit_value

    def test_disabling_autoinstall_on_the_connection_reaches_the_duckdb_config(self):
        """The deployment-hardening path: no runtime extension downloads, set once on the connection."""
        with mock.patch(
            GET_CONNECTION, return_value=make_connection(extra={"autoinstall_extensions": False})
        ):
            config = DuckDBHook().get_connect_config()
        assert config["autoinstall_known_extensions"] is False
        assert config["autoload_known_extensions"] is False


@mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope"))
class TestDuckDBHookAutocommit:
    """
    DuckDB is transactional but exposes no ``autocommit`` attribute, so the hook declares no support.

    These tests pin the current behaviour rather than endorse it: ``autocommit`` is accepted and
    ignored, and statements are still committed explicitly by ``DbApiHook``.
    """

    def test_autocommit_is_declared_unsupported(self, mock_get_connection):
        assert DuckDBHook.supports_autocommit is False

    def test_duckdb_connections_reject_the_autocommit_attribute(self, mock_get_connection):
        """Why the flag cannot simply be flipped: ``DbApiHook.set_autocommit`` would raise."""
        with DuckDBHook().get_conn() as conn:
            with pytest.raises(AttributeError, match="autocommit"):
                conn.autocommit = True

    def test_statements_are_committed_even_though_autocommit_is_unsupported(
        self, mock_get_connection, tmp_path
    ):
        database = str(tmp_path / "committed.duckdb")
        hook = DuckDBHook(database=database)
        hook.run("CREATE TABLE t AS SELECT 1 AS x")
        assert DuckDBHook(database=database).get_records("SELECT x FROM t") == [(1,)]


@mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope"))
class TestDuckDBHookDialect:
    """
    ``dialect_name`` is pinned rather than inferred, and no DuckDB dialect is registered yet.

    Pinned so that adding a dialect, or choosing a reserved-word source, is a deliberate change.
    """

    def test_placeholder_is_duckdb_style(self, mock_get_connection):
        assert DuckDBHook.placeholder == "?"

    def test_dialect_name_is_pinned(self, mock_get_connection):
        assert DuckDBHook().dialect_name == "duckdb"

    def test_dialect_name_does_not_depend_on_sqlalchemy_dialect_resolution(self, mock_get_connection):
        """
        The point of pinning: whether ``duckdb-engine`` is installed must not change the answer.

        ``DbApiHook`` would call ``make_url(self.get_uri()).get_dialect()``, which resolves only when
        that third-party package is present.
        """
        with mock.patch(
            "airflow.providers.common.sql.hooks.sql.make_url", side_effect=AssertionError("resolved")
        ):
            assert DuckDBHook().dialect_name == "duckdb"

    def test_dialect_name_can_still_be_overridden_on_the_connection(self, mock_get_connection):
        with mock.patch(GET_CONNECTION, return_value=make_connection(extra={"dialect": "postgresql"})):
            assert DuckDBHook().dialect_name == "postgresql"

    def test_dialect_falls_back_to_the_generic_implementation(self, mock_get_connection):
        """No ``duckdb`` entry exists in the common.sql dialect registry yet."""
        assert type(DuckDBHook().dialect).__name__ == "Dialect"

    def test_no_reserved_words_are_known(self, mock_get_connection):
        """Pinning makes this deterministic, not populated: ``insert_rows`` still won't quote ``order``."""
        assert DuckDBHook().reserved_words == set()
