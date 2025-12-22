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

from unittest import mock
from unittest.mock import MagicMock, Mock, call, patch

import pendulum
import pytest
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import OperationalError

from airflow.cli import cli_parser
from airflow.cli.commands import db_command
from airflow.exceptions import AirflowException

pytestmark = pytest.mark.db_test


class TestCliDb:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.db_command.db.resetdb")
    def test_cli_resetdb(self, mock_resetdb):
        db_command.resetdb(self.parser.parse_args(["db", "reset", "--yes"]))

        mock_resetdb.assert_called_once_with(skip_init=False)

    @mock.patch("airflow.cli.commands.db_command.db.resetdb")
    def test_cli_resetdb_skip_init(self, mock_resetdb):
        db_command.resetdb(self.parser.parse_args(["db", "reset", "--yes", "--skip-init"]))
        mock_resetdb.assert_called_once_with(skip_init=True)

    def test_run_db_migrate_command_success_and_messages(self, capsys):
        class Args:
            to_revision = None
            to_version = None
            from_revision = None
            from_version = None
            show_sql_only = False

        called = {}

        def fake_command(**kwargs):
            called.update(kwargs)

        heads = {"2.10.0": "22ed7efa9da2"}

        db_command.run_db_migrate_command(Args(), fake_command, heads)
        out = capsys.readouterr().out
        assert "Performing upgrade" in out
        assert "Database migrating done!" in out
        assert called == {"to_revision": None, "from_revision": None, "show_sql_only": False}

    def test_run_db_migrate_command_offline_generation(self, capsys):
        class Args:
            to_revision = None
            to_version = None
            from_revision = None
            from_version = None
            show_sql_only = True

        called = {}

        def fake_command(**kwargs):
            called.update(kwargs)

        heads = {"2.10.0": "22ed7efa9da2"}

        db_command.run_db_migrate_command(Args(), fake_command, heads)
        out = capsys.readouterr().out
        assert "Generating sql for upgrade" in out
        assert called == {"to_revision": None, "from_revision": None, "show_sql_only": True}

    @pytest.mark.parametrize(
        "args, match",
        [
            (
                {
                    "to_revision": "abc",
                    "to_version": "2.10.0",
                    "from_revision": None,
                    "from_version": None,
                    "show_sql_only": False,
                },
                "Cannot supply both",
            ),
            (
                {
                    "to_revision": None,
                    "to_version": None,
                    "from_revision": "abc",
                    "from_version": "2.10.0",
                    "show_sql_only": True,
                },
                "Cannot supply both",
            ),
            (
                {
                    "to_revision": None,
                    "to_version": None,
                    "from_revision": "abc",
                    "from_version": None,
                    "show_sql_only": False,
                },
                "only .* with `--show-sql-only`",
            ),
            (
                {
                    "to_revision": None,
                    "to_version": "abc",
                    "from_revision": None,
                    "from_version": None,
                    "show_sql_only": False,
                },
                "Invalid version",
            ),
            (
                {
                    "to_revision": None,
                    "to_version": "2.1.25",
                    "from_revision": None,
                    "from_version": None,
                    "show_sql_only": False,
                },
                "Unknown version",
            ),
        ],
    )
    def test_run_db_migrate_command_validation_errors(self, args, match):
        class Args:
            to_revision = args["to_revision"]
            to_version = args["to_version"]
            from_revision = args["from_revision"]
            from_version = args["from_version"]
            show_sql_only = args["show_sql_only"]

        def fake_command(**kwargs):
            pass

        heads = {"2.10.0": "22ed7efa9da2"}

        with pytest.raises(SystemExit, match=match):
            db_command.run_db_migrate_command(Args(), fake_command, heads)

    @mock.patch("airflow.cli.commands.db_command.db.check_migrations")
    def test_cli_check_migrations(self, mock_wait_for_migrations):
        db_command.check_migrations(self.parser.parse_args(["db", "check-migrations"]))

        mock_wait_for_migrations.assert_called_once_with(timeout=60)

    @pytest.mark.parametrize(
        "args, called_with",
        [
            (
                [],
                dict(
                    to_revision=None,
                    from_revision=None,
                    show_sql_only=False,
                ),
            ),
            (
                ["--show-sql-only"],
                dict(
                    to_revision=None,
                    from_revision=None,
                    show_sql_only=True,
                ),
            ),
            (
                ["--to-revision", "abc"],
                dict(
                    to_revision="abc",
                    from_revision=None,
                    show_sql_only=False,
                ),
            ),
            (
                ["--to-revision", "abc", "--show-sql-only"],
                dict(to_revision="abc", from_revision=None, show_sql_only=True),
            ),
            (
                ["--to-version", "2.10.0"],
                dict(
                    to_revision="22ed7efa9da2",
                    from_revision=None,
                    show_sql_only=False,
                ),
            ),
            (
                ["--to-version", "2.10.0", "--show-sql-only"],
                dict(
                    to_revision="22ed7efa9da2",
                    from_revision=None,
                    show_sql_only=True,
                ),
            ),
            (
                ["--to-revision", "abc", "--from-revision", "abc123", "--show-sql-only"],
                dict(
                    to_revision="abc",
                    from_revision="abc123",
                    show_sql_only=True,
                ),
            ),
            (
                ["--to-revision", "abc", "--from-version", "2.10.0", "--show-sql-only"],
                dict(
                    to_revision="abc",
                    from_revision="22ed7efa9da2",
                    show_sql_only=True,
                ),
            ),
            (
                ["--to-version", "2.10.0", "--from-revision", "abc123", "--show-sql-only"],
                dict(
                    to_revision="22ed7efa9da2",
                    from_revision="abc123",
                    show_sql_only=True,
                ),
            ),
            (
                ["--to-version", "2.10.0", "--from-version", "2.10.0", "--show-sql-only"],
                dict(
                    to_revision="22ed7efa9da2",
                    from_revision="22ed7efa9da2",
                    show_sql_only=True,
                ),
            ),
        ],
    )
    @mock.patch("airflow.cli.commands.db_command.db.upgradedb")
    def test_cli_upgrade_success(self, mock_upgradedb, args, called_with):
        # TODO(ephraimbuddy): Revisit this when we add more migration files and use other versions/revisions other than 2.10.0/22ed7efa9da2
        db_command.migratedb(self.parser.parse_args(["db", "migrate", *args]))
        mock_upgradedb.assert_called_once_with(**called_with)

    @pytest.mark.parametrize(
        "args, pattern",
        [
            pytest.param(
                ["--to-revision", "abc", "--to-version", "2.10.0"],
                "Cannot supply both",
                id="to both version and revision",
            ),
            pytest.param(
                ["--from-revision", "abc", "--from-version", "2.10.0"],
                "Cannot supply both",
                id="from both version and revision",
            ),
            pytest.param(["--to-version", "2.1.25"], "Unknown version '2.1.25'", id="unknown to version"),
            pytest.param(["--to-version", "abc"], "Invalid version 'abc'", id="invalid to version"),
            pytest.param(
                ["--to-revision", "abc", "--from-revision", "abc123"],
                "used with `--show-sql-only`",
                id="requires offline",
            ),
            pytest.param(
                ["--to-revision", "abc", "--from-version", "2.10.0"],
                "used with `--show-sql-only`",
                id="requires offline",
            ),
            pytest.param(
                ["--to-revision", "2.10.0", "--from-version", "2.1.25", "--show-sql-only"],
                "Unknown version '2.1.25'",
                id="unknown from version",
            ),
            pytest.param(
                ["--to-revision", "2.10.0", "--from-version", "abc", "--show-sql-only"],
                "Invalid version 'abc'",
                id="invalid from version",
            ),
        ],
    )
    @mock.patch("airflow.cli.commands.db_command.db.upgradedb")
    def test_cli_sync_failure(self, mock_upgradedb, args, pattern):
        with pytest.raises(SystemExit, match=pattern):
            db_command.migratedb(self.parser.parse_args(["db", "migrate", *args]))

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch("airflow.cli.commands.db_command.NamedTemporaryFile")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("mysql://root@mysql:3306/airflow"),
    )
    def test_cli_shell_mysql(self, mock_tmp_file, mock_execute_interactive):
        mock_tmp_file.return_value.__enter__.return_value.name = "/tmp/name"
        db_command.shell(self.parser.parse_args(["db", "shell"]))
        mock_execute_interactive.assert_called_once_with(["mysql", "--defaults-extra-file=/tmp/name"])
        mock_tmp_file.return_value.__enter__.return_value.write.assert_called_once_with(
            b"[client]\nhost     = mysql\nuser     = root\npassword = \nport     = 3306\ndatabase = airflow"
        )

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch("airflow.cli.commands.db_command.NamedTemporaryFile")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("mysql://root@mysql/airflow"),
    )
    def test_cli_shell_mysql_without_port(self, mock_tmp_file, mock_execute_interactive):
        mock_tmp_file.return_value.__enter__.return_value.name = "/tmp/name"
        db_command.shell(self.parser.parse_args(["db", "shell"]))
        mock_execute_interactive.assert_called_once_with(["mysql", "--defaults-extra-file=/tmp/name"])
        mock_tmp_file.return_value.__enter__.return_value.write.assert_called_once_with(
            b"[client]\nhost     = mysql\nuser     = root\npassword = \nport     = 3306\ndatabase = airflow"
        )

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("sqlite:////root/airflow/airflow.db"),
    )
    def test_cli_shell_sqlite(self, mock_execute_interactive):
        db_command.shell(self.parser.parse_args(["db", "shell"]))
        mock_execute_interactive.assert_called_once_with(["sqlite3", "/root/airflow/airflow.db"])

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("postgresql+psycopg2://postgres:airflow@postgres:5432/airflow"),
    )
    def test_cli_shell_postgres(self, mock_execute_interactive):
        db_command.shell(self.parser.parse_args(["db", "shell"]))
        mock_execute_interactive.assert_called_once_with(["psql"], env=mock.ANY)
        _, kwargs = mock_execute_interactive.call_args
        env = kwargs["env"]
        postgres_env = {k: v for k, v in env.items() if k.startswith("PG")}
        assert postgres_env == {
            "PGDATABASE": "airflow",
            "PGHOST": "postgres",
            "PGPASSWORD": "airflow",
            "PGPORT": "5432",
            "PGUSER": "postgres",
        }

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("postgresql+psycopg://postgres:airflow@postgres:5432/airflow"),
    )
    def test_cli_shell_postgres_ppg3(self, mock_execute_interactive):
        pytest.importorskip("psycopg", reason="Test only runs when psycopg v3 is installed.")

        db_command.shell(self.parser.parse_args(["db", "shell"]))
        mock_execute_interactive.assert_called_once_with(["psql"], env=mock.ANY)
        _, kwargs = mock_execute_interactive.call_args
        env = kwargs["env"]
        postgres_env = {k: v for k, v in env.items() if k.startswith("PG")}
        assert postgres_env == {
            "PGDATABASE": "airflow",
            "PGHOST": "postgres",
            "PGPASSWORD": "airflow",
            "PGPORT": "5432",
            "PGUSER": "postgres",
        }

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("postgresql+psycopg2://postgres:airflow@postgres/airflow"),
    )
    def test_cli_shell_postgres_without_port(self, mock_execute_interactive):
        db_command.shell(self.parser.parse_args(["db", "shell"]))
        mock_execute_interactive.assert_called_once_with(["psql"], env=mock.ANY)
        _, kwargs = mock_execute_interactive.call_args
        env = kwargs["env"]
        postgres_env = {k: v for k, v in env.items() if k.startswith("PG")}
        assert postgres_env == {
            "PGDATABASE": "airflow",
            "PGHOST": "postgres",
            "PGPASSWORD": "airflow",
            "PGPORT": "5432",
            "PGUSER": "postgres",
        }

    @mock.patch("airflow.cli.commands.db_command.execute_interactive")
    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("postgresql+psycopg://postgres:airflow@postgres/airflow"),
    )
    def test_cli_shell_postgres_without_port_ppg3(self, mock_execute_interactive):
        pytest.importorskip("psycopg", reason="Test only runs when psycopg v3 is installed.")

        db_command.shell(self.parser.parse_args(["db", "shell"]))
        mock_execute_interactive.assert_called_once_with(["psql"], env=mock.ANY)
        _, kwargs = mock_execute_interactive.call_args
        env = kwargs["env"]
        postgres_env = {k: v for k, v in env.items() if k.startswith("PG")}
        assert postgres_env == {
            "PGDATABASE": "airflow",
            "PGHOST": "postgres",
            "PGPASSWORD": "airflow",
            "PGPORT": "5432",
            "PGUSER": "postgres",
        }

    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("invalid+psycopg2://postgres:airflow@postgres/airflow"),
    )
    def test_cli_shell_invalid(self):
        with pytest.raises(AirflowException, match=r"Unknown driver: invalid\+psycopg2"):
            db_command.shell(self.parser.parse_args(["db", "shell"]))

    @mock.patch(
        "airflow.cli.commands.db_command.settings.engine.url",
        make_url("invalid+psycopg://postgres:airflow@postgres/airflow"),
    )
    def test_cli_shell_invalid_ppg3(self):
        pytest.importorskip("psycopg", reason="Test only runs when psycopg v3 is installed.")

        with pytest.raises(AirflowException, match=r"Unknown driver: invalid\+psycopg"):
            db_command.shell(self.parser.parse_args(["db", "shell"]))

    def test_run_db_downgrade_command_success_and_messages(self, capsys):
        class Args:
            to_revision = "abc"
            to_version = None
            from_revision = None
            from_version = None
            show_sql_only = False
            yes = True

        called = {}

        def fake_command(**kwargs):
            called.update(kwargs)

        heads = {"2.10.0": "22ed7efa9da2"}

        db_command.run_db_downgrade_command(Args(), fake_command, heads)
        out = capsys.readouterr().out
        assert "Performing downgrade" in out
        assert "Downgrade complete" in out
        assert called == {"to_revision": "abc", "from_revision": None, "show_sql_only": False}

    def test_run_db_downgrade_command_offline_generation(self, capsys):
        class Args:
            to_revision = None
            to_version = "2.10.0"
            from_revision = None
            from_version = None
            show_sql_only = True
            yes = False

        called = {}

        def fake_command(**kwargs):
            called.update(kwargs)

        heads = {"2.10.0": "22ed7efa9da2"}

        db_command.run_db_downgrade_command(Args(), fake_command, heads)
        out = capsys.readouterr().out
        assert "Generating sql for downgrade" in out
        assert called == {"to_revision": "22ed7efa9da2", "from_revision": None, "show_sql_only": True}

    @pytest.mark.parametrize(
        "args, match",
        [
            (
                {
                    "to_revision": None,
                    "to_version": None,
                    "from_revision": None,
                    "from_version": None,
                    "show_sql_only": False,
                    "yes": False,
                },
                "Must provide either",
            ),
            (
                {
                    "to_revision": "abc",
                    "to_version": "2.10.0",
                    "from_revision": None,
                    "from_version": None,
                    "show_sql_only": False,
                    "yes": True,
                },
                "Cannot supply both",
            ),
            (
                {
                    "to_revision": "abc",
                    "to_version": None,
                    "from_revision": "abc1",
                    "from_version": "2.10.0",
                    "show_sql_only": True,
                    "yes": True,
                },
                "may not be combined",
            ),
            (
                {
                    "to_revision": None,
                    "to_version": "2.1.25",
                    "from_revision": None,
                    "from_version": None,
                    "show_sql_only": False,
                    "yes": True,
                },
                "not supported",
            ),
            (
                {
                    "to_revision": None,
                    "to_version": None,
                    "from_revision": "abc",
                    "from_version": None,
                    "show_sql_only": False,
                    "yes": True,
                },
                "only .* with `--show-sql-only`",
            ),
        ],
    )
    def test_run_db_downgrade_command_validation_errors(self, args, match):
        class Args:
            to_revision = args["to_revision"]
            to_version = args["to_version"]
            from_revision = args["from_revision"]
            from_version = args["from_version"]
            show_sql_only = args["show_sql_only"]
            yes = args["yes"]

        def fake_command(**kwargs):
            pass

        heads = {"2.10.0": "22ed7efa9da2"}

        with pytest.raises(SystemExit, match=match):
            db_command.run_db_downgrade_command(Args(), fake_command, heads)

    @mock.patch("airflow.cli.commands.db_command.input")
    def test_run_db_downgrade_command_confirmation_yes_calls_command(self, mock_input, capsys):
        mock_input.return_value = "Y"

        class Args:
            to_revision = "abc"
            to_version = None
            from_revision = None
            from_version = None
            show_sql_only = False
            yes = False

        called = {}

        def fake_command(**kwargs):
            called.update(kwargs)

        heads = {"2.10.0": "22ed7efa9da2"}

        db_command.run_db_downgrade_command(Args(), fake_command, heads)
        out = capsys.readouterr().out
        assert "Performing downgrade" in out
        assert called == {"to_revision": "abc", "from_revision": None, "show_sql_only": False}

    @mock.patch("airflow.cli.commands.db_command.input")
    def test_run_db_downgrade_command_confirmation_no_cancels(self, mock_input):
        mock_input.return_value = "n"

        class Args:
            to_revision = "abc"
            to_version = None
            from_revision = None
            from_version = None
            show_sql_only = False
            yes = False

        def fake_command(**kwargs):
            raise AssertionError("Command should not be called when cancelled")

        heads = {"2.10.0": "22ed7efa9da2"}

        with pytest.raises(SystemExit, match="Cancelled"):
            db_command.run_db_downgrade_command(Args(), fake_command, heads)

    @pytest.mark.parametrize(
        "args, match",
        [
            (["-y", "--to-revision", "abc", "--to-version", "2.2.0"], "Cannot supply both"),
            (["-y", "--to-revision", "abc1", "--from-revision", "abc2"], "only .* with `--show-sql-only`"),
            (["-y", "--to-revision", "abc1", "--from-version", "2.2.2"], "only .* with `--show-sql-only`"),
            (["-y", "--to-version", "2.2.2", "--from-version", "2.2.2"], "only .* with `--show-sql-only`"),
            (
                ["-y", "--to-revision", "abc", "--from-version", "2.2.0", "--from-revision", "abc"],
                "may not be combined",
            ),
            (["-y", "--to-version", "abc"], r"Downgrading to .* not supported\."),
            (["-y"], "Must provide either"),
        ],
    )
    @mock.patch("airflow.utils.db.downgrade")
    def test_cli_downgrade_invalid(self, mock_dg, args, match):
        """We test some options that should produce an error"""
        with pytest.raises(SystemExit, match=match):
            db_command.downgrade(self.parser.parse_args(["db", "downgrade", *args]))

    @pytest.mark.parametrize(
        "args, expected",
        [
            (["-y", "--to-revision", "abc1"], dict(to_revision="abc1")),
            (
                ["-y", "--to-revision", "abc1", "--from-revision", "abc2", "-s"],
                dict(to_revision="abc1", from_revision="abc2", show_sql_only=True),
            ),
            (
                ["-y", "--to-revision", "abc1", "--from-version", "2.10.0", "-s"],
                dict(to_revision="abc1", from_revision="22ed7efa9da2", show_sql_only=True),
            ),
            (
                ["-y", "--to-version", "2.10.0", "--from-version", "2.10.0", "-s"],
                dict(to_revision="22ed7efa9da2", from_revision="22ed7efa9da2", show_sql_only=True),
            ),
            (["-y", "--to-version", "2.10.0"], dict(to_revision="22ed7efa9da2")),
        ],
    )
    @mock.patch("airflow.utils.db.downgrade")
    def test_cli_downgrade_good(self, mock_dg, args, expected):
        defaults = dict(from_revision=None, show_sql_only=False)
        db_command.downgrade(self.parser.parse_args(["db", "downgrade", *args]))
        mock_dg.assert_called_with(**{**defaults, **expected})

    @pytest.mark.parametrize(
        "resp, raise_",
        [
            ("y", False),
            ("Y", False),
            ("n", True),
            ("a", True),  # any other value
        ],
    )
    @mock.patch("airflow.utils.db.downgrade")
    @mock.patch("airflow.cli.commands.db_command.input")
    def test_cli_downgrade_confirm(self, mock_input, mock_dg, resp, raise_):
        mock_input.return_value = resp
        if raise_:
            with pytest.raises(SystemExit):
                db_command.downgrade(self.parser.parse_args(["db", "downgrade", "--to-revision", "abc"]))
        else:
            db_command.downgrade(self.parser.parse_args(["db", "downgrade", "--to-revision", "abc"]))
            mock_dg.assert_called_with(to_revision="abc", from_revision=None, show_sql_only=False)

    def test_check(self):
        retry, retry_delay = 6, 9  # arbitrary but distinct number
        args = self.parser.parse_args(
            ["db", "check", "--retry", str(retry), "--retry-delay", str(retry_delay)]
        )
        sleep = MagicMock()
        always_pass = Mock()
        always_fail = Mock(side_effect=OperationalError("", None, None))

        with patch("time.sleep", new=sleep), patch("airflow.utils.db.check", new=always_pass):
            db_command.check(args)
            always_pass.assert_called_once()
            sleep.assert_not_called()

        with patch("time.sleep", new=sleep), patch("airflow.utils.db.check", new=always_fail):
            with pytest.raises(OperationalError):
                db_command.check(args)
            # With N retries there are N+1 total checks, hence N sleeps
            always_fail.assert_has_calls([call()] * (retry + 1))
            sleep.assert_has_calls([call(retry_delay)] * retry)


class TestCLIDBClean:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @pytest.mark.parametrize("timezone", ["UTC", "Europe/Berlin", "America/Los_Angeles"])
    @patch("airflow.cli.commands.db_command.run_cleanup")
    def test_date_timezone_omitted(self, run_cleanup_mock, timezone):
        """
        When timezone omitted we should always expect that the timestamp is
        coerced to tz-aware with default timezone
        """
        timestamp = "2021-01-01 00:00:00"
        with patch("airflow.settings._Timezone.initialized_timezone", pendulum.timezone(timezone)):
            args = self.parser.parse_args(["db", "clean", "--clean-before-timestamp", f"{timestamp}", "-y"])
            db_command.cleanup_tables(args)
        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse(timestamp, tz=timezone),
            verbose=False,
            confirm=False,
            skip_archive=False,
            batch_size=None,
        )

    @pytest.mark.parametrize("timezone", ["UTC", "Europe/Berlin", "America/Los_Angeles"])
    @patch("airflow.cli.commands.db_command.run_cleanup")
    def test_date_timezone_supplied(self, run_cleanup_mock, timezone):
        """
        When tz included in the string then default timezone should not be used.
        """
        timestamp = "2021-01-01 00:00:00+03:00"
        with patch("airflow.settings.TIMEZONE", pendulum.timezone(timezone)):
            args = self.parser.parse_args(["db", "clean", "--clean-before-timestamp", f"{timestamp}", "-y"])
            db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse(timestamp),
            verbose=False,
            confirm=False,
            skip_archive=False,
            batch_size=None,
        )

    @pytest.mark.parametrize("confirm_arg, expected", [(["-y"], False), ([], True)])
    @patch("airflow.cli.commands.db_command.run_cleanup")
    def test_confirm(self, run_cleanup_mock, confirm_arg, expected):
        """
        When ``-y`` provided, ``confirm`` should be false.
        """
        args = self.parser.parse_args(
            [
                "db",
                "clean",
                "--clean-before-timestamp",
                "2021-01-01",
                *confirm_arg,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse("2021-01-01 00:00:00Z"),
            verbose=False,
            confirm=expected,
            skip_archive=False,
            batch_size=None,
        )

    @pytest.mark.parametrize("extra_arg, expected", [(["--skip-archive"], True), ([], False)])
    @patch("airflow.cli.commands.db_command.run_cleanup")
    def test_skip_archive(self, run_cleanup_mock, extra_arg, expected):
        """
        When ``--skip-archive`` provided, ``skip_archive`` should be True (False otherwise).
        """
        args = self.parser.parse_args(
            [
                "db",
                "clean",
                "--clean-before-timestamp",
                "2021-01-01",
                *extra_arg,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse("2021-01-01 00:00:00Z"),
            verbose=False,
            confirm=True,
            skip_archive=expected,
            batch_size=None,
        )

    @pytest.mark.parametrize("dry_run_arg, expected", [(["--dry-run"], True), ([], False)])
    @patch("airflow.cli.commands.db_command.run_cleanup")
    def test_dry_run(self, run_cleanup_mock, dry_run_arg, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                "db",
                "clean",
                "--clean-before-timestamp",
                "2021-01-01",
                *dry_run_arg,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=expected,
            clean_before_timestamp=pendulum.parse("2021-01-01 00:00:00Z"),
            verbose=False,
            confirm=True,
            skip_archive=False,
            batch_size=None,
        )

    @pytest.mark.parametrize(
        "extra_args, expected", [(["--tables", "hello, goodbye"], ["hello", "goodbye"]), ([], None)]
    )
    @patch("airflow.cli.commands.db_command.run_cleanup")
    def test_tables(self, run_cleanup_mock, extra_args, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                "db",
                "clean",
                "--clean-before-timestamp",
                "2021-01-01",
                *extra_args,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=expected,
            dry_run=False,
            clean_before_timestamp=pendulum.parse("2021-01-01 00:00:00Z"),
            verbose=False,
            confirm=True,
            skip_archive=False,
            batch_size=None,
        )

    @pytest.mark.parametrize("extra_args, expected", [(["--verbose"], True), ([], False)])
    @patch("airflow.cli.commands.db_command.run_cleanup")
    def test_verbose(self, run_cleanup_mock, extra_args, expected):
        """
        When tz included in the string then default timezone should not be used.
        """
        args = self.parser.parse_args(
            [
                "db",
                "clean",
                "--clean-before-timestamp",
                "2021-01-01",
                *extra_args,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse("2021-01-01 00:00:00Z"),
            verbose=expected,
            confirm=True,
            skip_archive=False,
            batch_size=None,
        )

    @pytest.mark.parametrize("extra_args, expected", [(["--batch-size", "1234"], 1234), ([], None)])
    @patch("airflow.cli.commands.db_command.run_cleanup")
    def test_batch_size(self, run_cleanup_mock, extra_args, expected):
        """
        batch_size should be forwarded to run_cleanup with correct type.
        """
        args = self.parser.parse_args(
            [
                "db",
                "clean",
                "--clean-before-timestamp",
                "2021-01-01",
                *extra_args,
            ]
        )
        db_command.cleanup_tables(args)

        run_cleanup_mock.assert_called_once_with(
            table_names=None,
            dry_run=False,
            clean_before_timestamp=pendulum.parse("2021-01-01 00:00:00Z"),
            verbose=False,
            confirm=True,
            skip_archive=False,
            batch_size=expected,
        )

    @patch("airflow.cli.commands.db_command.export_archived_records")
    @patch("airflow.cli.commands.db_command.os.path.isdir", return_value=True)
    def test_export_archived_records(self, os_mock, export_archived_mock):
        args = self.parser.parse_args(
            [
                "db",
                "export-archived",
                "--output-path",
                "path",
            ]
        )
        db_command.export_archived(args)

        export_archived_mock.assert_called_once_with(
            export_format="csv", output_path="path", table_names=None, drop_archives=False, needs_confirm=True
        )

    @pytest.mark.parametrize(
        "extra_args, expected", [(["--tables", "hello, goodbye"], ["hello", "goodbye"]), ([], None)]
    )
    @patch("airflow.cli.commands.db_command.export_archived_records")
    @patch("airflow.cli.commands.db_command.os.path.isdir", return_value=True)
    def test_tables_in_export_archived_records_command(
        self, os_mock, export_archived_mock, extra_args, expected
    ):
        args = self.parser.parse_args(
            [
                "db",
                "export-archived",
                "--output-path",
                "path",
                *extra_args,
            ]
        )
        db_command.export_archived(args)
        export_archived_mock.assert_called_once_with(
            export_format="csv",
            output_path="path",
            table_names=expected,
            drop_archives=False,
            needs_confirm=True,
        )

    @pytest.mark.parametrize("extra_args, expected", [(["--drop-archives"], True), ([], False)])
    @patch("airflow.cli.commands.db_command.export_archived_records")
    @patch("airflow.cli.commands.db_command.os.path.isdir", return_value=True)
    def test_drop_archives_in_export_archived_records_command(
        self, os_mock, export_archived_mock, extra_args, expected
    ):
        args = self.parser.parse_args(
            [
                "db",
                "export-archived",
                "--output-path",
                "path",
                *extra_args,
            ]
        )
        db_command.export_archived(args)
        export_archived_mock.assert_called_once_with(
            export_format="csv",
            output_path="path",
            table_names=None,
            drop_archives=expected,
            needs_confirm=True,
        )

    @pytest.mark.parametrize(
        "extra_args, expected", [(["--tables", "hello, goodbye"], ["hello", "goodbye"]), ([], None)]
    )
    @patch("airflow.cli.commands.db_command.drop_archived_tables")
    def test_tables_in_drop_archived_records_command(self, mock_drop_archived_records, extra_args, expected):
        args = self.parser.parse_args(
            [
                "db",
                "drop-archived",
                *extra_args,
            ]
        )
        db_command.drop_archived(args)
        mock_drop_archived_records.assert_called_once_with(table_names=expected, needs_confirm=True)

    @pytest.mark.parametrize("extra_args, expected", [(["-y"], False), ([], True)])
    @patch("airflow.cli.commands.db_command.drop_archived_tables")
    def test_confirm_in_drop_archived_records_command(self, mock_drop_archived_records, extra_args, expected):
        args = self.parser.parse_args(
            [
                "db",
                "drop-archived",
                *extra_args,
            ]
        )
        db_command.drop_archived(args)
        mock_drop_archived_records.assert_called_once_with(table_names=None, needs_confirm=expected)


def test_get_version_revision():
    heads: dict[str, str] = {
        "2.10.0": "22ed7efa9da2",
        "2.10.3": "5f2621c13b39",
        "3.0.0": "29ce7909c52b",
        "3.0.3": "fe199e1abd77",
        "3.1.0": "808787349f22",
    }

    assert db_command._get_version_revision("3.1.0", heads) == "808787349f22"
    assert db_command._get_version_revision("3.1.1", heads) == "808787349f22"
    assert db_command._get_version_revision("2.11.1", heads) == "5f2621c13b39"
    assert db_command._get_version_revision("2.10.1", heads) == "22ed7efa9da2"
    assert db_command._get_version_revision("2.0.0", heads) is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("pa!sw0rd#", '"pa!sw0rd#"'),
        ('he"llo', '"he\\"llo"'),
        ("path\\file", '"path\\\\file"'),
        (None, ""),
    ],
)
def test_quote_mysql_password_for_cnf(raw, expected):
    password = db_command._quote_mysql_password_for_cnf(raw)
    assert password == expected
