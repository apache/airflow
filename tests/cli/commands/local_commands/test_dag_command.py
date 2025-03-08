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
from __future__ import annotations

import argparse
import contextlib
import os
from datetime import datetime
from io import StringIO
from unittest import mock
from unittest.mock import MagicMock

import pendulum
import pytest
from sqlalchemy import select

from airflow.cli import cli_parser
from airflow.cli.commands.local_commands import dag_command
from airflow.models import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone
from airflow.utils.state import DagRunState

from tests.models import TEST_DAGS_FOLDER
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_runs,
    parse_and_sync_to_db,
)

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1), timezone=timezone.utc)
if pendulum.__version__.startswith("3"):
    DEFAULT_DATE_REPR = DEFAULT_DATE.isoformat(sep=" ")
else:
    DEFAULT_DATE_REPR = DEFAULT_DATE.isoformat()

pytestmark = pytest.mark.db_test


class TestCliDags:
    parser: argparse.ArgumentParser

    @classmethod
    def setup_class(cls):
        parse_and_sync_to_db(os.devnull, include_examples=True)
        cls.parser = cli_parser.get_parser()

    @classmethod
    def teardown_class(cls) -> None:
        clear_db_runs()
        clear_db_dags()

    def setup_method(self):
        clear_db_runs()
        clear_db_import_errors()

    def teardown_method(self):
        clear_db_import_errors()

    def test_show_dag_dependencies_print(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_dependencies_show(self.parser.parse_args(["dags", "show-dependencies"]))
        out = temp_stdout.getvalue()
        assert "digraph" in out
        assert "graph [rankdir=LR]" in out

    @mock.patch("airflow.cli.commands.local_commands.dag_command.render_dag_dependencies")
    def test_show_dag_dependencies_save(self, mock_render_dag_dependencies):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_dependencies_show(
                self.parser.parse_args(["dags", "show-dependencies", "--save", "output.png"])
            )
        out = temp_stdout.getvalue()
        mock_render_dag_dependencies.return_value.render.assert_called_once_with(
            cleanup=True, filename="output", format="png"
        )
        assert "File output.png saved" in out

    def test_show_dag_print(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_show(self.parser.parse_args(["dags", "show", "example_bash_operator"]))
        out = temp_stdout.getvalue()
        assert "label=example_bash_operator" in out
        assert "graph [label=example_bash_operator labelloc=t rankdir=LR]" in out
        assert "runme_2 -> run_after_loop" in out

    @mock.patch("airflow.cli.commands.local_commands.dag_command.render_dag")
    def test_show_dag_save(self, mock_render_dag):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_show(
                self.parser.parse_args(["dags", "show", "example_bash_operator", "--save", "awesome.png"])
            )
        out = temp_stdout.getvalue()
        mock_render_dag.return_value.render.assert_called_once_with(
            cleanup=True, filename="awesome", format="png"
        )
        assert "File awesome.png saved" in out

    @mock.patch("airflow.cli.commands.local_commands.dag_command.subprocess.Popen")
    @mock.patch("airflow.cli.commands.local_commands.dag_command.render_dag")
    def test_show_dag_imgcat(self, mock_render_dag, mock_popen):
        mock_render_dag.return_value.pipe.return_value = b"DOT_DATA"
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (b"OUT", b"ERR")
        mock_popen.return_value.__enter__.return_value = mock_proc
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_show(
                self.parser.parse_args(["dags", "show", "example_bash_operator", "--imgcat"])
            )
        out = temp_stdout.getvalue()
        mock_render_dag.return_value.pipe.assert_called_once_with(format="png")
        mock_proc.communicate.assert_called_once_with(b"DOT_DATA")
        assert "OUT" in out
        assert "ERR" in out

    @mock.patch("airflow.cli.commands.local_commands.dag_command.get_dag")
    def test_dag_test(self, mock_get_dag):
        cli_args = self.parser.parse_args(["dags", "test", "example_bash_operator", DEFAULT_DATE.isoformat()])
        dag_command.dag_test(cli_args)

        mock_get_dag.assert_has_calls(
            [
                mock.call(subdir=cli_args.subdir, dag_id="example_bash_operator"),
                mock.call().test(
                    logical_date=timezone.parse(DEFAULT_DATE.isoformat()),
                    run_conf=None,
                    use_executor=False,
                    session=mock.ANY,
                    mark_success_pattern=None,
                ),
            ]
        )

    @mock.patch("airflow.cli.commands.local_commands.dag_command.get_dag")
    def test_dag_test_fail_raise_error(self, mock_get_dag):
        logical_date_str = DEFAULT_DATE.isoformat()
        mock_get_dag.return_value.test.return_value = DagRun(
            dag_id="example_bash_operator", logical_date=DEFAULT_DATE, state=DagRunState.FAILED
        )
        cli_args = self.parser.parse_args(["dags", "test", "example_bash_operator", logical_date_str])
        with pytest.raises(SystemExit, match=r"DagRun failed"):
            dag_command.dag_test(cli_args)

    @mock.patch("airflow.cli.commands.local_commands.dag_command.get_dag")
    @mock.patch("airflow.utils.timezone.utcnow")
    def test_dag_test_no_logical_date(self, mock_utcnow, mock_get_dag):
        now = pendulum.now()
        mock_utcnow.return_value = now
        cli_args = self.parser.parse_args(["dags", "test", "example_bash_operator"])

        assert cli_args.logical_date is None

        dag_command.dag_test(cli_args)

        mock_get_dag.assert_has_calls(
            [
                mock.call(subdir=cli_args.subdir, dag_id="example_bash_operator"),
                mock.call().test(
                    logical_date=mock.ANY,
                    run_conf=None,
                    use_executor=False,
                    session=mock.ANY,
                    mark_success_pattern=None,
                ),
            ]
        )

    @mock.patch("airflow.cli.commands.local_commands.dag_command.get_dag")
    def test_dag_test_conf(self, mock_get_dag):
        cli_args = self.parser.parse_args(
            [
                "dags",
                "test",
                "example_bash_operator",
                DEFAULT_DATE.isoformat(),
                "-c",
                '{"dag_run_conf_param": "param_value"}',
            ]
        )
        dag_command.dag_test(cli_args)

        mock_get_dag.assert_has_calls(
            [
                mock.call(subdir=cli_args.subdir, dag_id="example_bash_operator"),
                mock.call().test(
                    logical_date=timezone.parse(DEFAULT_DATE.isoformat()),
                    run_conf={"dag_run_conf_param": "param_value"},
                    use_executor=False,
                    session=mock.ANY,
                    mark_success_pattern=None,
                ),
            ]
        )

    @mock.patch(
        "airflow.cli.commands.local_commands.dag_command.render_dag", return_value=MagicMock(source="SOURCE")
    )
    @mock.patch("airflow.cli.commands.local_commands.dag_command.get_dag")
    def test_dag_test_show_dag(self, mock_get_dag, mock_render_dag):
        mock_get_dag.return_value.test.return_value.run_id = "__test_dag_test_show_dag_fake_dag_run_run_id__"

        cli_args = self.parser.parse_args(
            ["dags", "test", "example_bash_operator", DEFAULT_DATE.isoformat(), "--show-dagrun"]
        )
        with contextlib.redirect_stdout(StringIO()) as stdout:
            dag_command.dag_test(cli_args)

        output = stdout.getvalue()

        mock_get_dag.assert_has_calls(
            [
                mock.call(subdir=cli_args.subdir, dag_id="example_bash_operator"),
                mock.call().test(
                    logical_date=timezone.parse(DEFAULT_DATE.isoformat()),
                    run_conf=None,
                    use_executor=False,
                    session=mock.ANY,
                    mark_success_pattern=None,
                ),
            ]
        )
        mock_render_dag.assert_has_calls([mock.call(mock_get_dag.return_value, tis=[])])
        assert "SOURCE" in output

    @mock.patch("airflow.models.dag._get_or_create_dagrun")
    def test_dag_test_with_custom_timetable(self, mock__get_or_create_dagrun):
        """
        when calling `dags test` on dag with custom timetable, the DagRun object should be created with
         data_intervals.
        """
        cli_args = self.parser.parse_args(
            ["dags", "test", "example_workday_timetable", DEFAULT_DATE.isoformat()]
        )
        from airflow.example_dags.plugins.workday import AfterWorkdayTimetable

        with mock.patch.object(AfterWorkdayTimetable, "get_next_workday", side_effect=[DEFAULT_DATE]):
            dag_command.dag_test(cli_args)
        assert "data_interval" in mock__get_or_create_dagrun.call_args.kwargs

    @mock.patch("airflow.models.dag._get_or_create_dagrun")
    def test_dag_with_parsing_context(self, mock__get_or_create_dagrun):
        """
        airflow parsing context should be set when calling `dags test`.
        """
        cli_args = self.parser.parse_args(
            ["dags", "test", "test_dag_parsing_context", DEFAULT_DATE.isoformat()]
        )
        dag_command.dag_test(cli_args)

        # if dag_parsing_context is not set, this DAG will only have 1 task
        assert len(mock__get_or_create_dagrun.call_args[1]["dag"].task_ids) == 2


class TestCliDagsReserialize:
    parser = cli_parser.get_parser()

    test_bundles_config = {
        "bundle1": TEST_DAGS_FOLDER / "test_example_bash_operator.py",
        "bundle2": TEST_DAGS_FOLDER / "test_sensor.py",
        "bundle3": TEST_DAGS_FOLDER / "test_dag_with_no_tags.py",
    }

    @classmethod
    def setup_class(cls):
        clear_db_dags()

    def teardown_method(self):
        clear_db_dags()

    @conf_vars({("core", "load_examples"): "false"})
    def test_reserialize(self, configure_dag_bundles, session):
        with configure_dag_bundles(self.test_bundles_config):
            dag_command.dag_reserialize(self.parser.parse_args(["dags", "reserialize"]))

        serialized_dag_ids = set(session.execute(select(SerializedDagModel.dag_id)).scalars())
        assert serialized_dag_ids == {"test_example_bash_operator", "test_dag_with_no_tags", "test_sensor"}

    @conf_vars({("core", "load_examples"): "false"})
    def test_reserialize_should_support_bundle_name_argument(self, configure_dag_bundles, session):
        with configure_dag_bundles(self.test_bundles_config):
            dag_command.dag_reserialize(
                self.parser.parse_args(["dags", "reserialize", "--bundle-name", "bundle1"])
            )

        serialized_dag_ids = set(session.execute(select(SerializedDagModel.dag_id)).scalars())
        assert serialized_dag_ids == {"test_example_bash_operator"}

    @conf_vars({("core", "load_examples"): "false"})
    def test_reserialize_should_support_multiple_bundle_name_arguments(self, configure_dag_bundles, session):
        with configure_dag_bundles(self.test_bundles_config):
            dag_command.dag_reserialize(
                self.parser.parse_args(
                    ["dags", "reserialize", "--bundle-name", "bundle1", "--bundle-name", "bundle2"]
                )
            )

        serialized_dag_ids = set(session.execute(select(SerializedDagModel.dag_id)).scalars())
        assert serialized_dag_ids == {"test_example_bash_operator", "test_sensor"}
