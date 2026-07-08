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
import json
import logging
import os
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock

import msgspec
import pendulum
import pytest
import time_machine
from sqlalchemy import func, select

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.cli import cli_parser
from airflow.cli.commands import dag_command
from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess
from airflow.exceptions import AirflowException
from airflow.models import DagModel, DagRun
from airflow.models.dagbag import DBDagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.sdk import DAG, Asset, BaseOperator, CronPartitionTimetable, PartitionedAssetTimetable, task
from airflow.sdk.definitions.dag import _run_inline_trigger
from airflow.sdk.execution_time.comms import _RequestFrame, _ResponseFrame
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.timetables.base import Timetable
from airflow.triggers.base import TriggerEvent
from airflow.utils.cli import get_db_dag
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_runs,
    parse_and_sync_to_db,
)
from unit.models import TEST_DAGS_FOLDER

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1), timezone=timezone.utc)
if pendulum.__version__.startswith("3"):
    DEFAULT_DATE_REPR = DEFAULT_DATE.isoformat(sep=" ")
else:
    DEFAULT_DATE_REPR = DEFAULT_DATE.isoformat()

# TODO: Check if tests needs side effects - locally there's missing DAG

pytestmark = pytest.mark.db_test

jan_1 = DEFAULT_DATE
jan_6 = DEFAULT_DATE + timedelta(days=5)
dec_27 = DEFAULT_DATE + timedelta(days=-5)


class TestCliDags:
    parser: argparse.ArgumentParser

    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "load_examples"): "True"}):
            parse_and_sync_to_db(os.devnull)
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

    def test_show_dag_dependencies_print(self, stdout_capture):
        with stdout_capture as temp_stdout:
            dag_command.dag_dependencies_show(self.parser.parse_args(["dags", "show-dependencies"]))
        out = temp_stdout.getvalue()
        assert "digraph" in out
        assert "graph [rankdir=LR]" in out

    @mock.patch("airflow.cli.commands.dag_command.render_dag_dependencies")
    def test_show_dag_dependencies_save(self, mock_render_dag_dependencies, stdout_capture):
        with stdout_capture as temp_stdout:
            dag_command.dag_dependencies_show(
                self.parser.parse_args(["dags", "show-dependencies", "--save", "output.png"])
            )
        out = temp_stdout.getvalue()
        mock_render_dag_dependencies.return_value.render.assert_called_once_with(
            cleanup=True, filename="output", format="png"
        )
        assert "File output.png saved" in out

    def test_show_dag_print(self, stdout_capture):
        with stdout_capture as temp_stdout:
            dag_command.dag_show(self.parser.parse_args(["dags", "show", "example_bash_operator"]))
        out = temp_stdout.getvalue()
        assert "label=example_bash_operator" in out
        assert "graph [label=example_bash_operator labelloc=t rankdir=LR]" in out
        assert "runme_2 -> run_after_loop" in out

    @mock.patch("airflow.cli.commands.dag_command.render_dag")
    def test_show_dag_save(self, mock_render_dag, stdout_capture):
        with stdout_capture as temp_stdout:
            dag_command.dag_show(
                self.parser.parse_args(["dags", "show", "example_bash_operator", "--save", "awesome.png"])
            )
        out = temp_stdout.getvalue()
        mock_render_dag.return_value.render.assert_called_once_with(
            cleanup=True, filename="awesome", format="png"
        )
        assert "File awesome.png saved" in out

    @mock.patch("airflow.cli.commands.dag_command.subprocess.Popen")
    @mock.patch("airflow.cli.commands.dag_command.render_dag")
    def test_show_dag_imgcat(self, mock_render_dag, mock_popen, stdout_capture):
        mock_render_dag.return_value.pipe.return_value = b"DOT_DATA"
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (b"OUT", b"ERR")
        mock_popen.return_value.__enter__.return_value = mock_proc
        with stdout_capture as temp_stdout:
            dag_command.dag_show(
                self.parser.parse_args(["dags", "show", "example_bash_operator", "--imgcat"])
            )
        out = temp_stdout.getvalue()
        mock_render_dag.return_value.pipe.assert_called_once_with(format="png")
        mock_proc.communicate.assert_called_once_with(b"DOT_DATA")
        assert "OUT" in out
        assert "ERR" in out

    @pytest.mark.parametrize(
        ("dag_id", "delta", "schedule", "catchup", "first", "second"),
        [
            pytest.param(
                "future_schedule_daily",
                "timedelta(days=5)",
                "'0 0 * * *'",
                "True",
                jan_6.isoformat(),
                jan_6.isoformat() + os.linesep + (jan_6 + timedelta(days=1)).isoformat(),
                id="future_schedule_daily",
            ),
            pytest.param(
                "future_schedule_every_4_hours",
                "timedelta(days=5)",
                "timedelta(hours=4)",
                "True",
                jan_6.isoformat(),
                jan_6.isoformat() + os.linesep + (jan_6 + timedelta(hours=4)).isoformat(),
                id="future_schedule_every_4_hours",
            ),
            pytest.param(
                "future_schedule_once",
                "timedelta(days=5)",
                "'@once'",
                "True",
                jan_6.isoformat(),
                jan_6.isoformat() + os.linesep + "None",
                id="future_schedule_once",
            ),
            pytest.param(
                "future_schedule_none",
                "timedelta(days=5)",
                "None",
                "True",
                "None",
                "None",
                id="future_schedule_none",
            ),
            pytest.param(
                "past_schedule_once",
                "timedelta(days=-5)",
                "'@once'",
                "True",
                dec_27.isoformat(),
                "None",
                id="past_schedule_once",
            ),
            pytest.param(
                "past_schedule_daily",
                "timedelta(days=-5)",
                "'0 0 * * *'",
                "True",
                dec_27.isoformat(),
                dec_27.isoformat() + os.linesep + (dec_27 + timedelta(days=1)).isoformat(),
                id="past_schedule_daily",
            ),
            pytest.param(
                "past_schedule_daily_catchup_false",
                "timedelta(days=-5)",
                "'0 0 * * *'",
                "False",
                (jan_1 - timedelta(days=1)).isoformat(),
                (jan_1 - timedelta(days=1)).isoformat() + os.linesep + jan_1.isoformat(),
                id="past_schedule_daily_catchup_false",
            ),
            pytest.param(
                "partition_key",
                "timedelta(days=-5)",
                "CronPartitionTimetable('0 0 * * *', timezone='UTC')",
                "False",
                jan_1.strftime(r"%Y-%m-%dT%H:%M:%S"),
                os.linesep.join(
                    [
                        jan_1.strftime(r"%Y-%m-%dT%H:%M:%S"),
                        (jan_1 + timedelta(days=1)).strftime(r"%Y-%m-%dT%H:%M:%S"),
                    ],
                ),
                id="partitioned",
            ),
        ],
    )
    def test_next_execution(self, dag_id, delta, schedule, catchup, first, second, tmp_path, stdout_capture):
        file_content = os.linesep.join(
            [
                "from airflow import DAG",
                "from airflow.providers.standard.operators.empty import EmptyOperator",
                "from airflow.timetables.trigger import CronPartitionTimetable",
                "from datetime import timedelta; from pendulum import today",
                f"dag = DAG('{dag_id}', start_date=today(tz='UTC') + {delta}, schedule={schedule}, catchup={catchup})",
                "task = EmptyOperator(task_id='empty_task',dag=dag)",
            ]
        )
        dag_file = tmp_path / f"{dag_id}.py"
        dag_file.write_text(file_content)

        print(file_content)
        with time_machine.travel(DEFAULT_DATE):
            clear_db_dags()
            parse_and_sync_to_db(tmp_path)

        # Test num-executions = 1 (default)
        args = self.parser.parse_args(["dags", "next-execution", dag_id])
        with stdout_capture as temp_stdout:
            dag_command.dag_next_execution(args)
            out = temp_stdout.getvalue()
        assert first in out

        # Test num-executions = 2
        args = self.parser.parse_args(["dags", "next-execution", dag_id, "--num-executions", "2"])
        with stdout_capture as temp_stdout:
            dag_command.dag_next_execution(args)
            out = temp_stdout.getvalue()
        assert second in out

        # Rebuild Test DB for other tests
        clear_db_dags()
        self.setup_class()

    @conf_vars({("core", "load_examples"): "false"})
    @pytest.mark.parametrize(
        ("dag_id", "schedule"),
        [
            pytest.param("table_none_schedule", "None", id="schedule_none"),
            pytest.param("table_once_schedule", "'@once'", id="schedule_once_with_two_executions"),
        ],
    )
    def test_next_execution_table_flag_with_no_next_run(
        self, dag_id, schedule, tmp_path, stdout_capture, stderr_capture
    ):
        """Regression test for #67394: --table must not crash when schedule yields None."""
        file_content = os.linesep.join(
            [
                "from airflow import DAG",
                "from airflow.providers.standard.operators.empty import EmptyOperator",
                "from datetime import timedelta; from pendulum import today",
                f"dag = DAG('{dag_id}', start_date=today(tz='UTC') + timedelta(days=-5), schedule={schedule})",
                "task = EmptyOperator(task_id='empty_task', dag=dag)",
            ]
        )
        dag_file = tmp_path / f"{dag_id}.py"
        dag_file.write_text(file_content)

        with time_machine.travel(DEFAULT_DATE):
            clear_db_dags()
            parse_and_sync_to_db(tmp_path)

        args = self.parser.parse_args(["dags", "next-execution", dag_id, "--table", "--num-executions", "2"])
        # Must not raise AttributeError on None DagRunInfo
        with stdout_capture:
            with stderr_capture as temp_stderr:
                dag_command.dag_next_execution(args)
        assert "No following schedule" in temp_stderr.getvalue()

        # Rebuild Test DB for other tests
        clear_db_dags()
        self.setup_class()

    def test_cli_report(self, stdout_capture):
        args = self.parser.parse_args(["dags", "report", "--output", "json"])
        with stdout_capture as temp_stdout:
            dag_command.dag_report(args)
            out = temp_stdout.getvalue()

        data = json.loads(out)
        assert any(item["file"].endswith("example_complex.py") for item in data)
        assert any("example_complex" in item["dags"] for item in data)

    def test_cli_get_dag_details(self, stdout_capture):
        args = self.parser.parse_args(["dags", "details", "example_complex", "--output", "yaml"])
        with stdout_capture as temp_stdout:
            dag_command.dag_details(args)
            out = temp_stdout.getvalue()

        # Check if DAG Details field are present
        for field in dag_command.DAG_DETAIL_FIELDS:
            assert field in out

        # Check if identifying values are present
        dag_details_values = ["airflow", "airflow/example_dags/example_complex.py", "16", "example_complex"]

        for value in dag_details_values:
            assert value in out

    def test_cli_list_dags(self, stdout_capture):
        args = self.parser.parse_args(["dags", "list", "--output", "json"])
        with stdout_capture as temp_stdout:
            dag_command.dag_list_dags(args)
            out = temp_stdout.getvalue()
            dag_list = json.loads(out)
        for key in ["dag_id", "fileloc", "owners", "is_paused"]:  # "bundle_name", "bundle_version"?
            assert key in dag_list[0]
        assert any("airflow/example_dags/example_complex.py" in d["fileloc"] for d in dag_list)

    def test_cli_list_local_dags(self, stdout_capture):
        # Clear the database
        clear_db_dags()
        args = self.parser.parse_args(
            ["dags", "list", "--output", "json", "--local", "--bundle-name", "example_dags"]
        )
        with stdout_capture as temp_stdout:
            dag_command.dag_list_dags(args)
            out = temp_stdout.getvalue()
            dag_list = json.loads(out)
        for key in ["dag_id", "fileloc", "owners", "is_paused"]:
            assert key in dag_list[0]
        assert any("airflow/example_dags/example_complex.py" in d["fileloc"] for d in dag_list)
        # Rebuild Test DB for other tests
        self.setup_class()

    @conf_vars({("core", "load_examples"): "false"})
    def test_cli_list_local_dags_with_bundle_name(self, configure_testing_dag_bundle, stdout_capture):
        # Clear the database
        clear_db_dags()
        path_to_parse = TEST_DAGS_FOLDER / "test_example_bash_operator.py"
        args = self.parser.parse_args(
            ["dags", "list", "--output", "json", "--local", "--bundle-name", "testing"]
        )
        with configure_testing_dag_bundle(path_to_parse):
            with stdout_capture as temp_stdout:
                dag_command.dag_list_dags(args)
                out = temp_stdout.getvalue()
                dag_list = json.loads(out)
            for key in ["dag_id", "fileloc", "owners", "is_paused"]:
                assert key in dag_list[0]
            assert any(
                str(TEST_DAGS_FOLDER / "test_example_bash_operator.py") in d["fileloc"] for d in dag_list
            )
        # Rebuild Test DB for other tests
        self.setup_class()

    def test_cli_list_dags_custom_cols(self, stdout_capture):
        args = self.parser.parse_args(
            ["dags", "list", "--output", "json", "--columns", "dag_id,last_parsed_time"]
        )
        with stdout_capture as temp_stdout:
            dag_command.dag_list_dags(args)
            out = temp_stdout.getvalue()
            dag_list = json.loads(out)
        for key in ["dag_id", "last_parsed_time"]:
            assert key in dag_list[0]
        for key in ["fileloc", "owners", "is_paused"]:
            assert key not in dag_list[0]

    def test_cli_list_dags_invalid_cols(self, stderr_capture):
        args = self.parser.parse_args(["dags", "list", "--output", "json", "--columns", "dag_id,invalid_col"])
        with stderr_capture as temp_stderr:
            dag_command.dag_list_dags(args)
            out = temp_stderr.getvalue()
        assert "Ignoring the following invalid columns: ['invalid_col']" in out

    @conf_vars({("core", "load_examples"): "false"})
    def test_cli_list_dags_prints_import_errors(
        self, configure_testing_dag_bundle, get_test_dag, stderr_capture
    ):
        path_to_parse = TEST_DAGS_FOLDER / "test_invalid_cron.py"
        get_test_dag("test_invalid_cron")

        args = self.parser.parse_args(["dags", "list", "--output", "yaml", "--bundle-name", "testing"])

        with configure_testing_dag_bundle(path_to_parse):
            with stderr_capture as temp_stderr:
                dag_command.dag_list_dags(args)
                out = temp_stderr.getvalue()

        assert "Failed to load all files." in out

    @conf_vars({("core", "load_examples"): "false"})
    def test_cli_list_dags_prints_local_import_errors(
        self, configure_testing_dag_bundle, get_test_dag, stderr_capture
    ):
        # Clear the database
        clear_db_dags()
        path_to_parse = TEST_DAGS_FOLDER / "test_invalid_cron.py"
        get_test_dag("test_invalid_cron")

        args = self.parser.parse_args(
            ["dags", "list", "--output", "yaml", "--bundle-name", "testing", "--local"]
        )

        with configure_testing_dag_bundle(path_to_parse):
            with stderr_capture as temp_stderr:
                dag_command.dag_list_dags(args)
                out = temp_stderr.getvalue()

        assert "Failed to load all files." in out
        # Rebuild Test DB for other tests
        self.setup_class()

    @mock.patch("airflow.models.DagModel.get_dagmodel")
    def test_list_dags_none_get_dagmodel(self, mock_get_dagmodel, stdout_capture):
        mock_get_dagmodel.return_value = None
        args = self.parser.parse_args(["dags", "list", "--output", "json"])
        with stdout_capture as temp_stdout:
            dag_command.dag_list_dags(args)
            out = temp_stdout.getvalue()
            dag_list = json.loads(out)
        for key in ["dag_id", "fileloc", "owners", "is_paused"]:
            assert key in dag_list[0]
        assert any("airflow/example_dags/example_complex.py" in d["fileloc"] for d in dag_list)

    def test_dagbag_dag_col(self, session):
        dagbag = DBDagBag()
        dag_details = dag_command._get_dagbag_dag_details(
            dagbag.get_latest_version_of_dag("tutorial_dag", session=session),
        )
        assert sorted(dag_details) == sorted(dag_command.DAG_DETAIL_FIELDS)

    @conf_vars({("core", "load_examples"): "false"})
    def test_cli_list_import_errors(self, get_test_dag, configure_testing_dag_bundle, caplog):
        path_to_parse = TEST_DAGS_FOLDER / "test_invalid_cron.py"
        get_test_dag("test_invalid_cron")

        args = self.parser.parse_args(
            ["dags", "list-import-errors", "--output", "yaml", "--bundle-name", "testing"]
        )
        with configure_testing_dag_bundle(path_to_parse):
            with pytest.raises(SystemExit) as err_ctx:
                with caplog.at_level(logging.ERROR):
                    dag_command.dag_list_import_errors(args)

        log_output = caplog.text

        assert err_ctx.value.code == 1
        assert str(path_to_parse) in log_output
        assert "[0 100 * * *] is not acceptable, out of range" in log_output

    def test_cli_list_dag_runs(self):
        dag_command.dag_trigger(
            self.parser.parse_args(
                [
                    "dags",
                    "trigger",
                    "example_bash_operator",
                ]
            )
        )
        args = self.parser.parse_args(
            [
                "dags",
                "list-runs",
                "example_bash_operator",
                "--no-backfill",
                "--start-date",
                DEFAULT_DATE.isoformat(),
                "--end-date",
                timezone.make_aware(datetime.max).isoformat(),
            ]
        )
        dag_command.dag_list_dag_runs(args)

    def test_cli_list_jobs_with_args(self):
        args = self.parser.parse_args(
            [
                "dags",
                "list-jobs",
                "--dag-id",
                "example_bash_operator",
                "--state",
                "success",
                "--limit",
                "100",
                "--output",
                "json",
            ]
        )
        dag_command.dag_list_jobs(args)

    def test_pause(self):
        args = self.parser.parse_args(["dags", "pause", "example_bash_operator"])
        dag_command.dag_pause(args)
        assert DagModel.get_dagmodel("example_bash_operator").is_paused
        dag_command.dag_unpause(args)
        assert not DagModel.get_dagmodel("example_bash_operator").is_paused

    @mock.patch("airflow.cli.commands.dag_command.ask_yesno")
    def test_pause_regex(self, mock_yesno):
        args = self.parser.parse_args(["dags", "pause", "^example_.*$", "--treat-dag-id-as-regex"])
        dag_command.dag_pause(args)
        mock_yesno.assert_called_once()
        assert DagModel.get_dagmodel("example_bash_decorator").is_paused
        assert DagModel.get_dagmodel("example_kubernetes_executor").is_paused
        assert DagModel.get_dagmodel("example_xcom_args").is_paused

        args = self.parser.parse_args(["dags", "unpause", "^example_.*$", "--treat-dag-id-as-regex"])
        dag_command.dag_unpause(args)
        assert not DagModel.get_dagmodel("example_bash_decorator").is_paused
        assert not DagModel.get_dagmodel("example_kubernetes_executor").is_paused
        assert not DagModel.get_dagmodel("example_xcom_args").is_paused

    @mock.patch("airflow.cli.commands.dag_command.ask_yesno")
    def test_pause_regex_operation_cancelled(self, ask_yesno, capsys):
        args = self.parser.parse_args(["dags", "pause", "example_bash_operator", "--treat-dag-id-as-regex"])
        ask_yesno.return_value = False
        dag_command.dag_pause(args)
        stdout = capsys.readouterr().out
        assert "Operation cancelled by user" in stdout

    @mock.patch("airflow.cli.commands.dag_command.ask_yesno")
    def test_pause_regex_yes(self, mock_yesno):
        args = self.parser.parse_args(["dags", "pause", ".*", "--treat-dag-id-as-regex", "--yes"])
        dag_command.dag_pause(args)
        mock_yesno.assert_not_called()
        dag_command.dag_unpause(args)

    def test_pause_non_existing_dag_do_not_error(self, stdout_capture):
        args = self.parser.parse_args(["dags", "pause", "non_existing_dag"])
        with stdout_capture as temp_stdout:
            dag_command.dag_pause(args)
        out = temp_stdout.splitlines()[-1]
        assert out == "No unpaused DAGs were found"

    def test_unpause_non_existing_dag_do_not_error(self, stdout_capture):
        args = self.parser.parse_args(["dags", "unpause", "non_existing_dag"])
        with stdout_capture as temp_stdout:
            dag_command.dag_unpause(args)
        out = temp_stdout.splitlines()[-1]
        assert out == "No paused DAGs were found"

    def test_unpause_already_unpaused_dag_do_not_error(self, stdout_capture):
        args = self.parser.parse_args(["dags", "unpause", "example_bash_operator", "--yes"])
        with stdout_capture as temp_stdout:
            dag_command.dag_unpause(args)
        out = temp_stdout.splitlines()[-1]
        assert out == "No paused DAGs were found"

    def test_pausing_already_paused_dag_do_not_error(self, stdout_capture):
        args = self.parser.parse_args(["dags", "pause", "example_bash_operator", "--yes"])
        with stdout_capture as temp_stdout:
            dag_command.dag_pause(args)
            dag_command.dag_pause(args)
        out = temp_stdout.splitlines()[-1]
        assert out == "No unpaused DAGs were found"

    def test_trigger_dag(self):
        dag_command.dag_trigger(
            self.parser.parse_args(
                [
                    "dags",
                    "trigger",
                    "example_bash_operator",
                    "--run-id=test_trigger_dag",
                    '--conf={"foo": "bar"}',
                ],
            ),
        )
        with create_session() as session:
            dagrun = session.scalars(select(DagRun).where(DagRun.run_id == "test_trigger_dag")).one()

        assert dagrun, "DagRun not created"
        assert dagrun.run_type == DagRunType.MANUAL
        assert dagrun.conf == {"foo": "bar"}

        # logical_date is None as it's not provided
        assert dagrun.logical_date is None

        # data_interval is None as logical_date is None
        assert dagrun.data_interval_start is None
        assert dagrun.data_interval_end is None

    def test_trigger_dag_empty_object_conf(self):
        dag_command.dag_trigger(
            self.parser.parse_args(
                [
                    "dags",
                    "trigger",
                    "example_bash_operator",
                    "--run-id=test_trigger_dag_empty_object_conf",
                    "--conf={}",
                ],
            ),
        )
        with create_session() as session:
            dagrun = session.scalars(
                select(DagRun).where(DagRun.run_id == "test_trigger_dag_empty_object_conf")
            ).one()

        assert dagrun.conf == {}

    def test_trigger_dag_json_null_conf(self):
        dag_command.dag_trigger(
            self.parser.parse_args(
                [
                    "dags",
                    "trigger",
                    "example_bash_operator",
                    "--run-id=test_trigger_dag_json_null_conf",
                    "--conf=null",
                ],
            ),
        )
        with create_session() as session:
            dagrun = session.scalars(
                select(DagRun).where(DagRun.run_id == "test_trigger_dag_json_null_conf")
            ).one()

        assert dagrun.conf == {}

    def test_trigger_dag_with_microseconds(self):
        dag_command.dag_trigger(
            self.parser.parse_args(
                [
                    "dags",
                    "trigger",
                    "example_bash_operator",
                    "--run-id=test_trigger_dag_with_micro",
                    "--logical-date=2021-06-04T09:00:00.000001+08:00",
                    "--no-replace-microseconds",
                ],
            )
        )

        with create_session() as session:
            dagrun = session.scalars(
                select(DagRun).where(DagRun.run_id == "test_trigger_dag_with_micro")
            ).one()

        assert dagrun, "DagRun not created"
        assert dagrun.run_type == DagRunType.MANUAL
        assert dagrun.logical_date.isoformat(timespec="microseconds") == "2021-06-04T01:00:00.000001+00:00"

    @pytest.mark.parametrize("conf", ["NOT JSON", ""])
    def test_trigger_dag_invalid_conf(self, conf):
        with pytest.raises(ValueError, match=r"Expecting value: line \d+ column \d+ \(char \d+\)"):
            dag_command.dag_trigger(
                self.parser.parse_args(
                    [
                        "dags",
                        "trigger",
                        "example_bash_operator",
                        "--run-id",
                        "trigger_dag_xxx",
                        "--conf",
                        conf,
                    ]
                ),
            )

    @pytest.mark.parametrize("conf", ["[]", '"str"', "1", "false"])
    def test_trigger_dag_rejects_non_object_conf(self, conf):
        with pytest.raises(ValueError, match="DagRun conf must be a JSON object or null"):
            dag_command.dag_trigger(
                self.parser.parse_args(
                    [
                        "dags",
                        "trigger",
                        "example_bash_operator",
                        "--run-id",
                        "trigger_dag_xxx",
                        "--conf",
                        conf,
                    ]
                ),
            )

    def test_trigger_dag_output_as_json(self, stdout_capture):
        args = self.parser.parse_args(
            [
                "dags",
                "trigger",
                "example_bash_operator",
                "--run-id",
                "trigger_dag_xxx",
                "--conf",
                '{"conf1": "val1", "conf2": "val2"}',
                "--output=json",
            ]
        )
        with stdout_capture as temp_stdout:
            dag_command.dag_trigger(args)
            # get the last line from the logs ignoring all logging lines
            out = temp_stdout.getvalue().strip().splitlines()[-1]
        parsed_out = json.loads(out)

        assert len(parsed_out) == 1
        assert parsed_out[0]["dag_id"] == "example_bash_operator"
        assert parsed_out[0]["dag_run_id"] == "trigger_dag_xxx"
        assert parsed_out[0]["conf"] == {"conf1": "val1", "conf2": "val2"}

    def test_delete_dag(self):
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        session.add(DM(dag_id=key, bundle_name="dags-folder"))
        session.commit()
        dag_command.dag_delete(self.parser.parse_args(["dags", "delete", key, "--yes"]))
        assert session.scalar(select(func.count()).select_from(DM).where(DM.dag_id == key)) == 0
        with pytest.raises(AirflowException):
            dag_command.dag_delete(
                self.parser.parse_args(["dags", "delete", "does_not_exist_dag", "--yes"]),
            )

    def test_dag_delete_when_backfill_and_dagrun_exist(self):
        # Test to check that the DAG should be deleted even if
        # there are backfill records associated with it.
        from airflow.models.backfill import Backfill

        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        session.add(DM(dag_id=key, bundle_name="dags-folder"))
        _backfill = Backfill(dag_id=key, from_date=DEFAULT_DATE, to_date=DEFAULT_DATE + timedelta(days=1))
        session.add(_backfill)
        # To create the backfill_id in DagRun
        session.flush()
        session.add(
            DagRun(
                dag_id=key,
                run_id="backfill__" + key,
                state=DagRunState.SUCCESS,
                run_type="backfill",
                backfill_id=_backfill.id,
            )
        )
        session.commit()
        dag_command.dag_delete(self.parser.parse_args(["dags", "delete", key, "--yes"]))
        assert session.scalar(select(func.count()).select_from(DM).where(DM.dag_id == key)) == 0
        with pytest.raises(AirflowException):
            dag_command.dag_delete(
                self.parser.parse_args(["dags", "delete", "does_not_exist_dag", "--yes"]),
            )

    def test_delete_dag_existing_file(self, tmp_path):
        # Test to check that the DAG should be deleted even if
        # the file containing it is not deleted
        path = tmp_path / "testfile"
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        session.add(DM(dag_id=key, bundle_name="dags-folder", fileloc=os.fspath(path)))
        session.commit()
        dag_command.dag_delete(self.parser.parse_args(["dags", "delete", key, "--yes"]))
        assert session.scalar(select(func.count()).select_from(DM).where(DM.dag_id == key)) == 0

    def test_cli_list_jobs(self):
        args = self.parser.parse_args(["dags", "list-jobs"])
        dag_command.dag_list_jobs(args)

    def test_dag_state(self):
        assert (
            dag_command.dag_state(
                self.parser.parse_args(["dags", "state", "example_bash_operator", DEFAULT_DATE.isoformat()])
            )
            is None
        )

    @mock.patch("airflow.cli.commands.dag_command.get_bagged_dag")
    def test_dag_test(self, mock_get_dag):
        cli_args = self.parser.parse_args(["dags", "test", "example_bash_operator", DEFAULT_DATE.isoformat()])
        dag_command.dag_test(cli_args)

        mock_get_dag.assert_has_calls(
            [
                mock.call(bundle_names=None, dag_id="example_bash_operator", dagfile_path=None),
                mock.call().__bool__(),
                mock.call().test(
                    logical_date=timezone.parse(DEFAULT_DATE.isoformat()),
                    run_conf=None,
                    use_executor=False,
                    mark_success_pattern=None,
                ),
            ]
        )

    @mock.patch("airflow.cli.commands.dag_command.get_bagged_dag")
    def test_dag_test_fail_raise_error(self, mock_get_dag):
        logical_date_str = DEFAULT_DATE.isoformat()
        mock_get_dag.return_value.test.return_value = DagRun(
            dag_id="example_bash_operator", logical_date=DEFAULT_DATE, state=DagRunState.FAILED
        )
        cli_args = self.parser.parse_args(["dags", "test", "example_bash_operator", logical_date_str])
        with pytest.raises(SystemExit, match=r"DagRun failed"):
            dag_command.dag_test(cli_args)

    @mock.patch("airflow.cli.commands.dag_command.get_bagged_dag")
    def test_dag_test_no_logical_date(self, mock_get_dag, time_machine):
        now = pendulum.now()
        time_machine.move_to(now, tick=False)
        cli_args = self.parser.parse_args(["dags", "test", "example_bash_operator"])

        assert cli_args.logical_date is None

        dag_command.dag_test(cli_args)

        mock_get_dag.assert_has_calls(
            [
                mock.call(bundle_names=None, dag_id="example_bash_operator", dagfile_path=None),
                mock.call().__bool__(),
                mock.call().test(
                    logical_date=mock.ANY,
                    run_conf=None,
                    use_executor=False,
                    mark_success_pattern=None,
                ),
            ]
        )

    @mock.patch("airflow.cli.commands.dag_command.get_bagged_dag")
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
                mock.call(bundle_names=None, dag_id="example_bash_operator", dagfile_path=None),
                mock.call().__bool__(),
                mock.call().test(
                    logical_date=timezone.parse(DEFAULT_DATE.isoformat()),
                    run_conf={"dag_run_conf_param": "param_value"},
                    use_executor=False,
                    mark_success_pattern=None,
                ),
            ]
        )

    @mock.patch("airflow.cli.commands.dag_command.render_dag", return_value=MagicMock(source="SOURCE"))
    @mock.patch("airflow.cli.commands.dag_command.get_bagged_dag")
    def test_dag_test_show_dag(self, mock_get_dag, mock_render_dag, stdout_capture):
        mock_get_dag.return_value.test.return_value.run_id = "__test_dag_test_show_dag_fake_dag_run_run_id__"

        cli_args = self.parser.parse_args(
            ["dags", "test", "example_bash_operator", DEFAULT_DATE.isoformat(), "--show-dagrun"]
        )
        with stdout_capture as stdout:
            dag_command.dag_test(cli_args)

        output = stdout.getvalue()

        mock_get_dag.assert_has_calls(
            [
                mock.call(bundle_names=None, dag_id="example_bash_operator", dagfile_path=None),
                mock.call().__bool__(),
                mock.call().test(
                    logical_date=timezone.parse(DEFAULT_DATE.isoformat()),
                    run_conf=None,
                    use_executor=False,
                    mark_success_pattern=None,
                ),
            ]
        )
        mock_render_dag.assert_has_calls([mock.call(mock_get_dag.return_value, tis=[])])
        assert "SOURCE" in output

    @mock.patch("airflow.dag_processing.dagbag.BundleDagBag")
    def test_dag_test_with_bundle_name(self, mock_dagbag, configure_dag_bundles):
        """Test that DAG can be tested using bundle name."""
        mock_dagbag.return_value.get_dag.return_value.test.return_value = DagRun(
            dag_id="test_example_bash_operator", logical_date=DEFAULT_DATE, state=DagRunState.SUCCESS
        )

        cli_args = self.parser.parse_args(
            [
                "dags",
                "test",
                "test_example_bash_operator",
                DEFAULT_DATE.isoformat(),
                "--bundle-name",
                "testing",
            ]
        )

        with configure_dag_bundles({"testing": TEST_DAGS_FOLDER}):
            dag_command.dag_test(cli_args)

        mock_dagbag.assert_called_once_with(
            bundle_path=TEST_DAGS_FOLDER,
            dag_folder=TEST_DAGS_FOLDER,
            bundle_name="testing",
        )

    @mock.patch("airflow.dag_processing.dagbag.BundleDagBag")
    def test_dag_test_with_dagfile_path(self, mock_dagbag, configure_dag_bundles):
        """Test that DAG can be tested using dagfile path."""
        mock_dagbag.return_value.get_dag.return_value.test.return_value = DagRun(
            dag_id="test_example_bash_operator", logical_date=DEFAULT_DATE, state=DagRunState.SUCCESS
        )

        dag_file = TEST_DAGS_FOLDER / "test_example_bash_operator.py"

        cli_args = self.parser.parse_args(
            ["dags", "test", "test_example_bash_operator", "--dagfile-path", str(dag_file)]
        )
        with configure_dag_bundles({"testing": TEST_DAGS_FOLDER}):
            dag_command.dag_test(cli_args)

        mock_dagbag.assert_called_once_with(
            bundle_path=TEST_DAGS_FOLDER,
            dag_folder=str(dag_file),
            bundle_name="testing",
        )

    @mock.patch("airflow.dag_processing.dagbag.BundleDagBag")
    def test_dag_test_with_both_bundle_and_dagfile_path(self, mock_dagbag, configure_dag_bundles):
        """Test that DAG can be tested using both bundle name and dagfile path."""
        mock_dagbag.return_value.get_dag.return_value.test.return_value = DagRun(
            dag_id="test_example_bash_operator", logical_date=DEFAULT_DATE, state=DagRunState.SUCCESS
        )

        dag_file = TEST_DAGS_FOLDER / "test_example_bash_operator.py"

        cli_args = self.parser.parse_args(
            [
                "dags",
                "test",
                "test_example_bash_operator",
                DEFAULT_DATE.isoformat(),
                "--bundle-name",
                "testing",
                "--dagfile-path",
                str(dag_file),
            ]
        )

        with configure_dag_bundles({"testing": TEST_DAGS_FOLDER}):
            dag_command.dag_test(cli_args)

        mock_dagbag.assert_called_once_with(
            bundle_path=TEST_DAGS_FOLDER,
            dag_folder=str(dag_file),
            bundle_name="testing",
        )

    @mock.patch("airflow.models.dagrun.get_or_create_dagrun")
    def test_dag_test_with_custom_timetable(self, mock_get_or_create_dagrun):
        """
        when calling `dags test` on dag with custom timetable, the DagRun object should be created with
         data_intervals.
        """
        cli_args = self.parser.parse_args(
            ["dags", "test", "example_workday_timetable", DEFAULT_DATE.isoformat()]
        )
        from airflow.example_dags.plugins.workday import AfterWorkdayTimetable

        with mock.patch.object(AfterWorkdayTimetable, "get_next_workday", return_value=DEFAULT_DATE):
            dag_command.dag_test(cli_args)
        assert "data_interval" in mock_get_or_create_dagrun.call_args.kwargs

    @mock.patch("airflow.models.dagrun.get_or_create_dagrun")
    def test_dag_with_parsing_context(
        self, mock_get_or_create_dagrun, testing_dag_bundle, configure_testing_dag_bundle
    ):
        """
        airflow parsing context should be set when calling `dags test`.
        """
        path_to_parse = TEST_DAGS_FOLDER / "test_dag_parsing_context.py"

        with configure_testing_dag_bundle(path_to_parse):
            bag = DagBag(dag_folder=path_to_parse)
            sync_bag_to_db(bag, "testing", None)
            cli_args = self.parser.parse_args(
                ["dags", "test", "test_dag_parsing_context", DEFAULT_DATE.isoformat()]
            )
            dag_command.dag_test(cli_args)

        # if dag_parsing_context is not set, this DAG will only have 1 task
        assert len(mock_get_or_create_dagrun.call_args[1]["dag"].task_ids) == 2

    def test_dag_test_run_inline_trigger(self, dag_maker):
        now = timezone.utcnow()
        trigger = DateTimeTrigger(moment=now)
        task_sdk_ti = MagicMock()
        task_sdk_ti.id = 1234
        e = _run_inline_trigger(trigger, task_sdk_ti)
        assert isinstance(e, TriggerEvent)
        assert e.payload == now

    def test_dag_test_no_triggerer_running(self, dag_maker):
        with mock.patch(
            "airflow.sdk.definitions.dag._run_inline_trigger", wraps=_run_inline_trigger
        ) as mock_run:
            with dag_maker() as dag:

                @task
                def one():
                    return 1

                @task
                def two(val):
                    return val + 1

                trigger = TimeDeltaTrigger(timedelta(seconds=0))

                class MyOp(BaseOperator):
                    template_fields = ("tfield",)

                    def __init__(self, tfield, **kwargs):
                        self.tfield = tfield
                        super().__init__(**kwargs)

                    def execute(self, context, event=None):
                        if event is None:
                            print("I AM DEFERRING")
                            self.defer(trigger=trigger, method_name="execute")
                            return
                        print("RESUMING")
                        assert self.tfield + 1 == 3

                task_one = one()
                task_two = two(task_one)
                op = MyOp(task_id="abc", tfield=task_two)
                task_two >> op
            sync_dag_to_db(dag)
            dr = dag.test()

            trigger_arg = mock_run.call_args_list[0].args[0]
            assert isinstance(trigger_arg, DateTimeTrigger)
            assert trigger_arg.moment == trigger.moment

            tis = dr.get_task_instances()
            assert next(x for x in tis if x.task_id == "abc").state == "success"

    @mock.patch("airflow.sdk.execution_time.task_runner._execute_task")
    def test_dag_test_with_mark_success(self, mock__execute_task):
        """
        option `--mark-success-pattern` should mark matching tasks as success without executing them.
        """
        cli_args = self.parser.parse_args(
            [
                "dags",
                "test",
                "example_sensor_decorator",
                datetime(2024, 1, 1, 0, 0, 0).isoformat(),
                "--mark-success-pattern",
                "wait_for_upstream",
            ]
        )
        dag_command.dag_test(cli_args)

        # only second operator was actually executed, first one was marked as success
        assert len(mock__execute_task.call_args_list) == 1
        assert mock__execute_task.call_args_list[0].kwargs["ti"].task_id == "dummy_operator"

    @conf_vars({("core", "load_examples"): "false"})
    def test_get_dag_excludes_examples_with_bundle(self, configure_testing_dag_bundle):
        """Test that example DAGs are excluded when bundle names are passed."""
        try:
            from airflow.utils.cli import get_bagged_dag
        except ImportError:  # Prior to Airflow 3.1.0.
            from airflow.utils.cli import get_dag as get_bagged_dag  # type: ignore

        with configure_testing_dag_bundle(TEST_DAGS_FOLDER / "test_sensor.py"):
            # example DAG should not be found since the testing bundle only exposes test_sensor.py
            with pytest.raises(AirflowException, match="could not be found"):
                get_bagged_dag(bundle_names=["testing"], dag_id="example_simplest_dag")

            # However, "test_sensor.py" should exist
            dag = get_bagged_dag(bundle_names=["testing"], dag_id="test_sensor")
            assert dag.dag_id == "test_sensor"


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

        example_bash_op = session.execute(
            select(DagModel).where(DagModel.dag_id == "test_example_bash_operator")
        ).scalar()
        assert example_bash_op.relative_fileloc == "."  # the file _is_ the bundle path
        assert example_bash_op.fileloc == str(TEST_DAGS_FOLDER / "test_example_bash_operator.py")

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

    @conf_vars({("core", "load_examples"): "false"})
    def test_reserialize_should_make_equal_hash_with_dag_processor(self, configure_dag_bundles, session):
        bundles = {"bundle_reserialize": TEST_DAGS_FOLDER / "test_dag_reserialize.py"}
        with configure_dag_bundles(bundles):
            dag_command.dag_reserialize(
                self.parser.parse_args(["dags", "reserialize", "--bundle-name", "bundle_reserialize"])
            )

        dagbag = DagBag(bundles["bundle_reserialize"], bundle_path=bundles["bundle_reserialize"])
        dag_parsing_result = DagFileParsingResult(
            fileloc=bundles["bundle_reserialize"].name,
            serialized_dags=[
                LazyDeserializedDAG(data=DagSerialization.to_dict(dag)) for dag in dagbag.dags.values()
            ],
        )

        frame = _ResponseFrame(id=0, body=dag_parsing_result.model_dump()).as_bytes()
        request_frame = msgspec.msgpack.Decoder[_RequestFrame](_RequestFrame).decode(frame[4:])
        dag_processor_parsing_result = DagFileProcessorProcess.decoder.validate_python(request_frame.body)

        serialized_dag_hash = list(session.execute(select(SerializedDagModel.dag_hash)).scalars())

        assert len(dag_processor_parsing_result.serialized_dags) == 1
        assert len(serialized_dag_hash) == 1
        assert dag_processor_parsing_result.serialized_dags[0].hash == serialized_dag_hash[0]


class _NoTzTimetable(Timetable):
    """Stub timetable: partitioned=True but no timezone attribute.

    Used by tests that exercise the no-tz fallback branch in dag_clear.
    """

    partitioned = True


class TestCliDagsClear:
    """Tests for the `airflow dags clear` partition-range subcommand."""

    DAG_ID = "test_dags_clear_partitioned"

    @pytest.fixture
    def parser(self) -> argparse.ArgumentParser:
        return cli_parser.get_parser()

    @pytest.fixture(autouse=True)
    def _clear_db(self):
        yield
        clear_db_runs()
        clear_db_dags()

    @pytest.fixture
    def seeded_partitioned_runs(self, dag_maker):
        with dag_maker(
            self.DAG_ID,
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2026, 3, 1, tzinfo=pendulum.UTC),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        # Three partitioned runs, plus one unpartitioned run that must never be touched.
        dag_maker.create_dagrun(
            run_id="part_2026_03_08",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 3, 8, tzinfo=pendulum.UTC),
            partition_key="2026-03-08T00:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_2026_03_10",
            state=DagRunState.FAILED,
            logical_date=None,
            partition_date=datetime(2026, 3, 10, tzinfo=pendulum.UTC),
            partition_key="2026-03-10T00:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_2026_03_14",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 3, 14, tzinfo=pendulum.UTC),
            partition_key="2026-03-14T00:00:00",
        )
        dag_maker.create_dagrun(
            run_id="non_partitioned",
            state=DagRunState.SUCCESS,
            logical_date=datetime(2026, 3, 9, tzinfo=pendulum.UTC),
            partition_date=None,
        )
        dag_maker.sync_dagbag_to_db()

    def _get_run_states(self):
        with create_session() as session:
            return {
                row.run_id: row.state
                for row in session.scalars(select(DagRun).where(DagRun.dag_id == self.DAG_ID)).all()
            }

    def _get_run_clear_numbers(self):
        with create_session() as session:
            return {
                row.run_id: row.clear_number
                for row in session.scalars(select(DagRun).where(DagRun.dag_id == self.DAG_ID)).all()
            }

    def test_requires_a_selector(self, parser):
        args = parser.parse_args(["dags", "clear", self.DAG_ID, "--yes"])
        with pytest.raises(SystemExit, match="One of --run-id, --partition-key"):
            dag_command.dag_clear(args)

    @pytest.mark.parametrize(
        "extra_args",
        [
            pytest.param(
                ["--run-id", "part_2026_03_10", "--partition-key", "2026-03-10T00:00:00"],
                id="run-id+partition-key",
            ),
            pytest.param(
                ["--run-id", "part_2026_03_10", "--partition-date-start", "2026-03-08T00:00:00"],
                id="run-id+date-range",
            ),
            pytest.param(
                ["--partition-key", "2026-03-10T00:00:00", "--partition-date-end", "2026-03-14T00:00:00"],
                id="partition-key+date-range",
            ),
        ],
    )
    def test_rejects_multiple_selectors(self, parser, extra_args):
        args = parser.parse_args(["dags", "clear", self.DAG_ID, "--yes", *extra_args])
        with pytest.raises(SystemExit, match="mutually exclusive"):
            dag_command.dag_clear(args)

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_rejects_inverted_window(self, parser):
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-03-14T00:00:00",
                "--partition-date-end",
                "2026-03-08T00:00:00",
                "--yes",
            ]
        )
        with pytest.raises(SystemExit, match="--partition-date-start must be on or before"):
            dag_command.dag_clear(args)

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_clears_runs_in_window_inclusive(self, parser):
        # Literal flag values from issue #65921: short ISO `YYYY-MM-DDTHH` form.
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-03-08T00",
                "--partition-date-end",
                "2026-03-14T23",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # Inclusive both ends: 03-08 and 03-14 boundary runs are cleared (state -> QUEUED).
        assert states["part_2026_03_08"] == DagRunState.QUEUED
        assert states["part_2026_03_10"] == DagRunState.QUEUED
        assert states["part_2026_03_14"] == DagRunState.QUEUED
        # Run with NULL partition_date is never matched.
        assert states["non_partitioned"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_open_lower_bound(self, parser):
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-end",
                "2026-03-09T00:00:00",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        assert states["part_2026_03_08"] == DagRunState.QUEUED
        assert states["part_2026_03_10"] == DagRunState.FAILED
        assert states["part_2026_03_14"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_open_upper_bound(self, parser):
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-03-13T00:00:00",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        assert states["part_2026_03_08"] == DagRunState.SUCCESS
        assert states["part_2026_03_10"] == DagRunState.FAILED
        assert states["part_2026_03_14"] == DagRunState.QUEUED

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_no_matching_runs_is_a_no_op(self, parser, capsys):
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2027-01-01T00:00:00",
                "--partition-date-end",
                "2027-12-31T00:00:00",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)
        out = capsys.readouterr().out
        assert "No matching Dag runs" in out
        assert self._get_run_states()["part_2026_03_10"] == DagRunState.FAILED

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    @mock.patch("airflow.cli.commands.dag_command.ask_yesno", return_value=False)
    def test_prompt_decline_does_not_clear(self, mock_ask, parser):
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-03-08T00:00:00",
                "--partition-date-end",
                "2026-03-14T00:00:00",
            ]
        )
        dag_command.dag_clear(args)
        mock_ask.assert_called_once()
        states = self._get_run_states()
        assert states["part_2026_03_08"] == DagRunState.SUCCESS
        assert states["part_2026_03_10"] == DagRunState.FAILED
        assert states["part_2026_03_14"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_clears_by_run_id(self, parser):
        args = parser.parse_args(["dags", "clear", self.DAG_ID, "--run-id", "part_2026_03_10", "--yes"])
        dag_command.dag_clear(args)

        states = self._get_run_states()
        assert states["part_2026_03_08"] == DagRunState.SUCCESS
        assert states["part_2026_03_10"] == DagRunState.QUEUED
        assert states["part_2026_03_14"] == DagRunState.SUCCESS
        assert states["non_partitioned"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_clears_by_partition_key(self, parser):
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-key",
                "2026-03-10T00:00:00",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        assert states["part_2026_03_08"] == DagRunState.SUCCESS
        assert states["part_2026_03_10"] == DagRunState.QUEUED
        assert states["part_2026_03_14"] == DagRunState.SUCCESS
        assert states["non_partitioned"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_run_id_not_found_is_a_no_op(self, parser, capsys):
        args = parser.parse_args(["dags", "clear", self.DAG_ID, "--run-id", "does_not_exist", "--yes"])
        dag_command.dag_clear(args)
        assert "No matching Dag runs" in capsys.readouterr().out
        assert self._get_run_states()["part_2026_03_10"] == DagRunState.FAILED

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_only_failed_skips_non_failed_task_instances(self, parser):
        # Explicitly set TI states so we can assert selectively.
        # part_2026_03_10 has a FAILED DagRun; mark its single TI as FAILED.
        # part_2026_03_08 has a SUCCESS DagRun; mark its single TI as SUCCESS.
        with create_session() as session:
            for run_id, ti_state in [
                ("part_2026_03_08", TaskInstanceState.SUCCESS),
                ("part_2026_03_10", TaskInstanceState.FAILED),
            ]:
                session.execute(
                    TaskInstance.__table__.update()
                    .where(TaskInstance.dag_id == self.DAG_ID)
                    .where(TaskInstance.run_id == run_id)
                    .values(state=ti_state)
                )

        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-03-08T00:00:00",
                "--partition-date-end",
                "2026-03-14T00:00:00",
                "--only-failed",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # part_2026_03_10 had a FAILED TI — its run should be re-queued.
        assert states["part_2026_03_10"] == DagRunState.QUEUED
        # part_2026_03_08 had no FAILED TI — its run state must be unchanged.
        assert states["part_2026_03_08"] == DagRunState.SUCCESS
        # Non-partitioned run is always untouched.
        assert states["non_partitioned"] == DagRunState.SUCCESS

    def test_missing_dag_raises(self, parser):
        args = parser.parse_args(
            [
                "dags",
                "clear",
                "does_not_exist",
                "--partition-date-start",
                "2026-03-08T00:00:00",
                "--yes",
            ]
        )
        with pytest.raises(AirflowException, match="could not be found in the database"):
            dag_command.dag_clear(args)

    @pytest.fixture
    def seeded_taipei_runs(self, dag_maker):
        """Seed DagRuns for a Asia/Taipei (UTC+8) CronPartitionTimetable.

        Local midnight in Taipei is stored as UTC-8h in partition_date.
        Written as explicit UTC instants so the oracle is independent of the
        timetable under test:

          local 2026-02-18 midnight  → datetime(2026, 2, 17, 16, 0, 0, UTC)
          local 2026-02-19 midnight  → datetime(2026, 2, 18, 16, 0, 0, UTC)
          local 2026-02-20 midnight  → datetime(2026, 2, 19, 16, 0, 0, UTC)  (outside window)
        """
        with dag_maker(
            self.DAG_ID,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei"),
            start_date=datetime(2026, 2, 1, tzinfo=pendulum.UTC),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        runs = [
            (
                "taipei_2026_02_18",
                datetime(2026, 2, 17, 16, 0, 0, tzinfo=pendulum.UTC),
                "2026-02-18T00:00:00",
            ),
            (
                "taipei_2026_02_19",
                datetime(2026, 2, 18, 16, 0, 0, tzinfo=pendulum.UTC),
                "2026-02-19T00:00:00",
            ),
            (
                "taipei_2026_02_20",
                datetime(2026, 2, 19, 16, 0, 0, tzinfo=pendulum.UTC),
                "2026-02-20T00:00:00",
            ),
        ]
        for run_id, partition_date, partition_key in runs:
            dag_maker.create_dagrun(
                run_id=run_id,
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=partition_date,
                partition_key=partition_key,
            )
        dag_maker.sync_dagbag_to_db()

    @pytest.mark.usefixtures("seeded_taipei_runs")
    def test_taipei_lower_bound_selects_correct_partition(self, parser):
        """--partition-date-start 2026-02-19 must match the run stored at 2026-02-18T16Z.

        Without the timezone fix, parsedate("2026-02-19") yields 2026-02-19T00:00:00Z
        under the UTC default timezone.  The old filter compares
        partition_date >= 2026-02-19T00:00Z; the run for the local 2026-02-19 partition
        is stored as 2026-02-18T16:00Z, which is *before* that UTC boundary, so the run
        is NOT selected and the requested boundary day is silently missed.  Converting
        the bound through the timetable timezone fixes the off-by-one: the run whose
        partition_date is the UTC representation of Taipei 2026-02-19 midnight is
        selected, the earlier run is not.
        """
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-02-19",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # 2026-02-19 local midnight stored as 2026-02-18T16Z — must be cleared.
        assert states["taipei_2026_02_19"] == DagRunState.QUEUED
        # 2026-02-20 local midnight stored as 2026-02-19T16Z — also in window (no upper bound).
        assert states["taipei_2026_02_20"] == DagRunState.QUEUED
        # 2026-02-18 local midnight stored as 2026-02-17T16Z — before the start, must NOT be cleared.
        assert states["taipei_2026_02_18"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_taipei_runs")
    def test_taipei_upper_bound_at_cap(self, parser):
        """--partition-date-end 2026-02-19 must include the run stored at 2026-02-18T16Z (at-cap)."""
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-end",
                "2026-02-19",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # Both 2026-02-18 and 2026-02-19 local dates are within the window.
        assert states["taipei_2026_02_18"] == DagRunState.QUEUED
        assert states["taipei_2026_02_19"] == DagRunState.QUEUED
        # 2026-02-20 is outside the half-open upper bound — must NOT be cleared.
        assert states["taipei_2026_02_20"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_taipei_runs")
    def test_taipei_upper_bound_over_cap(self, parser):
        """--partition-date-end 2026-02-18 must NOT include the run stored at 2026-02-18T16Z (over-cap).

        The half-open upper bound is strictly less than 2026-02-19 midnight in
        Taipei (= 2026-02-18T16Z), so the run for local 2026-02-19 falls outside
        the window.
        """
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-end",
                "2026-02-18",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # Only the 2026-02-18 local date run is within the window.
        assert states["taipei_2026_02_18"] == DagRunState.QUEUED
        # 2026-02-19 and 2026-02-20 are outside — must NOT be cleared.
        assert states["taipei_2026_02_19"] == DagRunState.SUCCESS
        assert states["taipei_2026_02_20"] == DagRunState.SUCCESS

    @pytest.fixture
    def seeded_ny_runs(self, dag_maker):
        """Seed DagRuns for an America/New_York (UTC-5, EST in February) CronPartitionTimetable.

        Local midnight in New York is stored as UTC+5h in partition_date, so for a
        west-of-UTC timetable the pre-fix off-by-one lands on the *upper* bound (the
        end day's run sits later in the UTC day than the user-typed boundary) rather
        than the lower bound exercised by the Asia/Taipei cases.  Explicit UTC instants
        keep the oracle independent of the timetable under test:

          local 2026-02-18 midnight  → datetime(2026, 2, 18, 5, 0, 0, UTC)
          local 2026-02-19 midnight  → datetime(2026, 2, 19, 5, 0, 0, UTC)
          local 2026-02-20 midnight  → datetime(2026, 2, 20, 5, 0, 0, UTC)  (outside window)
        """
        with dag_maker(
            self.DAG_ID,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="America/New_York"),
            start_date=datetime(2026, 2, 1, tzinfo=pendulum.UTC),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        runs = [
            ("ny_2026_02_18", datetime(2026, 2, 18, 5, 0, 0, tzinfo=pendulum.UTC), "2026-02-18T00:00:00"),
            ("ny_2026_02_19", datetime(2026, 2, 19, 5, 0, 0, tzinfo=pendulum.UTC), "2026-02-19T00:00:00"),
            ("ny_2026_02_20", datetime(2026, 2, 20, 5, 0, 0, tzinfo=pendulum.UTC), "2026-02-20T00:00:00"),
        ]
        for run_id, partition_date, partition_key in runs:
            dag_maker.create_dagrun(
                run_id=run_id,
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=partition_date,
                partition_key=partition_key,
            )
        dag_maker.sync_dagbag_to_db()

    @pytest.mark.usefixtures("seeded_ny_runs")
    def test_ny_upper_bound_includes_end_day(self, parser):
        """--partition-date-end 2026-02-19 must include the local 2026-02-19 run (stored 2026-02-19T05Z).

        parsedate("2026-02-19") yields 2026-02-19T00:00Z, so the pre-fix filter compares
        partition_date <= 2026-02-19T00:00Z.  The local 2026-02-19 run is stored at
        2026-02-19T05:00Z, which is *after* that UTC boundary, so the old code wrongly
        drops the requested end day.  The timezone-aware half-open bound includes it.
        """
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-end",
                "2026-02-19",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # local 2026-02-18 and 2026-02-19 are within the window (no lower bound).
        assert states["ny_2026_02_18"] == DagRunState.QUEUED
        # The end-day run: dropped by the pre-fix code, included after the fix.
        assert states["ny_2026_02_19"] == DagRunState.QUEUED
        # local 2026-02-20 is outside the half-open upper bound.
        assert states["ny_2026_02_20"] == DagRunState.SUCCESS

    @pytest.fixture
    def seeded_no_tz_runs(self, dag_maker):
        """Seed runs with UTC midnight partition_dates for the no-tz fallback path.

        The Dag is created with CronPartitionTimetable(timezone=UTC) so that
        serialization works normally.  The tests monkeypatch get_db_dag to swap
        the timetable for a stub that has partitioned=True but no timezone
        attribute, forcing the else-branch in dag_clear.

        Stored partition_dates are plain UTC midnights:
          2026-03-08 → datetime(2026, 3, 8, 0, 0, 0, UTC)
          2026-03-09 → datetime(2026, 3, 9, 0, 0, 0, UTC)
        """
        with dag_maker(
            self.DAG_ID,
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2026, 3, 1, tzinfo=pendulum.UTC),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        runs = [
            ("no_tz_2026_03_08", datetime(2026, 3, 8, tzinfo=pendulum.UTC), "2026-03-08T00:00:00"),
            ("no_tz_2026_03_09", datetime(2026, 3, 9, tzinfo=pendulum.UTC), "2026-03-09T00:00:00"),
        ]
        for run_id, partition_date, partition_key in runs:
            dag_maker.create_dagrun(
                run_id=run_id,
                state=DagRunState.SUCCESS,
                logical_date=None,
                partition_date=partition_date,
                partition_key=partition_key,
            )
        dag_maker.sync_dagbag_to_db()

    @pytest.mark.usefixtures("seeded_no_tz_runs")
    def test_no_tz_lower_bound_honours_time_of_day(self, parser, monkeypatch):
        """--partition-date-start with a non-midnight time-of-day is honoured, not truncated.

        A start of 2026-03-08T12:00:00 passes through as lower = 2026-03-08T12:00Z
        (no-tz fallback keeps the wall-clock as UTC).  The stored 2026-03-08T00:00Z
        run is *before* that bound and is excluded; the 2026-03-09T00:00Z run is
        after it and is cleared.
        """

        def _patched(*, bundle_names, dag_id):
            dag = get_db_dag(bundle_names=bundle_names, dag_id=dag_id)
            dag.timetable = _NoTzTimetable()
            return dag

        monkeypatch.setattr(dag_command, "get_db_dag", _patched)

        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-03-08T12:00:00",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # 2026-03-08T00Z is before the 12:00Z lower bound → excluded.
        assert states["no_tz_2026_03_08"] == DagRunState.SUCCESS
        # 2026-03-09T00Z is after the lower bound (no upper bound) → cleared.
        assert states["no_tz_2026_03_09"] == DagRunState.QUEUED

    @pytest.mark.usefixtures("seeded_no_tz_runs")
    def test_no_tz_upper_bound_is_half_open(self, parser, monkeypatch):
        """--partition-date-end 2026-03-08T00:00:00 must include 2026-03-08 and exclude 2026-03-09.

        The end date truncates to 2026-03-08; next_day = 2026-03-09, upper =
        2026-03-09T00:00Z (half-open).  The 2026-03-08T00Z run satisfies
        partition_date < 2026-03-09T00Z (included).  The 2026-03-09T00Z run
        does NOT satisfy partition_date < 2026-03-09T00Z (excluded).
        """

        def _patched(*, bundle_names, dag_id):
            dag = get_db_dag(bundle_names=bundle_names, dag_id=dag_id)
            dag.timetable = _NoTzTimetable()
            return dag

        monkeypatch.setattr(dag_command, "get_db_dag", _patched)

        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-end",
                "2026-03-08T00:00:00",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # 2026-03-08T00Z < 2026-03-09T00Z (upper, half-open) → included.
        assert states["no_tz_2026_03_08"] == DagRunState.QUEUED
        # 2026-03-09T00Z is NOT < 2026-03-09T00Z → excluded.
        assert states["no_tz_2026_03_09"] == DagRunState.SUCCESS

    @pytest.fixture
    def seeded_asset_partitioned_runs(self, dag_maker):
        """Seed DagRuns for a PartitionedAssetTimetable Dag.

        PartitionedAssetTimetable has partitioned=True but is NOT a CronMixin
        subclass, so isinstance(dag.timetable, CronMixin) is False and
        tt_tz=None.  partition_date values are plain UTC midnights (just like
        the no-tz fallback path).

        Runs:
          asset_2026_04_10 → partition_date = 2026-04-10T00:00:00Z  (at lower boundary)
          asset_2026_04_12 → partition_date = 2026-04-12T00:00:00Z  (within window)
          asset_2026_04_14 → partition_date = 2026-04-14T00:00:00Z  (at upper boundary)
          asset_2026_04_15 → partition_date = 2026-04-15T00:00:00Z  (just outside upper boundary)
          asset_non_part   → partition_date = None                   (never cleared)
        """
        with dag_maker(
            self.DAG_ID,
            schedule=PartitionedAssetTimetable(assets=Asset("test_asset_for_clear")),
            start_date=datetime(2026, 4, 1, tzinfo=pendulum.UTC),
            catchup=False,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        runs = [
            (
                "asset_2026_04_10",
                DagRunState.SUCCESS,
                datetime(2026, 4, 10, tzinfo=pendulum.UTC),
                "2026-04-10T00:00:00",
            ),
            (
                "asset_2026_04_12",
                DagRunState.FAILED,
                datetime(2026, 4, 12, tzinfo=pendulum.UTC),
                "2026-04-12T00:00:00",
            ),
            (
                "asset_2026_04_14",
                DagRunState.SUCCESS,
                datetime(2026, 4, 14, tzinfo=pendulum.UTC),
                "2026-04-14T00:00:00",
            ),
            (
                "asset_2026_04_15",
                DagRunState.SUCCESS,
                datetime(2026, 4, 15, tzinfo=pendulum.UTC),
                "2026-04-15T00:00:00",
            ),
        ]
        for run_id, state, partition_date, partition_key in runs:
            dag_maker.create_dagrun(
                run_id=run_id,
                state=state,
                logical_date=None,
                partition_date=partition_date,
                partition_key=partition_key,
            )
        dag_maker.create_dagrun(
            run_id="asset_non_part",
            state=DagRunState.SUCCESS,
            logical_date=datetime(2026, 4, 11, tzinfo=pendulum.UTC),
            partition_date=None,
        )
        dag_maker.sync_dagbag_to_db()

    @pytest.mark.usefixtures("seeded_asset_partitioned_runs")
    def test_asset_timetable_clears_window_inclusive(self, parser):
        """PartitionedAssetTimetable uses the base (UTC) localization; datetime-precision bounds are correct.

        PartitionedAssetTimetable has no local timezone, so localize_partition_datetime
        is a UTC pass-through.  The full window 2026-04-10 to 2026-04-14 (inclusive)
        should clear the at-boundary and within-window runs; 2026-04-15 and
        partition_date=None must not be touched.
        """
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-04-10",
                "--partition-date-end",
                "2026-04-14",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        assert states == {
            "asset_2026_04_10": DagRunState.QUEUED,
            "asset_2026_04_12": DagRunState.QUEUED,
            "asset_2026_04_14": DagRunState.QUEUED,
            # Beyond the inclusive upper bound — must NOT be cleared.
            "asset_2026_04_15": DagRunState.SUCCESS,
            # NULL partition_date is never matched by the date-range filter.
            "asset_non_part": DagRunState.SUCCESS,
        }

    @pytest.mark.usefixtures("seeded_asset_partitioned_runs")
    def test_asset_timetable_upper_bound_at_cap(self, parser):
        """--partition-date-end 2026-04-14 includes the run at exactly that UTC midnight (at-cap, inclusive)."""
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-end",
                "2026-04-14",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        # All runs on or before 2026-04-14 UTC midnight must be cleared.
        assert states["asset_2026_04_10"] == DagRunState.QUEUED
        assert states["asset_2026_04_12"] == DagRunState.QUEUED
        assert states["asset_2026_04_14"] == DagRunState.QUEUED
        # 2026-04-15 is beyond the inclusive upper bound.
        assert states["asset_2026_04_15"] == DagRunState.SUCCESS
        assert states["asset_non_part"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_asset_partitioned_runs")
    def test_asset_timetable_upper_bound_over_cap(self, parser):
        """--partition-date-end 2026-04-13 must NOT include the 2026-04-14 run (over-cap).

        Inclusive upper bound: end=2026-04-13T00:00Z → partition_date <= 2026-04-13T00:00Z.
        The run stored at 2026-04-14T00:00Z is excluded because Apr 14 > Apr 13.
        """
        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-end",
                "2026-04-13",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        states = self._get_run_states()
        assert states["asset_2026_04_10"] == DagRunState.QUEUED
        assert states["asset_2026_04_12"] == DagRunState.QUEUED
        # 2026-04-14 is beyond the inclusive end (Apr 13) — must NOT be cleared.
        assert states["asset_2026_04_14"] == DagRunState.SUCCESS
        assert states["asset_2026_04_15"] == DagRunState.SUCCESS
        assert states["asset_non_part"] == DagRunState.SUCCESS

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    @pytest.mark.parametrize(
        ("chunk_size", "expected_calls"),
        [
            pytest.param(500, 1, id="single-chunk"),
            pytest.param(2, 2, id="multiple-chunks"),
        ],
    )
    def test_clears_each_matching_run_once_across_chunks(self, parser, chunk_size, expected_calls):
        """Every matching run is cleared exactly once, however run_ids split into chunks.

        clear_task_instances is called once per chunk (not once per run), every matching
        run is re-queued, and each run's clear_number advances by exactly 1 — proving a
        run's TIs are never split across chunks.
        """
        call_count = 0

        def counting_clear(tis, session, **kwargs):
            nonlocal call_count
            call_count += 1
            return clear_task_instances(tis, session, **kwargs)

        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-03-08T00:00:00",
                "--partition-date-end",
                "2026-03-14T00:00:00",
                "--yes",
            ]
        )
        with (
            mock.patch.object(dag_command, "_RUN_CHUNK_SIZE", chunk_size),
            mock.patch(
                "airflow.cli.commands.dag_command.clear_task_instances",
                side_effect=counting_clear,
            ),
        ):
            dag_command.dag_clear(args)

        assert call_count == expected_calls

        states = self._get_run_states()
        assert states["part_2026_03_08"] == DagRunState.QUEUED
        assert states["part_2026_03_10"] == DagRunState.QUEUED
        assert states["part_2026_03_14"] == DagRunState.QUEUED
        assert states["non_partitioned"] == DagRunState.SUCCESS

        clear_numbers = self._get_run_clear_numbers()
        assert clear_numbers["part_2026_03_08"] == 1
        assert clear_numbers["part_2026_03_10"] == 1
        assert clear_numbers["part_2026_03_14"] == 1
        assert clear_numbers["non_partitioned"] == 0

    @pytest.mark.usefixtures("seeded_partitioned_runs")
    def test_does_not_clear_runs_of_other_dags(self, parser, dag_maker):
        """A run_id collision across DAGs must not clear the other DAG's task instances."""
        other_dag_id = "test_dags_clear_other_dag"
        with dag_maker(
            other_dag_id,
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2026, 3, 1, tzinfo=pendulum.UTC),
            catchup=True,
            serialized=True,
        ):
            EmptyOperator(task_id="t1")
        # Same run_id and partition_date as a run cleared below, but a different DAG.
        dag_maker.create_dagrun(
            run_id="part_2026_03_08",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2026, 3, 8, tzinfo=pendulum.UTC),
            partition_key="2026-03-08T00:00:00",
        )
        dag_maker.sync_dagbag_to_db()
        # If dag_id is not filtered, clearing the other DAG would reset this TI to None.
        with create_session() as session:
            session.execute(
                TaskInstance.__table__.update()
                .where(TaskInstance.dag_id == other_dag_id)
                .values(state=TaskInstanceState.SUCCESS)
            )

        args = parser.parse_args(
            [
                "dags",
                "clear",
                self.DAG_ID,
                "--partition-date-start",
                "2026-03-08T00:00:00",
                "--partition-date-end",
                "2026-03-14T00:00:00",
                "--yes",
            ]
        )
        dag_command.dag_clear(args)

        # The target DAG's same-named run must be cleared.
        assert self._get_run_states()["part_2026_03_08"] == DagRunState.QUEUED

        # The other DAG's same-named run must be left untouched.
        with create_session() as session:
            other_run = session.scalars(
                select(DagRun).where(DagRun.dag_id == other_dag_id, DagRun.run_id == "part_2026_03_08")
            ).one()
            assert other_run.state == DagRunState.SUCCESS
            assert other_run.clear_number == 0
            other_ti = session.scalars(select(TaskInstance).where(TaskInstance.dag_id == other_dag_id)).one()
            assert other_ti.state == TaskInstanceState.SUCCESS


class TestDagDetailsIsBackfillable:
    """Tests for the is_backfillable computation in _get_dagbag_dag_details."""

    @pytest.mark.parametrize(
        ("schedule", "allowed_run_types", "expected"),
        [
            pytest.param("@daily", None, True, id="periodic-allowed-none"),
            pytest.param(
                "@daily",
                [DagRunType.SCHEDULED, DagRunType.MANUAL, DagRunType.BACKFILL_JOB],
                True,
                id="periodic-backfill-included",
            ),
            pytest.param(
                "@daily",
                [DagRunType.SCHEDULED, DagRunType.MANUAL],
                False,
                id="periodic-backfill-excluded",
            ),
            pytest.param(None, None, False, id="non-periodic-null-schedule"),
            pytest.param("@once", None, False, id="non-periodic-once-schedule"),
        ],
    )
    def test_is_backfillable(self, schedule, allowed_run_types, expected):
        dag = DAG(
            dag_id="test_is_backfillable",
            schedule=schedule,
            allowed_run_types=allowed_run_types,
        )
        dag_details = dag_command._get_dagbag_dag_details(dag)
        assert dag_details["is_backfillable"] is expected
