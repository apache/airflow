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

import contextlib
import json
import os
from datetime import datetime, timedelta
from io import StringIO
from unittest import TestCase, mock
from unittest.mock import MagicMock

import pendulum
import pytest
import time_machine

from airflow import settings
from airflow.api_connexion.schemas.dag_schema import DAGSchema
from airflow.cli import cli_parser
from airflow.cli.commands import dag_command
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import DagBag, DagModel, DagRun
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import _StopDagTest
from airflow.models.serialized_dag import SerializedDagModel
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.types import DagRunType
from tests.models import TEST_DAGS_FOLDER
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1), timezone=timezone.utc)

# TODO: Check if tests needs side effects - locally there's missing DAG

pytestmark = pytest.mark.db_test


class TestCliDags:
    @classmethod
    def setup_class(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.dagbag.sync_to_db()
        cls.parser = cli_parser.get_parser()

    @classmethod
    def teardown_class(cls) -> None:
        clear_db_runs()
        clear_db_dags()

    def setup_method(self):
        clear_db_runs()  # clean-up all dag run before start each test

    def test_reserialize(self):
        # Assert that there are serialized Dags
        with create_session() as session:
            serialized_dags_before_command = session.query(SerializedDagModel).all()
        assert len(serialized_dags_before_command)  # There are serialized DAGs to delete

        # Run clear of serialized dags
        dag_command.dag_reserialize(self.parser.parse_args(["dags", "reserialize", "--clear-only"]))
        # Assert no serialized Dags
        with create_session() as session:
            serialized_dags_after_clear = session.query(SerializedDagModel).all()
        assert not len(serialized_dags_after_clear)

        # Serialize manually
        dag_command.dag_reserialize(self.parser.parse_args(["dags", "reserialize"]))

        # Check serialized DAGs are back
        with create_session() as session:
            serialized_dags_after_reserialize = session.query(SerializedDagModel).all()
        assert len(serialized_dags_after_reserialize) >= 40  # Serialized DAGs back

    def test_reserialize_should_support_subdir_argument(self):
        # Run clear of serialized dags
        dag_command.dag_reserialize(self.parser.parse_args(["dags", "reserialize", "--clear-only"]))

        # Assert no serialized Dags
        with create_session() as session:
            serialized_dags_after_clear = session.query(SerializedDagModel).all()
        assert len(serialized_dags_after_clear) == 0

        # Serialize manually
        dag_path = self.dagbag.dags["example_bash_operator"].fileloc
        # Set default value of include_examples parameter to false
        dagbag_default = list(DagBag.__init__.__defaults__)
        dagbag_default[1] = False
        with mock.patch(
            "airflow.cli.commands.dag_command.DagBag.__init__.__defaults__", tuple(dagbag_default)
        ):
            dag_command.dag_reserialize(self.parser.parse_args(["dags", "reserialize", "--subdir", dag_path]))

        # Check serialized DAG are back
        with create_session() as session:
            serialized_dags_after_reserialize = session.query(SerializedDagModel).all()
        assert len(serialized_dags_after_reserialize) == 1  # Serialized DAG back

    @mock.patch("airflow.cli.commands.dag_command.DAG.run")
    def test_backfill(self, mock_run):
        dag_command.dag_backfill(
            self.parser.parse_args(
                ["dags", "backfill", "example_bash_operator", "--start-date", DEFAULT_DATE.isoformat()]
            )
        )

        mock_run.assert_called_once_with(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=True,
            ignore_task_deps=False,
            local=False,
            mark_success=False,
            pool=None,
            rerun_failed_tasks=False,
            run_backwards=False,
            verbose=False,
            continue_on_failures=False,
            disable_retry=False,
        )
        mock_run.reset_mock()
        dag = self.dagbag.get_dag("example_bash_operator")

        with contextlib.redirect_stdout(StringIO()) as stdout:
            dag_command.dag_backfill(
                self.parser.parse_args(
                    [
                        "dags",
                        "backfill",
                        "example_bash_operator",
                        "--task-regex",
                        "runme_0",
                        "--dry-run",
                        "--start-date",
                        DEFAULT_DATE.isoformat(),
                    ]
                ),
                dag=dag,
            )

        output = stdout.getvalue()
        assert f"Dry run of DAG example_bash_operator on {DEFAULT_DATE.isoformat()}\n" in output
        assert "Task runme_0 located in DAG example_bash_operator\n" in output

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        dag_command.dag_backfill(
            self.parser.parse_args(
                [
                    "dags",
                    "backfill",
                    "example_bash_operator",
                    "--dry-run",
                    "--start-date",
                    DEFAULT_DATE.isoformat(),
                ]
            ),
            dag=dag,
        )

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        dag_command.dag_backfill(
            self.parser.parse_args(
                [
                    "dags",
                    "backfill",
                    "example_bash_operator",
                    "--local",
                    "--start-date",
                    DEFAULT_DATE.isoformat(),
                ]
            ),
            dag=dag,
        )

        mock_run.assert_called_once_with(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=True,
            ignore_task_deps=False,
            local=True,
            mark_success=False,
            pool=None,
            rerun_failed_tasks=False,
            run_backwards=False,
            verbose=False,
            continue_on_failures=False,
            disable_retry=False,
        )
        mock_run.reset_mock()

        with contextlib.redirect_stdout(StringIO()) as stdout:
            dag_command.dag_backfill(
                self.parser.parse_args(
                    [
                        "dags",
                        "backfill",
                        "example_branch_(python_){0,1}operator(_decorator){0,1}",
                        "--task-regex",
                        "run_this_first",
                        "--dry-run",
                        "--treat-dag-as-regex",
                        "--start-date",
                        DEFAULT_DATE.isoformat(),
                    ]
                ),
            )

        output = stdout.getvalue()

        assert (
            f"Dry run of DAG example_branch_python_operator_decorator on "
            f"{DEFAULT_DATE.isoformat()}\n" in output
        )
        assert "Task run_this_first located in DAG example_branch_python_operator_decorator\n" in output
        assert f"Dry run of DAG example_branch_operator on {DEFAULT_DATE.isoformat()}\n" in output
        assert "Task run_this_first located in DAG example_branch_operator\n" in output

    @mock.patch("airflow.cli.commands.dag_command.get_dag")
    def test_backfill_fails_without_loading_dags(self, mock_get_dag):
        cli_args = self.parser.parse_args(["dags", "backfill", "example_bash_operator"])

        with pytest.raises(AirflowException):
            dag_command.dag_backfill(cli_args)

        mock_get_dag.assert_not_called()

    def test_show_dag_dependencies_print(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_dependencies_show(self.parser.parse_args(["dags", "show-dependencies"]))
        out = temp_stdout.getvalue()
        assert "digraph" in out
        assert "graph [rankdir=LR]" in out

    @mock.patch("airflow.cli.commands.dag_command.render_dag_dependencies")
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

    @mock.patch("airflow.cli.commands.dag_command.render_dag")
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

    @mock.patch("airflow.cli.commands.dag_command.subprocess.Popen")
    @mock.patch("airflow.cli.commands.dag_command.render_dag")
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

    @mock.patch("airflow.cli.commands.dag_command.DAG.run")
    def test_cli_backfill_depends_on_past(self, mock_run):
        """
        Test that CLI respects -I argument

        We just check we call dag.run() right. The behaviour of that kwarg is
        tested in test_jobs
        """
        dag_id = "test_dagrun_states_deadlock"
        run_date = DEFAULT_DATE + timedelta(days=1)
        args = [
            "dags",
            "backfill",
            dag_id,
            "--local",
            "--start-date",
            run_date.isoformat(),
            "--ignore-first-depends-on-past",
        ]
        dag = self.dagbag.get_dag(dag_id)

        dag_command.dag_backfill(self.parser.parse_args(args), dag=dag)

        mock_run.assert_called_once_with(
            start_date=run_date,
            end_date=run_date,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=True,
            ignore_task_deps=False,
            local=True,
            mark_success=False,
            pool=None,
            rerun_failed_tasks=False,
            run_backwards=False,
            verbose=False,
            continue_on_failures=False,
            disable_retry=False,
        )

    @mock.patch("airflow.cli.commands.dag_command.DAG.run")
    def test_cli_backfill_depends_on_past_backwards(self, mock_run):
        """
        Test that CLI respects -B argument and raises on interaction with depends_on_past
        """
        dag_id = "test_depends_on_past"
        start_date = DEFAULT_DATE + timedelta(days=1)
        end_date = start_date + timedelta(days=1)
        args = [
            "dags",
            "backfill",
            dag_id,
            "--local",
            "--start-date",
            start_date.isoformat(),
            "--end-date",
            end_date.isoformat(),
            "--ignore-first-depends-on-past",
            "--run-backwards",
        ]
        dag = self.dagbag.get_dag(dag_id)

        dag_command.dag_backfill(self.parser.parse_args(args), dag=dag)
        mock_run.assert_called_once_with(
            start_date=start_date,
            end_date=end_date,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=True,
            ignore_task_deps=False,
            local=True,
            mark_success=False,
            pool=None,
            rerun_failed_tasks=False,
            run_backwards=True,
            verbose=False,
            continue_on_failures=False,
            disable_retry=False,
        )

    @mock.patch("workday.AfterWorkdayTimetable")
    @mock.patch("airflow.models.taskinstance.TaskInstance.dry_run")
    @mock.patch("airflow.cli.commands.dag_command.DagRun")
    def test_backfill_with_custom_timetable(self, mock_dagrun, mock_dry_run, mock_AfterWorkdayTimetable):
        """
        when calling `dags backfill` on dag with custom timetable, the DagRun object should be created with
         data_intervals.
        """
        start_date = DEFAULT_DATE + timedelta(days=1)
        end_date = start_date + timedelta(days=1)
        cli_args = self.parser.parse_args(
            [
                "dags",
                "backfill",
                "example_workday_timetable",
                "--start-date",
                start_date.isoformat(),
                "--end-date",
                end_date.isoformat(),
                "--dry-run",
            ]
        )
        dag_command.dag_backfill(cli_args)
        assert "data_interval" in mock_dagrun.call_args.kwargs

    def test_next_execution(self, tmp_path):
        dag_test_list = [
            ("future_schedule_daily", "timedelta(days=5)", "'0 0 * * *'", "True"),
            ("future_schedule_every_4_hours", "timedelta(days=5)", "timedelta(hours=4)", "True"),
            ("future_schedule_once", "timedelta(days=5)", "'@once'", "True"),
            ("future_schedule_none", "timedelta(days=5)", "None", "True"),
            ("past_schedule_once", "timedelta(days=-5)", "'@once'", "True"),
            ("past_schedule_daily", "timedelta(days=-5)", "'0 0 * * *'", "True"),
            ("past_schedule_daily_catchup_false", "timedelta(days=-5)", "'0 0 * * *'", "False"),
        ]

        for f in dag_test_list:
            file_content = os.linesep.join(
                [
                    "from airflow import DAG",
                    "from airflow.operators.empty import EmptyOperator",
                    "from datetime import timedelta; from pendulum import today",
                    f"dag = DAG('{f[0]}', start_date=today() + {f[1]}, schedule={f[2]}, catchup={f[3]})",
                    "task = EmptyOperator(task_id='empty_task',dag=dag)",
                ]
            )
            dag_file = tmp_path / f"{f[0]}.py"
            dag_file.write_text(file_content)

        with time_machine.travel(DEFAULT_DATE):
            clear_db_dags()
            self.dagbag = DagBag(dag_folder=tmp_path, include_examples=False)
            self.dagbag.sync_to_db()

        default_run = DEFAULT_DATE
        future_run = default_run + timedelta(days=5)
        past_run = default_run + timedelta(days=-5)

        expected_output = {
            "future_schedule_daily": (
                future_run.isoformat(),
                future_run.isoformat() + os.linesep + (future_run + timedelta(days=1)).isoformat(),
            ),
            "future_schedule_every_4_hours": (
                future_run.isoformat(),
                future_run.isoformat() + os.linesep + (future_run + timedelta(hours=4)).isoformat(),
            ),
            "future_schedule_once": (future_run.isoformat(), future_run.isoformat() + os.linesep + "None"),
            "future_schedule_none": ("None", "None"),
            "past_schedule_once": (past_run.isoformat(), "None"),
            "past_schedule_daily": (
                past_run.isoformat(),
                past_run.isoformat() + os.linesep + (past_run + timedelta(days=1)).isoformat(),
            ),
            "past_schedule_daily_catchup_false": (
                (default_run - timedelta(days=1)).isoformat(),
                (default_run - timedelta(days=1)).isoformat() + os.linesep + default_run.isoformat(),
            ),
        }

        for dag_id in expected_output:
            # Test num-executions = 1 (default)
            args = self.parser.parse_args(["dags", "next-execution", dag_id, "-S", str(tmp_path)])
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                dag_command.dag_next_execution(args)
                out = temp_stdout.getvalue()
            assert expected_output[dag_id][0] in out

            # Test num-executions = 2
            args = self.parser.parse_args(
                ["dags", "next-execution", dag_id, "--num-executions", "2", "-S", str(tmp_path)]
            )
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                dag_command.dag_next_execution(args)
                out = temp_stdout.getvalue()
            assert expected_output[dag_id][1] in out

        # Rebuild Test DB for other tests
        clear_db_dags()
        TestCliDags.dagbag = DagBag(include_examples=True)
        TestCliDags.dagbag.sync_to_db()

    @conf_vars({("core", "load_examples"): "true"})
    def test_cli_report(self):
        args = self.parser.parse_args(["dags", "report", "--output", "json"])
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_report(args)
            out = temp_stdout.getvalue()

        assert "airflow/example_dags/example_complex.py" in out
        assert "example_complex" in out

    @conf_vars({("core", "load_examples"): "true"})
    def test_cli_get_dag_details(self):
        args = self.parser.parse_args(["dags", "details", "example_complex", "--output", "yaml"])
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_details(args)
            out = temp_stdout.getvalue()

        dag_detail_fields = DAGSchema().fields.keys()

        # Check if DAG Details field are present
        for field in dag_detail_fields:
            assert field in out

        # Check if identifying values are present
        dag_details_values = ["airflow", "airflow/example_dags/example_complex.py", "16", "example_complex"]

        for value in dag_details_values:
            assert value in out

    @conf_vars({("core", "load_examples"): "true"})
    def test_cli_list_dags(self):
        args = self.parser.parse_args(["dags", "list", "--output", "yaml"])
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_list_dags(args)
            out = temp_stdout.getvalue()
        assert "owner" in out
        assert "airflow" in out
        assert "paused" in out
        assert "airflow/example_dags/example_complex.py" in out
        assert "- dag_id:" in out

    @conf_vars({("core", "load_examples"): "false"})
    def test_cli_list_dags_prints_import_errors(self):
        dag_path = os.path.join(TEST_DAGS_FOLDER, "test_invalid_cron.py")
        args = self.parser.parse_args(["dags", "list", "--output", "yaml", "--subdir", dag_path])
        with contextlib.redirect_stderr(StringIO()) as temp_stderr:
            dag_command.dag_list_dags(args)
            out = temp_stderr.getvalue()
        assert "Failed to load all files." in out

    @conf_vars({("core", "load_examples"): "false"})
    def test_cli_list_import_errors(self):
        dag_path = os.path.join(TEST_DAGS_FOLDER, "test_invalid_cron.py")
        args = self.parser.parse_args(["dags", "list", "--output", "yaml", "--subdir", dag_path])
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            with TestCase.assertRaises(TestCase(), SystemExit) as context:
                dag_command.dag_list_import_errors(args)
            out = temp_stdout.getvalue()
        assert "[0 100 * * *] is not acceptable, out of range" in out
        assert dag_path in out
        assert context.exception.code == 1

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
                "--dag-id",
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
        assert self.dagbag.dags["example_bash_operator"].get_is_paused() in [True, 1]

        args = self.parser.parse_args(["dags", "unpause", "example_bash_operator"])
        dag_command.dag_unpause(args)
        assert self.dagbag.dags["example_bash_operator"].get_is_paused() in [False, 0]

    def test_trigger_dag(self):
        dag_command.dag_trigger(
            self.parser.parse_args(
                [
                    "dags",
                    "trigger",
                    "example_bash_operator",
                    "--run-id=test_trigger_dag",
                    "--exec-date=2021-06-04T09:00:00+08:00",
                    '--conf={"foo": "bar"}',
                ],
            ),
        )
        with create_session() as session:
            dagrun = session.query(DagRun).filter(DagRun.run_id == "test_trigger_dag").one()

        assert dagrun, "DagRun not created"
        assert dagrun.run_type == DagRunType.MANUAL
        assert dagrun.external_trigger
        assert dagrun.conf == {"foo": "bar"}

        # Coerced to UTC.
        assert dagrun.execution_date.isoformat(timespec="seconds") == "2021-06-04T01:00:00+00:00"

        # example_bash_operator runs every day at midnight, so the data interval
        # should be aligned to the previous day.
        assert dagrun.data_interval_start.isoformat(timespec="seconds") == "2021-06-03T00:00:00+00:00"
        assert dagrun.data_interval_end.isoformat(timespec="seconds") == "2021-06-04T00:00:00+00:00"

    def test_trigger_dag_with_microseconds(self):
        dag_command.dag_trigger(
            self.parser.parse_args(
                [
                    "dags",
                    "trigger",
                    "example_bash_operator",
                    "--run-id=test_trigger_dag_with_micro",
                    "--exec-date=2021-06-04T09:00:00.000001+08:00",
                    "--no-replace-microseconds",
                ],
            )
        )

        with create_session() as session:
            dagrun = session.query(DagRun).filter(DagRun.run_id == "test_trigger_dag_with_micro").one()

        assert dagrun, "DagRun not created"
        assert dagrun.run_type == DagRunType.MANUAL
        assert dagrun.external_trigger
        assert dagrun.execution_date.isoformat(timespec="microseconds") == "2021-06-04T01:00:00.000001+00:00"

    def test_trigger_dag_invalid_conf(self):
        with pytest.raises(ValueError):
            dag_command.dag_trigger(
                self.parser.parse_args(
                    [
                        "dags",
                        "trigger",
                        "example_bash_operator",
                        "--run-id",
                        "trigger_dag_xxx",
                        "--conf",
                        "NOT JSON",
                    ]
                ),
            )

    def test_trigger_dag_output_as_json(self):
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
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            dag_command.dag_trigger(args)
            # get the last line from the logs ignoring all logging lines
            out = temp_stdout.getvalue().strip().splitlines()[-1]
        parsed_out = json.loads(out)

        assert 1 == len(parsed_out)
        assert "example_bash_operator" == parsed_out[0]["dag_id"]
        assert "trigger_dag_xxx" == parsed_out[0]["dag_run_id"]
        assert {"conf1": "val1", "conf2": "val2"} == parsed_out[0]["conf"]

    def test_delete_dag(self):
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        session.add(DM(dag_id=key))
        session.commit()
        dag_command.dag_delete(self.parser.parse_args(["dags", "delete", key, "--yes"]))
        assert session.query(DM).filter_by(dag_id=key).count() == 0
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
        session.add(DM(dag_id=key, fileloc=os.fspath(path)))
        session.commit()
        dag_command.dag_delete(self.parser.parse_args(["dags", "delete", key, "--yes"]))
        assert session.query(DM).filter_by(dag_id=key).count() == 0

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

    @mock.patch("airflow.cli.commands.dag_command.get_dag")
    def test_dag_test(self, mock_get_dag):
        cli_args = self.parser.parse_args(["dags", "test", "example_bash_operator", DEFAULT_DATE.isoformat()])
        dag_command.dag_test(cli_args)

        mock_get_dag.assert_has_calls(
            [
                mock.call(subdir=cli_args.subdir, dag_id="example_bash_operator"),
                mock.call().test(
                    execution_date=timezone.parse(DEFAULT_DATE.isoformat()), run_conf=None, session=mock.ANY
                ),
            ]
        )

    @mock.patch("airflow.cli.commands.dag_command.get_dag")
    @mock.patch("airflow.utils.timezone.utcnow")
    def test_dag_test_no_execution_date(self, mock_utcnow, mock_get_dag):
        now = pendulum.now()
        mock_utcnow.return_value = now
        cli_args = self.parser.parse_args(["dags", "test", "example_bash_operator"])

        assert cli_args.execution_date is None

        dag_command.dag_test(cli_args)

        mock_get_dag.assert_has_calls(
            [
                mock.call(subdir=cli_args.subdir, dag_id="example_bash_operator"),
                mock.call().test(execution_date=mock.ANY, run_conf=None, session=mock.ANY),
            ]
        )

    @mock.patch("airflow.cli.commands.dag_command.get_dag")
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
                    execution_date=timezone.parse(DEFAULT_DATE.isoformat()),
                    run_conf={"dag_run_conf_param": "param_value"},
                    session=mock.ANY,
                ),
            ]
        )

    @mock.patch("airflow.cli.commands.dag_command.render_dag", return_value=MagicMock(source="SOURCE"))
    @mock.patch("airflow.cli.commands.dag_command.get_dag")
    def test_dag_test_show_dag(self, mock_get_dag, mock_render_dag):
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
                    execution_date=timezone.parse(DEFAULT_DATE.isoformat()), run_conf=None, session=mock.ANY
                ),
            ]
        )
        mock_render_dag.assert_has_calls([mock.call(mock_get_dag.return_value, tis=[])])
        assert "SOURCE" in output

    @mock.patch("workday.AfterWorkdayTimetable")
    @mock.patch("airflow.models.dag._get_or_create_dagrun")
    def test_dag_test_with_custom_timetable(self, mock__get_or_create_dagrun, _):
        """
        when calling `dags test` on dag with custom timetable, the DagRun object should be created with
         data_intervals.
        """
        cli_args = self.parser.parse_args(
            ["dags", "test", "example_workday_timetable", DEFAULT_DATE.isoformat()]
        )
        dag_command.dag_test(cli_args)
        assert "data_interval" in mock__get_or_create_dagrun.call_args.kwargs

    def test_dag_test_no_triggerer(self, dag_maker):
        with dag_maker() as dag:

            @task
            def one():
                return 1

            @task
            def two(val):
                return val + 1

            class MyOp(BaseOperator):
                template_fields = ("tfield",)

                def __init__(self, tfield, **kwargs):
                    self.tfield = tfield
                    super().__init__(**kwargs)

                def execute(self, context, event=None):
                    if event is None:
                        print("I AM DEFERRING")
                        self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=20)), method_name="execute")
                        return
                    print("RESUMING")
                    return self.tfield + 1

            task_one = one()
            task_two = two(task_one)
            op = MyOp(task_id="abc", tfield=str(task_two))
            task_two >> op
        with pytest.raises(_StopDagTest, match="Task has deferred but triggerer component is not running"):
            dag.test()
