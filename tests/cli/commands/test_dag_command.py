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
import io
import os
import tempfile
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock

import pendulum
import pytest

from airflow import settings
from airflow.cli import cli_parser
from airflow.cli.commands import dag_command
from airflow.exceptions import AirflowException
from airflow.models import DagBag, DagModel, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.models import TEST_DAGS_FOLDER
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1), timezone=timezone.utc)

# TODO: Check if tests needs side effects - locally there's missing DAG


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

        with contextlib.redirect_stdout(io.StringIO()) as stdout:
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

        with contextlib.redirect_stdout(io.StringIO()) as stdout:
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
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_dependencies_show(self.parser.parse_args(["dags", "show-dependencies"]))
        out = temp_stdout.getvalue()
        assert "digraph" in out
        assert "graph [rankdir=LR]" in out

    @mock.patch("airflow.cli.commands.dag_command.render_dag_dependencies")
    def test_show_dag_dependencies_save(self, mock_render_dag_dependencies):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_dependencies_show(
                self.parser.parse_args(["dags", "show-dependencies", "--save", "output.png"])
            )
        out = temp_stdout.getvalue()
        mock_render_dag_dependencies.return_value.render.assert_called_once_with(
            cleanup=True, filename="output", format="png"
        )
        assert "File output.png saved" in out

    def test_show_dag_print(self):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_show(self.parser.parse_args(["dags", "show", "example_bash_operator"]))
        out = temp_stdout.getvalue()
        assert "label=example_bash_operator" in out
        assert "graph [label=example_bash_operator labelloc=t rankdir=LR]" in out
        assert "runme_2 -> run_after_loop" in out

    @mock.patch("airflow.cli.commands.dag_command.render_dag")
    def test_show_dag_save(self, mock_render_dag):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
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
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
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

    def test_next_execution(self):
        dag_ids = [
            "example_bash_operator",  # schedule='0 0 * * *'
            "latest_only",  # schedule=timedelta(hours=4)
            "example_python_operator",  # schedule=None
            "example_xcom",  # schedule="@once"
        ]

        # Delete DagRuns
        with create_session() as session:
            dr = session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids))
            dr.delete(synchronize_session=False)

        # Test None output
        args = self.parser.parse_args(["dags", "next-execution", dag_ids[0]])
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_next_execution(args)
            out = temp_stdout.getvalue()
            # `next_execution` function is inapplicable if no execution record found
            # It prints `None` in such cases
            assert "None" in out

        # The details below is determined by the schedule_interval of example DAGs
        now = DEFAULT_DATE
        expected_output = [
            (now + timedelta(days=1)).isoformat(),
            (now + timedelta(hours=4)).isoformat(),
            "None",
            "None",
        ]
        expected_output_2 = [
            (now + timedelta(days=1)).isoformat() + os.linesep + (now + timedelta(days=2)).isoformat(),
            (now + timedelta(hours=4)).isoformat() + os.linesep + (now + timedelta(hours=8)).isoformat(),
            "None",
            "None",
        ]

        for i, dag_id in enumerate(dag_ids):
            dag = self.dagbag.dags[dag_id]
            # Create a DagRun for each DAG, to prepare for next step
            dag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=now,
                start_date=now,
                state=State.FAILED,
            )

            # Test num-executions = 1 (default)
            args = self.parser.parse_args(["dags", "next-execution", dag_id])
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                dag_command.dag_next_execution(args)
                out = temp_stdout.getvalue()
            assert expected_output[i] in out

            # Test num-executions = 2
            args = self.parser.parse_args(["dags", "next-execution", dag_id, "--num-executions", "2"])
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                dag_command.dag_next_execution(args)
                out = temp_stdout.getvalue()
            assert expected_output_2[i] in out

        # Clean up before leaving
        with create_session() as session:
            dr = session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids))
            dr.delete(synchronize_session=False)

    @conf_vars({("core", "load_examples"): "true"})
    def test_cli_report(self):
        args = self.parser.parse_args(["dags", "report", "--output", "json"])
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_report(args)
            out = temp_stdout.getvalue()

        assert "airflow/example_dags/example_complex.py" in out
        assert "example_complex" in out

    @conf_vars({("core", "load_examples"): "true"})
    def test_cli_list_dags(self):
        args = self.parser.parse_args(["dags", "list", "--output", "yaml"])
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
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
        with contextlib.redirect_stderr(io.StringIO()) as temp_stderr:
            dag_command.dag_list_dags(args)
            out = temp_stderr.getvalue()
        assert "Failed to load all files." in out

    @conf_vars({("core", "load_examples"): "false"})
    def test_cli_list_import_errors(self):
        dag_path = os.path.join(TEST_DAGS_FOLDER, "test_invalid_cron.py")
        args = self.parser.parse_args(["dags", "list", "--output", "yaml", "--subdir", dag_path])
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_list_import_errors(args)
            out = temp_stdout.getvalue()
        assert "Invalid timetable expression" in out
        assert dag_path in out

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

    def test_delete_dag_existing_file(self):
        # Test to check that the DAG should be deleted even if
        # the file containing it is not deleted
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        with tempfile.NamedTemporaryFile() as f:
            session.add(DM(dag_id=key, fileloc=f.name))
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
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
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
