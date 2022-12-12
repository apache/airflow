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

import io
import json
import logging
import os
import re
import shutil
import tempfile
import unittest
from argparse import ArgumentParser
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from unittest import mock

import pendulum
import pytest
from parameterized import parameterized

from airflow import DAG
from airflow.cli import cli_parser
from airflow.cli.commands import task_command
from airflow.configuration import conf
from airflow.exceptions import AirflowException, DagRunNotFound
from airflow.models import DagBag, DagRun, Pool, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools, clear_db_runs

DEFAULT_DATE = timezone.datetime(2022, 1, 1)
ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


def reset(dag_id):
    with create_session() as session:
        tis = session.query(TaskInstance).filter_by(dag_id=dag_id)
        tis.delete()
        runs = session.query(DagRun).filter_by(dag_id=dag_id)
        runs.delete()


@contextmanager
def move_back(old_path, new_path):
    shutil.move(old_path, new_path)
    yield
    shutil.move(new_path, old_path)


# TODO: Check if tests needs side effects - locally there's missing DAG
class TestCliTasks:
    run_id = "TEST_RUN_ID"
    dag_id = "example_python_operator"
    parser: ArgumentParser
    dagbag: DagBag
    dag: DAG
    dag_run: DagRun

    @classmethod
    def setup_class(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()
        clear_db_runs()

        cls.dag = cls.dagbag.get_dag(cls.dag_id)
        cls.dagbag.sync_to_db()
        cls.dag_run = cls.dag.create_dagrun(
            state=State.NONE, run_id=cls.run_id, run_type=DagRunType.MANUAL, execution_date=DEFAULT_DATE
        )

    @classmethod
    def teardown_class(cls) -> None:
        clear_db_runs()

    @pytest.mark.execution_timeout(120)
    def test_cli_list_tasks(self):
        for dag_id in self.dagbag.dags:
            args = self.parser.parse_args(["tasks", "list", dag_id])
            task_command.task_list(args)

        args = self.parser.parse_args(["tasks", "list", "example_bash_operator", "--tree"])
        task_command.task_list(args)

    @pytest.mark.filterwarnings("ignore::airflow.utils.context.AirflowContextDeprecationWarning")
    def test_test(self):
        """Test the `airflow test` command"""
        args = self.parser.parse_args(
            ["tasks", "test", "example_python_operator", "print_the_context", "2018-01-01"]
        )

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(args)

        # Check that prints, and log messages, are shown
        assert "'example_python_operator__print_the_context__20180101'" in stdout.getvalue()

    @pytest.mark.filterwarnings("ignore::airflow.utils.context.AirflowContextDeprecationWarning")
    @mock.patch("airflow.utils.timezone.utcnow")
    def test_test_no_execution_date(self, mock_utcnow):
        """Test the `airflow test` command"""
        now = pendulum.now("UTC")
        mock_utcnow.return_value = now
        ds = now.strftime("%Y%m%d")
        args = self.parser.parse_args(["tasks", "test", "example_python_operator", "print_the_context"])

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(args)

        # Check that prints, and log messages, are shown
        assert f"'example_python_operator__print_the_context__{ds}'" in stdout.getvalue()

    @pytest.mark.filterwarnings("ignore::airflow.utils.context.AirflowContextDeprecationWarning")
    def test_test_with_existing_dag_run(self, caplog):
        """Test the `airflow test` command"""
        task_id = "print_the_context"
        args = self.parser.parse_args(["tasks", "test", self.dag_id, task_id, DEFAULT_DATE.isoformat()])
        with caplog.at_level("INFO", logger="airflow.task"):
            task_command.task_test(args)
        assert f"Marking task as SUCCESS. dag_id={self.dag_id}, task_id={task_id}" in caplog.text

    @pytest.mark.filterwarnings("ignore::airflow.utils.context.AirflowContextDeprecationWarning")
    def test_test_filters_secrets(self, capsys):
        """Test ``airflow test`` does not print secrets to stdout.

        Output should be filtered by SecretsMasker.
        """
        password = "somepassword1234!"
        logging.getLogger("airflow.task").filters[0].add_mask(password)
        args = self.parser.parse_args(
            ["tasks", "test", "example_python_operator", "print_the_context", "2018-01-01"],
        )

        with mock.patch("airflow.models.TaskInstance.run", new=lambda *_, **__: print(password)):
            task_command.task_test(args)
        assert capsys.readouterr().out.endswith("***\n")

        not_password = "!4321drowssapemos"
        with mock.patch("airflow.models.TaskInstance.run", new=lambda *_, **__: print(not_password)):
            task_command.task_test(args)
        assert capsys.readouterr().out.endswith(f"{not_password}\n")

    def test_cli_test_different_path(self, session):
        """
        When thedag processor has a different dags folder
        from the worker, ``airflow tasks run --local`` should still work.
        """
        repo_root = Path(__file__).parent.parent.parent.parent
        orig_file_path = repo_root / "tests/dags/test_dags_folder.py"
        orig_dags_folder = orig_file_path.parent

        # parse dag in original path
        with conf_vars({("core", "dags_folder"): orig_dags_folder.as_posix()}):
            dagbag = DagBag(include_examples=False)
            dag = dagbag.get_dag("test_dags_folder")
            dagbag.sync_to_db(session=session)

        dag.create_dagrun(
            state=State.NONE,
            run_id="abc123",
            run_type=DagRunType.MANUAL,
            execution_date=pendulum.now("UTC"),
            session=session,
        )
        session.commit()

        # now let's move the file
        # additionally let's update the dags folder to be the new path
        # ideally since dags_folder points correctly to the file, airflow
        # should be able to find the dag.
        with tempfile.TemporaryDirectory() as td:
            new_file_path = Path(td) / Path(orig_file_path).name
            new_dags_folder = new_file_path.parent
            with move_back(orig_file_path, new_file_path), conf_vars(
                {("core", "dags_folder"): new_dags_folder.as_posix()}
            ):
                ser_dag = (
                    session.query(SerializedDagModel)
                    .filter(SerializedDagModel.dag_id == "test_dags_folder")
                    .one()
                )
                # confirm that the serialized dag location has not been updated
                assert ser_dag.fileloc == orig_file_path.as_posix()
                assert ser_dag.data["dag"]["_processor_dags_folder"] == orig_dags_folder.as_posix()
                assert ser_dag.data["dag"]["fileloc"] == orig_file_path.as_posix()
                assert ser_dag.dag._processor_dags_folder == orig_dags_folder.as_posix()
                from airflow.settings import DAGS_FOLDER

                assert DAGS_FOLDER == new_dags_folder.as_posix() != orig_dags_folder.as_posix()
                task_command.task_run(
                    self.parser.parse_args(
                        [
                            "tasks",
                            "run",
                            "--ignore-all-dependencies",
                            "--local",
                            "test_dags_folder",
                            "task",
                            "abc123",
                        ]
                    )
                )
            ti = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.task_id == "task",
                    TaskInstance.dag_id == "test_dags_folder",
                    TaskInstance.run_id == "abc123",
                    TaskInstance.map_index == -1,
                )
                .one()
            )
            assert ti.state == "success"
            # verify that the file was in different location when run
            assert ti.xcom_pull(ti.task_id) == new_file_path.as_posix()

    @mock.patch("airflow.cli.commands.task_command.LocalTaskJob")
    def test_run_with_existing_dag_run_id(self, mock_local_job):
        """
        Test that we can run with existing dag_run_id
        """
        task0_id = self.dag.task_ids[0]
        args0 = [
            "tasks",
            "run",
            "--ignore-all-dependencies",
            "--local",
            self.dag_id,
            task0_id,
            self.run_id,
        ]

        task_command.task_run(self.parser.parse_args(args0), dag=self.dag)
        mock_local_job.assert_called_once_with(
            task_instance=mock.ANY,
            mark_success=False,
            ignore_all_deps=True,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            pickle_id=None,
            pool=None,
            external_executor_id=None,
        )

    @mock.patch("airflow.cli.commands.task_command.LocalTaskJob")
    def test_run_raises_when_theres_no_dagrun(self, mock_local_job):
        """
        Test that run raises when there's run_id but no dag_run
        """
        dag_id = "test_run_ignores_all_dependencies"
        dag = self.dagbag.get_dag(dag_id)
        task0_id = "test_run_dependent_task"
        run_id = "TEST_RUN_ID"
        args0 = [
            "tasks",
            "run",
            "--ignore-all-dependencies",
            "--local",
            dag_id,
            task0_id,
            run_id,
        ]
        with pytest.raises(DagRunNotFound):
            task_command.task_run(self.parser.parse_args(args0), dag=dag)

    def test_cli_test_with_params(self):
        task_command.task_test(
            self.parser.parse_args(
                [
                    "tasks",
                    "test",
                    "example_passing_params_via_test_command",
                    "run_this",
                    DEFAULT_DATE.isoformat(),
                    "--task-params",
                    '{"foo":"bar"}',
                ]
            )
        )
        task_command.task_test(
            self.parser.parse_args(
                [
                    "tasks",
                    "test",
                    "example_passing_params_via_test_command",
                    "also_run_this",
                    DEFAULT_DATE.isoformat(),
                    "--task-params",
                    '{"foo":"bar"}',
                ]
            )
        )

    def test_cli_test_with_env_vars(self):
        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(
                self.parser.parse_args(
                    [
                        "tasks",
                        "test",
                        "example_passing_params_via_test_command",
                        "env_var_test_task",
                        DEFAULT_DATE.isoformat(),
                        "--env-vars",
                        '{"foo":"bar"}',
                    ]
                )
            )
        output = stdout.getvalue()
        assert "foo=bar" in output
        assert "AIRFLOW_TEST_MODE=True" in output

    @parameterized.expand(
        [
            ("--ignore-all-dependencies",),
            ("--ignore-depends-on-past",),
            ("--ignore-dependencies",),
            ("--force",),
        ],
    )
    def test_cli_run_invalid_raw_option(self, option: str):
        with pytest.raises(
            AirflowException,
            match="Option --raw does not work with some of the other options on this command.",
        ):
            task_command.task_run(
                self.parser.parse_args(
                    [  # type: ignore
                        "tasks",
                        "run",
                        "example_bash_operator",
                        "runme_0",
                        DEFAULT_DATE.isoformat(),
                        "--raw",
                        option,
                    ]
                )
            )

    def test_cli_run_mutually_exclusive(self):
        with pytest.raises(AirflowException, match="Option --raw and --local are mutually exclusive."):
            task_command.task_run(
                self.parser.parse_args(
                    [
                        "tasks",
                        "run",
                        "example_bash_operator",
                        "runme_0",
                        DEFAULT_DATE.isoformat(),
                        "--raw",
                        "--local",
                    ]
                )
            )

    def test_task_render(self):
        """
        tasks render should render and displays templated fields for a given task
        """
        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_render(
                self.parser.parse_args(["tasks", "render", "tutorial", "templated", "2016-01-01"])
            )

        output = stdout.getvalue()

        assert 'echo "2016-01-01"' in output
        assert 'echo "2016-01-08"' in output

    def test_cli_run_when_pickle_and_dag_cli_method_selected(self):
        """
        tasks run should return an AirflowException when invalid pickle_id is passed
        """
        pickle_id = "pickle_id"

        with pytest.raises(
            AirflowException,
            match=re.escape("You cannot use the --pickle option when using DAG.cli() method."),
        ):
            task_command.task_run(
                self.parser.parse_args(
                    [
                        "tasks",
                        "run",
                        "example_bash_operator",
                        "runme_0",
                        DEFAULT_DATE.isoformat(),
                        "--pickle",
                        pickle_id,
                    ]
                ),
                self.dag,
            )

    def test_task_state(self):
        task_command.task_state(
            self.parser.parse_args(
                ["tasks", "state", self.dag_id, "print_the_context", DEFAULT_DATE.isoformat()]
            )
        )

    def test_task_states_for_dag_run(self):

        dag2 = DagBag().dags["example_python_operator"]
        task2 = dag2.get_task(task_id="print_the_context")
        default_date2 = timezone.datetime(2016, 1, 9)
        dag2.clear()
        dagrun = dag2.create_dagrun(
            state=State.RUNNING,
            execution_date=default_date2,
            run_type=DagRunType.MANUAL,
            external_trigger=True,
        )
        ti2 = TaskInstance(task2, dagrun.execution_date)
        ti2.set_state(State.SUCCESS)
        ti_start = ti2.start_date
        ti_end = ti2.end_date

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_states_for_dag_run(
                self.parser.parse_args(
                    [
                        "tasks",
                        "states-for-dag-run",
                        "example_python_operator",
                        default_date2.isoformat(),
                        "--output",
                        "json",
                    ]
                )
            )
        actual_out = json.loads(stdout.getvalue())

        assert len(actual_out) == 1
        assert actual_out[0] == {
            "dag_id": "example_python_operator",
            "execution_date": "2016-01-09T00:00:00+00:00",
            "task_id": "print_the_context",
            "state": "success",
            "start_date": ti_start.isoformat(),
            "end_date": ti_end.isoformat(),
        }

    def test_task_states_for_dag_run_when_dag_run_not_exists(self):
        """
        task_states_for_dag_run should return an AirflowException when invalid dag id is passed
        """
        with pytest.raises(DagRunNotFound):
            default_date2 = timezone.datetime(2016, 1, 9)
            task_command.task_states_for_dag_run(
                self.parser.parse_args(
                    [
                        "tasks",
                        "states-for-dag-run",
                        "not_exists_dag",
                        default_date2.isoformat(),
                        "--output",
                        "json",
                    ]
                )
            )

    def test_subdag_clear(self):
        args = self.parser.parse_args(["tasks", "clear", "example_subdag_operator", "--yes"])
        task_command.task_clear(args)
        args = self.parser.parse_args(
            ["tasks", "clear", "example_subdag_operator", "--yes", "--exclude-subdags"]
        )
        task_command.task_clear(args)

    def test_parentdag_downstream_clear(self):
        args = self.parser.parse_args(["tasks", "clear", "example_subdag_operator.section-1", "--yes"])
        task_command.task_clear(args)
        args = self.parser.parse_args(
            ["tasks", "clear", "example_subdag_operator.section-1", "--yes", "--exclude-parentdag"]
        )
        task_command.task_clear(args)


class TestLogsfromTaskRunCommand:
    def setup_method(self) -> None:
        self.dag_id = "test_logging_dag"
        self.task_id = "test_task"
        self.run_id = "test_run"
        self.dag_path = os.path.join(ROOT_FOLDER, "dags", "test_logging_in_dag.py")
        reset(self.dag_id)
        self.execution_date = timezone.datetime(2017, 1, 1)
        self.execution_date_str = self.execution_date.isoformat()
        self.task_args = ["tasks", "run", self.dag_id, self.task_id, "--local", self.execution_date_str]
        self.log_dir = conf.get_mandatory_value("logging", "base_log_folder")
        self.log_filename = f"dag_id={self.dag_id}/run_id={self.run_id}/task_id={self.task_id}/attempt=1.log"
        self.ti_log_file_path = os.path.join(self.log_dir, self.log_filename)
        self.parser = cli_parser.get_parser()

        DagBag().get_dag(self.dag_id).create_dagrun(
            run_id=self.run_id,
            execution_date=self.execution_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            run_type=DagRunType.MANUAL,
        )

        root = self.root_logger = logging.getLogger()
        self.root_handlers = root.handlers.copy()
        self.root_filters = root.filters.copy()
        self.root_level = root.level

        try:
            os.remove(self.ti_log_file_path)
        except OSError:
            pass

    def teardown_method(self) -> None:
        root = self.root_logger
        root.setLevel(self.root_level)
        root.handlers[:] = self.root_handlers
        root.filters[:] = self.root_filters

        reset(self.dag_id)
        try:
            os.remove(self.ti_log_file_path)
        except OSError:
            pass

    def assert_log_line(self, text, logs_list, expect_from_logging_mixin=False):
        """
        Get Log Line and assert only 1 Entry exists with the given text. Also check that
        "logging_mixin" line does not appear in that log line to avoid duplicate logging as below:

        [2020-06-24 16:47:23,537] {logging_mixin.py:91} INFO - [2020-06-24 16:47:23,536] {python.py:135}
        """
        log_lines = [log for log in logs_list if text in log]
        assert len(log_lines) == 1
        log_line = log_lines[0]
        if not expect_from_logging_mixin:
            # Logs from print statement still show with logging_mixing as filename
            # Example: [2020-06-24 17:07:00,482] {logging_mixin.py:91} INFO - Log from Print statement
            assert "logging_mixin.py" not in log_line
        return log_line

    @mock.patch("airflow.cli.commands.task_command.LocalTaskJob")
    def test_external_executor_id_present_for_fork_run_task(self, mock_local_job):
        args = self.parser.parse_args(self.task_args)
        args.external_executor_id = "ABCD12345"

        task_command.task_run(args)
        mock_local_job.assert_called_once_with(
            task_instance=mock.ANY,
            mark_success=False,
            pickle_id=None,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            pool=None,
            external_executor_id="ABCD12345",
        )

    @mock.patch("airflow.cli.commands.task_command.LocalTaskJob")
    def test_external_executor_id_present_for_process_run_task(self, mock_local_job):
        args = self.parser.parse_args(self.task_args)
        args.external_executor_id = "ABCD12345"

        with mock.patch.dict(os.environ, {"external_executor_id": "12345FEDCBA"}):
            task_command.task_run(args)
            mock_local_job.assert_called_once_with(
                task_instance=mock.ANY,
                mark_success=False,
                pickle_id=None,
                ignore_all_deps=False,
                ignore_depends_on_past=False,
                ignore_task_deps=False,
                ignore_ti_state=False,
                pool=None,
                external_executor_id="ABCD12345",
            )

    @unittest.skipIf(not hasattr(os, "fork"), "Forking not available")
    def test_logging_with_run_task(self):
        with conf_vars({("core", "dags_folder"): self.dag_path}):
            task_command.task_run(self.parser.parse_args(self.task_args))

        with open(self.ti_log_file_path) as l_file:
            logs = l_file.read()

        print(logs)  # In case of a test failures this line would show detailed log
        logs_list = logs.splitlines()

        assert "INFO - Started process" in logs
        assert f"Subtask {self.task_id}" in logs
        assert "standard_task_runner.py" in logs
        assert (
            f"INFO - Running: ['airflow', 'tasks', 'run', '{self.dag_id}', "
            f"'{self.task_id}', '{self.run_id}'," in logs
        )

        self.assert_log_line("Log from DAG Logger", logs_list)
        self.assert_log_line("Log from TI Logger", logs_list)
        self.assert_log_line("Log from Print statement", logs_list, expect_from_logging_mixin=True)

        assert (
            f"INFO - Marking task as SUCCESS. dag_id={self.dag_id}, "
            f"task_id={self.task_id}, execution_date=20170101T000000" in logs
        )

    @unittest.skipIf(not hasattr(os, "fork"), "Forking not available")
    def test_run_task_with_pool(self):
        pool_name = "test_pool_run"

        clear_db_pools()
        with create_session() as session:
            pool = Pool(pool=pool_name, slots=1)
            session.add(pool)
            session.commit()

            assert session.query(TaskInstance).filter_by(pool=pool_name).first() is None
            task_command.task_run(self.parser.parse_args(self.task_args + ["--pool", pool_name]))
            assert session.query(TaskInstance).filter_by(pool=pool_name).first() is not None

            session.delete(pool)
            session.commit()

    @mock.patch("airflow.task.task_runner.standard_task_runner.CAN_FORK", False)
    def test_logging_with_run_task_subprocess(self):
        with conf_vars({("core", "dags_folder"): self.dag_path}):
            task_command.task_run(self.parser.parse_args(self.task_args))

        with open(self.ti_log_file_path) as l_file:
            logs = l_file.read()

        print(logs)  # In case of a test failures this line would show detailed log
        logs_list = logs.splitlines()

        assert f"Subtask {self.task_id}" in logs
        assert "base_task_runner.py" in logs
        self.assert_log_line("Log from DAG Logger", logs_list)
        self.assert_log_line("Log from TI Logger", logs_list)
        self.assert_log_line("Log from Print statement", logs_list, expect_from_logging_mixin=True)

        assert f"INFO - Running: ['airflow', 'tasks', 'run', '{self.dag_id}', '{self.task_id}'," in logs
        assert (
            f"INFO - Marking task as SUCCESS. dag_id={self.dag_id}, "
            f"task_id={self.task_id}, execution_date=20170101T000000" in logs
        )

    def test_log_file_template_with_run_task(self):
        """Verify that the taskinstance has the right context for log_filename_template"""

        with conf_vars({("core", "dags_folder"): self.dag_path}):
            # increment the try_number of the task to be run
            with create_session() as session:
                ti = session.query(TaskInstance).filter_by(run_id=self.run_id).first()
                ti.try_number = 1

            log_file_path = os.path.join(os.path.dirname(self.ti_log_file_path), "attempt=2.log")

            try:
                task_command.task_run(self.parser.parse_args(self.task_args))

                assert os.path.exists(log_file_path)
            finally:
                try:
                    os.remove(log_file_path)
                except OSError:
                    pass

    @mock.patch.object(task_command, "_run_task_by_selected_method")
    def test_root_logger_restored(self, run_task_mock, caplog):
        """Verify that the root logging context is restored"""

        logger = logging.getLogger("foo.bar")

        def task_inner(*args, **kwargs):
            logger.warning("redirected log message")

        run_task_mock.side_effect = task_inner

        config = {
            ("core", "dags_folder"): self.dag_path,
            ("logging", "logging_level"): "INFO",
        }

        with caplog.at_level(level=logging.WARNING):
            with conf_vars(config):
                logger.warning("not redirected")
                task_command.task_run(self.parser.parse_args(self.task_args))
                assert "not redirected" in caplog.text
                assert self.root_logger.level == logging.WARNING

        assert self.root_logger.handlers == self.root_handlers

    @mock.patch.object(task_command, "_run_task_by_selected_method")
    @pytest.mark.parametrize("do_not_modify_handler", [True, False])
    def test_disable_handler_modifying(self, run_task_mock, caplog, do_not_modify_handler):
        """If [core] donot_modify_handlers is set to True, the root logger is untouched"""
        from airflow import settings

        logger = logging.getLogger("foo.bar")

        def task_inner(*args, **kwargs):
            logger.warning("not redirected")

        run_task_mock.side_effect = task_inner

        config = {
            ("core", "dags_folder"): self.dag_path,
            ("logging", "logging_level"): "INFO",
        }
        with caplog.at_level(logging.WARNING, logger="foo.bar"):
            with conf_vars(config):
                old_value = settings.DONOT_MODIFY_HANDLERS
                settings.DONOT_MODIFY_HANDLERS = do_not_modify_handler
                try:
                    task_command.task_run(self.parser.parse_args(self.task_args))
                    if do_not_modify_handler:
                        assert "not redirected" in caplog.text
                    else:
                        assert "not redirected" not in caplog.text
                finally:
                    settings.DONOT_MODIFY_HANDLERS = old_value


def test_context_with_run():
    dag_id = "test_parsing_context"
    task_id = "task1"
    run_id = "test_run"
    dag_path = os.path.join(ROOT_FOLDER, "dags", "test_parsing_context.py")
    reset(dag_id)
    execution_date = timezone.datetime(2017, 1, 1)
    execution_date_str = execution_date.isoformat()
    task_args = ["tasks", "run", dag_id, task_id, "--local", execution_date_str]
    parser = cli_parser.get_parser()

    DagBag().get_dag(dag_id).create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        start_date=timezone.utcnow(),
        state=State.RUNNING,
        run_type=DagRunType.MANUAL,
    )
    with conf_vars({("core", "dags_folder"): dag_path}):
        task_command.task_run(parser.parse_args(task_args))

    context_file = Path("/tmp/airflow_parsing_context")
    text = context_file.read_text()
    assert (
        text == "_AIRFLOW_PARSING_CONTEXT_DAG_ID=test_parsing_context\n"
        "_AIRFLOW_PARSING_CONTEXT_TASK_ID=task1\n"
    )
