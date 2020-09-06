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
#
import io
import os
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from unittest import mock

import pytest
from parameterized import parameterized
from tabulate import tabulate

from airflow.cli import cli_parser
from airflow.cli.commands import task_command
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DagBag, TaskInstance
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.cli import get_dag
from airflow.utils.state import State
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools, clear_db_runs

DEFAULT_DATE = timezone.make_aware(datetime(2016, 1, 1))
ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


def reset(dag_id):
    session = Session()
    tis = session.query(TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()


class TestCliTasks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()

    def test_cli_list_tasks(self):
        for dag_id in self.dagbag.dags:
            args = self.parser.parse_args(['tasks', 'list', dag_id])
            task_command.task_list(args)

        args = self.parser.parse_args([
            'tasks', 'list', 'example_bash_operator', '--tree'])
        task_command.task_list(args)

    def test_test(self):
        """Test the `airflow test` command"""
        args = self.parser.parse_args([
            "tasks", "test", "example_python_operator", 'print_the_context', '2018-01-01'
        ])

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(args)
        # Check that prints, and log messages, are shown
        self.assertIn("'example_python_operator__print_the_context__20180101'", stdout.getvalue())

    @mock.patch("airflow.cli.commands.task_command.LocalTaskJob")
    def test_run_naive_taskinstance(self, mock_local_job):
        """
        Test that we can run naive (non-localized) task instances
        """
        naive_date = datetime(2016, 1, 1)
        dag_id = 'test_run_ignores_all_dependencies'

        dag = self.dagbag.get_dag('test_run_ignores_all_dependencies')

        task0_id = 'test_run_dependent_task'
        args0 = ['tasks',
                 'run',
                 '--ignore-all-dependencies',
                 '--local',
                 dag_id,
                 task0_id,
                 naive_date.isoformat()]

        task_command.task_run(self.parser.parse_args(args0), dag=dag)
        mock_local_job.assert_called_once_with(
            task_instance=mock.ANY,
            mark_success=False,
            ignore_all_deps=True,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            pickle_id=None,
            pool=None,
        )

    def test_cli_test(self):
        task_command.task_test(self.parser.parse_args([
            'tasks', 'test', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))
        task_command.task_test(self.parser.parse_args([
            'tasks', 'test', 'example_bash_operator', 'runme_0', '--dry-run',
            DEFAULT_DATE.isoformat()]))

    def test_cli_test_with_params(self):
        task_command.task_test(self.parser.parse_args([
            'tasks', 'test', 'example_passing_params_via_test_command', 'run_this',
            '--task-params', '{"foo":"bar"}', DEFAULT_DATE.isoformat()]))
        task_command.task_test(self.parser.parse_args([
            'tasks', 'test', 'example_passing_params_via_test_command', 'also_run_this',
            '--task-params', '{"foo":"bar"}', DEFAULT_DATE.isoformat()]))

    def test_cli_test_with_env_vars(self):
        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(self.parser.parse_args([
                'tasks', 'test', 'example_passing_params_via_test_command', 'env_var_test_task',
                '--env-vars', '{"foo":"bar"}', DEFAULT_DATE.isoformat()]))
        output = stdout.getvalue()
        self.assertIn('foo=bar', output)
        self.assertIn('AIRFLOW_TEST_MODE=True', output)

    def test_cli_run(self):
        task_command.task_run(self.parser.parse_args([
            'tasks', 'run', 'example_bash_operator', 'runme_0', '--local',
            DEFAULT_DATE.isoformat()]))

    @parameterized.expand(
        [
            ("--ignore-all-dependencies", ),
            ("--ignore-depends-on-past", ),
            ("--ignore-dependencies",),
            ("--force",),
        ],

    )
    def test_cli_run_invalid_raw_option(self, option: str):
        with self.assertRaisesRegex(
            AirflowException,
            "Option --raw does not work with some of the other options on this command."
        ):
            task_command.task_run(self.parser.parse_args([  # type: ignore
                'tasks', 'run', 'example_bash_operator', 'runme_0', DEFAULT_DATE.isoformat(), '--raw', option
            ]))

    def test_cli_run_mutually_exclusive(self):
        with self.assertRaisesRegex(
            AirflowException,
            "Option --raw and --local are mutually exclusive."
        ):
            task_command.task_run(self.parser.parse_args([
                'tasks', 'run', 'example_bash_operator', 'runme_0', DEFAULT_DATE.isoformat(), '--raw',
                '--local'
            ]))

    def test_task_state(self):
        task_command.task_state(self.parser.parse_args([
            'tasks', 'state', 'example_bash_operator', 'runme_0',
            DEFAULT_DATE.isoformat()]))

    def test_task_states_for_dag_run(self):

        dag2 = DagBag().dags['example_python_operator']
        task2 = dag2.get_task(task_id='print_the_context')
        defaut_date2 = timezone.make_aware(datetime(2016, 1, 9))
        dag2.clear()

        ti2 = TaskInstance(task2, defaut_date2)

        ti2.set_state(State.SUCCESS)
        ti_start = ti2.start_date
        ti_end = ti2.end_date

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_states_for_dag_run(self.parser.parse_args([
                'tasks', 'states-for-dag-run', 'example_python_operator', defaut_date2.isoformat()]))
        actual_out = stdout.getvalue()

        formatted_rows = [('example_python_operator',
                           '2016-01-09 00:00:00+00:00',
                           'print_the_context',
                           'success',
                           ti_start,
                           ti_end)]

        expected = tabulate(formatted_rows,
                            ['dag',
                             'exec_date',
                             'task',
                             'state',
                             'start_date',
                             'end_date'],
                            tablefmt="plain")

        # Check that prints, and log messages, are shown
        self.assertIn(expected.replace("\n", ""), actual_out.replace("\n", ""))

    def test_subdag_clear(self):
        args = self.parser.parse_args([
            'tasks', 'clear', 'example_subdag_operator', '--yes'])
        task_command.task_clear(args)
        args = self.parser.parse_args([
            'tasks', 'clear', 'example_subdag_operator', '--yes', '--exclude-subdags'])
        task_command.task_clear(args)

    def test_parentdag_downstream_clear(self):
        args = self.parser.parse_args([
            'tasks', 'clear', 'example_subdag_operator.section-1', '--yes'])
        task_command.task_clear(args)
        args = self.parser.parse_args([
            'tasks', 'clear', 'example_subdag_operator.section-1', '--yes',
            '--exclude-parentdag'])
        task_command.task_clear(args)

    @pytest.mark.quarantined
    def test_local_run(self):
        args = self.parser.parse_args([
            'tasks',
            'run',
            'example_python_operator',
            'print_the_context',
            '2018-04-27T08:39:51.298439+00:00',
            '--interactive',
            '--subdir',
            '/root/dags/example_python_operator.py'
        ])

        dag = get_dag(args.subdir, args.dag_id)
        reset(dag.dag_id)

        task_command.task_run(args)
        task = dag.get_task(task_id=args.task_id)
        ti = TaskInstance(task, args.execution_date)
        ti.refresh_from_db()
        state = ti.current_state()
        self.assertEqual(state, State.SUCCESS)


class TestLogsfromTaskRunCommand(unittest.TestCase):

    def setUp(self) -> None:
        self.dag_id = "test_logging_dag"
        self.task_id = "test_task"
        reset(self.dag_id)
        self.execution_date_str = timezone.make_aware(datetime(2017, 1, 1)).isoformat()
        self.log_dir = conf.get('logging', 'base_log_folder')
        self.log_filename = f"{self.dag_id}/{self.task_id}/{self.execution_date_str}/1.log"
        self.ti_log_file_path = os.path.join(self.log_dir, self.log_filename)
        self.parser = cli_parser.get_parser()
        try:
            os.remove(self.ti_log_file_path)
        except OSError:
            pass

    def tearDown(self) -> None:
        reset(self.dag_id)
        try:
            os.remove(self.ti_log_file_path)
        except OSError:
            pass

    def assert_log_line(self, text, logs_list, expect_from_logging_mixin=False):
        """
        Get Log Line and assert only 1 Entry exists with the given text. Also check that
        "logging_mixin" line does not appear in that log line to avoid duplicate loggigng as below:

        [2020-06-24 16:47:23,537] {logging_mixin.py:91} INFO - [2020-06-24 16:47:23,536] {python.py:135}
        """
        log_lines = [log for log in logs_list if text in log]
        self.assertEqual(len(log_lines), 1)
        log_line = log_lines[0]
        if not expect_from_logging_mixin:
            # Logs from print statement still show with logging_mixing as filename
            # Example: [2020-06-24 17:07:00,482] {logging_mixin.py:91} INFO - Log from Print statement
            self.assertNotIn("logging_mixin.py", log_line)
        return log_line

    @unittest.skipIf(not hasattr(os, 'fork'), "Forking not available")
    def test_logging_with_run_task(self):
        #  We are not using self.assertLogs as we want to verify what actually is stored in the Log file
        # as that is what gets displayed

        with conf_vars({('core', 'dags_folder'): os.path.join(ROOT_FOLDER, f"tests/dags/{self.dag_id}")}):
            task_command.task_run(self.parser.parse_args([
                'tasks', 'run', self.dag_id, self.task_id, '--local', self.execution_date_str]))

        with open(self.ti_log_file_path) as l_file:
            logs = l_file.read()

        print(logs)     # In case of a test failures this line would show detailed log
        logs_list = logs.splitlines()

        self.assertIn("INFO - Started process", logs)
        self.assertIn(f"Subtask {self.task_id}", logs)
        self.assertIn("standard_task_runner.py", logs)
        self.assertIn(f"INFO - Running: ['airflow', 'tasks', 'run', '{self.dag_id}', "
                      f"'{self.task_id}', '{self.execution_date_str}',", logs)

        self.assert_log_line("Log from DAG Logger", logs_list)
        self.assert_log_line("Log from TI Logger", logs_list)
        self.assert_log_line("Log from Print statement", logs_list, expect_from_logging_mixin=True)

        self.assertIn(f"INFO - Marking task as SUCCESS.dag_id={self.dag_id}, "
                      f"task_id={self.task_id}, execution_date=20170101T000000", logs)

    @mock.patch("airflow.task.task_runner.standard_task_runner.CAN_FORK", False)
    def test_logging_with_run_task_subprocess(self):
        # We are not using self.assertLogs as we want to verify what actually is stored in the Log file
        # as that is what gets displayed
        with conf_vars({('core', 'dags_folder'): os.path.join(ROOT_FOLDER, f"tests/dags/{self.dag_id}")}):
            task_command.task_run(self.parser.parse_args([
                'tasks', 'run', self.dag_id, self.task_id, '--local', self.execution_date_str]))

        with open(self.ti_log_file_path) as l_file:
            logs = l_file.read()

        print(logs)     # In case of a test failures this line would show detailed log
        logs_list = logs.splitlines()

        self.assertIn(f"Subtask {self.task_id}", logs)
        self.assertIn("base_task_runner.py", logs)
        self.assert_log_line("Log from DAG Logger", logs_list)
        self.assert_log_line("Log from TI Logger", logs_list)
        self.assert_log_line("Log from Print statement", logs_list, expect_from_logging_mixin=True)

        self.assertIn(f"INFO - Running: ['airflow', 'tasks', 'run', '{self.dag_id}', "
                      f"'{self.task_id}', '{self.execution_date_str}',", logs)
        self.assertIn(f"INFO - Marking task as SUCCESS.dag_id={self.dag_id}, "
                      f"task_id={self.task_id}, execution_date=20170101T000000", logs)


class TestCliTaskBackfill(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)

    def setUp(self):
        clear_db_runs()
        clear_db_pools()

        self.parser = cli_parser.get_parser()

    @pytest.mark.quarantined
    def test_run_ignores_all_dependencies(self):
        """
        Test that run respects ignore_all_dependencies
        """
        dag_id = 'test_run_ignores_all_dependencies'

        dag = self.dagbag.get_dag('test_run_ignores_all_dependencies')
        dag.clear()

        task0_id = 'test_run_dependent_task'
        args0 = ['tasks',
                 'run',
                 '--ignore-all-dependencies',
                 dag_id,
                 task0_id,
                 DEFAULT_DATE.isoformat()]
        task_command.task_run(self.parser.parse_args(args0))
        ti_dependent0 = TaskInstance(
            task=dag.get_task(task0_id),
            execution_date=DEFAULT_DATE)

        ti_dependent0.refresh_from_db()
        self.assertEqual(ti_dependent0.state, State.FAILED)

        task1_id = 'test_run_dependency_task'
        args1 = ['tasks',
                 'run',
                 '--ignore-all-dependencies',
                 dag_id,
                 task1_id,
                 (DEFAULT_DATE + timedelta(days=1)).isoformat()]
        task_command.task_run(self.parser.parse_args(args1))

        ti_dependency = TaskInstance(
            task=dag.get_task(task1_id),
            execution_date=DEFAULT_DATE + timedelta(days=1))
        ti_dependency.refresh_from_db()
        self.assertEqual(ti_dependency.state, State.FAILED)

        task2_id = 'test_run_dependent_task'
        args2 = ['tasks',
                 'run',
                 '--ignore-all-dependencies',
                 dag_id,
                 task2_id,
                 (DEFAULT_DATE + timedelta(days=1)).isoformat()]
        task_command.task_run(self.parser.parse_args(args2))

        ti_dependent = TaskInstance(
            task=dag.get_task(task2_id),
            execution_date=DEFAULT_DATE + timedelta(days=1))
        ti_dependent.refresh_from_db()
        self.assertEqual(ti_dependent.state, State.SUCCESS)
