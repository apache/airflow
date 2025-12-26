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
import json
import logging
import os
import shutil
from argparse import ArgumentParser
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from sqlalchemy import delete

from airflow._shared.timezones import timezone
from airflow.cli import cli_parser
from airflow.cli.commands import task_command
from airflow.configuration import conf
from airflow.dag_processing.dagbag import DagBag
from airflow.exceptions import DagRunNotFound
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.bash import BashOperator
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_runs, parse_and_sync_to_db

if TYPE_CHECKING:
    from airflow.models.dag import DAG

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2022, 1, 1)
ROOT_FOLDER = Path(__file__).parents[4].resolve()


def reset(dag_id):
    with create_session() as session:
        session.execute(delete(TaskInstance).where(TaskInstance.dag_id == dag_id))
        session.execute(delete(DagRun).where(DagRun.dag_id == dag_id))
        session.execute(delete(DagModel).where(DagModel.dag_id == dag_id))
        session.execute(delete(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id))


@contextmanager
def move_back(old_path, new_path):
    shutil.move(old_path, new_path)
    yield
    shutil.move(new_path, old_path)


class TestCliTasks:
    run_id = "TEST_RUN_ID"
    dag_id = "example_python_operator"
    parser: ArgumentParser
    dagbag: DBDagBag
    dag: DAG
    dag_run: DagRun

    @classmethod
    def setup_class(cls):
        parse_and_sync_to_db(os.devnull, include_examples=True)
        cls.parser = cli_parser.get_parser()
        clear_db_runs()

        cls.dagbag = DBDagBag()
        with create_session() as session:
            cls.dag = cls.dagbag.get_latest_version_of_dag(cls.dag_id, session=session)
        data_interval = cls.dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE)
        cls.dag_run = cls.dag.create_dagrun(
            state=State.RUNNING,
            run_id=cls.run_id,
            run_type=DagRunType.MANUAL,
            logical_date=DEFAULT_DATE,
            data_interval=data_interval,
            run_after=DEFAULT_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )

    @classmethod
    def teardown_class(cls) -> None:
        clear_db_runs()

    @conf_vars({("core", "load_examples"): "true"})
    @pytest.mark.execution_timeout(120)
    def test_cli_list_tasks(self, session):
        for dag in self.dagbag.iter_all_latest_version_dags(session=session):
            args = self.parser.parse_args(["tasks", "list", dag.dag_id])
            task_command.task_list(args)

    def test_test(self):
        """Test the `airflow test` command"""
        args = self.parser.parse_args(
            ["tasks", "test", "example_python_operator", "print_the_context", "2018-01-01"]
        )

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(args)

        # Check that prints, and log messages, are shown
        assert "'example_python_operator__print_the_context__20180101'" in stdout.getvalue()

    def test_test_no_logical_date(self):
        """Test the `airflow test` command"""
        args = self.parser.parse_args(["tasks", "test", "example_python_operator", "print_the_context"])

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_test(args)

        # Check that prints, and log messages, are shown
        assert "example_python_operator" in stdout.getvalue()
        assert "print_the_context" in stdout.getvalue()

    @mock.patch("airflow.cli.commands.task_command.fetch_dag_run_from_run_id_or_logical_date_string")
    def test_task_render_with_custom_timetable(self, mock_fetch_dag_run_from_run_id_or_logical_date_string):
        """
        Test that the `tasks render` CLI command queries the database correctly
        for a DAG with a custom timetable. Verifies that a query is executed to
        fetch the appropriate DagRun and that the database interaction occurs as expected.
        """
        mock_fetch_dag_run_from_run_id_or_logical_date_string.return_value = (None, None)

        task_command.task_render(
            self.parser.parse_args(["tasks", "render", "example_workday_timetable", "run_this", "2022-01-01"])
        )

        mock_fetch_dag_run_from_run_id_or_logical_date_string.assert_called_once()

    def test_test_with_existing_dag_run(self, caplog):
        """Test the `airflow test` command"""
        task_id = "print_the_context"
        args = self.parser.parse_args(["tasks", "test", self.dag_id, task_id, DEFAULT_DATE.isoformat()])
        with caplog.at_level("INFO", logger="airflow.task"):
            task_command.task_test(args)
        ti = self.dag_run.get_task_instance(task_id=task_id)
        assert ti is not None
        assert ti.state == State.SUCCESS

    @pytest.mark.enable_redact
    def test_test_filters_secrets(self, capsys):
        """
        Test ``airflow test`` does not print secrets to stdout.

        Output should be filtered by SecretsMasker.
        """
        # TODO: revisit during https://github.com/apache/airflow/issues/54658
        from airflow.sdk.log import mask_secret

        password = "somepassword1234!"
        mask_secret(password)

        args = self.parser.parse_args(
            ["tasks", "test", "example_python_operator", "print_the_context", "2018-01-01"],
        )

        with mock.patch(
            "airflow.cli.commands.task_command._run_task", side_effect=lambda *_, **__: print(password)
        ):
            task_command.task_test(args)
        assert capsys.readouterr().out.endswith("***\n")

        not_password = "!4321drowssapemos"
        with mock.patch(
            "airflow.cli.commands.task_command._run_task", side_effect=lambda *_, **__: print(not_password)
        ):
            task_command.task_test(args)
        assert capsys.readouterr().out.endswith(f"{not_password}\n")

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

    @mock.patch("airflow.providers.standard.triggers.file.os.path.getmtime", return_value=0)
    @mock.patch(
        "airflow.providers.standard.triggers.file.glob", return_value=["/tmp/temporary_file_for_testing"]
    )
    @mock.patch("airflow.providers.standard.triggers.file.os")
    @mock.patch("airflow.providers.standard.sensors.filesystem.FileSensor.poke", return_value=False)
    def test_cli_test_with_deferrable_operator(self, mock_pock, mock_os, mock_glob, mock_getmtime, caplog):
        mock_os.path.isfile.return_value = True
        with caplog.at_level(level=logging.INFO):
            task_command.task_test(
                self.parser.parse_args(
                    [
                        "tasks",
                        "test",
                        "example_sensors",
                        "wait_for_file_async",
                        DEFAULT_DATE.isoformat(),
                    ]
                )
            )
            output = caplog.text
        assert "Found File /tmp/temporary_file_for_testing" in output

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

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_mapped_task_render(self):
        """
        tasks render should render and displays templated fields for a given mapping task
        """
        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_render(
                self.parser.parse_args(
                    [
                        "tasks",
                        "render",
                        "test_mapped_classic",
                        "consumer_literal",
                        "2022-01-01",
                        "--map-index",
                        "0",
                    ]
                )
            )
        # the dag test_mapped_classic has op_args=[[1], [2], [3]], so the first mapping task should have
        # op_args=[1]
        output = stdout.getvalue()
        assert "[1]" in output
        assert "[2]" not in output
        assert "[3]" not in output
        assert "property: op_args" in output

    def test_mapped_task_render_with_template(self, dag_maker):
        """
        tasks render should render and displays templated fields for a given mapping task
        """
        with dag_maker() as dag:
            templated_command = """
            {% for i in range(5) %}
                echo "{{ ds }}"
                echo "{{ macros.ds_add(ds, 7)}}"
            {% endfor %}
            """
            commands = [templated_command, "echo 1"]
            BashOperator.partial(task_id="some_command").expand(bash_command=commands)
        sync_dag_to_db(dag)

        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_render(
                self.parser.parse_args(
                    [
                        "tasks",
                        "render",
                        "test_dag",
                        "some_command",
                        "2022-01-01",
                        "--map-index",
                        "0",
                    ]
                ),
                dag=dag,
            )

        output = stdout.getvalue()
        assert 'echo "2022-01-01"' in output
        assert 'echo "2022-01-08"' in output

    def test_task_state(self):
        task_command.task_state(
            self.parser.parse_args(
                ["tasks", "state", self.dag_id, "print_the_context", DEFAULT_DATE.isoformat()]
            )
        )

    def test_task_states_for_dag_run(self):
        dag2 = DagBag().dags["example_python_operator"]
        lazy_deserialized_dag2 = LazyDeserializedDAG.from_dag(dag2)

        SerializedDagModel.write_dag(lazy_deserialized_dag2, bundle_name="testing")

        task2 = dag2.get_task(task_id="print_the_context")
        dag2 = DagSerialization.from_dict(lazy_deserialized_dag2.data)

        default_date2 = timezone.datetime(2016, 1, 9)
        dag2.clear()
        data_interval = dag2.timetable.infer_manual_data_interval(run_after=default_date2)
        dagrun = dag2.create_dagrun(
            run_id="test",
            state=State.RUNNING,
            logical_date=default_date2,
            data_interval=data_interval,
            run_after=default_date2,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.CLI,
        )
        dag_version = DagVersion.get_latest_version(dag2.dag_id)
        ti2 = TaskInstance(task2, run_id=dagrun.run_id, dag_version_id=dag_version.id)
        ti2.priority_weight = task2.priority_weight
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
            "logical_date": "2016-01-09T00:00:00+00:00",
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
            task_command.task_states_for_dag_run(
                self.parser.parse_args(
                    [
                        "tasks",
                        "states-for-dag-run",
                        "not_exists_dag",
                        timezone.datetime(2016, 1, 9).isoformat(),
                        "--output",
                        "json",
                    ]
                )
            )

    def test_task_render_for_multi_line_properties(self, dag_maker):
        """
        tasks render should render and displays templated fields for a given task
        """
        with redirect_stdout(io.StringIO()) as stdout:
            task_command.task_render(
                self.parser.parse_args(["tasks", "render", "tutorial", "templated", "2016-01-01"])
            )

        output = stdout.getvalue()
        # no indentation before property name
        assert "# property: bash_command" in output.split("\n")


def _set_state_and_try_num(ti, session):
    ti.state = TaskInstanceState.QUEUED
    ti.try_number += 1
    session.commit()


class TestLogsfromTaskRunCommand:
    def setup_method(self) -> None:
        self.dag_id = "test_logging_dag"
        self.task_id = "test_task"
        self.run_id = "test_run"
        self.dag_path = os.path.join(ROOT_FOLDER, "dags", "test_logging_in_dag.py")
        reset(self.dag_id)
        self.logical_date = timezone.datetime(2017, 1, 1)
        self.logical_date_str = self.logical_date.isoformat()
        self.task_args = ["tasks", "run", self.dag_id, self.task_id, "--local", self.logical_date_str]
        self.log_dir = conf.get_mandatory_value("logging", "base_log_folder")
        self.log_filename = f"dag_id={self.dag_id}/run_id={self.run_id}/task_id={self.task_id}/attempt=1.log"
        self.ti_log_file_path = os.path.join(self.log_dir, self.log_filename)
        # Clearing the cache before calling it
        cli_parser.get_parser.cache_clear()
        self.parser = cli_parser.get_parser()

        dag = DagBag().get_dag(self.dag_id)
        data_interval = dag.timetable.infer_manual_data_interval(run_after=self.logical_date)
        self.dr = dag.create_dagrun(
            run_id=self.run_id,
            logical_date=self.logical_date,
            data_interval=data_interval,
            run_after=self.logical_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        self.tis = self.dr.get_task_instances()
        assert len(self.tis) == 1
        self.ti = self.tis[0]

        root = self.root_logger = logging.getLogger()
        self.root_handlers = root.handlers.copy()
        self.root_filters = root.filters.copy()
        self.root_level = root.level

        with contextlib.suppress(OSError):
            os.remove(self.ti_log_file_path)

    def teardown_method(self) -> None:
        root = self.root_logger
        root.setLevel(self.root_level)
        root.handlers[:] = self.root_handlers
        root.filters[:] = self.root_filters

        reset(self.dag_id)
        with contextlib.suppress(OSError):
            os.remove(self.ti_log_file_path)

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
