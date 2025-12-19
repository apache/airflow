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

import ast
import json
import os
import sys
from argparse import Namespace
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import select

import airflow
from airflow import settings
from airflow._shared.timezones import timezone
from airflow.exceptions import AirflowException
from airflow.models.log import Log
from airflow.utils import cli, cli_action_loggers
from airflow.utils.cli import _search_for_dag_file

# Mark entire module as db_test because ``action_cli`` wrapper still could use DB on callbacks:
# - ``cli_action_loggers.on_pre_execution``
# - ``cli_action_loggers.on_post_execution``
pytestmark = pytest.mark.db_test
repo_root = Path(airflow.__file__).parents[1]


class TestCliUtil:
    def test_metrics_build(self):
        func_name = "test"
        namespace = Namespace(dag_id="foo", task_id="bar", subcommand="test")
        metrics = cli._build_metrics(func_name, namespace)

        expected = {
            "user": os.environ.get("USER"),
            "sub_command": "test",
            "dag_id": "foo",
            "task_id": "bar",
        }
        for k, v in expected.items():
            assert v == metrics.get(k)

        assert metrics.get("start_datetime") <= timezone.utcnow()
        assert metrics.get("full_command")

    def test_fail_function(self):
        """
        Actual function is failing and fail needs to be propagated.
        :return:
        """
        with pytest.raises(NotImplementedError):
            fail_func(Namespace())

    def test_success_function(self):
        """
        Test success function but with failing callback.
        In this case, failure should not propagate.
        :return:
        """
        with fail_action_logger_callback():
            success_func(Namespace())

    def test_process_subdir_path_with_placeholder(self):
        assert os.path.join(settings.DAGS_FOLDER, "abc") == cli.process_subdir("DAGS_FOLDER/abc")

    def test_get_dags(self):
        dags = cli.get_dags(None, "test_example_bash_operator")
        assert len(dags) == 1

        with pytest.raises(AirflowException):
            cli.get_dags(None, "foobar", True)

    @pytest.mark.parametrize(
        ("given_command", "expected_masked_command", "is_command_list"),
        [
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password ********",
                False,
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p ********",
                False,
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password=test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password=********",
                False,
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p=test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p=********",
                False,
            ),
            (
                "airflow connections add dsfs --conn-login asd --conn-password test --conn-type google",
                "airflow connections add dsfs --conn-login asd --conn-password ******** --conn-type google",
                False,
            ),
            (
                "airflow connections add my_postgres_conn --conn-uri postgresql://user:my-password@localhost:5432/mydatabase",
                "airflow connections add my_postgres_conn --conn-uri postgresql://user:********@localhost:5432/mydatabase",
                False,
            ),
            (
                [
                    "airflow",
                    "connections",
                    "add",
                    "my_new_conn",
                    "--conn-json",
                    '{"conn_type": "my-conn-type", "login": "my-login", "password": "my-password", "host": "my-host", "port": 1234, "schema": "my-schema", "extra": {"param1": "val1", "param2": "val2"}}',
                ],
                [
                    "airflow",
                    "connections",
                    "add",
                    "my_new_conn",
                    "--conn-json",
                    '{"conn_type": "my-conn-type", "login": "my-login", "password": "********", '
                    '"host": "my-host", "port": 1234, "schema": "my-schema", "extra": {"param1": '
                    '"val1", "param2": "val2"}}',
                ],
                True,
            ),
            (
                "airflow scheduler -p",
                "airflow scheduler -p",
                False,
            ),
            (
                "airflow celery flower -p 8888",
                "airflow celery flower -p 8888",
                False,
            ),
        ],
    )
    def test_cli_create_user_supplied_password_is_masked(
        self, given_command, expected_masked_command, is_command_list, session
    ):
        # '-p' value which is not password, like 'airflow scheduler -p'
        # or 'airflow celery flower -p 8888', should not be masked
        args = given_command if is_command_list else given_command.split()
        expected_command = expected_masked_command if is_command_list else expected_masked_command.split()

        exec_date = timezone.utcnow()
        namespace = Namespace(dag_id="foo", task_id="bar", subcommand="test", logical_date=exec_date)
        with (
            mock.patch.object(sys, "argv", args),
            mock.patch("airflow.utils.session.create_session") as mock_create_session,
        ):
            metrics = cli._build_metrics(args[1], namespace)
            # Make it so the default_action_log doesn't actually commit the txn, by giving it a next txn
            # instead
            mock_create_session.return_value = session.begin_nested()
            mock_create_session.return_value.bulk_insert_mappings = session.bulk_insert_mappings
            cli_action_loggers.default_action_log(**metrics)

            log = session.scalar(select(Log).order_by(Log.dttm.desc()))

        assert metrics.get("start_datetime") <= timezone.utcnow()

        command: str = json.loads(log.extra).get("full_command")
        # Replace single quotes to double quotes to avoid json decode error
        command = ast.literal_eval(command)
        assert command == expected_command

    def test_setup_locations_relative_pid_path(self):
        relative_pid_path = "fake.pid"
        pid_full_path = os.path.join(os.getcwd(), relative_pid_path)
        pid, _, _, _ = cli.setup_locations(process="fake_process", pid=relative_pid_path)
        assert pid == pid_full_path

    def test_setup_locations_absolute_pid_path(self):
        abs_pid_path = os.path.join(os.getcwd(), "fake.pid")
        pid, _, _, _ = cli.setup_locations(process="fake_process", pid=abs_pid_path)
        assert pid == abs_pid_path

    def test_setup_locations_none_pid_path(self):
        process_name = "fake_process"
        default_pid_path = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process_name}.pid")
        pid, _, _, _ = cli.setup_locations(process=process_name)
        assert pid == default_pid_path

    @pytest.mark.parametrize(
        ("given_command", "expected_masked_command"),
        [
            (
                "airflow variables set --description 'needed for dag 4' client_secret_234 7fh4375f5gy353wdf",
                "airflow variables set --description 'needed for dag 4' client_secret_234 ********",
            ),
            (
                "airflow variables set cust_secret_234 7fh4375f5gy353wdf",
                "airflow variables set cust_secret_234 ********",
            ),
        ],
    )
    def test_cli_set_variable_supplied_sensitive_value_is_masked(
        self, given_command, expected_masked_command, session
    ):
        args = given_command.split()

        expected_command = expected_masked_command.split()

        exec_date = timezone.utcnow()
        namespace = Namespace(dag_id="foo", task_id="bar", subcommand="test", execution_date=exec_date)
        with (
            mock.patch.object(sys, "argv", args),
            mock.patch("airflow.utils.session.create_session") as mock_create_session,
        ):
            metrics = cli._build_metrics(args[1], namespace)
            # Make it so the default_action_log doesn't actually commit the txn, by giving it a next txn
            # instead
            mock_create_session.return_value = session.begin_nested()
            mock_create_session.return_value.bulk_insert_mappings = session.bulk_insert_mappings
            cli_action_loggers.default_action_log(**metrics)

            log = session.scalar(select(Log).order_by(Log.dttm.desc()))

        assert metrics.get("start_datetime") <= timezone.utcnow()

        command: str = json.loads(log.extra).get("full_command")
        # Replace single quotes to double quotes to avoid json decode error
        command = ast.literal_eval(command)
        assert command == expected_command


@contextmanager
def fail_action_logger_callback():
    """Adding failing callback and revert it back when closed."""
    tmp = cli_action_loggers.__pre_exec_callbacks[:]

    def fail_callback(**_):
        raise NotImplementedError

    cli_action_loggers.register_pre_exec_callback(fail_callback)
    yield
    cli_action_loggers.__pre_exec_callbacks = tmp


@cli.action_cli(check_db=False)
def fail_func(_):
    raise NotImplementedError


@cli.action_cli(check_db=False)
def success_func(_):
    pass


def test__search_for_dags_file():
    dags_folder = settings.DAGS_FOLDER
    assert _search_for_dag_file("") is None
    assert _search_for_dag_file(None) is None
    # if it's a file, and one can be find in subdir, should return full path
    assert _search_for_dag_file("any/hi/test_example_bash_operator.py") == str(
        Path(dags_folder) / "test_example_bash_operator.py"
    )
    # if a folder, even if exists, should return dags folder
    existing_folder = Path(settings.DAGS_FOLDER, "subdir1")
    assert existing_folder.exists()
    assert _search_for_dag_file(existing_folder.as_posix()) is None
    # when multiple files found, default to the dags folder
    assert _search_for_dag_file("any/hi/__init__.py") is None


def test_validate_dag_bundle_arg():
    with pytest.raises(SystemExit, match="Bundles not found: (x, y)|(y, x)"):
        cli.validate_dag_bundle_arg(["x", "y", "dags-folder"])

    # doesn't raise
    cli.validate_dag_bundle_arg(["dags-folder"])


@pytest.mark.parametrize(
    ("dev_flag", "env_var", "expected"),
    [
        # --dev flag tests
        (True, None, True),
        (False, None, False),
        (None, None, False),  # no dev flag attribute
        # DEV_MODE env var tests
        (False, "true", True),
        (False, "false", False),
        (False, "TRUE", True),
        (False, "True", True),
        # --dev flag takes precedence
        (True, "false", True),
        # Invalid env var values
        (False, "yes", False),
        (False, "1", False),
    ],
)
def test_should_enable_hot_reload(dev_flag, env_var, expected):
    """Test should_enable_hot_reload with various --dev flag and DEV_MODE env var combinations."""
    args = Namespace() if dev_flag is None else Namespace(dev=dev_flag)
    env = {} if env_var is None else {"DEV_MODE": env_var}

    with mock.patch.dict(os.environ, env, clear=True):
        assert cli.should_enable_hot_reload(args) is expected
