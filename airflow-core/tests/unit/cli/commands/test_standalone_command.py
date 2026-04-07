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

import os
from collections import deque
from importlib import reload
from unittest import mock

import pytest

from airflow.cli.commands.standalone_command import StandaloneCommand, SubCommand
from airflow.executors import executor_loader
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
)


class TestStandaloneCommand:
    @pytest.mark.parametrize(
        "conf_executor_name",
        [LOCAL_EXECUTOR, CELERY_EXECUTOR, KUBERNETES_EXECUTOR],
    )
    def test_calculate_env(self, conf_executor_name):
        """Should always force a local executor compatible with the db."""
        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW__CORE__EXECUTOR": conf_executor_name,
            },
        ):
            reload(executor_loader)
            env = StandaloneCommand().calculate_env()
            # all non local executors will fall back to localesecutor
            assert env["AIRFLOW__CORE__EXECUTOR"] == LOCAL_EXECUTOR

    @mock.patch("airflow.cli.commands.standalone_command.ExecutorLoader.import_default_executor_cls")
    @mock.patch("airflow.cli.commands.standalone_command.conf.get")
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_calculate_env_force_executor_and_auth(self, mock_conf_get, mock_import):
        class FakeExecutor:
            is_local = False

        mock_import.return_value = (FakeExecutor, None)
        mock_conf_get.return_value = "wrong.auth.manager"
        cmd = StandaloneCommand()
        env = cmd.calculate_env()

        assert env["AIRFLOW__CORE__EXECUTOR"] == "LocalExecutor"
        assert "AIRFLOW__CORE__AUTH_MANAGER" in env

    @mock.patch("airflow.cli.commands.standalone_command.ExecutorLoader.import_default_executor_cls")
    @mock.patch("airflow.cli.commands.standalone_command.conf.get")
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_calculate_env_does_not_override_auth_if_already_set(self, mock_conf_get, mock_import):
        class FakeExecutor:
            is_local = True

        mock_import.return_value = (FakeExecutor, None)
        mock_conf_get.return_value = (
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
        )
        cmd = StandaloneCommand()
        env = cmd.calculate_env()

        assert "AIRFLOW__CORE__AUTH_MANAGER" not in env

    @mock.patch("airflow.cli.commands.standalone_command.os.path.exists", return_value=False)
    @mock.patch("airflow.cli.commands.standalone_command.create_auth_manager")
    @mock.patch("airflow.cli.commands.standalone_command.conf.get")
    def test_find_user_info_generates_password(self, mock_conf, mock_auth, mock_exists):
        def conf_side(section, key):
            if key == "simple_auth_manager_all_admins":
                return "false"
            if key == "simple_auth_manager_users":
                return "admin:admin"
            return ""

        mock_conf.side_effect = conf_side
        am = mock.Mock()
        am.get_generated_password_file.return_value = "/tmp/fake"
        mock_auth.return_value = am
        cmd = StandaloneCommand()
        cmd.find_user_info()

        am.init.assert_called_once()

    @mock.patch("airflow.cli.commands.standalone_command.create_auth_manager")
    @mock.patch("airflow.cli.commands.standalone_command.conf.get")
    def test_find_user_info_skips_when_all_admins_true(self, mock_conf_get, mock_create_auth):
        def conf_side(section, key):
            if key == "simple_auth_manager_all_admins":
                return "true"
            return ""

        mock_conf_get.side_effect = conf_side
        cmd = StandaloneCommand()
        cmd.find_user_info()

        mock_create_auth.assert_not_called()

    @mock.patch("airflow.cli.commands.standalone_command.create_auth_manager")
    @mock.patch("airflow.cli.commands.standalone_command.conf.get")
    def test_find_user_info_skips_when_users_already_configured(self, mock_conf_get, mock_create_auth):
        def conf_side(section, key):
            if key == "simple_auth_manager_all_admins":
                return "false"
            if key == "simple_auth_manager_users":
                return "custom:password"
            return ""

        mock_conf_get.side_effect = conf_side
        cmd = StandaloneCommand()
        cmd.find_user_info()

        mock_create_auth.assert_not_called()

    @mock.patch("airflow.cli.commands.standalone_command.os.path.exists", return_value=True)
    @mock.patch("airflow.cli.commands.standalone_command.create_auth_manager")
    @mock.patch("airflow.cli.commands.standalone_command.conf.get")
    def test_find_user_info_skips_when_password_file_exists(
        self, mock_conf_get, mock_create_auth, mock_exists
    ):
        def conf_side(section, key):
            if key == "simple_auth_manager_all_admins":
                return "false"
            if key == "simple_auth_manager_users":
                return "admin:admin"
            return ""

        mock_conf_get.side_effect = conf_side
        am = mock.Mock()
        am.get_generated_password_file.return_value = "/tmp/fake"
        mock_create_auth.return_value = am
        cmd = StandaloneCommand()
        cmd.find_user_info()

        am.init.assert_not_called()

    @mock.patch("airflow.cli.commands.standalone_command.db.initdb")
    def test_initialize_database(self, mock_initdb, monkeypatch):
        cmd = StandaloneCommand()
        monkeypatch.setattr(cmd, "print_output", lambda *a: None)
        cmd.initialize_database()

        mock_initdb.assert_called_once()

    @mock.patch("builtins.print")
    def test_print_output(self, mock_print):
        cmd = StandaloneCommand()
        cmd.print_output("scheduler", "hello\nworld")

        assert mock_print.call_count == 2

    @mock.patch(
        "airflow.cli.commands.standalone_command.most_recent_job",
        return_value=None,
    )
    def test_job_running_returns_false_if_no_recent_job(self, mock_most_recent_job):
        result = StandaloneCommand().job_running(mock.Mock(job_type="scheduler"))
        assert result is False

    @mock.patch("airflow.cli.commands.standalone_command.most_recent_job")
    def test_job_running_returns_false_if_job_not_alive(self, mock_most_recent_job):
        fake_job = mock.Mock()
        fake_job.is_alive.return_value = False
        mock_most_recent_job.return_value = fake_job

        result = StandaloneCommand().job_running(mock.Mock(job_type="scheduler"))
        assert result is False

    @mock.patch("airflow.cli.commands.standalone_command.most_recent_job")
    def test_job_running_returns_true_if_alive(self, mock_most_recent_job):
        fake_job = mock.Mock()
        fake_job.is_alive.return_value = True
        mock_most_recent_job.return_value = fake_job

        result = StandaloneCommand().job_running(mock.Mock(job_type="scheduler"))
        assert result is True

    def test_is_ready_true_when_all_components_running(self, monkeypatch):
        cmd = StandaloneCommand()
        monkeypatch.setattr(cmd, "job_running", lambda *_: True)

        assert cmd.is_ready() is True

    def test_is_ready_false_when_any_component_missing(self, monkeypatch):
        cmd = StandaloneCommand()
        calls = iter([True, False, True])
        monkeypatch.setattr(cmd, "job_running", lambda *_: next(calls))

        assert cmd.is_ready() is False

    @pytest.mark.parametrize("exc", [OSError, ValueError])
    def test_port_open_returns_false_on_errors(self, monkeypatch, exc):
        cmd = StandaloneCommand()

        class FakeSocket:
            def settimeout(self, *_):
                pass

            def connect(self, *_):
                raise exc()

            def close(self):
                pass

        monkeypatch.setattr(
            "airflow.cli.commands.standalone_command.socket.socket",
            lambda *a, **k: FakeSocket(),
        )
        assert cmd.port_open(1234) is False

    def test_port_open_returns_true_when_connect_succeeds(self, monkeypatch):
        cmd = StandaloneCommand()

        class FakeSocket:
            def settimeout(self, *_):
                pass

            def connect(self, *_):
                pass

            def close(self):
                pass

        monkeypatch.setattr(
            "airflow.cli.commands.standalone_command.socket.socket",
            lambda *a, **k: FakeSocket(),
        )

        assert cmd.port_open(1234) is True

    def test_update_output_drains_queue_and_prints(self, monkeypatch):
        cmd = StandaloneCommand()
        printed = deque()
        monkeypatch.setattr(cmd, "print_output", lambda name, msg: printed.append((name, msg)))
        cmd.output_queue.append(("scheduler", b"hello\n"))
        cmd.output_queue.append(("api-server", b"world\n"))
        cmd.update_output()

        assert printed == deque(
            [
                ("scheduler", "hello"),
                ("api-server", "world"),
            ]
        )
        assert len(cmd.output_queue) == 0

    def test_print_error_calls_print_output(self, monkeypatch):
        cmd = StandaloneCommand()
        called = []
        monkeypatch.setattr(cmd, "print_output", lambda name, msg: called.append((name, msg)))
        cmd.print_error("scheduler", "boom")

        assert called

    def test_subcommand_run_appends_output(self, monkeypatch):
        parent = mock.Mock()
        parent.output_queue = deque()
        fake_process = mock.Mock()
        fake_process.stdout = [b"line1\n", b"line2\n"]
        monkeypatch.setattr(
            "airflow.cli.commands.standalone_command.subprocess.Popen",
            lambda *a, **k: fake_process,
        )
        cmd = SubCommand(parent, "scheduler", ["scheduler"], {})
        cmd.run()

        assert parent.output_queue == deque(
            [
                ("scheduler", b"line1\n"),
                ("scheduler", b"line2\n"),
            ]
        )

    def test_subcommand_stop_calls_terminate(self, monkeypatch):
        parent = mock.Mock()
        fake_process = mock.Mock()
        monkeypatch.setattr(
            "airflow.cli.commands.standalone_command.subprocess.Popen",
            lambda *a, **k: fake_process,
        )
        cmd = SubCommand(parent, "scheduler", ["scheduler"], {})
        cmd.process = fake_process
        cmd.stop()

        fake_process.terminate.assert_called_once()

    @mock.patch("airflow.cli.commands.standalone_command.ExecutorLoader.import_default_executor_cls")
    @mock.patch("airflow.cli.commands.standalone_command.conf.get")
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_calculate_env_does_not_force_executor_when_already_local(self, mock_conf_get, mock_import):
        class LocalExecutor:
            is_local = True

        mock_import.return_value = (LocalExecutor, None)
        mock_conf_get.return_value = (
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
        )
        cmd = StandaloneCommand()
        env = cmd.calculate_env()

        assert "AIRFLOW__CORE__EXECUTOR" not in env
