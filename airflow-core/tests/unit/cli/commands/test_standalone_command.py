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

import json
import os
import signal
import subprocess
import sys
import time
from collections import deque
from importlib import reload
from unittest import mock

import psutil
import pytest

from airflow.cli.commands.standalone_command import StandaloneCommand, SubCommand
from airflow.configuration import conf
from airflow.executors import executor_loader
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
)

from tests_common.test_utils.config import conf_vars


def default_dag_discovery_config():
    """Dag discovery options at their defaults, as the generated airflow.cfg writes them."""
    return {
        ("dag_processor", key): conf.get_default_value("dag_processor", key)
        for key in ("refresh_interval", "dag_bundle_config_list")
    }


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

    def test_calculate_env_refreshes_dags_folder_on_every_bundle_check_by_default(self):
        """The generated airflow.cfg spells out every default, which must not count as a user choice."""
        config = {**default_dag_discovery_config(), ("dag_processor", "bundle_refresh_check_interval"): "5"}
        with conf_vars(config), mock.patch.dict(os.environ):
            env = StandaloneCommand().calculate_env()

        assert env["AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL"] == "5"

    @pytest.mark.parametrize(
        ("config", "environ"),
        [
            pytest.param({("dag_processor", "refresh_interval"): "60"}, {}, id="interval-in-airflow-cfg"),
            pytest.param({}, {"AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL": "60"}, id="interval-in-env"),
            pytest.param(
                {}, {"AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL": "300"}, id="default-interval-in-env"
            ),
            pytest.param(
                {
                    ("dag_processor", "dag_bundle_config_list"): json.dumps(
                        [
                            {
                                "name": "my-git-repo",
                                "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
                                "kwargs": {},
                            }
                        ]
                    )
                },
                {},
                id="custom-bundles",
            ),
        ],
    )
    def test_calculate_env_keeps_user_chosen_dag_discovery(self, config, environ):
        with conf_vars({**default_dag_discovery_config(), **config}), mock.patch.dict(os.environ, environ):
            env = StandaloneCommand().calculate_env()

        assert env.get("AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL") == environ.get(
            "AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL"
        )

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

    @pytest.mark.parametrize(
        ("api_server_listening", "jobs_running", "expected"),
        [
            pytest.param(True, [True, True, True], True, id="every-component-ready"),
            pytest.param(True, [True, False], False, id="component-not-heartbeating"),
            pytest.param(False, [], False, id="api-server-port-closed"),
        ],
    )
    def test_is_ready(self, monkeypatch, api_server_listening, jobs_running, expected):
        cmd = StandaloneCommand()
        cmd.api_server_port = 9999
        port_open = mock.Mock(return_value=api_server_listening)
        jobs = iter(jobs_running)
        monkeypatch.setattr(cmd, "port_open", port_open)
        monkeypatch.setattr(cmd, "job_running", lambda *_: next(jobs))

        assert cmd.is_ready() is expected
        port_open.assert_called_once_with(9999)

    @conf_vars({("api", "port"): "9999"})
    @mock.patch("airflow.cli.commands.standalone_command.SubCommand", autospec=True)
    @mock.patch.object(StandaloneCommand, "initialize_database")
    @mock.patch.object(StandaloneCommand, "find_user_info")
    @mock.patch.object(StandaloneCommand, "calculate_env", return_value={})
    @mock.patch.object(StandaloneCommand, "print_output")
    def test_run_resolves_the_api_server_port_from_config(self, *_):
        cmd = StandaloneCommand()
        with mock.patch.object(cmd, "update_output", side_effect=KeyboardInterrupt):
            cmd.run()

        assert cmd.api_server_port == 9999

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

    @staticmethod
    def _record_killpg(monkeypatch) -> list[tuple[int, int]]:
        calls: list[tuple[int, int]] = []
        monkeypatch.setattr(
            "airflow.cli.commands.standalone_command.os.killpg",
            lambda pgid, signum: calls.append((pgid, signum)),
        )
        return calls

    def test_subcommand_run_appends_output(self, monkeypatch):
        self._record_killpg(monkeypatch)
        parent = mock.Mock()
        parent.output_queue = deque()
        fake_process = mock.Mock()
        fake_process.stdout = [b"line1\n", b"line2\n"]
        monkeypatch.setattr(
            "airflow.cli.commands.standalone_command.subprocess.Popen",
            lambda *a, **k: fake_process,
        )
        cmd = SubCommand(parent, "scheduler", ["scheduler"], {})
        fake_process.wait.side_effect = lambda: (cmd.stop(), 0)[1]
        cmd.run()

        assert parent.output_queue == deque(
            [
                ("scheduler", b"line1\n"),
                ("scheduler", b"line2\n"),
            ]
        )

    @staticmethod
    def _exiting_process(returncode: int, pid: int = 1000) -> mock.Mock:
        process = mock.Mock(spec=subprocess.Popen, pid=pid)
        process.stdout = []
        process.wait.return_value = returncode
        return process

    def test_subcommand_run_restarts_exited_process_with_backoff(self, monkeypatch):
        killpg = self._record_killpg(monkeypatch)
        parent = mock.Mock()
        parent.output_queue = deque()
        cmd = SubCommand(parent, "triggerer", ["triggerer"], {})
        last = self._exiting_process(0, pid=3)
        last.wait.side_effect = lambda: (cmd.stop(), 0)[1]
        processes = [self._exiting_process(1, pid=1), self._exiting_process(-9, pid=2), last]
        popen = mock.Mock(side_effect=processes)
        monkeypatch.setattr("airflow.cli.commands.standalone_command.subprocess.Popen", popen)
        monkeypatch.setattr("airflow.cli.commands.standalone_command.time.monotonic", lambda: 0.0)
        delays: list[float] = []
        monkeypatch.setattr(cmd._stopping, "wait", lambda timeout: delays.append(timeout) or False)

        cmd.run()

        assert popen.call_count == 3
        assert delays == [1.0, 2.0]
        assert parent.output_queue == deque(
            [
                ("triggerer", b"Exited with code 1, restarting in 1s"),
                ("triggerer", b"Killed by SIGKILL, restarting in 2s"),
            ]
        )
        for process in processes:
            process.wait.assert_called_once()
        assert all(call.kwargs["start_new_session"] is True for call in popen.call_args_list)
        # Whatever each run left behind in its process group is killed before the next one.
        assert killpg == [(1, signal.SIGKILL), (2, signal.SIGKILL), (3, signal.SIGTERM), (3, signal.SIGKILL)]

    def test_subcommand_run_resets_backoff_after_a_stable_run(self, monkeypatch):
        self._record_killpg(monkeypatch)
        parent = mock.Mock()
        parent.output_queue = deque()
        cmd = SubCommand(parent, "scheduler", ["scheduler"], {})
        last = self._exiting_process(0)
        last.wait.side_effect = lambda: (cmd.stop(), 0)[1]
        monkeypatch.setattr(
            "airflow.cli.commands.standalone_command.subprocess.Popen",
            mock.Mock(side_effect=[self._exiting_process(1), self._exiting_process(1), last]),
        )
        # (start, exit) per run: the first run is short, the second outlives stable_run_seconds.
        clock = iter([0.0, 1.0, 10.0, 10.0 + SubCommand.stable_run_seconds, 100.0])
        monkeypatch.setattr("airflow.cli.commands.standalone_command.time.monotonic", lambda: next(clock))
        delays: list[float] = []
        monkeypatch.setattr(cmd._stopping, "wait", lambda timeout: delays.append(timeout) or False)

        cmd.run()

        assert delays == [1.0, 1.0]

    def test_subcommand_run_does_not_restart_after_stop_during_backoff(self, monkeypatch):
        self._record_killpg(monkeypatch)
        parent = mock.Mock()
        parent.output_queue = deque()
        popen = mock.Mock(return_value=self._exiting_process(1))
        monkeypatch.setattr("airflow.cli.commands.standalone_command.subprocess.Popen", popen)
        cmd = SubCommand(parent, "scheduler", ["scheduler"], {})
        real_wait = cmd._stopping.wait

        def stop_while_waiting(timeout: float) -> bool:
            cmd.stop()
            return real_wait(timeout)

        monkeypatch.setattr(cmd._stopping, "wait", stop_while_waiting)

        cmd.run()

        popen.assert_called_once()

    def test_subcommand_run_does_not_spawn_after_stop(self, monkeypatch):
        popen = mock.Mock()
        monkeypatch.setattr("airflow.cli.commands.standalone_command.subprocess.Popen", popen)
        cmd = SubCommand(mock.Mock(), "scheduler", ["scheduler"], {})
        cmd.stop()

        cmd.run()

        popen.assert_not_called()

    def test_subcommand_reaps_and_restarts_a_real_process(self, monkeypatch):
        """End to end with a real child: it is waited on (no zombie) and started again."""
        real_popen = subprocess.Popen
        spawned: list[subprocess.Popen[bytes]] = []

        def popen(_args: list[str], **kwargs) -> subprocess.Popen[bytes]:
            process = real_popen([sys.executable, "-c", "print('hi')"], **kwargs)
            spawned.append(process)
            return process

        class StopOnSecondRun(deque):
            def append(self, item: tuple[str, bytes]) -> None:
                super().append(item)
                if item[1].strip() == b"hi" and len(spawned) == 2:
                    cmd.stop()

        parent = mock.Mock()
        parent.output_queue = StopOnSecondRun()
        monkeypatch.setattr("airflow.cli.commands.standalone_command.subprocess.Popen", popen)
        monkeypatch.setattr(SubCommand, "restart_backoff_initial", 0.01)
        cmd = SubCommand(parent, "scheduler", ["scheduler"], {})

        cmd.start()
        cmd.join(timeout=30)

        assert not cmd.is_alive()
        assert len(spawned) == 2
        # Both children were waited on; stop() may terminate the second one before it exits.
        assert spawned[0].returncode == 0
        assert spawned[1].returncode is not None
        assert ("scheduler", b"Exited with code 0, restarting in 0.01s") in parent.output_queue

    def test_subcommand_restarts_a_process_whose_children_outlive_it(self, monkeypatch):
        """A child left behind holds the output pipe; it must not stop the restart, and is killed."""
        real_popen = subprocess.Popen
        spawned: list[subprocess.Popen[bytes]] = []
        orphan_pids: list[int] = []
        # Prints the PID of a background child that inherits stdout, then exits without waiting for it.
        script = "import subprocess, sys; c = subprocess.Popen(['sleep', '300']); print(c.pid, flush=True); sys.exit(3)"

        def popen(_args: list[str], **kwargs) -> subprocess.Popen[bytes]:
            process = real_popen([sys.executable, "-c", script], **kwargs)
            spawned.append(process)
            return process

        class StopOnSecondRun(deque):
            def append(self, item: tuple[str, bytes]) -> None:
                super().append(item)
                if item[1].strip().isdigit():
                    orphan_pids.append(int(item[1]))
                    if len(orphan_pids) == 2:
                        cmd.stop()

        parent = mock.Mock()
        parent.output_queue = StopOnSecondRun()
        monkeypatch.setattr("airflow.cli.commands.standalone_command.subprocess.Popen", popen)
        monkeypatch.setattr(SubCommand, "restart_backoff_initial", 0.01)
        monkeypatch.setattr(SubCommand, "stop_grace_seconds", 0.5)
        cmd = SubCommand(parent, "triggerer", ["triggerer"], {})

        cmd.start()
        cmd.join(timeout=30)

        assert not cmd.is_alive()
        assert len(spawned) == 2
        assert ("triggerer", b"Exited with code 3, restarting in 0.01s") in parent.output_queue
        assert len(orphan_pids) == 2
        for pid in orphan_pids:
            assert self._wait_until_gone(pid), f"child {pid} left behind by a run is still running"

    @staticmethod
    def _wait_until_gone(pid: int, timeout: float = 10.0) -> bool:
        """Whether the process is gone; a killed orphan may linger as a zombie until PID 1 reaps it."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
                    return True
            except psutil.NoSuchProcess:
                return True
            time.sleep(0.05)
        return False

    @pytest.mark.parametrize(
        ("returncode", "expected"),
        [
            (0, "Exited with code 0"),
            (3, "Exited with code 3"),
            (-15, "Killed by SIGTERM"),
            (-200, "Killed by signal 200"),
        ],
    )
    def test_subcommand_describe_exit(self, returncode: int, expected: str):
        assert SubCommand._describe_exit(returncode) == expected

    def test_subcommand_stop_terminates_the_process_group(self, monkeypatch):
        killpg = self._record_killpg(monkeypatch)
        cmd = SubCommand(mock.Mock(), "scheduler", ["scheduler"], {})
        cmd.process = mock.Mock(spec=subprocess.Popen, pid=1234)
        cmd.stop()

        assert killpg == [(1234, signal.SIGTERM)]

    def test_subcommand_stop_does_not_block_siblings_when_a_process_never_started(self, monkeypatch):
        """Shutdown stops each component in turn, so one that never spawned must not abort the loop."""
        killpg = self._record_killpg(monkeypatch)
        never_started = SubCommand(mock.Mock(spec=StandaloneCommand), "scheduler", ["scheduler"], {})
        running = SubCommand(mock.Mock(spec=StandaloneCommand), "triggerer", ["triggerer"], {})
        running.process = mock.Mock(spec=subprocess.Popen, pid=1234)

        assert never_started.process is None
        for command in (never_started, running):
            command.stop()

        assert killpg == [(1234, signal.SIGTERM)]

    @pytest.mark.parametrize("error", [ProcessLookupError, PermissionError])
    def test_subcommand_stop_ignores_an_already_gone_process_group(self, monkeypatch, error: type[OSError]):
        monkeypatch.setattr("airflow.cli.commands.standalone_command.os.killpg", mock.Mock(side_effect=error))
        cmd = SubCommand(mock.Mock(), "scheduler", ["scheduler"], {})
        cmd.process = mock.Mock(spec=subprocess.Popen, pid=1234)

        cmd.stop()

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
