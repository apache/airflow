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

import random
import time
from datetime import timedelta
from unittest import mock

import pytest
from paramiko.client import SSHClient

from airflow.exceptions import AirflowException, AirflowSkipException, AirflowTaskTimeout
from airflow.models import TaskInstance
from airflow.providers.common.compat.sdk import timezone
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.types import NOTSET

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

datetime = timezone.datetime

if AIRFLOW_V_3_0_PLUS:
    from airflow.models.dag_version import DagVersion

pytestmark = pytest.mark.db_test


TEST_DAG_ID = "unit_tests_ssh_test_op"
TEST_CONN_ID = "conn_id_for_testing"
DEFAULT_TIMEOUT = 10
CONN_TIMEOUT = 5
CMD_TIMEOUT = 7
TIMEOUT = 12
DEFAULT_DATE = datetime(2017, 1, 1)
COMMAND = "echo -n airflow"
COMMAND_WITH_SUDO = "sudo " + COMMAND


class SSHClientSideEffect:
    def __init__(self, hook):
        self.hook = hook


class TestSSHOperator:
    def setup_method(self):
        hook = SSHHook(ssh_conn_id="ssh_default")
        hook.no_host_key_check = True

        ssh_client = mock.create_autospec(SSHClient)
        # `with ssh_client` should return itself.
        ssh_client.__enter__.return_value = ssh_client
        hook.get_conn = mock.MagicMock(return_value=ssh_client)
        self.hook = hook

    # Make sure nothing in this test actually connects to SSH -- that's for hook tests.
    @pytest.fixture(autouse=True)
    def patch_exec_ssh_client(self):
        with mock.patch.object(self.hook, "exec_ssh_client_command") as exec_ssh_client_command:
            self.exec_ssh_client_command = exec_ssh_client_command
            exec_ssh_client_command.return_value = (0, b"airflow", "")
            yield exec_ssh_client_command

    @pytest.mark.parametrize(
        ("cmd_timeout", "cmd_timeout_expected"),
        [(45, 45), ("Not Set", 10), (None, None)],
    )
    def test_hook_created_correctly(self, cmd_timeout, cmd_timeout_expected):
        conn_timeout = 20
        if cmd_timeout == "Not Set":
            task = SSHOperator(
                task_id="test",
                command=COMMAND,
                conn_timeout=conn_timeout,
                ssh_conn_id="ssh_default",
            )
        else:
            task = SSHOperator(
                task_id="test",
                command=COMMAND,
                conn_timeout=conn_timeout,
                cmd_timeout=cmd_timeout,
                ssh_conn_id="ssh_default",
            )
        assert conn_timeout == task.hook.conn_timeout
        assert cmd_timeout_expected == task.hook.cmd_timeout
        assert task.hook.ssh_conn_id == "ssh_default"

    @pytest.mark.parametrize(
        ("enable_xcom_pickling", "output", "expected"),
        [(False, b"airflow", "YWlyZmxvdw=="), (True, b"airflow", b"airflow"), (True, b"", b"")],
    )
    def test_return_value(self, enable_xcom_pickling, output, expected):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=COMMAND,
            environment={"TEST": "value"},
        )
        with conf_vars({("core", "enable_xcom_pickling"): str(enable_xcom_pickling)}):
            self.exec_ssh_client_command.return_value = (0, output, b"")
            result = task.execute(None)
            assert result == expected
            self.exec_ssh_client_command.assert_called_with(
                mock.ANY, COMMAND, timeout=NOTSET, environment={"TEST": "value"}, get_pty=False
            )

    @mock.patch("os.environ", {"AIRFLOW_CONN_" + TEST_CONN_ID.upper(): "ssh://test_id@localhost"})
    @mock.patch.object(SSHOperator, "run_ssh_client_command")
    @mock.patch.object(SSHHook, "get_conn")
    def test_arg_checking(self, get_conn, run_ssh_client_command):
        run_ssh_client_command.return_value = b""

        # Exception should be raised if neither ssh_hook nor ssh_conn_id is provided.
        task_0 = SSHOperator(task_id="test", command=COMMAND)
        with pytest.raises(AirflowException, match="Cannot operate without ssh_hook or ssh_conn_id."):
            task_0.execute(None)

        # If ssh_hook is invalid/not provided, use ssh_conn_id to create SSHHook.
        task_1 = SSHOperator(
            task_id="test_1",
            ssh_hook="string_rather_than_SSHHook",  # Invalid ssh_hook.
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
        )
        task_1.execute(None)
        assert task_1.ssh_hook.ssh_conn_id == TEST_CONN_ID

        task_2 = SSHOperator(
            task_id="test_2",
            ssh_conn_id=TEST_CONN_ID,  # No ssh_hook provided.
            command=COMMAND,
        )
        task_2.execute(None)
        assert task_2.ssh_hook.ssh_conn_id == TEST_CONN_ID

        # If both valid ssh_hook and ssh_conn_id are provided, ignore ssh_conn_id.
        task_3 = SSHOperator(
            task_id="test_3",
            ssh_hook=self.hook,
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
        )
        task_3.execute(None)
        assert task_3.ssh_hook.ssh_conn_id == self.hook.ssh_conn_id
        # If remote_host was specified, ensure it is used
        task_4 = SSHOperator(
            task_id="test_4",
            ssh_hook=self.hook,
            ssh_conn_id=TEST_CONN_ID,
            command=COMMAND,
            remote_host="operator_remote_host",
        )
        task_4.execute(None)
        assert task_4.ssh_hook.ssh_conn_id == self.hook.ssh_conn_id
        assert task_4.ssh_hook.remote_host == "operator_remote_host"

        with pytest.raises(
            AirflowException, match="SSH operator error: SSH command not specified. Aborting."
        ):
            SSHOperator(
                task_id="test_5",
                ssh_hook=self.hook,
                command=None,
            ).execute(None)

    @pytest.mark.parametrize(
        ("command", "get_pty_in", "get_pty_out"),
        [
            (COMMAND, False, False),
            (COMMAND, True, True),
            (COMMAND_WITH_SUDO, False, True),
            (COMMAND_WITH_SUDO, True, True),
        ],
    )
    def test_get_pyt_set_correctly(self, command, get_pty_in, get_pty_out):
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=command,
            get_pty=get_pty_in,
        )
        task.execute(None)
        assert task.get_pty == get_pty_out

    def test_ssh_client_managed_correctly(self):
        # Ensure connection gets closed once (via context_manager) using on_kill
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command="ls",
        )
        task.execute()

        self.hook.get_conn.assert_called_once()
        self.hook.get_conn.return_value.__exit__.assert_called_once()

    @pytest.mark.parametrize(
        ("extra_kwargs", "actual_exit_code", "expected_exc"),
        [
            ({}, 0, None),
            ({}, 100, AirflowException),
            ({}, 101, AirflowException),
            ({"skip_on_exit_code": None}, 0, None),
            ({"skip_on_exit_code": None}, 100, AirflowException),
            ({"skip_on_exit_code": None}, 101, AirflowException),
            ({"skip_on_exit_code": 100}, 0, None),
            ({"skip_on_exit_code": 100}, 100, AirflowSkipException),
            ({"skip_on_exit_code": 100}, 101, AirflowException),
            ({"skip_on_exit_code": 0}, 0, AirflowSkipException),
            ({"skip_on_exit_code": [100]}, 0, None),
            ({"skip_on_exit_code": [100]}, 100, AirflowSkipException),
            ({"skip_on_exit_code": [100]}, 101, AirflowException),
            ({"skip_on_exit_code": [100, 102]}, 101, AirflowException),
            ({"skip_on_exit_code": (100,)}, 0, None),
            ({"skip_on_exit_code": (100,)}, 100, AirflowSkipException),
            ({"skip_on_exit_code": (100,)}, 101, AirflowException),
        ],
    )
    def test_skip(self, extra_kwargs, actual_exit_code, expected_exc):
        command = "not_a_real_command"
        self.exec_ssh_client_command.return_value = (actual_exit_code, b"", b"")

        operator = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=command,
            **extra_kwargs,
        )

        if expected_exc is None:
            operator.execute({})
        else:
            with pytest.raises(expected_exc):
                operator.execute({})

    def test_command_errored(self):
        # Test that run_ssh_client_command works on invalid commands
        command = "not_a_real_command"
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command=command,
        )
        self.exec_ssh_client_command.return_value = (1, b"", b"Error here")
        with pytest.raises(AirflowException, match="SSH operator error: exit status = 1"):
            task.execute(None)

    def test_push_ssh_exit_to_xcom(self, request, dag_maker):
        # Test pulls the value previously pushed to xcom and checks if it's the same
        command = "not_a_real_command"
        ssh_exit_code = random.randrange(1, 100)
        self.exec_ssh_client_command.return_value = (ssh_exit_code, b"", b"ssh output")

        with dag_maker(dag_id=f"dag_{request.node.name}") as dag:
            task = SSHOperator(task_id="push_xcom", ssh_hook=self.hook, command=command)
        dr = dag_maker.create_dagrun(run_id="push_xcom")
        if AIRFLOW_V_3_0_PLUS:
            sync_dag_to_db(dag)
            dag_version = DagVersion.get_latest_version(dag.dag_id)
            ti = TaskInstance(task=task, run_id=dr.run_id, dag_version_id=dag_version.id)
        else:
            ti = TaskInstance(task=task, run_id=dr.run_id)
        with pytest.raises(AirflowException, match=f"SSH operator error: exit status = {ssh_exit_code}"):
            dag_maker.run_ti("push_xcom", dr)
        assert ti.xcom_pull(task_ids=task.task_id, key="ssh_exit") == ssh_exit_code

    def test_timeout_triggers_on_kill(self, request, dag_maker):
        def command_sleep_forever(*args, **kwargs):
            time.sleep(100)  # This will be interrupted by the timeout

        self.exec_ssh_client_command.side_effect = command_sleep_forever

        with dag_maker(dag_id=f"dag_{request.node.name}"):
            _ = SSHOperator(
                task_id="test_timeout",
                ssh_hook=self.hook,
                command="sleep 100",
                execution_timeout=timedelta(seconds=1),
            )
        dr = dag_maker.create_dagrun(run_id="test_timeout")

        with mock.patch.object(SSHOperator, "on_kill") as mock_on_kill:
            with pytest.raises(AirflowTaskTimeout):
                dag_maker.run_ti("test_timeout", dr)

            # Wait a bit to ensure on_kill has time to be called
            time.sleep(1)

            mock_on_kill.assert_called_once()

    def test_remote_host_passed_at_hook_init(self):
        remote_host = "test_host.internal"
        task = SSHOperator(
            task_id="test_remote_host_passed",
            ssh_conn_id="ssh_default",
            remote_host=remote_host,
            command=COMMAND,
        )
        assert task.hook.remote_host == remote_host
