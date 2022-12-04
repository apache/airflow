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

from random import randrange
from unittest import mock

import pendulum
import pytest
from paramiko.client import SSHClient

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.session import create_session
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars

TEST_DAG_ID = "unit_tests_ssh_test_op"
TEST_CONN_ID = "conn_id_for_testing"
DEFAULT_TIMEOUT = 10
CONN_TIMEOUT = 5
CMD_TIMEOUT = 7
TIMEOUT = 12
DEFAULT_DATE = datetime(2017, 1, 1)
COMMAND = "echo -n airflow"
COMMAND_WITH_SUDO = "sudo " + COMMAND


def create_context(task, persist_to_db=False, map_index=None):
    if task.has_dag():
        dag = task.dag
    else:
        dag = DAG(dag_id="dag", start_date=pendulum.now())
        dag.add_task(task)
    dag_run = DagRun(
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE),
        run_type=DagRunType.MANUAL,
        dag_id=dag.dag_id,
    )
    task_instance = TaskInstance(task=task, run_id=dag_run.run_id)
    task_instance.dag_run = dag_run
    if map_index is not None:
        task_instance.map_index = map_index
    if persist_to_db:
        with create_session() as session:
            session.add(DagModel(dag_id=dag.dag_id))
            session.add(dag_run)
            session.add(task_instance)
            session.commit()
    return {
        "dag": dag,
        "ts": DEFAULT_DATE.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": "test",
    }


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
    def _patch_exec_ssh_client(self):
        with mock.patch.object(self.hook, "exec_ssh_client_command") as exec_ssh_client_command:
            self.exec_ssh_client_command = exec_ssh_client_command
            exec_ssh_client_command.return_value = (0, b"airflow", "")
            yield exec_ssh_client_command

    def test_hook_created_correctly(self):
        conn_timeout = 20
        cmd_timeout = 45
        task = SSHOperator(
            task_id="test",
            command=COMMAND,
            conn_timeout=conn_timeout,
            cmd_timeout=cmd_timeout,
            ssh_conn_id="ssh_default",
        )
        ssh_hook = task.get_hook()
        assert conn_timeout == ssh_hook.conn_timeout
        assert "ssh_default" == ssh_hook.ssh_conn_id

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
                mock.ANY, COMMAND, timeout=10, environment={"TEST": "value"}, get_pty=False
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
                task_id="test_4",
                ssh_hook=self.hook,
                command=None,
            ).execute(None)
            task_0.execute(None)

    @pytest.mark.parametrize(
        "command, get_pty_in, get_pty_out",
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
        # Ensure connection gets closed once (via context_manager)
        task = SSHOperator(
            task_id="test",
            ssh_hook=self.hook,
            command="ls",
        )
        task.execute()
        self.hook.get_conn.assert_called_once()
        self.hook.get_conn.return_value.__exit__.assert_called_once()

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

    def test_push_ssh_exit_to_xcom(self, context=None):
        # Test pulls the value previously pushed to xcom and checks if it's the same
        command = "not_a_real_command"
        ssh_exit_code = randrange(0, 100)
        task_push = SSHOperator(task_id="test_push", ssh_hook=self.hook, command=command)
        task_context = create_context(task_push, persist_to_db=True)
        self.exec_ssh_client_command.return_value = (ssh_exit_code, b"", b"ssh output")
        try:
            task_push.execute(context=task_context)
        except AirflowException:
            pass
        finally:
            assert (
                task_push.xcom_pull(key="ssh_exit", context=task_context, task_ids=["test_push"])[0]
                == ssh_exit_code
            )
