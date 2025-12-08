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
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.teradata.hooks.bteq import BteqHook


@pytest.fixture
def dummy_bteq_script():
    return "SELECT * FROM dbc.tables;"


@pytest.fixture
def dummy_remote_dir():
    return "/tmp"


@pytest.fixture
def dummy_encoding():
    return "utf-8"


@pytest.fixture
def dummy_password():
    return "dummy_password"


@pytest.fixture
def hook_without_ssh():
    return BteqHook(ssh_conn_id=None, teradata_conn_id="teradata_conn")


@patch("airflow.providers.teradata.hooks.bteq.SSHHook")
def test_init_sets_ssh_hook(mock_ssh_hook_class):
    mock_ssh_instance = MagicMock()
    mock_ssh_hook_class.return_value = mock_ssh_instance

    hook = BteqHook(ssh_conn_id="ssh_conn_id", teradata_conn_id="teradata_conn")

    # Validate the call and assignment
    mock_ssh_hook_class.assert_called_once_with(ssh_conn_id="ssh_conn_id")
    assert hook.ssh_hook == mock_ssh_instance


@patch("subprocess.Popen")
@patch.object(
    BteqHook,
    "get_conn",
    return_value={
        "host": "localhost",
        "login": "user",
        "password": "pass",
        "sp": None,
    },
)
@patch("airflow.providers.teradata.utils.bteq_util.verify_bteq_installed")
@patch("airflow.providers.teradata.utils.bteq_util.prepare_bteq_command_for_local_execution")
def test_execute_bteq_script_at_local_timeout(
    mock_prepare_cmd,
    mock_verify_bteq,
    mock_get_conn,
    mock_popen,
):
    hook = BteqHook(ssh_conn_id=None, teradata_conn_id="teradata_conn")

    # Create mock process with timeout simulation
    mock_process = MagicMock()
    mock_process.communicate.return_value = (b"some output", None)
    mock_process.wait.side_effect = subprocess.TimeoutExpired(cmd="bteq_command", timeout=5)
    mock_process.returncode = None
    mock_popen.return_value = mock_process
    mock_prepare_cmd.return_value = "bteq_command"

    with pytest.raises(AirflowException):
        hook.execute_bteq_script_at_local(
            bteq_script="SELECT * FROM test;",
            bteq_script_encoding="utf-8",
            timeout=5,
            timeout_rc=None,
            bteq_quit_rc=0,
            bteq_session_encoding=None,
            temp_file_read_encoding=None,
        )


@patch("subprocess.Popen")
@patch.object(
    BteqHook,
    "get_conn",
    return_value={
        "host": "localhost",
        "login": "user",
        "password": "pass",
        "sp": None,
    },
)
@patch("airflow.providers.teradata.hooks.bteq.verify_bteq_installed")  # <- patch here
@patch("airflow.providers.teradata.hooks.bteq.prepare_bteq_command_for_local_execution")  # <- patch here too
def test_execute_bteq_script_at_local_success(
    mock_prepare_cmd,
    mock_verify_bteq,
    mock_get_conn,
    mock_popen,
):
    hook = BteqHook(teradata_conn_id="teradata_conn")

    mock_process = MagicMock()
    mock_process.communicate.return_value = (b"Output line 1\nOutput line 2\n", None)
    mock_process.wait.return_value = 0
    mock_process.returncode = 0
    mock_popen.return_value = mock_process
    mock_prepare_cmd.return_value = "bteq_command"

    ret_code = hook.execute_bteq_script_at_local(
        bteq_script="SELECT * FROM test;",
        bteq_script_encoding="utf-8",
        timeout=10,
        timeout_rc=None,
        bteq_quit_rc=0,
        bteq_session_encoding=None,
        temp_file_read_encoding=None,
    )

    mock_verify_bteq.assert_called_once()
    mock_prepare_cmd.assert_called_once()
    mock_popen.assert_called_once_with(
        "bteq_command",
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        start_new_session=True,
    )
    assert ret_code == 0


@patch("subprocess.Popen")
@patch.object(
    BteqHook,
    "get_conn",
    return_value={
        "host": "localhost",
        "login": "user",
        "password": "pass",
        "sp": None,
    },
)
@patch("airflow.providers.teradata.hooks.bteq.verify_bteq_installed")
@patch("airflow.providers.teradata.hooks.bteq.prepare_bteq_command_for_local_execution")
def test_execute_bteq_script_at_local_failure_raises(
    mock_prepare_cmd,
    mock_verify_bteq,
    mock_get_conn,
    mock_popen,
):
    hook = BteqHook(ssh_conn_id=None, teradata_conn_id="teradata_conn")

    failure_message = "Failure: some error occurred"

    mock_process = MagicMock()
    # The output contains "Failure"
    mock_process.communicate.return_value = (failure_message.encode("utf-8"), None)
    mock_process.wait.return_value = 1
    mock_process.returncode = 1
    mock_popen.return_value = mock_process
    mock_prepare_cmd.return_value = "bteq_command"

    with pytest.raises(
        AirflowException,
        match="Failure while executing BTEQ script due to unexpected error.: Failure: some error occurred",
    ):
        hook.execute_bteq_script_at_local(
            bteq_script="SELECT * FROM test;",
            bteq_script_encoding="utf-8",
            timeout=10,
            timeout_rc=None,
            bteq_quit_rc=0,  # 1 is not allowed here
            bteq_session_encoding=None,
            temp_file_read_encoding=None,
        )


@pytest.fixture(autouse=False)
def patch_ssh_hook_class():
    # Patch SSHHook where bteq.py imports it
    with patch("airflow.providers.teradata.hooks.bteq.SSHHook") as mock_ssh_hook_class:
        mock_ssh_instance = MagicMock()
        mock_ssh_hook_class.return_value = mock_ssh_instance
        yield mock_ssh_hook_class


@pytest.fixture
def hook_with_ssh(patch_ssh_hook_class):
    # Now the BteqHook() call will use the patched SSHHook
    return BteqHook(ssh_conn_id="ssh_conn_id", teradata_conn_id="teradata_conn")


@patch("airflow.providers.teradata.hooks.bteq.SSHHook")
@patch("airflow.providers.teradata.hooks.bteq.verify_bteq_installed_remote")
@patch("airflow.providers.teradata.hooks.bteq.generate_random_password", return_value="test_password")
@patch("airflow.providers.teradata.hooks.bteq.generate_encrypted_file_with_openssl")
@patch("airflow.providers.teradata.hooks.bteq.transfer_file_sftp")
@patch(
    "airflow.providers.teradata.hooks.bteq.prepare_bteq_command_for_remote_execution",
    return_value="bteq_command",
)
@patch(
    "airflow.providers.teradata.hooks.bteq.decrypt_remote_file_to_string", return_value=(0, ["output"], [])
)
def test_execute_bteq_script_at_remote_success(
    mock_decrypt,
    mock_prepare_cmd,
    mock_transfer,
    mock_encrypt,
    mock_password,
    mock_verify,
    mock_ssh_hook_class,
):
    # Mock SSHHook instance and its get_conn() context manager
    mock_ssh_hook = MagicMock()
    mock_ssh_client = MagicMock()
    mock_ssh_hook.get_conn.return_value.__enter__.return_value = mock_ssh_client
    mock_ssh_hook_class.return_value = mock_ssh_hook

    # Mock exec_command to simulate 'uname || ver'
    mock_stdin = MagicMock()
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()
    mock_stdout.read.return_value = b"Linux\n"
    mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

    # Instantiate BteqHook

    hook = BteqHook(ssh_conn_id="ssh_conn_id", teradata_conn_id="teradata_conn")

    # Call method under test
    ret_code = hook.execute_bteq_script_at_remote(
        bteq_script="SELECT 1;",
        remote_working_dir="/tmp",
        bteq_script_encoding="utf-8",
        timeout=10,
        timeout_rc=None,
        bteq_session_encoding="utf-8",
        bteq_quit_rc=0,
        temp_file_read_encoding=None,
    )

    # Assert mocks called as expected
    mock_verify.assert_called_once_with(mock_ssh_client)
    mock_password.assert_called_once()
    mock_encrypt.assert_called_once()
    mock_transfer.assert_called_once()
    mock_prepare_cmd.assert_called_once()
    mock_decrypt.assert_called_once()

    # Assert the return code is what decrypt_remote_file_to_string returns (0 here)
    assert ret_code == 0


def test_on_kill_terminates_process(hook_without_ssh):
    process_mock = MagicMock()
    # Patch the hook's get_conn method to return a dict with the mocked process
    with patch.object(hook_without_ssh, "get_conn", return_value={"sp": process_mock}):
        hook_without_ssh.on_kill()

        process_mock.terminate.assert_called_once()
        process_mock.wait.assert_called_once()


def test_on_kill_no_process(hook_without_ssh):
    # Mock get_connection to avoid AirflowNotFoundException
    with patch.object(hook_without_ssh, "get_connection", return_value={"host": "dummy_host"}):
        # Provide a dummy conn dict to avoid errors
        with patch.object(hook_without_ssh, "get_conn", return_value={"sp": None}):
            # This should not raise any exceptions even if sp (process) is None
            hook_without_ssh.on_kill()


@patch("airflow.providers.teradata.hooks.bteq.verify_bteq_installed_remote")
def test_transfer_to_and_execute_bteq_on_remote_ssh_failure(mock_verify, hook_with_ssh):
    # Patch get_conn to simulate SSH failure by returning None
    hook_with_ssh.ssh_hook.get_conn = MagicMock(return_value=None)

    # Patch helper functions used in the tested function to avoid side effects
    with (
        patch("airflow.providers.teradata.hooks.bteq.generate_random_password", return_value="password"),
        patch("airflow.providers.teradata.hooks.bteq.generate_encrypted_file_with_openssl"),
        patch("airflow.providers.teradata.hooks.bteq.transfer_file_sftp"),
        patch(
            "airflow.providers.teradata.hooks.bteq.prepare_bteq_command_for_remote_execution",
            return_value="cmd",
        ),
        patch(
            "airflow.providers.teradata.hooks.bteq.decrypt_remote_file_to_string", return_value=(0, [], [])
        ),
    ):
        with pytest.raises(AirflowException) as excinfo:
            hook_with_ssh._transfer_to_and_execute_bteq_on_remote(
                file_path="/tmp/fakefile",
                remote_working_dir="/tmp",
                bteq_script_encoding="utf-8",
                timeout=10,
                timeout_rc=None,
                bteq_quit_rc=0,
                bteq_session_encoding="utf-8",
                tmp_dir="/tmp",
            )
        assert (
            "Failed to establish a SSH connection to the remote machine for executing the BTEQ script."
            in str(excinfo.value)
        )


@patch("airflow.providers.teradata.hooks.bteq.verify_bteq_installed_remote")
@patch("airflow.providers.teradata.hooks.bteq.generate_random_password", return_value="testpass")
@patch("airflow.providers.teradata.hooks.bteq.generate_encrypted_file_with_openssl")
@patch("airflow.providers.teradata.hooks.bteq.transfer_file_sftp")
@patch(
    "airflow.providers.teradata.hooks.bteq.prepare_bteq_command_for_remote_execution",
    return_value="bteq_remote_command",
)
@patch(
    "airflow.providers.teradata.hooks.bteq.decrypt_remote_file_to_string",
    side_effect=Exception("mocked exception"),
)
def test_remote_execution_cleanup_on_exception(
    mock_decrypt,
    mock_prepare,
    mock_transfer,
    mock_generate_enc,
    mock_generate_pass,
    mock_verify_remote,
    hook_with_ssh,
):
    temp_dir = "/tmp"
    local_file_path = os.path.join(temp_dir, "bteq_script.txt")
    remote_working_dir = temp_dir
    encrypted_file_path = os.path.join(temp_dir, "bteq_script.enc")

    # Create dummy local encrypted file
    with open(encrypted_file_path, "w") as f:
        f.write("dummy")

    # Simulate decrypt failing
    mock_decrypt.side_effect = Exception("mocked exception")

    # Patch exec_command for remote cleanup (identify_os, rm)
    ssh_client = hook_with_ssh.ssh_hook.get_conn.return_value.__enter__.return_value

    mock_stdin = MagicMock()
    mock_stdout = MagicMock()
    mock_stderr = MagicMock()

    # For identify_os ("uname || ver")
    mock_stdout.read.return_value = b"Linux\n"
    ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

    # Run the test
    with pytest.raises(AirflowException, match="mocked exception"):
        hook_with_ssh._transfer_to_and_execute_bteq_on_remote(
            file_path=local_file_path,
            remote_working_dir=remote_working_dir,
            bteq_script_encoding="utf-8",
            timeout=5,
            timeout_rc=None,
            bteq_quit_rc=0,
            bteq_session_encoding="utf-8",
            tmp_dir=temp_dir,
        )

    # After exception, encrypted file should be deleted
    assert not os.path.exists(encrypted_file_path)
