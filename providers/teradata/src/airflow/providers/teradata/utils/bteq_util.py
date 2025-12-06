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
import shutil
import stat
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from paramiko import SSHClient

from airflow.providers.common.compat.sdk import AirflowException


def identify_os(ssh_client: SSHClient) -> str:
    stdin, stdout, stderr = ssh_client.exec_command("uname || ver")
    return stdout.read().decode().lower()


def verify_bteq_installed():
    """Verify if BTEQ is installed and available in the system's PATH."""
    if shutil.which("bteq") is None:
        raise AirflowException("BTEQ is not installed or not available in the system's PATH.")


def verify_bteq_installed_remote(ssh_client: SSHClient):
    """Verify if BTEQ is installed on the remote machine."""
    # Detect OS
    os_info = identify_os(ssh_client)

    if "windows" in os_info:
        check_cmd = "where bteq"
    elif "darwin" in os_info:
        # Check if zsh exists first
        stdin, stdout, stderr = ssh_client.exec_command("command -v zsh")
        zsh_path = stdout.read().strip()
        if zsh_path:
            check_cmd = 'zsh -l -c "which bteq"'
        else:
            check_cmd = "which bteq"
    else:
        check_cmd = "which bteq"

    stdin, stdout, stderr = ssh_client.exec_command(check_cmd)
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().strip()
    error = stderr.read().strip()

    if exit_status != 0 or not output:
        raise AirflowException(
            f"BTEQ is not installed or not available in PATH. stderr: {error.decode() if error else 'N/A'}"
        )


def transfer_file_sftp(ssh_client, local_path, remote_path):
    sftp = ssh_client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()


def get_remote_tmp_dir(ssh_client):
    os_info = identify_os(ssh_client)

    if "windows" in os_info:
        # Try getting Windows temp dir
        stdin, stdout, stderr = ssh_client.exec_command("echo %TEMP%")
        tmp_dir = stdout.read().decode().strip()
        if not tmp_dir:
            tmp_dir = "C:\\Temp"
    else:
        tmp_dir = "/tmp"
    return tmp_dir


# We can not pass host details with bteq command when executing on remote machine. Instead, we will prepare .logon in bteq script itself to avoid risk of
# exposing sensitive information
def prepare_bteq_script_for_remote_execution(conn: dict[str, Any], sql: str) -> str:
    """Build a BTEQ script with necessary connection and session commands."""
    script_lines = []
    host = conn["host"]
    login = conn["login"]
    password = conn["password"]
    script_lines.append(f" .LOGON {host}/{login},{password}")
    return _prepare_bteq_script(script_lines, sql)


def prepare_bteq_script_for_local_execution(
    sql: str,
) -> str:
    """Build a BTEQ script with necessary connection and session commands."""
    script_lines: list[str] = []
    return _prepare_bteq_script(script_lines, sql)


def _prepare_bteq_script(script_lines: list[str], sql: str) -> str:
    script_lines.append(sql.strip())
    script_lines.append(".EXIT")
    return "\n".join(script_lines)


def _prepare_bteq_command(
    timeout: int,
    bteq_script_encoding: str,
    bteq_session_encoding: str,
    timeout_rc: int,
) -> list[str]:
    bteq_core_cmd = ["bteq"]
    if bteq_session_encoding:
        bteq_core_cmd.append(f" -e {bteq_script_encoding}")
        bteq_core_cmd.append(f" -c {bteq_session_encoding}")
    bteq_core_cmd.append('"')
    bteq_core_cmd.append(f".SET EXITONDELAY ON MAXREQTIME {timeout}")
    if timeout_rc is not None and timeout_rc >= 0:
        bteq_core_cmd.append(f" RC {timeout_rc}")
    bteq_core_cmd.append(";")
    # Airflow doesn't display the script of BTEQ in UI but only in log so WIDTH is 500 enough
    bteq_core_cmd.append(" .SET WIDTH 500;")
    return bteq_core_cmd


def prepare_bteq_command_for_remote_execution(
    timeout: int,
    bteq_script_encoding: str,
    bteq_session_encoding: str,
    timeout_rc: int,
) -> str:
    """Prepare the BTEQ command with necessary parameters."""
    bteq_core_cmd = _prepare_bteq_command(timeout, bteq_script_encoding, bteq_session_encoding, timeout_rc)
    bteq_core_cmd.append('"')
    return " ".join(bteq_core_cmd)


def prepare_bteq_command_for_local_execution(
    conn: dict[str, Any],
    timeout: int,
    bteq_script_encoding: str,
    bteq_session_encoding: str,
    timeout_rc: int,
) -> str:
    """Prepare the BTEQ command with necessary parameters."""
    bteq_core_cmd = _prepare_bteq_command(timeout, bteq_script_encoding, bteq_session_encoding, timeout_rc)
    host = conn["host"]
    login = conn["login"]
    password = conn["password"]
    bteq_core_cmd.append(f" .LOGON {host}/{login},{password}")
    bteq_core_cmd.append('"')
    bteq_command_str = " ".join(bteq_core_cmd)
    return bteq_command_str


def is_valid_file(file_path: str) -> bool:
    return os.path.isfile(file_path)


def is_valid_encoding(file_path: str, encoding: str = "UTF-8") -> bool:
    """
    Check if the file can be read with the specified encoding.

    :param file_path: Path to the file to be checked.
    :param encoding: Encoding to use for reading the file.
    :return: True if the file can be read with the specified encoding, False otherwise.
    """
    with open(file_path, encoding=encoding) as f:
        f.read()
        return True


def read_file(file_path: str, encoding: str = "UTF-8") -> str:
    """
    Read the content of a file with the specified encoding.

    :param file_path: Path to the file to be read.
    :param encoding: Encoding to use for reading the file.
    :return: Content of the file as a string.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, encoding=encoding) as f:
        return f.read()


def is_valid_remote_bteq_script_file(ssh_client: SSHClient, remote_file_path: str, logger=None) -> bool:
    """Check if the given remote file path is a valid BTEQ script file."""
    if remote_file_path:
        sftp_client = ssh_client.open_sftp()
        try:
            # Get file metadata
            file_stat = sftp_client.stat(remote_file_path)
            if file_stat.st_mode:
                is_regular_file = stat.S_ISREG(file_stat.st_mode)
                return is_regular_file
            return False
        except FileNotFoundError:
            if logger:
                logger.error("File does not exist on remote at : %s", remote_file_path)
            return False
        finally:
            sftp_client.close()
    else:
        return False
