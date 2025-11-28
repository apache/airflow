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

import logging
import os
import shutil
import stat
import subprocess
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from paramiko import SSHClient


class TPTConfig:
    """Configuration constants for TPT operations."""

    DEFAULT_TIMEOUT = 5
    FILE_PERMISSIONS_READ_ONLY = 0o400
    TEMP_DIR_WINDOWS = "C:\\Windows\\Temp"
    TEMP_DIR_UNIX = "/tmp"


def execute_remote_command(ssh_client: SSHClient, command: str) -> tuple[int, str, str]:
    """
    Execute a command on remote host and properly manage SSH channels.

    :param ssh_client: SSH client connection
    :param command: Command to execute
    :return: Tuple of (exit_status, stdout, stderr)
    """
    stdin, stdout, stderr = ssh_client.exec_command(command)
    try:
        exit_status = stdout.channel.recv_exit_status()
        stdout_data = stdout.read().decode().strip()
        stderr_data = stderr.read().decode().strip()
        return exit_status, stdout_data, stderr_data
    finally:
        stdin.close()
        stdout.close()
        stderr.close()


def write_file(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def secure_delete(file_path: str, logger: logging.Logger | None = None) -> None:
    """
    Securely delete a file using shred if available, otherwise use os.remove.

    :param file_path: Path to the file to be deleted
    :param logger: Optional logger instance
    """
    logger = logger or logging.getLogger(__name__)
    if not os.path.exists(file_path):
        return

    try:
        # Check if shred is available
        if shutil.which("shred") is not None:
            # Use shred to securely delete the file
            subprocess.run(["shred", "--remove", file_path], check=True, timeout=TPTConfig.DEFAULT_TIMEOUT)
            logger.info("Securely removed file using shred: %s", file_path)
        else:
            # Fall back to regular deletion
            os.remove(file_path)
            logger.info("Removed file: %s", file_path)

    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.warning("Failed to remove file %s: %s", file_path, str(e))


def remote_secure_delete(
    ssh_client: SSHClient, remote_files: list[str], logger: logging.Logger | None = None
) -> None:
    """
    Securely delete remote files via SSH. Attempts shred first, falls back to rm if shred is unavailable.

    :param ssh_client: SSH client connection
    :param remote_files: List of remote file paths to delete
    :param logger: Optional logger instance
    """
    logger = logger or logging.getLogger(__name__)
    if not ssh_client or not remote_files:
        return

    try:
        # Detect remote OS
        remote_os = get_remote_os(ssh_client, logger)
        windows_remote = remote_os == "windows"

        # Check if shred is available on remote system (UNIX/Linux)
        shred_available = False
        if not windows_remote:
            exit_status, output, _ = execute_remote_command(ssh_client, "command -v shred")
            shred_available = exit_status == 0 and output.strip() != ""

        for file_path in remote_files:
            try:
                if windows_remote:
                    # Windows remote host - use del command
                    replace_slash = file_path.replace("/", "\\")
                    execute_remote_command(
                        ssh_client, f'if exist "{replace_slash}" del /f /q "{replace_slash}"'
                    )
                elif shred_available:
                    # UNIX/Linux with shred
                    execute_remote_command(ssh_client, f"shred --remove {file_path}")
                else:
                    # UNIX/Linux without shred - overwrite then delete
                    execute_remote_command(
                        ssh_client,
                        f"if [ -f {file_path} ]; then "
                        f"dd if=/dev/zero of={file_path} bs=4096 count=$(($(stat -c '%s' {file_path})/4096+1)) 2>/dev/null; "
                        f"rm -f {file_path}; fi",
                    )
            except Exception as e:
                logger.warning("Failed to process remote file %s: %s", file_path, str(e))

        logger.info("Processed remote files: %s", ", ".join(remote_files))
    except Exception as e:
        logger.warning("Failed to remove remote files: %s", str(e))


def terminate_subprocess(sp: subprocess.Popen | None, logger: logging.Logger | None = None) -> None:
    """
    Terminate a subprocess gracefully with proper error handling.

    :param sp: Subprocess to terminate
    :param logger: Optional logger instance
    """
    logger = logger or logging.getLogger(__name__)

    if not sp or sp.poll() is not None:
        # Process is None or already terminated
        return

    logger.info("Terminating subprocess (PID: %s)", sp.pid)

    try:
        sp.terminate()  # Attempt to terminate gracefully
        sp.wait(timeout=TPTConfig.DEFAULT_TIMEOUT)
        logger.info("Subprocess terminated gracefully")
    except subprocess.TimeoutExpired:
        logger.warning(
            "Subprocess did not terminate gracefully within %d seconds, killing it", TPTConfig.DEFAULT_TIMEOUT
        )
        try:
            sp.kill()
            sp.wait(timeout=2)  # Brief wait after kill
            logger.info("Subprocess killed successfully")
        except Exception as e:
            logger.error("Error killing subprocess: %s", str(e))
    except Exception as e:
        logger.error("Error terminating subprocess: %s", str(e))


def get_remote_os(ssh_client: SSHClient, logger: logging.Logger | None = None) -> str:
    """
    Detect the operating system of the remote host via SSH.

    :param ssh_client: SSH client connection
    :param logger: Optional logger instance
    :return: Operating system type as string ('windows' or 'unix')
    """
    logger = logger or logging.getLogger(__name__)

    if not ssh_client:
        logger.warning("No SSH client provided for OS detection")
        return "unix"

    try:
        # Check for Windows first
        exit_status, stdout_data, stderr_data = execute_remote_command(ssh_client, "echo %OS%")

        if "Windows" in stdout_data:
            return "windows"

        # All other systems are treated as Unix-like
        return "unix"

    except Exception as e:
        logger.error("Error detecting remote OS: %s", str(e))
        return "unix"


def set_local_file_permissions(local_file_path: str, logger: logging.Logger | None = None) -> None:
    """
    Set permissions for a local file to be read-only for the owner.

    :param local_file_path: Path to the local file
    :param logger: Optional logger instance
    :raises FileNotFoundError: If the file does not exist
    :raises OSError: If setting permissions fails
    """
    logger = logger or logging.getLogger(__name__)

    if not local_file_path:
        logger.warning("No file path provided for permission setting")
        return

    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"File does not exist: {local_file_path}")

    try:
        # Set file permission to read-only for the owner (400)
        os.chmod(local_file_path, TPTConfig.FILE_PERMISSIONS_READ_ONLY)
        logger.info("Set read-only permissions for file %s", local_file_path)
    except (OSError, PermissionError) as e:
        raise OSError(f"Error setting permissions for local file {local_file_path}: {e}") from e


def _set_windows_file_permissions(
    ssh_client: SSHClient, remote_file_path: str, logger: logging.Logger
) -> None:
    """Set restrictive permissions on Windows remote file."""
    command = f'icacls "{remote_file_path}" /inheritance:r /grant:r "%USERNAME%":R'

    exit_status, stdout_data, stderr_data = execute_remote_command(ssh_client, command)

    if exit_status != 0:
        raise RuntimeError(
            f"Failed to set restrictive permissions on Windows remote file {remote_file_path}. "
            f"Exit status: {exit_status}, Error: {stderr_data if stderr_data else 'N/A'}"
        )
    logger.info("Set restrictive permissions (owner read-only) for Windows remote file %s", remote_file_path)


def _set_unix_file_permissions(ssh_client: SSHClient, remote_file_path: str, logger: logging.Logger) -> None:
    """Set read-only permissions on Unix/Linux remote file."""
    command = f"chmod 400 {remote_file_path}"

    exit_status, stdout_data, stderr_data = execute_remote_command(ssh_client, command)

    if exit_status != 0:
        raise RuntimeError(
            f"Failed to set permissions (400) on remote file {remote_file_path}. "
            f"Exit status: {exit_status}, Error: {stderr_data if stderr_data else 'N/A'}"
        )
    logger.info("Set read-only permissions for remote file %s", remote_file_path)


def set_remote_file_permissions(
    ssh_client: SSHClient, remote_file_path: str, logger: logging.Logger | None = None
) -> None:
    """
    Set permissions for a remote file to be read-only for the owner.

    :param ssh_client: SSH client connection
    :param remote_file_path: Path to the remote file
    :param logger: Optional logger instance
    :raises RuntimeError: If permission setting fails
    """
    logger = logger or logging.getLogger(__name__)

    if not ssh_client or not remote_file_path:
        logger.warning(
            "Invalid parameters: ssh_client=%s, remote_file_path=%s", bool(ssh_client), remote_file_path
        )
        return

    try:
        # Detect remote OS once
        remote_os = get_remote_os(ssh_client, logger)

        if remote_os == "windows":
            _set_windows_file_permissions(ssh_client, remote_file_path, logger)
        else:
            _set_unix_file_permissions(ssh_client, remote_file_path, logger)

    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Error setting permissions for remote file {remote_file_path}: {e}") from e


def get_remote_temp_directory(ssh_client: SSHClient, logger: logging.Logger | None = None) -> str:
    """
    Get the remote temporary directory path based on the operating system.

    :param ssh_client: SSH client connection
    :param logger: Optional logger instance
    :return: Path to the remote temporary directory
    """
    logger = logger or logging.getLogger(__name__)

    try:
        # Detect OS once
        remote_os = get_remote_os(ssh_client, logger)

        if remote_os == "windows":
            exit_status, temp_dir, stderr_data = execute_remote_command(ssh_client, "echo %TEMP%")

            if exit_status == 0 and temp_dir and temp_dir != "%TEMP%":
                return temp_dir
            logger.warning("Could not get TEMP directory, using default: %s", TPTConfig.TEMP_DIR_WINDOWS)
            return TPTConfig.TEMP_DIR_WINDOWS

        # Unix/Linux - use /tmp
        return TPTConfig.TEMP_DIR_UNIX

    except Exception as e:
        logger.warning("Error getting remote temp directory: %s", str(e))
        return TPTConfig.TEMP_DIR_UNIX


def is_valid_file(file_path: str) -> bool:
    return os.path.isfile(file_path)


def verify_tpt_utility_installed(utility: str) -> None:
    """Verify if a TPT utility (e.g., tbuild) is installed and available in the system's PATH."""
    if shutil.which(utility) is None:
        raise FileNotFoundError(
            f"TPT utility '{utility}' is not installed or not available in the system's PATH"
        )


def verify_tpt_utility_on_remote_host(
    ssh_client: SSHClient, utility: str, logger: logging.Logger | None = None
) -> None:
    """
    Verify if a TPT utility (tbuild) is installed on the remote host via SSH.

    :param ssh_client: SSH client connection
    :param utility: Name of the utility to verify
    :param logger: Optional logger instance
    :raises FileNotFoundError: If utility is not found on remote host
    :raises RuntimeError: If verification fails unexpectedly
    """
    logger = logger or logging.getLogger(__name__)

    try:
        # Detect remote OS once
        remote_os = get_remote_os(ssh_client, logger)

        if remote_os == "windows":
            command = f"where {utility}"
        else:
            command = f"which {utility}"

        exit_status, output, error = execute_remote_command(ssh_client, command)

        if exit_status != 0 or not output:
            raise FileNotFoundError(
                f"TPT utility '{utility}' is not installed or not available in PATH on the remote host. "
                f"Command: {command}, Exit status: {exit_status}, "
                f"stderr: {error if error else 'N/A'}"
            )

        logger.info("TPT utility '%s' found at: %s", utility, output.split("\n")[0])

    except (FileNotFoundError, RuntimeError):
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to verify TPT utility '{utility}' on remote host: {e}") from e


def prepare_tpt_ddl_script(
    sql: list[str],
    error_list: list[int] | None,
    source_conn: dict[str, Any],
    job_name: str | None = None,
) -> str:
    """
    Prepare a TPT script for executing DDL statements.

    This method generates a TPT script that defines a DDL operator and applies the provided SQL statements.
    It also supports specifying a list of error codes to handle during the operation.

    :param sql: A list of DDL statements to execute.
    :param error_list: A list of error codes to handle during the operation.
    :param source_conn: Connection details for the source database.
    :param job_name: The name of the TPT job. Defaults to unique name if None.
    :return: A formatted TPT script as a string.
    :raises ValueError: If the SQL statement list is empty.
    """
    if not sql or not isinstance(sql, list):
        raise ValueError("SQL statement list must be a non-empty list")

    # Clean and escape each SQL statement:
    sql_statements = [
        stmt.strip().rstrip(";").replace("'", "''")
        for stmt in sql
        if stmt and isinstance(stmt, str) and stmt.strip()
    ]

    if not sql_statements:
        raise ValueError("No valid SQL statements found in the provided input")

    # Format for TPT APPLY block, indenting after the first line
    apply_sql = ",\n".join(
        [f"('{stmt};')" if i == 0 else f"            ('{stmt};')" for i, stmt in enumerate(sql_statements)]
    )

    if job_name is None:
        job_name = f"airflow_tptddl_{uuid.uuid4().hex}"

    # Format error list for inclusion in the TPT script
    if not error_list:
        error_list_stmt = "ErrorList = ['']"
    else:
        error_list_str = ", ".join([f"'{error}'" for error in error_list])
        error_list_stmt = f"ErrorList = [{error_list_str}]"

    host = source_conn["host"]
    login = source_conn["login"]
    password = source_conn["password"]

    tpt_script = f"""
        DEFINE JOB {job_name}
        DESCRIPTION 'TPT DDL Operation'
        (
        APPLY
            {apply_sql}
        TO OPERATOR ( $DDL ()
            ATTR
            (
                TdpId = '{host}',
                UserName = '{login}',
                UserPassword = '{password}',
                {error_list_stmt}
            )
        );
        );
        """
    return tpt_script


def prepare_tdload_job_var_file(
    mode: str,
    source_table: str | None,
    select_stmt: str | None,
    insert_stmt: str | None,
    target_table: str | None,
    source_file_name: str | None,
    target_file_name: str | None,
    source_format: str,
    target_format: str,
    source_text_delimiter: str,
    target_text_delimiter: str,
    source_conn: dict[str, Any],
    target_conn: dict[str, Any] | None = None,
) -> str:
    """
    Prepare a tdload job variable file based on the specified mode.

    :param mode: The operation mode ('file_to_table', 'table_to_file', or 'table_to_table')
    :param source_table: Name of the source table
    :param select_stmt: SQL SELECT statement for data extraction
    :param insert_stmt: SQL INSERT statement for data loading
    :param target_table: Name of the target table
    :param source_file_name: Path to the source file
    :param target_file_name: Path to the target file
    :param source_format: Format of source data
    :param target_format: Format of target data
    :param source_text_delimiter: Source text delimiter
    :param target_text_delimiter: Target text delimiter
    :return: The content of the job variable file
    :raises ValueError: If invalid parameters are provided
    """
    # Create a dictionary to store job variables
    job_vars = {}

    # Add appropriate parameters based on the mode
    if mode == "file_to_table":
        job_vars.update(
            {
                "TargetTdpId": source_conn["host"],
                "TargetUserName": source_conn["login"],
                "TargetUserPassword": source_conn["password"],
                "TargetTable": target_table,
                "SourceFileName": source_file_name,
            }
        )
        if insert_stmt:
            job_vars["InsertStmt"] = insert_stmt

    elif mode == "table_to_file":
        job_vars.update(
            {
                "SourceTdpId": source_conn["host"],
                "SourceUserName": source_conn["login"],
                "SourceUserPassword": source_conn["password"],
                "TargetFileName": target_file_name,
            }
        )

        if source_table:
            job_vars["SourceTable"] = source_table
        elif select_stmt:
            job_vars["SourceSelectStmt"] = select_stmt

    elif mode == "table_to_table":
        if target_conn is None:
            raise ValueError("target_conn must be provided for 'table_to_table' mode")
        job_vars.update(
            {
                "SourceTdpId": source_conn["host"],
                "SourceUserName": source_conn["login"],
                "SourceUserPassword": source_conn["password"],
                "TargetTdpId": target_conn["host"],
                "TargetUserName": target_conn["login"],
                "TargetUserPassword": target_conn["password"],
                "TargetTable": target_table,
            }
        )

        if source_table:
            job_vars["SourceTable"] = source_table
        elif select_stmt:
            job_vars["SourceSelectStmt"] = select_stmt
        if insert_stmt:
            job_vars["InsertStmt"] = insert_stmt

    # Add common parameters if not empty
    if source_format:
        job_vars["SourceFormat"] = source_format
    if target_format:
        job_vars["TargetFormat"] = target_format
    if source_text_delimiter:
        job_vars["SourceTextDelimiter"] = source_text_delimiter
    if target_text_delimiter:
        job_vars["TargetTextDelimiter"] = target_text_delimiter

    # Format job variables content
    job_var_content = "".join([f"{key}='{value}',\n" for key, value in job_vars.items()])
    job_var_content = job_var_content.rstrip(",\n")

    return job_var_content


def is_valid_remote_job_var_file(
    ssh_client: SSHClient, remote_job_var_file_path: str, logger: logging.Logger | None = None
) -> bool:
    """Check if the given remote job variable file path is a valid file."""
    if remote_job_var_file_path:
        sftp_client = ssh_client.open_sftp()
        try:
            # Get file metadata
            file_stat = sftp_client.stat(remote_job_var_file_path)
            if file_stat.st_mode:
                is_regular_file = stat.S_ISREG(file_stat.st_mode)
                return is_regular_file
            return False
        except FileNotFoundError:
            if logger:
                logger.error("File does not exist on remote at : %s", remote_job_var_file_path)
            return False
        finally:
            sftp_client.close()
    else:
        return False


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


def decrypt_remote_file(
    ssh_client: SSHClient,
    remote_enc_file: str,
    remote_dec_file: str,
    password: str,
    logger: logging.Logger | None = None,
) -> int:
    """
    Decrypt a remote file using OpenSSL.

    :param ssh_client: SSH client connection
    :param remote_enc_file: Path to the encrypted file
    :param remote_dec_file: Path for the decrypted file
    :param password: Decryption password
    :param logger: Optional logger instance
    :return: Exit status of the decryption command
    :raises RuntimeError: If decryption fails
    """
    logger = logger or logging.getLogger(__name__)

    # Detect remote OS
    remote_os = get_remote_os(ssh_client, logger)
    windows_remote = remote_os == "windows"

    if windows_remote:
        # Windows - use different quoting and potentially different OpenSSL parameters
        password_escaped = password.replace('"', '""')  # Escape double quotes for Windows
        decrypt_cmd = (
            f'openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass pass:"{password_escaped}" '
            f'-in "{remote_enc_file}" -out "{remote_dec_file}"'
        )
    else:
        # Unix/Linux - use single quote escaping
        password_escaped = password.replace("'", "'\\''")  # Escape single quotes
        decrypt_cmd = (
            f"openssl enc -d -aes-256-cbc -salt -pbkdf2 -pass pass:'{password_escaped}' "
            f"-in {remote_enc_file} -out {remote_dec_file}"
        )

    exit_status, stdout_data, stderr_data = execute_remote_command(ssh_client, decrypt_cmd)

    if exit_status != 0:
        raise RuntimeError(
            f"Decryption failed with exit status {exit_status}. Error: {stderr_data if stderr_data else 'N/A'}"
        )

    logger.info("Successfully decrypted remote file %s to %s", remote_enc_file, remote_dec_file)
    return exit_status


def transfer_file_sftp(
    ssh_client: SSHClient, local_path: str, remote_path: str, logger: logging.Logger | None = None
) -> None:
    """
    Transfer a file from local to remote host using SFTP.

    :param ssh_client: SSH client connection
    :param local_path: Local file path
    :param remote_path: Remote file path
    :param logger: Optional logger instance
    :raises FileNotFoundError: If local file does not exist
    :raises RuntimeError: If file transfer fails
    """
    logger = logger or logging.getLogger(__name__)

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local file does not exist: {local_path}")

    sftp = None
    try:
        sftp = ssh_client.open_sftp()
        sftp.put(local_path, remote_path)
        logger.info("Successfully transferred file from %s to %s", local_path, remote_path)
    except Exception as e:
        raise RuntimeError(f"Failed to transfer file from {local_path} to {remote_path}: {e}") from e
    finally:
        if sftp:
            sftp.close()
