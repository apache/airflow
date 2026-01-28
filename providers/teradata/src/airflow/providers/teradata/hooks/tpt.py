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
import socket
import subprocess
import tempfile
import uuid
from collections.abc import Generator
from contextlib import contextmanager

from paramiko import SSHException

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.teradata.hooks.ttu import TtuHook
from airflow.providers.teradata.utils.encryption_utils import (
    generate_encrypted_file_with_openssl,
    generate_random_password,
)
from airflow.providers.teradata.utils.tpt_util import (
    decrypt_remote_file,
    execute_remote_command,
    remote_secure_delete,
    secure_delete,
    set_local_file_permissions,
    set_remote_file_permissions,
    terminate_subprocess,
    transfer_file_sftp,
    verify_tpt_utility_on_remote_host,
    write_file,
)


class TptHook(TtuHook):
    """
    Hook for executing Teradata Parallel Transporter (TPT) operations.

    This hook provides methods to execute TPT operations both locally and remotely via SSH.
    It supports DDL operations using tbuild utility. and data loading operations using tdload.
    It extends the `TtuHook` and integrates with Airflow's SSHHook for remote execution.

    The TPT operations are used to interact with Teradata databases for DDL operations
    such as creating, altering, or dropping tables and high-performance data loading and
    DDL operations.

    Features:
    - Supports both local and remote execution of TPT operations.
    - Secure file encryption for remote transfers.
    - Comprehensive error handling and logging.
    - Resource cleanup and management.

    .. seealso::
        - :ref:`hook API connection <howto/connection:teradata>`

    :param ssh_conn_id: SSH connection ID for remote execution. If None, executes locally.
    """

    def __init__(self, ssh_conn_id: str | None = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id) if ssh_conn_id else None

    def execute_ddl(
        self,
        tpt_script: str | list[str],
        remote_working_dir: str,
    ) -> int:
        """
        Execute a DDL statement using TPT.

        Args:
            tpt_script: TPT script content as string or list of strings
            remote_working_dir: Remote working directory for SSH execution

        Returns:
            Exit code from the TPT operation

        Raises:
            ValueError: If tpt_script is empty or invalid
            RuntimeError: Non-zero tbuild exit status or unexpected execution failure
            ConnectionError: SSH connection not established or fails
            TimeoutError: SSH connection/network timeout
            FileNotFoundError: tbuild binary not found in PATH
        """
        if not tpt_script:
            raise ValueError("TPT script must not be empty.")

        tpt_script_content = "\n".join(tpt_script) if isinstance(tpt_script, list) else tpt_script

        # Validate script content
        if not tpt_script_content.strip():
            raise ValueError("TPT script content must not be empty after processing.")

        if self.ssh_hook:
            self.log.info("Executing DDL statements via SSH on remote host")
            return self._execute_tbuild_via_ssh(tpt_script_content, remote_working_dir)
        self.log.info("Executing DDL statements locally")
        return self._execute_tbuild_locally(tpt_script_content)

    def _execute_tbuild_via_ssh(
        self,
        tpt_script_content: str,
        remote_working_dir: str,
    ) -> int:
        """Execute tbuild command via SSH."""
        with self.preferred_temp_directory() as tmp_dir:
            local_script_file = os.path.join(tmp_dir, f"tbuild_script_{uuid.uuid4().hex}.sql")
            write_file(local_script_file, tpt_script_content)
            encrypted_file_path = f"{local_script_file}.enc"
            remote_encrypted_script_file = os.path.join(
                remote_working_dir, os.path.basename(encrypted_file_path)
            )
            remote_script_file = os.path.join(remote_working_dir, os.path.basename(local_script_file))
            job_name = f"tbuild_job_{uuid.uuid4().hex}"

            try:
                if not self.ssh_hook:
                    raise ConnectionError("SSH connection is not established. `ssh_hook` is None or invalid.")
                with self.ssh_hook.get_conn() as ssh_client:
                    verify_tpt_utility_on_remote_host(ssh_client, "tbuild", logging.getLogger(__name__))
                    password = generate_random_password()
                    generate_encrypted_file_with_openssl(local_script_file, password, encrypted_file_path)
                    transfer_file_sftp(
                        ssh_client,
                        encrypted_file_path,
                        remote_encrypted_script_file,
                        logging.getLogger(__name__),
                    )
                    decrypt_remote_file(
                        ssh_client,
                        remote_encrypted_script_file,
                        remote_script_file,
                        password,
                        logging.getLogger(__name__),
                    )

                    set_remote_file_permissions(ssh_client, remote_script_file, logging.getLogger(__name__))

                    tbuild_cmd = ["tbuild", "-f", remote_script_file, job_name]
                    self.log.info("Executing tbuild command on remote server: %s", " ".join(tbuild_cmd))
                    exit_status, output, error = execute_remote_command(ssh_client, " ".join(tbuild_cmd))
                    self.log.info("tbuild command output:\n%s", output)
                    self.log.info("tbuild command exited with status %s", exit_status)

                    # Clean up remote files before checking exit status
                    remote_secure_delete(
                        ssh_client,
                        [remote_encrypted_script_file, remote_script_file],
                        logging.getLogger(__name__),
                    )

                    if exit_status != 0:
                        raise RuntimeError(f"tbuild command failed with exit code {exit_status}: {error}")

                    return exit_status
            except ConnectionError:
                # Re-raise ConnectionError as-is (don't convert to TimeoutError)
                raise
            except (OSError, socket.gaierror) as e:
                self.log.error("SSH connection timed out: %s", str(e))
                raise TimeoutError(
                    "SSH connection timed out. Please check the network or server availability."
                ) from e
            except SSHException as e:
                raise ConnectionError(f"SSH error during connection: {str(e)}") from e
            except RuntimeError:
                raise
            except Exception as e:
                raise RuntimeError(
                    f"Unexpected error while executing tbuild script on remote machine: {str(e)}"
                ) from e
            finally:
                # Clean up local files
                secure_delete(encrypted_file_path, logging.getLogger(__name__))
                secure_delete(local_script_file, logging.getLogger(__name__))

    def _execute_tbuild_locally(
        self,
        tpt_script_content: str,
    ) -> int:
        """Execute tbuild command locally."""
        with self.preferred_temp_directory() as tmp_dir:
            local_script_file = os.path.join(tmp_dir, f"tbuild_script_{uuid.uuid4().hex}.sql")
            write_file(local_script_file, tpt_script_content)
            # Set file permission to read-only for the current user (no permissions for group/others)
            set_local_file_permissions(local_script_file, logging.getLogger(__name__))

            job_name = f"tbuild_job_{uuid.uuid4().hex}"
            tbuild_cmd = ["tbuild", "-f", local_script_file, job_name]

            if not shutil.which("tbuild"):
                raise FileNotFoundError("tbuild binary not found in PATH.")

            sp = None
            try:
                self.log.info("Executing tbuild command: %s", " ".join(tbuild_cmd))
                sp = subprocess.Popen(
                    tbuild_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True
                )
                error_lines = []
                if sp.stdout is not None:
                    for line in iter(sp.stdout.readline, b""):
                        decoded_line = line.decode("UTF-8").strip()
                        self.log.info(decoded_line)
                        if "error" in decoded_line.lower():
                            error_lines.append(decoded_line)
                sp.wait()
                self.log.info("tbuild command exited with return code %s", sp.returncode)
                if sp.returncode != 0:
                    error_msg = "\n".join(error_lines) if error_lines else "Unknown error"
                    raise RuntimeError(f"tbuild command failed with return code {sp.returncode}: {error_msg}")
                return sp.returncode
            except RuntimeError:
                raise
            except Exception as e:
                self.log.error("Error executing tbuild command: %s", str(e))
                raise RuntimeError(f"Error executing tbuild command: {str(e)}") from e
            finally:
                secure_delete(local_script_file, logging.getLogger(__name__))
                terminate_subprocess(sp, logging.getLogger(__name__))

    def execute_tdload(
        self,
        remote_working_dir: str,
        job_var_content: str | None = None,
        tdload_options: str | None = None,
        tdload_job_name: str | None = None,
    ) -> int:
        """
        Execute a tdload operation using the tdload command-line utility.

        Args:
            remote_working_dir: Remote working directory for SSH execution
            job_var_content: Content of the job variable file
            tdload_options: Additional command-line options for tdload
            tdload_job_name: Name for the tdload job

        Returns:
            Exit code from the tdload operation

        Raises:
            RuntimeError: Non-zero tdload exit status or unexpected execution failure
            ConnectionError: SSH connection not established or fails
            TimeoutError: SSH connection/network timeout
            FileNotFoundError: tdload binary not found in PATH
        """
        tdload_job_name = tdload_job_name or f"tdload_job_{uuid.uuid4().hex}"
        if self.ssh_hook:
            self.log.info("Executing tdload via SSH on remote host with job name: %s", tdload_job_name)
            return self._execute_tdload_via_ssh(
                remote_working_dir, job_var_content, tdload_options, tdload_job_name
            )
        self.log.info("Executing tdload locally with job name: %s", tdload_job_name)
        return self._execute_tdload_locally(job_var_content, tdload_options, tdload_job_name)

    def _execute_tdload_via_ssh(
        self,
        remote_working_dir: str,
        job_var_content: str | None,
        tdload_options: str | None,
        tdload_job_name: str | None,
    ) -> int:
        """
        Write job_var_content to a temporary file, then transfer and execute it on the remote host.

        Args:
            remote_working_dir: Remote working directory
            job_var_content: Content for the job variable file
            tdload_options: Additional tdload command options
            tdload_job_name: Name for the tdload job

        Returns:
            Exit code from the tdload operation
        """
        with self.preferred_temp_directory() as tmp_dir:
            local_job_var_file = os.path.join(tmp_dir, f"tdload_job_var_{uuid.uuid4().hex}.txt")
            write_file(local_job_var_file, job_var_content or "")
            return self._transfer_to_and_execute_tdload_on_remote(
                local_job_var_file, remote_working_dir, tdload_options, tdload_job_name
            )

    def _transfer_to_and_execute_tdload_on_remote(
        self,
        local_job_var_file: str,
        remote_working_dir: str,
        tdload_options: str | None,
        tdload_job_name: str | None,
    ) -> int:
        """Transfer job variable file to remote host and execute tdload command."""
        encrypted_file_path = f"{local_job_var_file}.enc"
        remote_encrypted_job_file = os.path.join(remote_working_dir, os.path.basename(encrypted_file_path))
        remote_job_file = os.path.join(remote_working_dir, os.path.basename(local_job_var_file))

        try:
            if not self.ssh_hook:
                raise ConnectionError("SSH connection is not established. `ssh_hook` is None or invalid.")
            with self.ssh_hook.get_conn() as ssh_client:
                verify_tpt_utility_on_remote_host(ssh_client, "tdload", logging.getLogger(__name__))
                password = generate_random_password()
                generate_encrypted_file_with_openssl(local_job_var_file, password, encrypted_file_path)
                transfer_file_sftp(
                    ssh_client, encrypted_file_path, remote_encrypted_job_file, logging.getLogger(__name__)
                )
                decrypt_remote_file(
                    ssh_client,
                    remote_encrypted_job_file,
                    remote_job_file,
                    password,
                    logging.getLogger(__name__),
                )

                set_remote_file_permissions(ssh_client, remote_job_file, logging.getLogger(__name__))

                # Build tdload command more robustly
                tdload_cmd = self._build_tdload_command(remote_job_file, tdload_options, tdload_job_name)

                self.log.info("Executing tdload command on remote server: %s", " ".join(tdload_cmd))
                exit_status, output, error = execute_remote_command(ssh_client, " ".join(tdload_cmd))
                self.log.info("tdload command output:\n%s", output)
                self.log.info("tdload command exited with status %s", exit_status)

                # Clean up remote files before checking exit status
                remote_secure_delete(
                    ssh_client, [remote_encrypted_job_file, remote_job_file], logging.getLogger(__name__)
                )

                if exit_status != 0:
                    raise RuntimeError(f"tdload command failed with exit code {exit_status}: {error}")

                return exit_status
        except ConnectionError:
            # Re-raise ConnectionError as-is (don't convert to TimeoutError)
            raise
        except (OSError, socket.gaierror) as e:
            self.log.error("SSH connection timed out: %s", str(e))
            raise TimeoutError(
                "SSH connection timed out. Please check the network or server availability."
            ) from e
        except SSHException as e:
            raise ConnectionError(f"SSH error during connection: {str(e)}") from e
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error while executing tdload script on remote machine: {str(e)}"
            ) from e
        finally:
            # Clean up local files
            secure_delete(encrypted_file_path, logging.getLogger(__name__))
            secure_delete(local_job_var_file, logging.getLogger(__name__))

    def _execute_tdload_locally(
        self,
        job_var_content: str | None,
        tdload_options: str | None,
        tdload_job_name: str | None,
    ) -> int:
        """
        Execute tdload command locally.

        Args:
            job_var_content: Content for the job variable file
            tdload_options: Additional tdload command options
            tdload_job_name: Name for the tdload job

        Returns:
            Exit code from the tdload operation
        """
        with self.preferred_temp_directory() as tmp_dir:
            local_job_var_file = os.path.join(tmp_dir, f"tdload_job_var_{uuid.uuid4().hex}.txt")
            write_file(local_job_var_file, job_var_content or "")

            # Set file permission to read-only for the current user (no permissions for group/others)
            set_local_file_permissions(local_job_var_file, logging.getLogger(__name__))

            # Log file permissions for debugging purposes
            file_permissions = oct(os.stat(local_job_var_file).st_mode & 0o777)
            self.log.debug("Local job variable file permissions: %s", file_permissions)

            # Build tdload command
            tdload_cmd = self._build_tdload_command(local_job_var_file, tdload_options, tdload_job_name)

            if not shutil.which("tdload"):
                raise FileNotFoundError("tdload binary not found in PATH.")

            sp = None
            try:
                # Print a visual separator for clarity in logs
                self.log.info("Executing tdload command: %s", " ".join(tdload_cmd))
                sp = subprocess.Popen(
                    tdload_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True
                )
                error_lines = []
                if sp.stdout is not None:
                    for line in iter(sp.stdout.readline, b""):
                        decoded_line = line.decode("UTF-8").strip()
                        self.log.info(decoded_line)
                        if "error" in decoded_line.lower():
                            error_lines.append(decoded_line)
                sp.wait()
                self.log.info("tdload command exited with return code %s", sp.returncode)
                if sp.returncode != 0:
                    error_msg = "\n".join(error_lines) if error_lines else ""
                    if error_msg:
                        raise RuntimeError(
                            f"tdload command failed with return code {sp.returncode}:\n{error_msg}"
                        )
                    raise RuntimeError(f"tdload command failed with return code {sp.returncode}")
                return sp.returncode
            except RuntimeError:
                raise
            except Exception as e:
                self.log.error("Error executing tdload command: %s", str(e))
                raise RuntimeError(f"Error executing tdload command: {str(e)}") from e
            finally:
                secure_delete(local_job_var_file, logging.getLogger(__name__))
                terminate_subprocess(sp, logging.getLogger(__name__))

    def _build_tdload_command(
        self, job_var_file: str, tdload_options: str | None, tdload_job_name: str | None
    ) -> list[str]:
        """
        Build the tdload command with proper option handling.

        Args:
            job_var_file: Path to the job variable file
            tdload_options: Additional tdload options as a space-separated string
            tdload_job_name: Name for the tdload job

        Returns:
            List of command arguments for tdload
        """
        tdload_cmd = ["tdload", "-j", job_var_file]

        # Add tdload_options if provided, with proper handling of quoted options
        if tdload_options:
            # Split options while preserving quoted arguments
            import shlex

            try:
                parsed_options = shlex.split(tdload_options)
                tdload_cmd.extend(parsed_options)
            except ValueError as e:
                self.log.warning(
                    "Failed to parse tdload_options using shlex, falling back to simple split: %s", str(e)
                )
                # Fallback to simple split if shlex parsing fails
                tdload_cmd.extend(tdload_options.split())

        # Add job name if provided (and not empty)
        if tdload_job_name:
            tdload_cmd.append(tdload_job_name)

        return tdload_cmd

    def on_kill(self) -> None:
        """
        Handle cleanup when the task is killed.

        This method is called when Airflow needs to terminate the hook,
        typically during task cancellation or shutdown.
        """
        self.log.info("TPT Hook cleanup initiated")
        # Note: SSH connections are managed by context managers and will be cleaned up automatically
        # Subprocesses are handled by terminate_subprocess in the finally blocks
        # This method is available for future enhancements if needed

    @contextmanager
    def preferred_temp_directory(self, prefix: str = "tpt_") -> Generator[str, None, None]:
        try:
            temp_dir = tempfile.gettempdir()
            if not os.path.isdir(temp_dir) or not os.access(temp_dir, os.W_OK):
                raise OSError("OS temp dir not usable")
        except Exception:
            temp_dir = self.get_airflow_home_dir()
        with tempfile.TemporaryDirectory(dir=temp_dir, prefix=prefix) as tmp:
            yield tmp

    def get_airflow_home_dir(self) -> str:
        """Return the Airflow home directory."""
        return os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
