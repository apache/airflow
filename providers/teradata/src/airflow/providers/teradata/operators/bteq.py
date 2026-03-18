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

from typing import TYPE_CHECKING, Literal

from airflow.providers.teradata.utils.bteq_util import (
    is_valid_encoding,
    is_valid_file,
    is_valid_remote_bteq_script_file,
    prepare_bteq_script_for_local_execution,
    prepare_bteq_script_for_remote_execution,
    read_file,
)
from airflow.providers.teradata.utils.constants import Constants

if TYPE_CHECKING:
    from paramiko import SSHClient

    from airflow.providers.common.compat.sdk import Context
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.teradata.hooks.bteq import BteqHook
from airflow.providers.teradata.hooks.teradata import TeradataHook


def contains_template(parameter_value):
    # Check if the parameter contains Jinja templating syntax
    return "{{" in parameter_value and "}}" in parameter_value


class BteqOperator(BaseOperator):
    """
    Teradata Operator to execute SQL Statements or BTEQ (Basic Teradata Query) scripts using Teradata BTEQ utility.

    This supports execution of BTEQ scripts either locally or remotely via SSH.

    The BTEQ scripts are used to interact with Teradata databases, allowing users to perform
    operations such as querying, data manipulation, and administrative tasks.

    Features:
    - Supports both local and remote execution of BTEQ scripts.
    - Handles connection details, script preparation, and execution.
    - Provides robust error handling and logging for debugging.
    - Allows configuration of session parameters like session and BTEQ I/O encoding.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BteqOperator`

    :param sql: SQL statement(s) to be executed using BTEQ. (templated)
    :param file_path: Optional path to an existing SQL or BTEQ script file. If provided, this file will be used instead of the `sql` content. This path represents remote file path when executing remotely via SSH, or local file path when executing locally.
    :param teradata_conn_id: Reference to a specific Teradata connection.
    :param ssh_conn_id: Optional SSH connection ID for remote execution. Used only when executing scripts remotely.
    :param remote_working_dir: Temporary directory location on the remote host (via SSH) where the BTEQ script will be transferred and executed. Defaults to `/tmp` if not specified. This is only applicable when `ssh_conn_id` is provided.
    :param bteq_session_encoding: Character set encoding for the BTEQ session. Defaults to ASCII if not specified.
    :param bteq_script_encoding: Character encoding for the BTEQ script file. Defaults to ASCII if not specified.
    :param bteq_quit_rc: Accepts a single integer, list, or tuple of return codes. Specifies which BTEQ return codes should be treated as successful, allowing subsequent tasks to continue execution.
    :param timeout: Timeout (in seconds) for executing the BTEQ command. Default is 600 seconds (10 minutes).
    :param timeout_rc: Return code to use if the BTEQ execution fails due to a timeout. To allow DAG execution to continue after a timeout, include this value in `bteq_quit_rc`. If not specified, a timeout will raise an exception and stop the DAG.
    """

    template_fields = "sql"
    ui_color = "#ff976d"

    def __init__(
        self,
        *,
        sql: str | None = None,
        file_path: str | None = None,
        teradata_conn_id: str = TeradataHook.default_conn_name,
        ssh_conn_id: str | None = None,
        remote_working_dir: str | None = None,
        bteq_session_encoding: str | None = None,
        bteq_script_encoding: str | None = None,
        bteq_quit_rc: int | list[int] | tuple[int, ...] | None = None,
        timeout: int | Literal[600] = 600,  # Default to 10 minutes
        timeout_rc: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.file_path = file_path
        self.teradata_conn_id = teradata_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.remote_working_dir = remote_working_dir
        self.timeout = timeout
        self.timeout_rc = timeout_rc
        self.bteq_session_encoding = bteq_session_encoding
        self.bteq_script_encoding = bteq_script_encoding
        self.bteq_quit_rc = bteq_quit_rc
        self._hook: BteqHook | None = None
        self._ssh_hook: SSHHook | None = None
        self.temp_file_read_encoding = "UTF-8"

    def execute(self, context: Context) -> int | None:
        """Execute BTEQ code using the BteqHook."""
        if not self.sql and not self.file_path:
            raise ValueError(Constants.BTEQ_MISSED_PARAMS)
        self._hook = BteqHook(teradata_conn_id=self.teradata_conn_id, ssh_conn_id=self.ssh_conn_id)
        self._ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id) if self.ssh_conn_id else None

        # Validate and set BTEQ session and script encoding
        if not self.bteq_session_encoding or self.bteq_session_encoding == "ASCII":
            self.bteq_session_encoding = ""
            if self.bteq_script_encoding == "UTF8":
                self.temp_file_read_encoding = "UTF-8"
            elif self.bteq_script_encoding == "UTF16":
                self.temp_file_read_encoding = "UTF-16"
            self.bteq_script_encoding = ""
        elif self.bteq_session_encoding == "UTF8" and (
            not self.bteq_script_encoding or self.bteq_script_encoding == "ASCII"
        ):
            self.bteq_script_encoding = "UTF8"
        elif self.bteq_session_encoding == "UTF16":
            if not self.bteq_script_encoding or self.bteq_script_encoding == "ASCII":
                self.bteq_script_encoding = "UTF8"
        # for file reading in python. Mapping BTEQ encoding to Python encoding
        if self.bteq_script_encoding == "UTF8":
            self.temp_file_read_encoding = "UTF-8"
        elif self.bteq_script_encoding == "UTF16":
            self.temp_file_read_encoding = "UTF-16"

        # Handling execution on local:
        if not self._ssh_hook:
            if self.sql:
                bteq_script = prepare_bteq_script_for_local_execution(
                    sql=self.sql,
                )
                return self._hook.execute_bteq_script(
                    bteq_script,
                    self.remote_working_dir,
                    self.bteq_script_encoding,
                    self.timeout,
                    self.timeout_rc,
                    self.bteq_session_encoding,
                    self.bteq_quit_rc,
                    self.temp_file_read_encoding,
                )
            if self.file_path:
                if not is_valid_file(self.file_path):
                    raise ValueError(Constants.BTEQ_INVALID_PATH % self.file_path)
                try:
                    is_valid_encoding(self.file_path, self.temp_file_read_encoding or "UTF-8")
                except UnicodeDecodeError as e:
                    errmsg = Constants.BTEQ_INVALID_CHARSET % (self.file_path, "UTF-8")
                    if self.bteq_script_encoding:
                        errmsg = Constants.BTEQ_INVALID_CHARSET % (self.file_path, self.bteq_script_encoding)
                    raise ValueError(errmsg) from e
                return self._handle_local_bteq_file(
                    file_path=self.file_path,
                    context=context,
                )
        # Execution on Remote machine
        elif self._ssh_hook:
            # When sql statement is provided as input through sql parameter, Preparing the bteq script
            if self.sql:
                bteq_script = prepare_bteq_script_for_remote_execution(
                    conn=self._hook.get_conn(),
                    sql=self.sql,
                )
                return self._hook.execute_bteq_script(
                    bteq_script,
                    self.remote_working_dir,
                    self.bteq_script_encoding,
                    self.timeout,
                    self.timeout_rc,
                    self.bteq_session_encoding,
                    self.bteq_quit_rc,
                    self.temp_file_read_encoding,
                )
            if self.file_path:
                with self._ssh_hook.get_conn() as ssh_client:
                    # When .sql or .bteq remote file path is provided as input through file_path parameter, executing on remote machine
                    if self.file_path and is_valid_remote_bteq_script_file(ssh_client, self.file_path):
                        return self._handle_remote_bteq_file(
                            ssh_client=self._ssh_hook.get_conn(),
                            file_path=self.file_path,
                            context=context,
                        )
                    raise ValueError(Constants.BTEQ_REMOTE_FILE_PATH_INVALID % self.file_path)
            else:
                raise ValueError(Constants.BTEQ_MISSED_PARAMS)
        return None

    def _handle_remote_bteq_file(
        self,
        ssh_client: SSHClient,
        file_path: str | None,
        context: Context,
    ) -> int | None:
        if file_path:
            with ssh_client:
                sftp = ssh_client.open_sftp()
                try:
                    with sftp.open(file_path, "r") as remote_file:
                        original_content = remote_file.read().decode(self.temp_file_read_encoding or "UTF-8")
                finally:
                    sftp.close()
                rendered_content = original_content
                if contains_template(original_content):
                    rendered_content = self.render_template(original_content, context)
                if self._hook:
                    bteq_script = prepare_bteq_script_for_remote_execution(
                        conn=self._hook.get_conn(),
                        sql=rendered_content,
                    )
                    return self._hook.execute_bteq_script_at_remote(
                        bteq_script,
                        self.remote_working_dir,
                        self.bteq_script_encoding,
                        self.timeout,
                        self.timeout_rc,
                        self.bteq_session_encoding,
                        self.bteq_quit_rc,
                        self.temp_file_read_encoding,
                    )
            return None
        raise ValueError(Constants.BTEQ_MISSED_PARAMS)

    def _handle_local_bteq_file(
        self,
        file_path: str,
        context: Context,
    ) -> int | None:
        if file_path and is_valid_file(file_path):
            file_content = read_file(file_path, encoding=str(self.temp_file_read_encoding or "UTF-8"))
            # Manually render using operator's context
            rendered_content = file_content
            if contains_template(file_content):
                rendered_content = self.render_template(file_content, context)
            bteq_script = prepare_bteq_script_for_local_execution(
                sql=rendered_content,
            )
            if self._hook:
                result = self._hook.execute_bteq_script(
                    bteq_script,
                    self.remote_working_dir,
                    self.bteq_script_encoding,
                    self.timeout,
                    self.timeout_rc,
                    self.bteq_session_encoding,
                    self.bteq_quit_rc,
                    self.temp_file_read_encoding,
                )
                return result
        return None

    def on_kill(self) -> None:
        """Handle task termination by invoking the on_kill method of BteqHook."""
        if self._hook:
            self._hook.on_kill()
        else:
            self.log.warning("BteqHook was not initialized. Nothing to terminate.")
