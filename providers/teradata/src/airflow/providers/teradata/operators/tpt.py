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
from typing import TYPE_CHECKING

from airflow.providers.teradata.utils.tpt_util import (
    get_remote_temp_directory,
    is_valid_file,
    is_valid_remote_job_var_file,
    prepare_tdload_job_var_file,
    prepare_tpt_ddl_script,
    read_file,
)

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context
    from paramiko import SSHClient

from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.hooks.tpt import TptHook


class DdlOperator(BaseOperator):
    """
    Operator to execute one or more DDL (Data Definition Language) statements on a Teradata Database.

    This operator is designed to facilitate DDL operations such as creating, altering, or dropping tables, indexes, views, or other database objects in a scalable and efficient manner.

    It leverages the TPT (Teradata Parallel Transporter) utility to perform the operations and supports templating for SQL statements, allowing dynamic generation of SQL at runtime.

    Key Features:
    - Executes one or more DDL statements sequentially on Teradata using TPT
    - Supports error handling with customizable error code list
    - Supports XCom push to share execution results with downstream tasks
    - Integrates with Airflow's templating engine for dynamic SQL generation
    - Can execute statements via SSH connection if needed

    :param ddl: A list of DDL statements to be executed. Each item should be a valid SQL
                DDL command supported by Teradata.
    :param error_list: Optional integer or list of error codes to ignore during execution.
                       If provided, the operator will not fail when these specific error codes occur.
                       Example: error_list=3803 or error_list=[3803, 3807]
    :param teradata_conn_id: The connection ID for the Teradata database.
                        Defaults to TeradataHook.default_conn_name.
    :param ssh_conn_id: Optional SSH connection ID if the commands need to be executed through SSH.
    :param remote_working_dir: Directory on the remote server where temporary files will be stored.
    :param ddl_job_name: Optional name for the DDL job.
    :raises ValueError: If the ddl parameter or error_list is invalid.
    :raises RuntimeError: If underlying TPT execution (tbuild) fails with non-zero exit status.
    :raises ConnectionError: If remote SSH connection cannot be established.
    :raises TimeoutError: If SSH connection attempt times out.
    :raises FileNotFoundError: If required TPT utility (tbuild) is missing locally or on remote host.

    Example usage::

        # Example of creating tables using DdlOperator
        create_tables = DdlOperator(
            task_id="create_tables_task",
            ddl=[
                "CREATE TABLE my_database.my_table1 (id INT, name VARCHAR(100))",
                "CREATE TABLE my_database.my_table2 (id INT, value FLOAT)",
            ],
            teradata_conn_id="my_teradata_conn",
            error_list=[3803],  # Ignore "Table already exists" errors
            ddl_job_name="create_tables_job",
        )

        # Example of dropping tables using DdlOperator
        drop_tables = DdlOperator(
            task_id="drop_tables_task",
            ddl=["DROP TABLE my_database.my_table1", "DROP TABLE my_database.my_table2"],
            teradata_conn_id="my_teradata_conn",
            error_list=3807,  # Ignore "Object does not exist" errors
            ddl_job_name="drop_tables_job",
        )

        # Example using templated SQL file
        alter_table = DdlOperator(
            task_id="alter_table_task",
            ddl="{{ var.value.get('ddl_directory') }}/alter_table.sql",
            teradata_conn_id="my_teradata_conn",
            ssh_conn_id="my_ssh_conn",
            ddl_job_name="alter_table_job",
        )
    """

    template_fields = ("ddl", "ddl_job_name")
    template_ext = (".sql",)
    ui_color = "#a8e4b1"

    def __init__(
        self,
        *,
        ddl: list[str],
        error_list: int | list[int] | None = None,
        teradata_conn_id: str = TeradataHook.default_conn_name,
        ssh_conn_id: str | None = None,
        remote_working_dir: str | None = None,
        ddl_job_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ddl = ddl
        self.error_list = error_list
        self.teradata_conn_id = teradata_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.remote_working_dir = remote_working_dir
        self.ddl_job_name = ddl_job_name
        self._hook: TptHook | None = None
        self._ssh_hook: SSHHook | None = None

    def execute(self, context: Context) -> int | None:
        """Execute the DDL operations using the TptHook."""
        # Validate the ddl parameter
        if (
            not self.ddl
            or not isinstance(self.ddl, list)
            or not all(isinstance(stmt, str) and stmt.strip() for stmt in self.ddl)
        ):
            raise ValueError(
                "ddl parameter must be a non-empty list of non-empty strings representing DDL statements."
            )

        # Normalize error_list to a list of ints
        normalized_error_list = self._normalize_error_list(self.error_list)

        self.log.info("Initializing Teradata connection using teradata_conn_id: %s", self.teradata_conn_id)
        self._hook = TptHook(teradata_conn_id=self.teradata_conn_id, ssh_conn_id=self.ssh_conn_id)
        self._ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id) if self.ssh_conn_id else None

        try:
            # Prepare TPT script for DDL execution
            tpt_ddl_script = prepare_tpt_ddl_script(
                sql=self.ddl,
                error_list=normalized_error_list,
                source_conn=self._hook.get_conn(),
                job_name=self.ddl_job_name,
            )

            # Set remote working directory if SSH is used
            if self._ssh_hook and not self.remote_working_dir:
                self.remote_working_dir = get_remote_temp_directory(
                    self._ssh_hook.get_conn(), logging.getLogger(__name__)
                )
            # Ensure remote_working_dir has a value even for local execution
            if not self.remote_working_dir:
                self.remote_working_dir = "/tmp"

            return self._hook.execute_ddl(
                tpt_ddl_script,
                self.remote_working_dir,
            )
        except Exception as e:
            self.log.error("Failed to execute DDL operations: %s", str(e))
            raise

    def _normalize_error_list(self, error_list: int | list[int] | None) -> list[int]:
        """
        Normalize error_list parameter to a list of integers.

        Args:
            error_list: An integer, list of integers, or None

        Returns:
            A list of integers representing error codes to ignore

        Raises:
            ValueError: If error_list is not of the expected type
        """
        if error_list is None:
            return []
        if isinstance(error_list, int):
            return [error_list]
        if isinstance(error_list, list) and all(isinstance(err, int) for err in error_list):
            return error_list
        raise ValueError(
            f"error_list must be an int or a list of ints, got {type(error_list).__name__}. "
            "Example: error_list=3803 or error_list=[3803, 3807]"
        )

    def on_kill(self):
        """Handle termination signals and ensure the hook is properly cleaned up."""
        self.log.info("Cleaning up TPT DDL connections on task kill")
        if self._hook:
            try:
                self._hook.on_kill()
                self.log.info("TPT DDL hook cleaned up successfully")
            except Exception as e:
                self.log.error("Error cleaning up TPT DDL hook: %s", str(e))
        else:
            self.log.warning("No TptHook initialized to clean up on task kill")


class TdLoadOperator(BaseOperator):
    """
    Operator to handle data transfers using Teradata Parallel Transporter (TPT) tdload utility.

    This operator supports three main scenarios:
    1. Load data from a file to a Teradata table
    2. Export data from a Teradata table to a file
    3. Transfer data between two Teradata tables (potentially across different databases)

    For all scenarios:
        :param teradata_conn_id: Connection ID for Teradata database (source for table operations)

    For file to table loading:
        :param source_file_name: Path to the source file (required for file to table)
        :param select_stmt: SQL SELECT statement to filter data (optional)
        :param insert_stmt: SQL INSERT statement to use for loading data (optional)
        :param target_table: Name of the target table (required for file to table)
        :param target_teradata_conn_id: Connection ID for target Teradata database (defaults to teradata_conn_id)

    For table to file export:
        :param source_table: Name of the source table (required for table to file)
        :param target_file_name: Path to the target file (required for table to file)

    For table to table transfer:
        :param source_table: Name of the source table (required for table to table)
        :param select_stmt: SQL SELECT statement to filter data (optional)
        :param insert_stmt: SQL INSERT statement to use for loading data (optional)
        :param target_table: Name of the target table (required for table to table)
        :param target_teradata_conn_id: Connection ID for target Teradata database (required for table to table)

    Optional configuration parameters:
        :param source_format: Format of source data (default: 'Delimited')
        :param target_format: Format of target data (default: 'Delimited')
        :param source_text_delimiter: Source text delimiter (default: ',')
        :param target_text_delimiter: Target text delimiter (default: ',')
        :param tdload_options: Additional options for tdload (optional)
        :param tdload_job_name: Name for the tdload job (optional)
        :param tdload_job_var_file: Path to tdload job variable file (optional)
        :param ssh_conn_id: SSH connection ID for secure file transfer (optional, used for file operations)

    :raises ValueError: If parameter combinations are invalid or required files are missing.
    :raises RuntimeError: If underlying TPT execution (tdload) fails with non-zero exit status.
    :raises ConnectionError: If remote SSH connection cannot be established.
    :raises TimeoutError: If SSH connection attempt times out.
    :raises FileNotFoundError: If required TPT utility (tdload) is missing locally or on remote host.

    Example usage::

        # Example usage for file to table:
        load_file = TdLoadOperator(
            task_id="load_from_file",
            source_file_name="/path/to/data.csv",
            target_table="my_database.my_table",
            target_teradata_conn_id="teradata_target_conn",
            insert_stmt="INSERT INTO my_database.my_table (col1, col2) VALUES (?, ?)",
        )

        # Example usage for table to file:
        export_data = TdLoadOperator(
            task_id="export_to_file",
            source_table="my_database.my_table",
            target_file_name="/path/to/export.csv",
            teradata_conn_id="teradata_source_conn",
            ssh_conn_id="ssh_default",
            tdload_job_name="export_job",
        )

        # Example usage for table to table:
        transfer_data = TdLoadOperator(
            task_id="transfer_between_tables",
            source_table="source_db.source_table",
            target_table="target_db.target_table",
            teradata_conn_id="teradata_source_conn",
            target_teradata_conn_id="teradata_target_conn",
            tdload_job_var_file="/path/to/vars.txt",
            insert_stmt="INSERT INTO target_db.target_table (col1, col2) VALUES (?, ?)",
        )


    """

    template_fields = (
        "source_table",
        "target_table",
        "select_stmt",
        "insert_stmt",
        "source_file_name",
        "target_file_name",
        "tdload_options",
    )
    ui_color = "#a8e4b1"

    def __init__(
        self,
        *,
        teradata_conn_id: str = TeradataHook.default_conn_name,
        target_teradata_conn_id: str | None = None,
        ssh_conn_id: str | None = None,
        source_table: str | None = None,
        select_stmt: str | None = None,
        insert_stmt: str | None = None,
        target_table: str | None = None,
        source_file_name: str | None = None,
        target_file_name: str | None = None,
        source_format: str = "Delimited",
        target_format: str = "Delimited",
        source_text_delimiter: str = ",",
        target_text_delimiter: str = ",",
        tdload_options: str | None = None,
        tdload_job_name: str | None = None,
        tdload_job_var_file: str | None = None,
        remote_working_dir: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.teradata_conn_id = teradata_conn_id
        self.target_teradata_conn_id = target_teradata_conn_id
        self.ssh_conn_id = ssh_conn_id
        self.source_table = source_table
        self.select_stmt = select_stmt
        self.insert_stmt = insert_stmt
        self.target_table = target_table
        self.source_file_name = source_file_name
        self.target_file_name = target_file_name
        self.source_format = source_format
        self.source_text_delimiter = source_text_delimiter
        self.target_format = target_format
        self.target_text_delimiter = target_text_delimiter
        self.tdload_options = tdload_options
        self.tdload_job_name = tdload_job_name
        self.tdload_job_var_file = tdload_job_var_file
        self.remote_working_dir = remote_working_dir
        self._src_hook: TptHook | None = None
        self._dest_hook: TptHook | None = None

    def execute(self, context: Context) -> int | None:
        """Execute the TdLoad operation based on the configured parameters."""
        # Validate parameter combinations
        mode = self._validate_and_determine_mode()

        # Initialize hooks
        self._initialize_hooks(mode)

        try:
            # Prepare job variable file content if not provided
            tdload_job_var_content = None
            tdload_job_var_file = self.tdload_job_var_file

            if not tdload_job_var_file:
                tdload_job_var_content = self._prepare_job_var_content(mode)
                self.log.info("Prepared tdload job variable content for mode '%s'", mode)

            # Set remote working directory if SSH is used
            if self._ssh_hook and not self.remote_working_dir:
                self.remote_working_dir = get_remote_temp_directory(
                    self._ssh_hook.get_conn(), logging.getLogger(__name__)
                )
            # Ensure remote_working_dir is always a str
            if not self.remote_working_dir:
                self.remote_working_dir = "/tmp"

            # Execute based on SSH availability and job var file source
            return self._execute_based_on_configuration(tdload_job_var_file, tdload_job_var_content, context)

        except Exception as e:
            self.log.error("Failed to execute TdLoad operation in mode '%s': %s", mode, str(e))
            raise

    def _validate_and_determine_mode(self) -> str:
        """
        Validate parameters and determine the operation mode.

        Returns:
            A string indicating the operation mode: 'file_to_table', 'table_to_file',
            'table_to_table', or 'job_var_file'

        Raises:
            ValueError: If parameter combinations are invalid
        """
        if self.source_table and self.select_stmt:
            raise ValueError(
                "Both source_table and select_stmt cannot be provided simultaneously. "
                "Please provide only one."
            )

        if self.insert_stmt and not self.target_table:
            raise ValueError(
                "insert_stmt is provided but target_table is not specified. "
                "Please provide a target_table for the insert operation."
            )

        # Determine the mode of operation based on provided parameters
        if self.source_file_name and self.target_table:
            mode = "file_to_table"
            if self.target_teradata_conn_id is None:
                self.target_teradata_conn_id = self.teradata_conn_id
            self.log.info(
                "Loading data from file '%s' to table '%s'", self.source_file_name, self.target_table
            )
        elif (self.source_table or self.select_stmt) and self.target_file_name:
            mode = "table_to_file"
            self.log.info(
                "Exporting data from %s to file '%s'",
                self.source_table or "custom select statement",
                self.target_file_name,
            )
        elif (self.source_table or self.select_stmt) and self.target_table:
            mode = "table_to_table"
            if self.target_teradata_conn_id is None:
                raise ValueError("For table to table transfer, target_teradata_conn_id must be provided.")
            self.log.info(
                "Transferring data from %s to table '%s'",
                self.source_table or "custom select statement",
                self.target_table,
            )
        else:
            if not self.tdload_job_var_file:
                raise ValueError(
                    "Invalid parameter combination for the TdLoadOperator. Please provide one of these valid combinations:\n"
                    "1. source_file_name and target_table: to load data from a file to a table\n"
                    "2. source_table/select_stmt and target_file_name: to export data from a table to a file\n"
                    "3. source_table/select_stmt and target_table: to transfer data between tables\n"
                    "4. tdload_job_var_file: to use a pre-configured job variable file"
                )
            mode = "job_var_file"
            self.log.info("Using pre-configured job variable file: %s", self.tdload_job_var_file)

        return mode

    def _initialize_hooks(self, mode: str) -> None:
        """
        Initialize the required hooks based on the operation mode.

        Args:
            mode: The operation mode ('file_to_table', 'table_to_file', 'table_to_table', etc.)
        """
        self.log.info("Initializing source connection using teradata_conn_id: %s", self.teradata_conn_id)
        self._src_hook = TptHook(teradata_conn_id=self.teradata_conn_id, ssh_conn_id=self.ssh_conn_id)

        if mode in ("table_to_table", "file_to_table"):
            self.log.info(
                "Initializing destination connection using target_teradata_conn_id: %s",
                self.target_teradata_conn_id,
            )
            self._dest_hook = TptHook(teradata_conn_id=self.target_teradata_conn_id)

        self._ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id) if self.ssh_conn_id else None

    def _prepare_job_var_content(self, mode: str) -> str:
        """
        Prepare the job variable file content.

        Args:
            mode: The operation mode

        Returns:
            The prepared job variable file content as a string
        """
        if not self._src_hook:
            raise ValueError("Source hook not initialized")

        return prepare_tdload_job_var_file(
            mode=mode,
            source_table=self.source_table,
            select_stmt=self.select_stmt,
            insert_stmt=self.insert_stmt,
            target_table=self.target_table,
            source_file_name=self.source_file_name,
            target_file_name=self.target_file_name,
            source_format=self.source_format,
            target_format=self.target_format,
            source_text_delimiter=self.source_text_delimiter,
            target_text_delimiter=self.target_text_delimiter,
            source_conn=self._src_hook.get_conn(),
            target_conn=self._dest_hook.get_conn() if self._dest_hook else None,
        )

    def _execute_based_on_configuration(
        self, tdload_job_var_file: str | None, tdload_job_var_content: str | None, context: Context
    ) -> int | None:
        """Execute TdLoad operation based on SSH and job var file configuration."""
        if self._ssh_hook:
            if tdload_job_var_file:
                with self._ssh_hook.get_conn() as ssh_client:
                    if is_valid_remote_job_var_file(
                        ssh_client, tdload_job_var_file, logging.getLogger(__name__)
                    ):
                        return self._handle_remote_job_var_file(
                            ssh_client=ssh_client,
                            file_path=tdload_job_var_file,
                            context=context,
                        )
                    raise ValueError(
                        f"The provided remote job variables file path '{tdload_job_var_file}' is invalid or does not exist on remote machine."
                    )
            else:
                if not self._src_hook:
                    raise ValueError("Source hook not initialized")
                # Ensure remote_working_dir is always a str
                remote_working_dir = self.remote_working_dir or "/tmp"
                return self._src_hook.execute_tdload(
                    remote_working_dir,
                    tdload_job_var_content,
                    self.tdload_options,
                    self.tdload_job_name,
                )
        else:
            if tdload_job_var_file:
                if is_valid_file(tdload_job_var_file):
                    return self._handle_local_job_var_file(
                        file_path=tdload_job_var_file,
                        context=context,
                    )
                raise ValueError(
                    f"The provided job variables file path '{tdload_job_var_file}' is invalid or does not exist."
                )
            if not self._src_hook:
                raise ValueError("Source hook not initialized")
            # Ensure remote_working_dir is always a str
            remote_working_dir = self.remote_working_dir or "/tmp"
            return self._src_hook.execute_tdload(
                remote_working_dir,
                tdload_job_var_content,
                self.tdload_options,
                self.tdload_job_name,
            )

    def _handle_remote_job_var_file(
        self,
        ssh_client: SSHClient,
        file_path: str | None,
        context: Context,
    ) -> int | None:
        """Handle execution using a remote job variable file."""
        if not file_path:
            raise ValueError("Please provide a valid job variables file path on the remote machine.")

        try:
            sftp = ssh_client.open_sftp()
            try:
                with sftp.open(file_path, "r") as remote_file:
                    tdload_job_var_content = remote_file.read().decode("UTF-8")
                self.log.info("Successfully read remote job variable file: %s", file_path)
            finally:
                sftp.close()

            if self._src_hook:
                # Ensure remote_working_dir is always a str
                remote_working_dir = self.remote_working_dir or "/tmp"
                return self._src_hook._execute_tdload_via_ssh(
                    remote_working_dir,
                    tdload_job_var_content,
                    self.tdload_options,
                    self.tdload_job_name,
                )
            raise ValueError("Source hook not initialized for remote execution.")
        except Exception as e:
            self.log.error("Failed to handle remote job variable file '%s': %s", file_path, str(e))
            raise

    def _handle_local_job_var_file(
        self,
        file_path: str | None,
        context: Context,
    ) -> int | None:
        """
        Handle execution using a local job variable file.

        Args:
            file_path: Path to the local job variable file
            context: Airflow context

        Returns:
            Exit code from the TdLoad operation

        Raises:
            ValueError: If file path is invalid or hook not initialized
        """
        if not file_path:
            raise ValueError("Please provide a valid local job variables file path.")

        if not is_valid_file(file_path):
            raise ValueError(f"The job variables file path '{file_path}' is invalid or does not exist.")

        try:
            tdload_job_var_content = read_file(file_path, encoding="UTF-8")
            self.log.info("Successfully read local job variable file: %s", file_path)

            if self._src_hook:
                return self._src_hook._execute_tdload_locally(
                    tdload_job_var_content,
                    self.tdload_options,
                    self.tdload_job_name,
                )
            raise ValueError("Source hook not initialized for local execution.")

        except Exception as e:
            self.log.error("Failed to handle local job variable file '%s': %s", file_path, str(e))
            raise

    def on_kill(self):
        """Handle termination signals and ensure all hooks are properly cleaned up."""
        self.log.info("Cleaning up TPT tdload connections on task kill")

        cleanup_errors = []

        # Clean up the source hook if it was initialized
        if self._src_hook:
            try:
                self.log.info("Cleaning up source connection")
                self._src_hook.on_kill()
            except Exception as e:
                cleanup_errors.append(f"Failed to cleanup source hook: {str(e)}")
                self.log.error("Error cleaning up source connection: %s", str(e))

        # Clean up the destination hook if it was initialized
        if self._dest_hook:
            try:
                self.log.info("Cleaning up destination connection")
                self._dest_hook.on_kill()
            except Exception as e:
                cleanup_errors.append(f"Failed to cleanup destination hook: {str(e)}")
                self.log.error("Error cleaning up destination connection: %s", str(e))

        # Log any cleanup errors but don't raise them during shutdown
        if cleanup_errors:
            self.log.warning("Some cleanup operations failed: %s", "; ".join(cleanup_errors))
        else:
            self.log.info("All TPT connections cleaned up successfully")
