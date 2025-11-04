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
    prepare_tpt_ddl_script,
)

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context

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
