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
"""
MariaDB cpimport operator for Apache Airflow.

This module provides an operator for executing cpimport commands for MariaDB ColumnStore tables.
"""

from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from airflow.providers.mariadb.hooks.mariadb import MariaDBHook


class MariaDBCpImportOperator(BaseOperator):
    """
    Execute cpimport command for MariaDB ColumnStore tables.

    This operator validates that the target table uses ColumnStore engine
    before executing the cpimport command. It provides comprehensive
    error handling and logging for the import process.

    :param table_name: Name of the target table.
    :param file_path: Path to the data file to import.
    :param schema: Database schema name (optional, uses connection schema if not provided).
    :param mariadb_conn_id: Airflow connection ID for MariaDB.
    :param cpimport_options: Additional cpimport command options.
    :param validate_engine: Whether to validate ColumnStore engine before import (default: True).
    :param ssh_conn_id: SSH connection ID for remote execution (required).
    """

    template_fields: Sequence[str] = ("table_name", "file_path", "schema")
    template_fields_renderers = {
        "file_path": "bash",
    }
    ui_color = "#f0f8ff"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        *,
        table_name: str,
        file_path: str,
        schema: str | None = None,
        mariadb_conn_id: str = "mariadb_default",
        cpimport_options: dict[str, Any] | None = None,
        validate_engine: bool = True,
        ssh_conn_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.file_path = file_path
        self.schema = schema
        self.mariadb_conn_id = mariadb_conn_id
        self.cpimport_options = cpimport_options or {}
        self.validate_engine = validate_engine
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context: Context) -> bool:
        """
        Execute the cpimport operation.

        :param context: Airflow task context.
        :return: True if import was successful.
        :raises AirflowException: If validation fails or cpimport command fails.
        """
        self.log.info("Starting cpimport operation for table: %s", self.table_name)
        self.log.info("Source file: %s", self.file_path)

        # Initialize MariaDB hook
        hook = MariaDBHook(mariadb_conn_id=self.mariadb_conn_id)

        try:
            # Validate ColumnStore engine if requested
            if self.validate_engine:
                self.log.info("Validating ColumnStore engine...")
                hook.validate_columnstore_engine(self.table_name, self.schema)
                self.log.info("✓ ColumnStore engine validation passed")
            else:
                self.log.warning("⚠️ Skipping ColumnStore engine validation")

            # Execute cpimport command
            self.log.info("Executing cpimport command...")
            result = hook.execute_cpimport(
                table_name=self.table_name,
                file_path=self.file_path,
                schema=self.schema,
                options=self.cpimport_options,
                ssh_conn_id=self.ssh_conn_id,
            )

            if result:
                self.log.info("✅ Successfully imported data into %s", self.table_name)
                return True
            else:
                raise AirflowException("cpimport command returned False")

        except Exception as e:
            self.log.error("❌ cpimport operation failed: %s", e)
            raise AirflowException(f"cpimport operation failed: {e}") from e

    def on_kill(self) -> None:
        """Called when the task is killed."""
        self.log.warning("cpimport operation was killed")
