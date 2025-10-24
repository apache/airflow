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
MariaDB S3 operators for Apache Airflow.

This module provides operators for loading data from S3 to MariaDB and dumping data from MariaDB to S3.
"""

from __future__ import annotations

from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from airflow.providers.mariadb.hooks.mariadb import MariaDBHook


class MariaDBS3LoadOperator(BaseOperator):
    """
    Load data from AWS S3 to MariaDB table.

    This operator downloads data from S3 and loads it into a MariaDB table.
    It automatically detects if the table uses ColumnStore engine and uses
    cpimport for optimal performance, or falls back to LOAD DATA INFILE
    for regular tables.

    :param s3_bucket: S3 bucket name.
    :param s3_key: S3 object key.
    :param table_name: Target MariaDB table name.
    :param schema: Database schema name (optional, uses connection schema if not provided).
    :param mariadb_conn_id: Airflow connection ID for MariaDB.
    :param aws_conn_id: Airflow connection ID for AWS.
    :param local_temp_dir: Local temporary directory for file download.
    :param ssh_conn_id: SSH connection ID for remote execution (required).
    """

    template_fields: Sequence[str] = ("s3_bucket", "s3_key", "table_name", "schema")
    template_fields_renderers = {
        "s3_bucket": "bash",
        "s3_key": "bash",
    }
    ui_color = "#ffd700"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        table_name: str,
        schema: str | None = None,
        mariadb_conn_id: str = "mariadb_default",
        aws_conn_id: str = "aws_default",
        local_temp_dir: str | None = None,
        ssh_conn_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_name = table_name
        self.schema = schema
        self.mariadb_conn_id = mariadb_conn_id
        self.aws_conn_id = aws_conn_id
        self.local_temp_dir = local_temp_dir
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context: Context) -> bool:
        """
        Execute the S3 to MariaDB load operation.

        :param context: Airflow task context.
        :return: True if load was successful.
        :raises AirflowException: If load operation fails.
        """
        self.log.info("Starting S3 to MariaDB load operation")
        self.log.info("S3 source: s3://%s/%s", self.s3_bucket, self.s3_key)
        self.log.info("Target table: %s.%s", self.schema, self.table_name)

        # Initialize MariaDB hook
        hook = MariaDBHook(mariadb_conn_id=self.mariadb_conn_id)

        try:
            # Load data from S3 to MariaDB
            result = hook.load_from_s3(
                s3_bucket=self.s3_bucket,
                s3_key=self.s3_key,
                table_name=self.table_name,
                schema=self.schema,
                aws_conn_id=self.aws_conn_id,
                local_temp_dir=self.local_temp_dir,
                ssh_conn_id=self.ssh_conn_id,
            )

            if result:
                self.log.info("✅ Successfully loaded data from S3 to %s", self.table_name)
                return True
            else:
                raise AirflowException("S3 load operation returned False")

        except Exception as e:
            self.log.error("❌ S3 load operation failed: %s", e)
            raise AirflowException(f"S3 load operation failed: {e}") from e

    def on_kill(self) -> None:
        """Called when the task is killed."""
        self.log.warning("S3 load operation was killed")


class MariaDBS3DumpOperator(BaseOperator):
    """
    Export MariaDB table data to AWS S3.

    This operator exports data from a MariaDB table and uploads it to S3.
    It supports multiple export formats including CSV, JSON, and SQL.

    :param table_name: Source MariaDB table name.
    :param s3_bucket: S3 bucket name.
    :param s3_key: S3 object key for the exported file.
    :param query: Custom query for export (optional).
    :param schema: Database schema name (optional, uses connection schema if not provided).
    :param mariadb_conn_id: Airflow connection ID for MariaDB.
    :param aws_conn_id: Airflow connection ID for AWS.
    :param local_temp_dir: Local temporary directory for file export.
    :param file_format: Export file format (csv, json, sql).
    :param ssh_conn_id: SSH connection ID for remote execution (required).
    """

    template_fields: Sequence[str] = ("table_name", "s3_bucket", "s3_key", "schema")
    template_fields_renderers = {
        "s3_bucket": "bash",
        "s3_key": "bash",
    }
    ui_color = "#98fb98"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        *,
        table_name: str,
        s3_bucket: str,
        s3_key: str,
        query: str | None = None,
        schema: str | None = None,
        mariadb_conn_id: str = "mariadb_default",
        aws_conn_id: str = "aws_default",
        local_temp_dir: str | None = None,
        file_format: str = "csv",
        ssh_conn_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.query = query
        self.schema = schema
        self.mariadb_conn_id = mariadb_conn_id
        self.aws_conn_id = aws_conn_id
        self.local_temp_dir = local_temp_dir
        self.file_format = file_format.lower()
        self.ssh_conn_id = ssh_conn_id

    def execute(self, context: Context) -> bool:
        """
        Execute the MariaDB to S3 dump operation.

        :param context: Airflow task context.
        :return: True if dump was successful.
        :raises AirflowException: If dump operation fails.
        """
        self.log.info("Starting MariaDB to S3 dump operation")
        self.log.info("Source table: %s.%s", self.schema, self.table_name)
        self.log.info("S3 destination: s3://%s/%s", self.s3_bucket, self.s3_key)
        self.log.info("Export format: %s", self.file_format)

        # Validate file format
        if self.file_format not in ["csv", "json", "sql"]:
            raise AirflowException(
                f"Unsupported file format: {self.file_format}. Supported formats: csv, json, sql"
            )

        # Initialize MariaDB hook
        hook = MariaDBHook(mariadb_conn_id=self.mariadb_conn_id)

        try:
            # Dump data from MariaDB to S3
            result = hook.dump_to_s3(
                table_name=self.table_name,
                s3_bucket=self.s3_bucket,
                s3_key=self.s3_key,
                query=self.query,
                schema=self.schema,
                aws_conn_id=self.aws_conn_id,
                local_temp_dir=self.local_temp_dir,
                file_format=self.file_format,
                ssh_conn_id=self.ssh_conn_id,
            )

            if result:
                self.log.info("✅ Successfully dumped data from %s to S3", self.table_name)
                return True
            else:
                raise AirflowException("S3 dump operation returned False")

        except Exception as e:
            self.log.error("❌ S3 dump operation failed: %s", e)
            raise AirflowException(f"S3 dump operation failed: {e}") from e

    def on_kill(self) -> None:
        """Called when the task is killed."""
        self.log.warning("S3 dump operation was killed")
