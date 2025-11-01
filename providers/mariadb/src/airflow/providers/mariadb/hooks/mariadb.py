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
MariaDB hook for Apache Airflow.

This module provides a hook for interacting with MariaDB databases using the native
MariaDB connector. It includes support for ColumnStore engine validation, bulk data
loading with cpimport utility, and S3 integration for data transfer operations.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from typing import Any

import mariadb
from airflow.exceptions import AirflowException
from airflow.hooks.dbapi import DbApiHook
from airflow.providers.ssh.hooks.ssh import SSHHook


class MariaDBHook(DbApiHook):
    """
    Interact with MariaDB database using the native MariaDB client library.

    This hook provides comprehensive MariaDB integration including:
    - Native MariaDB connector support
    - ColumnStore engine validation
    - Bulk data loading with cpimport utility
    - S3 integration for data transfer
    - SSH-based remote execution

    :param mariadb_conn_id: The connection ID to use when connecting to MariaDB.
    :param database: The database name to connect to.
    """

    conn_name_attr = "mariadb_conn_id"
    default_conn_name = "mariadb_default"
    conn_type = "mariadb"
    hook_name = "MariaDB"
    supports_autocommit = True

    def __init__(
        self,
        *,
        mariadb_conn_id: str = "mariadb_default",
        database: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mariadb_conn_id = mariadb_conn_id
        self.database = database

    def get_conn(self) -> mariadb.Connection:
        """
        Return a MariaDB connection object.

        :return: MariaDB connection object.
        """
        conn = self.get_connection(self.mariadb_conn_id)
        conn_config = {
            "host": conn.host,
            "port": conn.port or 3306,
            "user": conn.login,
            "password": conn.password,
            "database": self.database or conn.schema,
        }

        # Add any extras from the connection if necessary
        if conn.extra:
            try:
                extra_options = json.loads(conn.extra)
                conn_config.update(extra_options)
            except json.JSONDecodeError:
                self.log.warning("Connection extra is not valid JSON")

        self.log.info("Connecting to MariaDB database: %s", conn_config["database"])
        try:
            return mariadb.connect(**conn_config)
        except mariadb.Error as e:
            self.log.error("Error connecting to MariaDB: %s", e)
            raise AirflowException(f"Failed to connect to MariaDB: {e}") from e

    def validate_columnstore_engine(self, table_name: str, schema: str | None = None) -> bool:
        """
        Validate that a table uses ColumnStore engine.

        :param table_name: Name of the table to check.
        :param schema: Database schema name (optional, uses connection schema if not provided).
        :return: True if table uses ColumnStore engine, False otherwise.
        :raises AirflowException: If table doesn't exist or uses wrong engine.
        """
        if not schema:
            conn = self.get_connection(self.mariadb_conn_id)
            schema = conn.schema

        check_sql = """
        SELECT ENGINE 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """

        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(check_sql, (schema, table_name))
                result = cursor.fetchone()

                if not result:
                    raise AirflowException(f"Table {schema}.{table_name} does not exist")

                engine = result[0]
                self.log.info("Table %s.%s uses engine: %s", schema, table_name, engine)

                if engine.upper() != "COLUMNSTORE":
                    error_msg = (
                        f"Table {schema}.{table_name} uses {engine} engine, "
                        "but ColumnStore is required for cpimport"
                    )
                    self.log.error(error_msg)
                    raise AirflowException(error_msg)

                self.log.info("✓ Table %s.%s uses ColumnStore engine", schema, table_name)
                return True

        except mariadb.Error as e:
            self.log.error("Error checking table engine: %s", e)
            raise AirflowException(f"Failed to check table engine: {e}") from e

    def execute_cpimport(
        self,
        table_name: str,
        file_path: str,
        schema: str | None = None,
        options: dict[str, Any] | None = None,
        ssh_conn_id: str | None = None,
    ) -> bool:
        """
        Execute cpimport command for ColumnStore tables via SSH.

        :param table_name: Name of the target table.
        :param file_path: Path to the data file to import.
        :param schema: Database schema name (optional).
        :param options: Additional cpimport options.
        :param ssh_conn_id: SSH connection ID for remote execution (required).
        :return: True if import was successful.
        :raises AirflowException: If validation fails, SSH connection is missing, or cpimport command fails.
        """
        if not schema:
            conn = self.get_connection(self.mariadb_conn_id)
            schema = conn.schema

        # Validate ColumnStore engine first
        self.validate_columnstore_engine(table_name, schema)

        # Execute cpimport command via SSH
        if not ssh_conn_id:
            raise AirflowException("SSH connection ID is required for cpimport execution")

        return self._execute_cpimport_ssh(table_name, file_path, schema, options, ssh_conn_id)

    def _execute_cpimport_ssh(
        self,
        table_name: str,
        file_path: str,
        schema: str,
        options: dict[str, Any] | None,
        ssh_conn_id: str,
    ) -> bool:
        """Execute cpimport command via SSH."""
        try:
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

            # Build cpimport command for SSH execution
            cmd_parts = ["cpimport"]

            # Add separator option if not provided
            if not options or "-s" not in options:
                cmd_parts.extend(["-s", "','"])

            # Add additional options if provided
            if options:
                for key, value in options.items():
                    if key.startswith("-"):
                        # Handle special characters with proper escaping
                        if key == "-E" and value == '"':
                            cmd_parts.extend([key, '\\"'])
                        elif key == "-n" and value == r"\N":
                            cmd_parts.extend([key, "\\\\N"])
                        else:
                            cmd_parts.extend([key, str(value)])
                    else:
                        cmd_parts.extend([f"-{key}", str(value)])

            # Add schema, table, and file path at the end
            cmd_parts.extend([schema, table_name, file_path])

            # Join command parts with spaces
            cmd = " ".join(cmd_parts)
            self.log.info("Executing cpimport command via SSH: %s", cmd)

            # Execute command via SSH
            with ssh_hook.get_conn() as ssh_client:
                stdin, stdout, stderr = ssh_client.exec_command(cmd)
                exit_status = stdout.channel.recv_exit_status()

                # Read output and error messages
                stdout_content = stdout.read().decode("utf-8")
                stderr_content = stderr.read().decode("utf-8")

                if exit_status == 0:
                    self.log.info("cpimport completed successfully via SSH")
                    if stdout_content:
                        self.log.info("Output: %s", stdout_content)
                    return True
                else:
                    error_msg = f"cpimport failed via SSH with exit code {exit_status}: {stderr_content}"
                    self.log.error(error_msg)
                    raise AirflowException(error_msg)

        except Exception as e:
            error_msg = f"SSH execution failed: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e

    def _copy_file_via_ssh(self, local_file_path: str, remote_file_path: str, ssh_conn_id: str) -> None:
        """Copy file to remote server via SSH."""
        try:
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

            # Use SFTP to copy the file
            with ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                sftp_client.put(local_file_path, remote_file_path)
                sftp_client.close()

            self.log.info("Successfully copied %s to %s via SSH", local_file_path, remote_file_path)

        except Exception as e:
            error_msg = f"Failed to copy file via SSH: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e

    def _copy_file_from_ssh(self, remote_file_path: str, local_file_path: str, ssh_conn_id: str) -> None:
        """Copy file from remote server via SSH."""
        try:
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)

            # Use SFTP to copy the file
            with ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                sftp_client.get(remote_file_path, local_file_path)
                sftp_client.close()

            self.log.info("Successfully copied %s to %s via SSH", remote_file_path, local_file_path)

        except Exception as e:
            error_msg = f"Failed to copy file from SSH: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e

    def get_s3_client(self, aws_conn_id: str = "aws_default"):
        """
        Get S3 client using AWS connection.

        :param aws_conn_id: Airflow connection ID for AWS.
        :return: boto3 S3 client.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3_hook = S3Hook(aws_conn_id)
        return s3_hook.get_client_type("s3")

    def load_from_s3(
        self,
        s3_bucket: str,
        s3_key: str,
        table_name: str,
        schema: str | None = None,
        aws_conn_id: str = "aws_default",
        local_temp_dir: str | None = None,
        ssh_conn_id: str | None = None,
    ) -> bool:
        """
        Load data from S3 to MariaDB table.

        :param s3_bucket: S3 bucket name.
        :param s3_key: S3 object key.
        :param table_name: Target MariaDB table name.
        :param schema: Database schema name (optional).
        :param aws_conn_id: Airflow connection ID for AWS.
        :param local_temp_dir: Local temporary directory for file download.
        :param ssh_conn_id: SSH connection ID for remote execution (required).
        :return: True if load was successful.
        """
        if not schema:
            conn = self.get_connection(self.mariadb_conn_id)
            schema = conn.schema

        # Create temporary file for S3 download
        if not local_temp_dir:
            local_temp_dir = tempfile.gettempdir()

        local_file_path = os.path.join(local_temp_dir, f"{table_name}_s3_import_{os.getpid()}.csv")

        try:
            # Download file from S3
            self.log.info("Downloading %s/%s to %s", s3_bucket, s3_key, local_file_path)
            s3_client = self.get_s3_client(aws_conn_id)
            s3_client.download_file(s3_bucket, s3_key, local_file_path)
            self.log.info("S3 file downloaded: %s", local_file_path)

            # Copy file to remote server via SSH
            if not ssh_conn_id:
                raise AirflowException("SSH connection ID is required for S3 load operation")

            remote_file_path = f"/var/data/{table_name}_s3_import_{os.getpid()}.csv"
            self._copy_file_via_ssh(local_file_path, remote_file_path, ssh_conn_id)

            # Load data into MariaDB
            self.log.info("Loading data from %s to %s.%s", local_file_path, schema, table_name)

            # Check if table uses ColumnStore engine
            try:
                self.validate_columnstore_engine(table_name, schema)
                # Use cpimport for ColumnStore tables
                return self.execute_cpimport(table_name, remote_file_path, schema, ssh_conn_id=ssh_conn_id)
            except AirflowException:
                # Fall back to regular LOAD DATA for non-ColumnStore tables
                self.log.info("Table is not ColumnStore, using LOAD DATA INFILE")
                return self._load_data_infile(remote_file_path, table_name, schema)

        except Exception as e:
            self.log.error("Error loading data from S3: %s", e)
            raise AirflowException(f"Failed to load data from S3: {e}") from e
        finally:
            # Clean up temporary file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                self.log.info("Cleaned up temporary file: %s", local_file_path)

    def dump_to_s3(
        self,
        table_name: str,
        s3_bucket: str,
        s3_key: str,
        query: str | None = None,
        schema: str | None = None,
        aws_conn_id: str = "aws_default",
        local_temp_dir: str | None = None,
        file_format: str = "csv",
        ssh_conn_id: str | None = None,
    ) -> bool:
        """
        Export MariaDB table data to S3.

        :param table_name: Source MariaDB table name.
        :param s3_bucket: S3 bucket name.
        :param s3_key: S3 object key for the exported file.
        :param query: Custom query for export (optional).
        :param schema: Database schema name (optional).
        :param aws_conn_id: Airflow connection ID for AWS.
        :param local_temp_dir: Local temporary directory for file export.
        :param file_format: Export file format (csv, json, sql).
        :param ssh_conn_id: SSH connection ID for remote execution (required).
        :return: True if export was successful.
        """
        if not schema:
            conn = self.get_connection(self.mariadb_conn_id)
            schema = conn.schema

        # Create temporary file for export
        if not local_temp_dir:
            local_temp_dir = tempfile.gettempdir()

        file_extension = file_format.lower()
        local_file_path = os.path.join(local_temp_dir, f"{table_name}_export_{os.getpid()}.{file_extension}")

        # Ensure the local_temp_dir exists
        os.makedirs(local_temp_dir, exist_ok=True)

        self.log.info("Local file path: %s", local_file_path)
        self.log.info("Local temp dir: %s", local_temp_dir)

        try:
            # Export data from MariaDB
            self.log.info("Exporting data from %s.%s to %s", schema, table_name, local_file_path)

            remote_file_path = f"/var/outfiles/{table_name}_export_{os.getpid()}.{file_extension}"

            if file_format.lower() == "csv":
                self._export_to_csv(table_name, query, remote_file_path, schema)
            elif file_format.lower() == "json":
                self._export_to_json(table_name, query, remote_file_path, schema)
            elif file_format.lower() == "sql":
                self._export_to_sql(table_name, query, remote_file_path, schema)
            else:
                raise AirflowException(f"Unsupported file format: {file_format}")

            # Upload to S3
            self.log.info("Uploading %s to s3://%s/%s", local_file_path, s3_bucket, s3_key)

            # Copy file from remote server via SSH
            if not ssh_conn_id:
                raise AirflowException("SSH connection ID is required for S3 dump operation")

            self._copy_file_from_ssh(remote_file_path, local_file_path, ssh_conn_id)

            # Verify the file exists locally before uploading
            if not os.path.exists(local_file_path):
                raise AirflowException(f"Local file does not exist: {local_file_path}")

            file_size = os.path.getsize(local_file_path)
            self.log.info("Local file size: %s bytes", file_size)

            s3_client = self.get_s3_client(aws_conn_id)
            s3_client.upload_file(local_file_path, s3_bucket, s3_key)

            self.log.info("Successfully exported %s.%s to s3://%s/%s", schema, table_name, s3_bucket, s3_key)
            return True

        except Exception as e:
            self.log.error("Error exporting data to S3: %s", e)
            raise AirflowException(f"Failed to export data to S3: {e}") from e
        finally:
            # Clean up temporary file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                self.log.info("Cleaned up temporary file: %s", local_file_path)

    def _load_data_infile(self, file_path: str, table_name: str, schema: str) -> bool:
        """Load data using LOAD DATA INFILE for non-ColumnStore tables."""
        full_table_name = f"{schema}.{table_name}"
        load_sql = f"""
        LOAD DATA INFILE '{file_path}'
        INTO TABLE {full_table_name}
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS
        """

        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(load_sql)
                conn.commit()
                self.log.info("Successfully loaded data into %s", full_table_name)
                return True
        except mariadb.Error as e:
            self.log.error("Error loading data with LOAD DATA INFILE: %s", e)
            raise AirflowException(f"Failed to load data: {e}") from e

    def _export_to_csv(self, table_name: str, query: str | None, file_path: str, schema: str) -> None:
        """Export table data to CSV format."""
        if query:
            export_sql = f"""
                {query}
                INTO OUTFILE '{file_path}'
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                """
        else:
            full_table_name = f"{schema}.{table_name}"
            export_sql = f"""
            SELECT * FROM {full_table_name}
            INTO OUTFILE '{file_path}'
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            """
        self.log.info(export_sql)

        with self.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(export_sql)

    def _export_to_json(self, table_name: str, query: str | None, file_path: str, schema: str) -> None:
        """Export table data to JSON format."""
        if query:
            select_sql = query
        else:
            full_table_name = f"{schema}.{table_name}"
            select_sql = f"SELECT * FROM {full_table_name}"

        with self.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(select_sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))

            with open(file_path, "w") as f:
                json.dump(data, f, indent=2, default=str)

    def _export_to_sql(self, table_name: str, query: str | None, file_path: str, schema: str) -> None:
        """Export table data to SQL format using mysqldump."""
        conn = self.get_connection(self.mariadb_conn_id)

        # Build mysqldump command
        cmd = [
            "mysqldump",
            f"--host={conn.host}",
            f"--port={conn.port}",
            f"--user={conn.login}",
            f"--password={conn.password}",
            "--single-transaction",
            "--routines",
            "--triggers",
            schema,
            table_name,
        ]

        try:
            with open(file_path, "w") as f:
                subprocess.run(cmd, stdout=f, check=True)
        except subprocess.CalledProcessError as e:
            raise AirflowException(f"mysqldump failed: {e}") from e

    def insert_many(
        self,
        table_name: str,
        rows: list[Any],
        columns: list[str] | None = None,
        schema: str | None = None,
    ) -> bool:
        """
        Bulk insert rows into a MariaDB table using executemany.

        :param table_name: Target table name.
        :param rows: List of row tuples or dicts to insert.
        :param columns: List of column names (optional).
        :param schema: Database schema name (optional).
        :return: True if insert was successful.
        """
        if not rows:
            self.log.warning("No rows provided for insert_many.")
            return False
        if not schema:
            conn = self.get_connection(self.mariadb_conn_id)
            schema = conn.schema
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        if columns:
            cols = ", ".join([f"`{col}`" for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {full_table_name} ({cols}) VALUES ({placeholders})"
        else:
            placeholders = ", ".join(["%s"] * len(rows[0]))
            sql = f"INSERT INTO {full_table_name} VALUES ({placeholders})"
        self.log.info("Executing bulk insert: %s with %s rows", sql, len(rows))
        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.executemany(sql, rows)
                conn.commit()
            self.log.info("Successfully inserted %s rows into %s", len(rows), full_table_name)
            return True
        except mariadb.Error as e:
            self.log.error("Error in insert_many: %s", e)
            raise AirflowException(f"Failed to insert rows: {e}") from e
