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
"""This module contains a sqoop 1.x hook"""
from __future__ import annotations

import subprocess
from copy import deepcopy
from typing import Any

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class SqoopHook(BaseHook):
    """
    This hook is a wrapper around the sqoop 1 binary. To be able to use the hook
    it is required that "sqoop" is in the PATH.

    Additional arguments that can be passed via the 'extra' JSON field of the
    sqoop connection:

        * ``job_tracker``: Job tracker local|jobtracker:port.
        * ``namenode``: Namenode.
        * ``lib_jars``: Comma separated jar files to include in the classpath.
        * ``files``: Comma separated files to be copied to the map reduce cluster.
        * ``archives``: Comma separated archives to be unarchived on the compute
            machines.
        * ``password_file``: Path to file containing the password.

    :param conn_id: Reference to the sqoop connection.
    :param verbose: Set sqoop to verbose.
    :param num_mappers: Number of map tasks to import in parallel.
    :param properties: Properties to set via the -D argument
    """

    conn_name_attr = "conn_id"
    default_conn_name = "sqoop_default"
    conn_type = "sqoop"
    hook_name = "Sqoop"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        verbose: bool = False,
        num_mappers: int | None = None,
        hcatalog_database: str | None = None,
        hcatalog_table: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> None:
        # No mutable types in the default parameters
        super().__init__()
        self.conn = self.get_connection(conn_id)
        connection_parameters = self.conn.extra_dejson
        self.job_tracker = connection_parameters.get("job_tracker", None)
        self.namenode = connection_parameters.get("namenode", None)
        self.libjars = connection_parameters.get("libjars", None)
        self.files = connection_parameters.get("files", None)
        self.archives = connection_parameters.get("archives", None)
        self.password_file = connection_parameters.get("password_file", None)
        self.hcatalog_database = hcatalog_database
        self.hcatalog_table = hcatalog_table
        self.verbose = verbose
        self.num_mappers = num_mappers
        self.properties = properties or {}
        self.sub_process_pid: int
        self.log.info("Using connection to: %s:%s/%s", self.conn.host, self.conn.port, self.conn.schema)

    def get_conn(self) -> Any:
        return self.conn

    def cmd_mask_password(self, cmd_orig: list[str]) -> list[str]:
        """Mask command password for safety"""
        cmd = deepcopy(cmd_orig)
        try:
            password_index = cmd.index("--password")
            cmd[password_index + 1] = "MASKED"
        except ValueError:
            self.log.debug("No password in sqoop cmd")
        return cmd

    def popen(self, cmd: list[str], **kwargs: Any) -> None:
        """
        Remote Popen

        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        :return: handle to subprocess
        """
        masked_cmd = " ".join(self.cmd_mask_password(cmd))
        self.log.info("Executing command: %s", masked_cmd)
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs) as sub_process:
            self.sub_process_pid = sub_process.pid
            for line in iter(sub_process.stdout):  # type: ignore
                self.log.info(line.strip())
            sub_process.wait()
            self.log.info("Command exited with return code %s", sub_process.returncode)
            if sub_process.returncode:
                raise AirflowException(f"Sqoop command failed: {masked_cmd}")

    def _prepare_command(self, export: bool = False) -> list[str]:
        sqoop_cmd_type = "export" if export else "import"
        connection_cmd = ["sqoop", sqoop_cmd_type]

        for key, value in self.properties.items():
            connection_cmd += ["-D", f"{key}={value}"]

        if self.namenode:
            connection_cmd += ["-fs", self.namenode]
        if self.job_tracker:
            connection_cmd += ["-jt", self.job_tracker]
        if self.libjars:
            connection_cmd += ["-libjars", self.libjars]
        if self.files:
            connection_cmd += ["-files", self.files]
        if self.archives:
            connection_cmd += ["-archives", self.archives]
        if self.conn.login:
            connection_cmd += ["--username", self.conn.login]
        if self.conn.password:
            connection_cmd += ["--password", self.conn.password]
        if self.password_file:
            connection_cmd += ["--password-file", self.password_file]
        if self.verbose:
            connection_cmd += ["--verbose"]
        if self.num_mappers:
            connection_cmd += ["--num-mappers", str(self.num_mappers)]
        if self.hcatalog_database:
            connection_cmd += ["--hcatalog-database", self.hcatalog_database]
        if self.hcatalog_table:
            connection_cmd += ["--hcatalog-table", self.hcatalog_table]

        connect_str = self.conn.host
        if self.conn.port:
            connect_str += f":{self.conn.port}"
        if self.conn.schema:
            self.log.info("CONNECTION TYPE %s", self.conn.conn_type)
            if self.conn.conn_type != "mssql":
                connect_str += f"/{self.conn.schema}"
            else:
                connect_str += f";databaseName={self.conn.schema}"
        connection_cmd += ["--connect", connect_str]

        return connection_cmd

    @staticmethod
    def _get_export_format_argument(file_type: str = "text") -> list[str]:
        if file_type == "avro":
            return ["--as-avrodatafile"]
        elif file_type == "sequence":
            return ["--as-sequencefile"]
        elif file_type == "parquet":
            return ["--as-parquetfile"]
        elif file_type == "text":
            return ["--as-textfile"]
        else:
            raise AirflowException("Argument file_type should be 'avro', 'sequence', 'parquet' or 'text'.")

    def _import_cmd(
        self,
        target_dir: str | None,
        append: bool,
        file_type: str,
        split_by: str | None,
        direct: bool | None,
        driver: Any,
        extra_import_options: Any,
    ) -> list[str]:

        cmd = self._prepare_command(export=False)

        if target_dir:
            cmd += ["--target-dir", target_dir]

        if append:
            cmd += ["--append"]

        cmd += self._get_export_format_argument(file_type)

        if split_by:
            cmd += ["--split-by", split_by]

        if direct:
            cmd += ["--direct"]

        if driver:
            cmd += ["--driver", driver]

        if extra_import_options:
            for key, value in extra_import_options.items():
                cmd += [f"--{key}"]
                if value:
                    cmd += [str(value)]

        return cmd

    def import_table(
        self,
        table: str,
        target_dir: str | None = None,
        append: bool = False,
        file_type: str = "text",
        columns: str | None = None,
        split_by: str | None = None,
        where: str | None = None,
        direct: bool = False,
        driver: Any = None,
        extra_import_options: dict[str, Any] | None = None,
        schema: str | None = None,
    ) -> Any:
        """
        Imports table from remote location to target dir. Arguments are
        copies of direct sqoop command line arguments

        :param table: Table to read
        :param schema: Schema name
        :param target_dir: HDFS destination dir
        :param append: Append data to an existing dataset in HDFS
        :param file_type: "avro", "sequence", "text" or "parquet".
            Imports data to into the specified format. Defaults to text.
        :param columns: <col,col,col…> Columns to import from table
        :param split_by: Column of the table used to split work units
        :param where: WHERE clause to use during import
        :param direct: Use direct connector if exists for the database
        :param driver: Manually specify JDBC driver class to use
        :param extra_import_options: Extra import options to pass as dict.
            If a key doesn't have a value, just pass an empty string to it.
            Don't include prefix of -- for sqoop options.
        """
        cmd = self._import_cmd(target_dir, append, file_type, split_by, direct, driver, extra_import_options)

        cmd += ["--table", table]

        if columns:
            cmd += ["--columns", columns]
        if where:
            cmd += ["--where", where]
        if schema:
            cmd += ["--", "--schema", schema]

        self.popen(cmd)

    def import_query(
        self,
        query: str,
        target_dir: str | None = None,
        append: bool = False,
        file_type: str = "text",
        split_by: str | None = None,
        direct: bool | None = None,
        driver: Any | None = None,
        extra_import_options: dict[str, Any] | None = None,
    ) -> Any:
        """
        Imports a specific query from the rdbms to hdfs

        :param query: Free format query to run
        :param target_dir: HDFS destination dir
        :param append: Append data to an existing dataset in HDFS
        :param file_type: "avro", "sequence", "text" or "parquet"
            Imports data to hdfs into the specified format. Defaults to text.
        :param split_by: Column of the table used to split work units
        :param direct: Use direct import fast path
        :param driver: Manually specify JDBC driver class to use
        :param extra_import_options: Extra import options to pass as dict.
            If a key doesn't have a value, just pass an empty string to it.
            Don't include prefix of -- for sqoop options.
        """
        cmd = self._import_cmd(target_dir, append, file_type, split_by, direct, driver, extra_import_options)
        cmd += ["--query", query]

        self.popen(cmd)

    def _export_cmd(
        self,
        table: str,
        export_dir: str | None = None,
        input_null_string: str | None = None,
        input_null_non_string: str | None = None,
        staging_table: str | None = None,
        clear_staging_table: bool = False,
        enclosed_by: str | None = None,
        escaped_by: str | None = None,
        input_fields_terminated_by: str | None = None,
        input_lines_terminated_by: str | None = None,
        input_optionally_enclosed_by: str | None = None,
        batch: bool = False,
        relaxed_isolation: bool = False,
        extra_export_options: dict[str, Any] | None = None,
        schema: str | None = None,
    ) -> list[str]:

        cmd = self._prepare_command(export=True)

        if input_null_string:
            cmd += ["--input-null-string", input_null_string]

        if input_null_non_string:
            cmd += ["--input-null-non-string", input_null_non_string]

        if staging_table:
            cmd += ["--staging-table", staging_table]

        if clear_staging_table:
            cmd += ["--clear-staging-table"]

        if enclosed_by:
            cmd += ["--enclosed-by", enclosed_by]

        if escaped_by:
            cmd += ["--escaped-by", escaped_by]

        if input_fields_terminated_by:
            cmd += ["--input-fields-terminated-by", input_fields_terminated_by]

        if input_lines_terminated_by:
            cmd += ["--input-lines-terminated-by", input_lines_terminated_by]

        if input_optionally_enclosed_by:
            cmd += ["--input-optionally-enclosed-by", input_optionally_enclosed_by]

        if batch:
            cmd += ["--batch"]

        if relaxed_isolation:
            cmd += ["--relaxed-isolation"]

        if export_dir:
            cmd += ["--export-dir", export_dir]

        if extra_export_options:
            for key, value in extra_export_options.items():
                cmd += [f"--{key}"]
                if value:
                    cmd += [str(value)]

        # The required option
        cmd += ["--table", table]

        if schema:
            cmd += ["--", "--schema", schema]

        return cmd

    def export_table(
        self,
        table: str,
        export_dir: str | None = None,
        input_null_string: str | None = None,
        input_null_non_string: str | None = None,
        staging_table: str | None = None,
        clear_staging_table: bool = False,
        enclosed_by: str | None = None,
        escaped_by: str | None = None,
        input_fields_terminated_by: str | None = None,
        input_lines_terminated_by: str | None = None,
        input_optionally_enclosed_by: str | None = None,
        batch: bool = False,
        relaxed_isolation: bool = False,
        extra_export_options: dict[str, Any] | None = None,
        schema: str | None = None,
    ) -> None:
        """
        Exports Hive table to remote location. Arguments are copies of direct
        sqoop command line Arguments

        :param table: Table remote destination
        :param schema: Schema name
        :param export_dir: Hive table to export
        :param input_null_string: The string to be interpreted as null for
            string columns
        :param input_null_non_string: The string to be interpreted as null
            for non-string columns
        :param staging_table: The table in which data will be staged before
            being inserted into the destination table
        :param clear_staging_table: Indicate that any data present in the
            staging table can be deleted
        :param enclosed_by: Sets a required field enclosing character
        :param escaped_by: Sets the escape character
        :param input_fields_terminated_by: Sets the field separator character
        :param input_lines_terminated_by: Sets the end-of-line character
        :param input_optionally_enclosed_by: Sets a field enclosing character
        :param batch: Use batch mode for underlying statement execution
        :param relaxed_isolation: Transaction isolation to read uncommitted
            for the mappers
        :param extra_export_options: Extra export options to pass as dict.
            If a key doesn't have a value, just pass an empty string to it.
            Don't include prefix of -- for sqoop options.
        """
        cmd = self._export_cmd(
            table,
            export_dir,
            input_null_string,
            input_null_non_string,
            staging_table,
            clear_staging_table,
            enclosed_by,
            escaped_by,
            input_fields_terminated_by,
            input_lines_terminated_by,
            input_optionally_enclosed_by,
            batch,
            relaxed_isolation,
            extra_export_options,
            schema,
        )

        self.popen(cmd)
