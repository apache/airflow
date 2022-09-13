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
"""This module contains a sqoop 1 operator"""
from __future__ import annotations

import os
import signal
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.sqoop.hooks.sqoop import SqoopHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SqoopOperator(BaseOperator):
    """
    Execute a Sqoop job.
    Documentation for Apache Sqoop can be found here:
    https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html

    :param conn_id: str
    :param cmd_type: str specify command to execute "export" or "import"
    :param schema: Schema name
    :param table: Table to read
    :param query: Import result of arbitrary SQL query. Instead of using the table,
        columns and where arguments, you can specify a SQL statement with the query
        argument. Must also specify a destination directory with target_dir.
    :param target_dir: HDFS destination directory where the data
        from the rdbms will be written
    :param append: Append data to an existing dataset in HDFS
    :param file_type: "avro", "sequence", "text" Imports data to
        into the specified format. Defaults to text.
    :param columns: <col,col,col> Columns to import from table
    :param num_mappers: Use n mapper tasks to import/export in parallel
    :param split_by: Column of the table used to split work units
    :param where: WHERE clause to use during import
    :param export_dir: HDFS Hive database directory to export to the rdbms
    :param input_null_string: The string to be interpreted as null
        for string columns
    :param input_null_non_string: The string to be interpreted as null
        for non-string columns
    :param staging_table: The table in which data will be staged before
        being inserted into the destination table
    :param clear_staging_table: Indicate that any data present in the
        staging table can be deleted
    :param enclosed_by: Sets a required field enclosing character
    :param escaped_by: Sets the escape character
    :param input_fields_terminated_by: Sets the input field separator
    :param input_lines_terminated_by: Sets the input end-of-line character
    :param input_optionally_enclosed_by: Sets a field enclosing character
    :param batch: Use batch mode for underlying statement execution
    :param direct: Use direct export fast path
    :param driver: Manually specify JDBC driver class to use
    :param verbose: Switch to more verbose logging for debug purposes
    :param relaxed_isolation: use read uncommitted isolation level
    :param hcatalog_database: Specifies the database name for the HCatalog table
    :param hcatalog_table: The argument value for this option is the HCatalog table
    :param create_hcatalog_table: Have sqoop create the hcatalog table passed
        in or not
    :param properties: additional JVM properties passed to sqoop
    :param extra_import_options: Extra import options to pass as dict.
        If a key doesn't have a value, just pass an empty string to it.
        Don't include prefix of -- for sqoop options.
    :param extra_export_options: Extra export options to pass as dict.
        If a key doesn't have a value, just pass an empty string to it.
        Don't include prefix of -- for sqoop options.
    """

    template_fields: Sequence[str] = (
        'conn_id',
        'cmd_type',
        'table',
        'query',
        'target_dir',
        'file_type',
        'columns',
        'split_by',
        'where',
        'export_dir',
        'input_null_string',
        'input_null_non_string',
        'staging_table',
        'enclosed_by',
        'escaped_by',
        'input_fields_terminated_by',
        'input_lines_terminated_by',
        'input_optionally_enclosed_by',
        'properties',
        'extra_import_options',
        'driver',
        'extra_export_options',
        'hcatalog_database',
        'hcatalog_table',
        'schema',
    )
    template_fields_renderers = {'query': 'sql'}
    ui_color = '#7D8CA4'

    def __init__(
        self,
        *,
        conn_id: str = 'sqoop_default',
        cmd_type: str = 'import',
        table: str | None = None,
        query: str | None = None,
        target_dir: str | None = None,
        append: bool = False,
        file_type: str = 'text',
        columns: str | None = None,
        num_mappers: int | None = None,
        split_by: str | None = None,
        where: str | None = None,
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
        direct: bool = False,
        driver: Any | None = None,
        verbose: bool = False,
        relaxed_isolation: bool = False,
        properties: dict[str, Any] | None = None,
        hcatalog_database: str | None = None,
        hcatalog_table: str | None = None,
        create_hcatalog_table: bool = False,
        extra_import_options: dict[str, Any] | None = None,
        extra_export_options: dict[str, Any] | None = None,
        schema: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.cmd_type = cmd_type
        self.table = table
        self.query = query
        self.target_dir = target_dir
        self.append = append
        self.file_type = file_type
        self.columns = columns
        self.num_mappers = num_mappers
        self.split_by = split_by
        self.where = where
        self.export_dir = export_dir
        self.input_null_string = input_null_string
        self.input_null_non_string = input_null_non_string
        self.staging_table = staging_table
        self.clear_staging_table = clear_staging_table
        self.enclosed_by = enclosed_by
        self.escaped_by = escaped_by
        self.input_fields_terminated_by = input_fields_terminated_by
        self.input_lines_terminated_by = input_lines_terminated_by
        self.input_optionally_enclosed_by = input_optionally_enclosed_by
        self.batch = batch
        self.direct = direct
        self.driver = driver
        self.verbose = verbose
        self.relaxed_isolation = relaxed_isolation
        self.hcatalog_database = hcatalog_database
        self.hcatalog_table = hcatalog_table
        self.create_hcatalog_table = create_hcatalog_table
        self.properties = properties
        self.extra_import_options = extra_import_options or {}
        self.extra_export_options = extra_export_options or {}
        self.hook: SqoopHook | None = None
        self.schema = schema

    def execute(self, context: Context) -> None:
        """Execute sqoop job"""
        if self.hook is None:
            self.hook = self._get_hook()

        if self.cmd_type == 'export':
            self.hook.export_table(
                table=self.table,  # type: ignore
                export_dir=self.export_dir,
                input_null_string=self.input_null_string,
                input_null_non_string=self.input_null_non_string,
                staging_table=self.staging_table,
                clear_staging_table=self.clear_staging_table,
                enclosed_by=self.enclosed_by,
                escaped_by=self.escaped_by,
                input_fields_terminated_by=self.input_fields_terminated_by,
                input_lines_terminated_by=self.input_lines_terminated_by,
                input_optionally_enclosed_by=self.input_optionally_enclosed_by,
                batch=self.batch,
                relaxed_isolation=self.relaxed_isolation,
                extra_export_options=self.extra_export_options,
                schema=self.schema,
            )
        elif self.cmd_type == 'import':
            # add create hcatalog table to extra import options if option passed
            # if new params are added to constructor can pass them in here
            # so don't modify sqoop_hook for each param
            if self.create_hcatalog_table:
                self.extra_import_options['create-hcatalog-table'] = ''

            if self.table and self.query:
                raise AirflowException('Cannot specify query and table together. Need to specify either or.')

            if self.table:
                self.hook.import_table(
                    table=self.table,
                    target_dir=self.target_dir,
                    append=self.append,
                    file_type=self.file_type,
                    columns=self.columns,
                    split_by=self.split_by,
                    where=self.where,
                    direct=self.direct,
                    driver=self.driver,
                    extra_import_options=self.extra_import_options,
                    schema=self.schema,
                )
            elif self.query:
                self.hook.import_query(
                    query=self.query,
                    target_dir=self.target_dir,
                    append=self.append,
                    file_type=self.file_type,
                    split_by=self.split_by,
                    direct=self.direct,
                    driver=self.driver,
                    extra_import_options=self.extra_import_options,
                )
            else:
                raise AirflowException("Provide query or table parameter to import using Sqoop")
        else:
            raise AirflowException("cmd_type should be 'import' or 'export'")

    def on_kill(self) -> None:
        if self.hook is None:
            self.hook = self._get_hook()
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.hook.sub_process_pid), signal.SIGTERM)

    def _get_hook(self) -> SqoopHook:
        return SqoopHook(
            conn_id=self.conn_id,
            verbose=self.verbose,
            num_mappers=self.num_mappers,
            hcatalog_database=self.hcatalog_database,
            hcatalog_table=self.hcatalog_table,
            properties=self.properties,
        )
