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
#
"""This module contains Databricks operators."""

import csv
import json
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from databricks.sql.utils import ParamEscaper

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksSqlOperator(BaseOperator):
    """
    Executes SQL code in a Databricks SQL endpoint or a Databricks cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksSqlOperator`

    :param databricks_conn_id: Reference to
        :ref:`Databricks connection id<howto/connection:databricks>`
    :param http_path: Optional string specifying HTTP path of Databricks SQL Endpoint or cluster.
        If not specified, it should be either specified in the Databricks connection's extra parameters,
        or ``sql_endpoint_name`` must be specified.
    :param sql_endpoint_name: Optional name of Databricks SQL Endpoint. If not specified, ``http_path`` must
        be provided as described above.
    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file. (templated)
        Template references are recognized by str ending in '.sql'
    :param parameters: (optional) the parameters to render the SQL query with.
    :param session_configuration: An optional dictionary of Spark session parameters. Defaults to None.
        If not specified, it could be specified in the Databricks connection's extra parameters.
    :param client_parameters: Additional parameters internal to Databricks SQL Connector parameters
    :param http_headers: An optional list of (k, v) pairs that will be set as HTTP headers on every request.
         (templated)
    :param catalog: An optional initial catalog to use. Requires DBR version 9.0+ (templated)
    :param schema: An optional initial schema to use. Requires DBR version 9.0+ (templated)
    :param output_path: optional string specifying the file to which write selected data. (templated)
    :param output_format: format of output data if ``output_path` is specified.
        Possible values are ``csv``, ``json``, ``jsonl``. Default is ``csv``.
    :param csv_params: parameters that will be passed to the ``csv.DictWriter`` class used to write CSV data.
    """

    template_fields: Sequence[str] = ('sql', '_output_path', 'schema', 'catalog', 'http_headers')
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}

    def __init__(
        self,
        *,
        sql: Union[str, List[str]],
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: Optional[str] = None,
        sql_endpoint_name: Optional[str] = None,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        session_configuration=None,
        http_headers: Optional[List[Tuple[str, str]]] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        do_xcom_push: bool = False,
        output_path: Optional[str] = None,
        output_format: str = 'csv',
        csv_params: Optional[Dict[str, Any]] = None,
        client_parameters: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksSqlOperator``."""
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.sql = sql
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self._output_path = output_path
        self._output_format = output_format
        self._csv_params = csv_params
        self.parameters = parameters
        self.do_xcom_push = do_xcom_push
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.client_parameters = client_parameters or {}

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            http_path=self._http_path,
            session_configuration=self.session_config,
            sql_endpoint_name=self._sql_endpoint_name,
            http_headers=self.http_headers,
            catalog=self.catalog,
            schema=self.schema,
            **self.client_parameters,
        )

    def _format_output(self, schema, results):
        if not self._output_path:
            return
        if not self._output_format:
            raise AirflowException("Output format should be specified!")
        field_names = [field[0] for field in schema]
        if self._output_format.lower() == "csv":
            with open(self._output_path, "w", newline='') as file:
                if self._csv_params:
                    csv_params = self._csv_params
                else:
                    csv_params = {}
                write_header = csv_params.get("header", True)
                if "header" in csv_params:
                    del csv_params["header"]
                writer = csv.DictWriter(file, fieldnames=field_names, **csv_params)
                if write_header:
                    writer.writeheader()
                for row in results:
                    writer.writerow(row.asDict())
        elif self._output_format.lower() == "json":
            with open(self._output_path, "w") as file:
                file.write(json.dumps([row.asDict() for row in results]))
        elif self._output_format.lower() == "jsonl":
            with open(self._output_path, "w") as file:
                for row in results:
                    file.write(json.dumps(row.asDict()))
                    file.write("\n")
        else:
            raise AirflowException(f"Unsupported output format: '{self._output_format}'")

    def execute(self, context: 'Context') -> Any:
        self.log.info('Executing: %s', self.sql)
        hook = self._get_hook()
        schema, results = hook.run(self.sql, parameters=self.parameters)
        # self.log.info('Schema: %s', schema)
        # self.log.info('Results: %s', results)
        self._format_output(schema, results)
        if self.do_xcom_push:
            return results


COPY_INTO_APPROVED_FORMATS = ["CSV", "JSON", "AVRO", "ORC", "PARQUET", "TEXT", "BINARYFILE"]


class DatabricksCopyIntoOperator(BaseOperator):
    """
    Executes COPY INTO command in a Databricks SQL endpoint or a Databricks cluster.
    COPY INTO command is constructed from individual pieces, that are described in
    `documentation <https://docs.databricks.com/sql/language-manual/delta-copy-into.html>`_.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksSqlCopyIntoOperator`

    :param table_name: Required name of the table. (templated)
    :param file_location: Required location of files to import. (templated)
    :param file_format: Required file format. Supported formats are
        ``CSV``, ``JSON``, ``AVRO``, ``ORC``, ``PARQUET``, ``TEXT``, ``BINARYFILE``.
    :param databricks_conn_id: Reference to
        :ref:`Databricks connection id<howto/connection:databricks>`
    :param http_path: Optional string specifying HTTP path of Databricks SQL Endpoint or cluster.
        If not specified, it should be either specified in the Databricks connection's extra parameters,
        or ``sql_endpoint_name`` must be specified.
    :param sql_endpoint_name: Optional name of Databricks SQL Endpoint.
        If not specified, ``http_path`` must be provided as described above.
    :param session_configuration: An optional dictionary of Spark session parameters. Defaults to None.
        If not specified, it could be specified in the Databricks connection's extra parameters.
    :param http_headers: An optional list of (k, v) pairs that will be set as HTTP headers on every request
    :param catalog: An optional initial catalog to use. Requires DBR version 9.0+
    :param schema: An optional initial schema to use. Requires DBR version 9.0+
    :param client_parameters: Additional parameters internal to Databricks SQL Connector parameters
    :param files: optional list of files to import. Can't be specified together with ``pattern``. (templated)
    :param pattern: optional regex string to match file names to import.
        Can't be specified together with ``files``.
    :param expression_list: optional string that will be used in the ``SELECT`` expression.
    :param credential: optional credential configuration for authentication against a source location.
    :param storage_credential: optional Unity Catalog storage credential for destination.
    :param encryption: optional encryption configuration for a specified location.
    :param format_options: optional dictionary with options specific for a given file format.
    :param force_copy: optional bool to control forcing of data import
        (could be also specified in ``copy_options``).
    :param validate: optional configuration for schema & data validation. ``True`` forces validation
        of all rows, integer number - validate only N first rows
    :param copy_options: optional dictionary of copy options. Right now only ``force`` option is supported.
    """

    template_fields: Sequence[str] = (
        '_file_location',
        '_files',
        '_table_name',
    )

    def __init__(
        self,
        *,
        table_name: str,
        file_location: str,
        file_format: str,
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: Optional[str] = None,
        sql_endpoint_name: Optional[str] = None,
        session_configuration=None,
        http_headers: Optional[List[Tuple[str, str]]] = None,
        client_parameters: Optional[Dict[str, Any]] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        files: Optional[List[str]] = None,
        pattern: Optional[str] = None,
        expression_list: Optional[str] = None,
        credential: Optional[Dict[str, str]] = None,
        storage_credential: Optional[str] = None,
        encryption: Optional[Dict[str, str]] = None,
        format_options: Optional[Dict[str, str]] = None,
        force_copy: Optional[bool] = None,
        copy_options: Optional[Dict[str, str]] = None,
        validate: Optional[Union[bool, int]] = None,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksSqlOperator``."""
        super().__init__(**kwargs)
        if files is not None and pattern is not None:
            raise AirflowException("Only one of 'pattern' or 'files' should be specified")
        if table_name == "":
            raise AirflowException("table_name shouldn't be empty")
        if file_location == "":
            raise AirflowException("file_location shouldn't be empty")
        if file_format not in COPY_INTO_APPROVED_FORMATS:
            raise AirflowException(f"file_format '{file_format}' isn't supported")
        self._files = files
        self._pattern = pattern
        self._file_format = file_format
        self.databricks_conn_id = databricks_conn_id
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self.session_config = session_configuration
        self._table_name = table_name
        self._catalog = catalog
        self._schema = schema
        self._file_location = file_location
        self._expression_list = expression_list
        self._credential = credential
        self._storage_credential = storage_credential
        self._encryption = encryption
        self._format_options = format_options
        self._copy_options = copy_options or {}
        self._validate = validate
        self._http_headers = http_headers
        self._client_parameters = client_parameters or {}
        if force_copy is not None:
            self._copy_options["force"] = 'true' if force_copy else 'false'

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            http_path=self._http_path,
            session_configuration=self.session_config,
            sql_endpoint_name=self._sql_endpoint_name,
            http_headers=self._http_headers,
            catalog=self._catalog,
            schema=self._schema,
            **self._client_parameters,
        )

    @staticmethod
    def _generate_options(
        name: str,
        escaper: ParamEscaper,
        opts: Optional[Dict[str, str]] = None,
        escape_key: bool = True,
    ) -> str:
        formatted_opts = ""
        if opts is not None and len(opts) > 0:
            pairs = [
                f"{escaper.escape_item(k) if escape_key else k} = {escaper.escape_item(v)}"
                for k, v in opts.items()
            ]
            formatted_opts = f"{name} ({', '.join(pairs)})"

        return formatted_opts

    def _create_sql_query(self) -> str:
        escaper = ParamEscaper()
        maybe_with = ""
        if self._encryption is not None or self._credential is not None:
            maybe_encryption = ""
            if self._encryption is not None:
                maybe_encryption = self._generate_options("ENCRYPTION", escaper, self._encryption, False)
            maybe_credential = ""
            if self._credential is not None:
                maybe_credential = self._generate_options("CREDENTIAL", escaper, self._credential, False)
            maybe_with = f" WITH ({maybe_credential} {maybe_encryption})"
        location = escaper.escape_item(self._file_location) + maybe_with
        if self._expression_list is not None:
            location = f"(SELECT {self._expression_list} FROM {location})"
        files_or_pattern = ""
        if self._pattern is not None:
            files_or_pattern = f"PATTERN = {escaper.escape_item(self._pattern)}\n"
        elif self._files is not None:
            files_or_pattern = f"FILES = {escaper.escape_item(self._files)}\n"
        format_options = self._generate_options("FORMAT_OPTIONS", escaper, self._format_options) + "\n"
        copy_options = self._generate_options("COPY_OPTIONS", escaper, self._copy_options) + "\n"
        storage_cred = ""
        if self._storage_credential:
            storage_cred = f" WITH (CREDENTIAL {self._storage_credential})"
        validation = ""
        if self._validate is not None:
            if isinstance(self._validate, bool):
                if self._validate:
                    validation = "VALIDATE ALL\n"
            elif isinstance(self._validate, int):
                if self._validate < 0:
                    raise AirflowException(
                        "Number of rows for validation should be positive, got: " + str(self._validate)
                    )
                validation = f"VALIDATE {self._validate} ROWS\n"
            else:
                raise AirflowException(
                    "Incorrect data type for validate parameter: " + str(type(self._validate))
                )
        # TODO: think on how to make sure that table_name and expression_list aren't used for SQL injection
        sql = f"""COPY INTO {self._table_name}{storage_cred}
FROM {location}
FILEFORMAT = {self._file_format}
{validation}{files_or_pattern}{format_options}{copy_options}
"""
        return sql.strip()

    def execute(self, context: 'Context') -> Any:
        sql = self._create_sql_query()
        self.log.info('Executing: %s', sql)
        hook = self._get_hook()
        hook.run(sql)
