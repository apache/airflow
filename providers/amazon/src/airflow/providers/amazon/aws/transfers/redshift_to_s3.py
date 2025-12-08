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
"""Transfers data from AWS Redshift into a S3 Bucket."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block
from airflow.providers.amazon.version_compat import NOTSET, ArgNotSet, is_arg_set
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftToS3Operator(BaseOperator):
    """
    Execute an UNLOAD command to s3 as a CSV with headers.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftToS3Operator`

    :param s3_bucket: reference to a specific S3 bucket
    :param s3_key: reference to a specific S3 key. If ``table_as_file_name`` is set
        to False, this param must include the desired file name
    :param schema: reference to a specific schema in redshift database,
        used when ``table`` param provided and ``select_query`` param not provided.
        Do not provide when unloading a temporary table
    :param table: reference to a specific table in redshift database,
        used when ``schema`` param provided and ``select_query`` param not provided
    :param select_query: custom select query to fetch data from redshift database,
        has precedence over default query `SELECT * FROM ``schema``.``table``
    :param redshift_conn_id: reference to a specific redshift database
    :param aws_conn_id: reference to a specific S3 connection
        If the AWS connection contains 'aws_iam_role' in ``extras``
        the operator will use AWS STS credentials with a token
        https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html#copy-credentials
    :param verify: Whether to verify SSL certificates for S3 connection.
        By default, SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param unload_options: reference to a list of UNLOAD options
    :param autocommit: If set to True it will automatically commit the UNLOAD statement.
        Otherwise, it will be committed right before the redshift connection gets closed.
    :param include_header: If set to True the s3 file contains the header columns.
    :param parameters: (optional) the parameters to render the SQL query with.
    :param table_as_file_name: If set to True, the s3 file will be named as the table.
        Applicable when ``table`` param provided.
    :param redshift_data_api_kwargs: If using the Redshift Data API instead of the SQL-based connection,
        dict of arguments for the hook's ``execute_query`` method.
        Cannot include any of these kwargs: ``{'sql', 'parameters'}``
    """

    template_fields: Sequence[str] = (
        "s3_bucket",
        "s3_key",
        "schema",
        "table",
        "unload_options",
        "select_query",
        "redshift_conn_id",
        "redshift_data_api_kwargs",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"select_query": "sql"}
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        schema: str | None = None,
        table: str | None = None,
        select_query: str | None = None,
        redshift_conn_id: str = "redshift_default",
        aws_conn_id: str | None | ArgNotSet = NOTSET,
        verify: bool | str | None = None,
        unload_options: list | None = None,
        autocommit: bool = False,
        include_header: bool = False,
        parameters: Iterable | Mapping | None = None,
        table_as_file_name: bool = True,  # Set to True by default for not breaking current workflows
        redshift_data_api_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.verify = verify
        self.unload_options = unload_options or []
        self.autocommit = autocommit
        self.include_header = include_header
        self.parameters = parameters
        self.table_as_file_name = table_as_file_name
        self.redshift_data_api_kwargs = redshift_data_api_kwargs or {}
        self.select_query = select_query
        # In execute() we attempt to fetch this aws connection to check for extras. If the user didn't
        # actually provide a connection note that, because we don't want to let the exception bubble up in
        # that case (since we're silently injecting a connection on their behalf).
        self._aws_conn_id: str | None
        if is_arg_set(aws_conn_id):
            self.conn_set = True
            self._aws_conn_id = aws_conn_id
        else:
            self.conn_set = False
            self._aws_conn_id = "aws_default"

    def _build_unload_query(
        self, credentials_block: str, select_query: str, s3_key: str, unload_options: str
    ) -> str:
        # Un-escape already escaped queries
        select_query = re.sub(r"''(.+?)''", r"'\1'", select_query)
        return f"""
                    UNLOAD ($${select_query}$$)
                    TO 's3://{self.s3_bucket}/{s3_key}'
                    credentials
                    '{credentials_block}'
                    {unload_options};
        """

    @property
    def default_select_query(self) -> str | None:
        if not self.table:
            return None

        if self.schema:
            table = f"{self.schema}.{self.table}"
        else:
            # Relevant when unloading a temporary table
            table = self.table
        return f"SELECT * FROM {table}"

    @property
    def use_redshift_data(self):
        return bool(self.redshift_data_api_kwargs)

    def execute(self, context: Context) -> None:
        if self.table and self.table_as_file_name:
            self.s3_key = f"{self.s3_key}/{self.table}_"

        self.select_query = self.select_query or self.default_select_query

        if self.select_query is None:
            raise ValueError("Please specify either a table or `select_query` to fetch the data.")

        if self.include_header and "HEADER" not in [uo.upper().strip() for uo in self.unload_options]:
            self.unload_options = [*self.unload_options, "HEADER"]

        if self.use_redshift_data:
            redshift_data_hook = RedshiftDataHook(aws_conn_id=self.redshift_conn_id)
            for arg in ["sql", "parameters"]:
                if arg in self.redshift_data_api_kwargs:
                    raise AirflowException(f"Cannot include param '{arg}' in Redshift Data API kwargs")
        else:
            redshift_sql_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        conn = (
            S3Hook.get_connection(conn_id=self._aws_conn_id)
            # Only fetch the connection if it was set by the user and it is not None
            if self.conn_set and self._aws_conn_id
            else None
        )
        if conn and conn.extra_dejson.get("role_arn", False):
            credentials_block = f"aws_iam_role={conn.extra_dejson['role_arn']}"
        else:
            s3_hook = S3Hook(aws_conn_id=self._aws_conn_id, verify=self.verify)
            credentials = s3_hook.get_credentials()
            credentials_block = build_credentials_block(credentials)

        unload_options = "\n\t\t\t".join(self.unload_options)

        unload_query = self._build_unload_query(
            credentials_block, self.select_query, self.s3_key, unload_options
        )

        self.log.info("Executing UNLOAD command...")
        if self.use_redshift_data:
            redshift_data_hook.execute_query(
                sql=unload_query, parameters=self.parameters, **self.redshift_data_api_kwargs
            )
        else:
            redshift_sql_hook.run(unload_query, self.autocommit, parameters=self.parameters)
        self.log.info("UNLOAD command complete...")

    def get_openlineage_facets_on_complete(self, task_instance):
        """Implement on_complete as we may query for table details."""
        from airflow.providers.amazon.aws.utils.openlineage import (
            get_facets_from_redshift_table,
            get_identity_column_lineage_facet,
        )
        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            Error,
            ExtractionErrorRunFacet,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        output_dataset = Dataset(
            namespace=f"s3://{self.s3_bucket}",
            name=self.s3_key,
        )

        if self.use_redshift_data:
            redshift_data_hook = RedshiftDataHook(aws_conn_id=self.redshift_conn_id)
            database = self.redshift_data_api_kwargs.get("database")
            identifier = self.redshift_data_api_kwargs.get(
                "cluster_identifier", self.redshift_data_api_kwargs.get("workgroup_name")
            )
            port = self.redshift_data_api_kwargs.get("port", "5439")
            authority = f"{identifier}.{redshift_data_hook.region_name}:{port}"
        else:
            redshift_sql_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
            database = redshift_sql_hook.conn.schema
            authority = redshift_sql_hook.get_openlineage_database_info(redshift_sql_hook.conn).authority

        if self.select_query == self.default_select_query:
            if self.use_redshift_data:
                input_dataset_facets = get_facets_from_redshift_table(
                    redshift_data_hook, self.table, self.redshift_data_api_kwargs, self.schema
                )
            else:
                input_dataset_facets = get_facets_from_redshift_table(
                    redshift_sql_hook, self.table, {}, self.schema
                )

            input_dataset = Dataset(
                namespace=f"redshift://{authority}",
                name=f"{database}.{self.schema}.{self.table}" if database else f"{self.schema}.{self.table}",
                facets=input_dataset_facets,
            )

            # If default select query is used (SELECT *) output file matches the input table.
            output_dataset.facets = {
                "schema": input_dataset_facets["schema"],
                "columnLineage": get_identity_column_lineage_facet(
                    field_names=[field.name for field in input_dataset_facets["schema"].fields],
                    input_datasets=[input_dataset],
                ),
            }

            return OperatorLineage(inputs=[input_dataset], outputs=[output_dataset])

        try:
            from airflow.providers.openlineage.sqlparser import SQLParser, from_table_meta
        except ImportError:
            return OperatorLineage(outputs=[output_dataset])

        run_facets = {}
        parse_result = SQLParser(dialect="redshift", default_schema=self.schema).parse(self.select_query)
        if parse_result.errors:
            run_facets["extractionError"] = ExtractionErrorRunFacet(
                totalTasks=1,
                failedTasks=1,
                errors=[
                    Error(
                        errorMessage=error.message,
                        stackTrace=None,
                        task=error.origin_statement,
                        taskNumber=error.index,
                    )
                    for error in parse_result.errors
                ],
            )

        input_datasets = []
        for in_tb in parse_result.in_tables:
            ds = from_table_meta(in_tb, database, f"redshift://{authority}", False)
            schema, table = ds.name.split(".")[-2:]
            if self.use_redshift_data:
                input_dataset_facets = get_facets_from_redshift_table(
                    redshift_data_hook, table, self.redshift_data_api_kwargs, schema
                )
            else:
                input_dataset_facets = get_facets_from_redshift_table(redshift_sql_hook, table, {}, schema)

            ds.facets = input_dataset_facets
            input_datasets.append(ds)

        return OperatorLineage(inputs=input_datasets, outputs=[output_dataset], run_facets=run_facets)
