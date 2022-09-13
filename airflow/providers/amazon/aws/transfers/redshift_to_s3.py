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

from typing import TYPE_CHECKING, Iterable, List, Mapping, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block

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
    :param schema: reference to a specific schema in redshift database
        Applicable when ``table`` param provided.
    :param table: reference to a specific table in redshift database
        Used when ``select_query`` param not provided.
    :param select_query: custom select query to fetch data from redshift database
    :param redshift_conn_id: reference to a specific redshift database
    :param aws_conn_id: reference to a specific S3 connection
        If the AWS connection contains 'aws_iam_role' in ``extras``
        the operator will use AWS STS credentials with a token
        https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html#copy-credentials
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param unload_options: reference to a list of UNLOAD options
    :param autocommit: If set to True it will automatically commit the UNLOAD statement.
        Otherwise it will be committed right before the redshift connection gets closed.
    :param include_header: If set to True the s3 file contains the header columns.
    :param parameters: (optional) the parameters to render the SQL query with.
    :param table_as_file_name: If set to True, the s3 file will be named as the table.
        Applicable when ``table`` param provided.
    """

    template_fields: Sequence[str] = (
        's3_bucket',
        's3_key',
        'schema',
        'table',
        'unload_options',
        'select_query',
    )
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'select_query': 'sql'}
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        schema: str | None = None,
        table: str | None = None,
        select_query: str | None = None,
        redshift_conn_id: str = 'redshift_default',
        aws_conn_id: str = 'aws_default',
        verify: bool | str | None = None,
        unload_options: list | None = None,
        autocommit: bool = False,
        include_header: bool = False,
        parameters: Iterable | Mapping | None = None,
        table_as_file_name: bool = True,  # Set to True by default for not breaking current workflows
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = f'{s3_key}/{table}_' if (table and table_as_file_name) else s3_key
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.unload_options = unload_options or []  # type: List
        self.autocommit = autocommit
        self.include_header = include_header
        self.parameters = parameters
        self.table_as_file_name = table_as_file_name

        if select_query:
            self.select_query = select_query
        elif self.schema and self.table:
            self.select_query = f"SELECT * FROM {self.schema}.{self.table}"
        else:
            raise ValueError(
                'Please provide both `schema` and `table` params or `select_query` to fetch the data.'
            )

        if self.include_header and 'HEADER' not in [uo.upper().strip() for uo in self.unload_options]:
            self.unload_options = list(self.unload_options) + [
                'HEADER',
            ]

    def _build_unload_query(
        self, credentials_block: str, select_query: str, s3_key: str, unload_options: str
    ) -> str:
        return f"""
                    UNLOAD ('{select_query}')
                    TO 's3://{self.s3_bucket}/{s3_key}'
                    credentials
                    '{credentials_block}'
                    {unload_options};
        """

    def execute(self, context: Context) -> None:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        conn = S3Hook.get_connection(conn_id=self.aws_conn_id)
        if conn.extra_dejson.get('role_arn', False):
            credentials_block = f"aws_iam_role={conn.extra_dejson['role_arn']}"
        else:
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
            credentials = s3_hook.get_credentials()
            credentials_block = build_credentials_block(credentials)

        unload_options = '\n\t\t\t'.join(self.unload_options)

        unload_query = self._build_unload_query(
            credentials_block, self.select_query, self.s3_key, unload_options
        )

        self.log.info('Executing UNLOAD command...')
        redshift_hook.run(unload_query, self.autocommit, parameters=self.parameters)
        self.log.info("UNLOAD command complete...")
