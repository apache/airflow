# -*- coding: utf-8 -*-
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
from typing import Union, List, Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedshiftToS3Transfer(BaseOperator):
    """
    Executes an UNLOAD command to s3 as a CSV with headers

    :param schema: reference to a specific schema in redshift database
    :type schema: str
    :param table: reference to a specific table in redshift database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key
    :type s3_key: str
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param unload_options: reference to a list of UNLOAD options
    :type unload_options: list
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema: str,
            table: str,
            s3_bucket: str,
            s3_key: str,
            redshift_conn_id: str = 'redshift_default',
            aws_conn_id: str = 'aws_default',
            verify: Optional[Union[bool, str]] = None,
            unload_options: Optional[List] = None,
            autocommit: bool = False,
            include_header: bool = False,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.unload_options = unload_options or []  # type: List
        self.autocommit = autocommit
        self.include_header = include_header

        if self.include_header and 'PARALLEL OFF' not in [uo.upper().strip() for uo in self.unload_options]:
            self.unload_options = list(self.unload_options) + ['PARALLEL OFF', ]

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        credentials = self.s3.get_credentials()
        unload_options = '\n\t\t\t'.join(self.unload_options)

        if self.include_header:
            self.log.info("Retrieving headers from %s.%s...",
                          self.schema, self.table)

            columns_query = """SELECT column_name
                                        FROM information_schema.columns
                                        WHERE table_schema = '{schema}'
                                        AND   table_name = '{table}'
                                        ORDER BY ordinal_position
                            """.format(schema=self.schema,
                                       table=self.table)

            cursor = self.hook.get_conn().cursor()
            cursor.execute(columns_query)
            rows = cursor.fetchall()
            columns = [row[0] for row in rows]
            column_names = ', '.join("{0}".format(c) for c in columns)
            column_headers = ', '.join("\\'{0}\\'".format(c) for c in columns)
            column_castings = ', '.join("CAST({0} AS text) AS {0}".format(c)
                                        for c in columns)

            select_query = """SELECT {column_names} FROM
                                    (SELECT 2 sort_order, {column_castings}
                                     FROM {schema}.{table}
                                    UNION ALL
                                    SELECT 1 sort_order, {column_headers})
                                 ORDER BY sort_order"""\
                            .format(column_names=column_names,
                                    column_castings=column_castings,
                                    column_headers=column_headers,
                                    schema=self.schema,
                                    table=self.table)
        else:
            select_query = "SELECT * FROM {schema}.{table}"\
                .format(schema=self.schema,
                        table=self.table)

        unload_query = """
                    UNLOAD ('{select_query}')
                    TO 's3://{s3_bucket}/{s3_key}/{table}_'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {unload_options};
                    """.format(select_query=select_query,
                               table=self.table,
                               s3_bucket=self.s3_bucket,
                               s3_key=self.s3_key,
                               access_key=credentials.access_key,
                               secret_key=credentials.secret_key,
                               unload_options=unload_options)

        self.log.info('Executing UNLOAD command...')
        self.hook.run(unload_query, self.autocommit)
        self.log.info("UNLOAD command complete...")
