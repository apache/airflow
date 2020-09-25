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

from typing import List, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Redshift

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToRedshiftOperator`

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
    :param copy_options: reference to a list of COPY options
    :type copy_options: list
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        schema: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        redshift_conn_id: str = 'redshift_default',
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[bool, str]] = None,
        copy_options: Optional[List] = None,
        autocommit: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.copy_options = copy_options or []
        self.autocommit = autocommit
        self._s3_hook = None
        self._postgres_hook = None

    def execute(self, context):
        self._postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self._s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        credentials = self._s3_hook.get_credentials()
        copy_options = '\n\t\t\t'.join(self.copy_options)

        copy_query = """
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options};
        """.format(
            schema=self.schema,
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            copy_options=copy_options,
        )

        self.log.info('Executing COPY command...')
        self._postgres_hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")
