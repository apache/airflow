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
"""
Transfers data from Snowflake into a S3 Bucket.
"""
from typing import List, Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeToS3Operator(BaseOperator):
    """
    UNLOAD a query or a table from Snowflake to S3

    :param schema: reference to a specific schema in snowflake database
    :type schema: str
    :param table: reference to a specific table in snowflake database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket where the data will be saved
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key within the previous bucket
    :type s3_key: str
    :param file_format: can be either a previous file format created in Snowflake console
        or hardcoded one like ``type = csv field_delimiter = ',' skip_header = 1``
    :type file_format: str
    :param sql: optional parameter to unload a customized query instead an entire table
    :type sql: str
    :param snowflake_conn_id: reference to a specific snowflake database
    :type snowflake_conn_id: str
    :param unload_options: reference to a list of UNLOAD options (SINGLE, MAX_FILE_SIZE,
        OVERWRITE etc). Each element of the list has to be a string
    :type unload_options: list
    """

    template_fields = (
        's3_key',
        's3_bucket',
        'sql',
        'table',
        'snowflake_conn_id',
    )
    template_ext = ('.sql',)
    ui_color = '#ffebb2'

    @apply_defaults
    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        file_format: str,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        sql: Optional[str] = None,
        snowflake_conn_id: str = "snowflake_default",
        unload_options: Optional[list] = None,
        autocommit: bool = True,
        include_header: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.snowflake_conn_id = snowflake_conn_id
        self.schema = schema
        self.table = table
        self.sql = sql
        self.unload_options = unload_options or []  # type: List
        self.autocommit = autocommit
        self.include_header = include_header

    def execute(self, context):
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        unload_options = '\n\t'.join(self.unload_options)

        if not self.sql:
            self.sql = self.schema + '.' + self.table

        unload_query = f"""
        COPY INTO 's3://{self.s3_bucket}/{self.s3_key}'
        FROM ({self.sql})
        STORAGE_INTEGRATION = S3
        FILE_FORMAT = ({self.file_format})
        {unload_options}
        HEADER = {self.include_header};
        """

        self.log.info('Executing UNLOAD command...')
        snowflake_hook.run(unload_query, self.autocommit)
        self.log.info("UNLOAD command complete...")
