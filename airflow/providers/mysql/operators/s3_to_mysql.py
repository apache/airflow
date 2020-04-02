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

import os
from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.decorators import apply_defaults


class S3ToMySqlTransfer(BaseOperator):
    """
    Loads a file from S3 into a MySQL table.

    :param s3_source_key: The path to the file (S3 key) that will be loaded into MySQL.
    :type s3_source_key: str
    :param mysql_table: The MySQL table into where the data will be sent.
    :type mysql_table: str
    :param mysql_duplicate_key_handling: Specify what should happen to duplicate data.
        You can choose either `IGNORE` or `REPLACE`.

        .. seealso::
            https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-duplicate-key-handling
    :type mysql_duplicate_key_handling: str
    :param mysql_extra_options: MySQL options to specify exactly how to load the data.
    :type mysql_extra_options: Optional[str]
    :param aws_conn_id: The S3 connection that contains the credentials to the S3 Bucket.
    :type aws_conn_id: str
    :param mysql_conn_id: The MySQL connection that contains the credentials to the MySQL data base.
    :type mysql_conn_id: str
    """

    template_fields = ('s3_source_key', 'mysql_table',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 s3_source_key: str,
                 mysql_table: str,
                 mysql_duplicate_key_handling: str = 'IGNORE',
                 mysql_extra_options: Optional[str] = None,
                 aws_conn_id: str = 'aws_default',
                 mysql_conn_id: str = 'mysql_default',
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.s3_source_key = s3_source_key
        self.mysql_table = mysql_table
        self.mysql_duplicate_key_handling = mysql_duplicate_key_handling
        self.mysql_extra_options = mysql_extra_options or ''
        self.aws_conn_id = aws_conn_id
        self.mysql_conn_id = mysql_conn_id

    def execute(self, context: dict) -> None:
        """
        Executes the transfer operation from S3 to MySQL.

        :param context: The context that is being provided when executing.
        :type context: dict
        """
        self.log.info('Loading %s to MySql table %s...', self.s3_source_key, self.mysql_table)

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        file = s3_hook.download_file(key=self.s3_source_key)

        try:
            mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
            mysql.bulk_load_custom(
                table=self.mysql_table,
                tmp_file=file,
                duplicate_key_handling=self.mysql_duplicate_key_handling,
                extra_options=self.mysql_extra_options
            )
        finally:
            # Remove file downloaded from s3 to be idempotent.
            os.remove(file)
