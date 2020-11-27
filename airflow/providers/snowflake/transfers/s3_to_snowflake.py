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

"""This module contains AWS S3 to Snowflake operator."""
from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults


class S3ToSnowflakeOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Snowflake

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToSnowflakeOperator`

    :param s3_keys: reference to a list of S3 keys
    :type s3_keys: list
    :param table: reference to a specific table in snowflake database
    :type table: str
    :param stage: reference to a specific snowflake stage
    :type stage: str
    :param file_format: reference to a specific file format
    :type file_format: str
    :param schema: reference to a specific schema in snowflake database
    :type schema: str
    :param columns_array: reference to a specific columns array in snowflake database
    :type columns_array: list
    :param snowflake_conn_id: reference to a specific snowflake database
    :type snowflake_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        *,
        s3_keys: list,
        table: str,
        stage: Any,
        file_format: str,
        schema: str,  # TODO: shouldn't be required, rely on session/user defaults
        columns_array: Optional[list] = None,
        autocommit: bool = True,
        snowflake_conn_id: str = 'snowflake_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_keys = s3_keys
        self.table = table
        self.stage = stage
        self.file_format = file_format
        self.schema = schema
        self.columns_array = columns_array
        self.autocommit = autocommit
        self.snowflake_conn_id = snowflake_conn_id

    def execute(self, context: Any) -> None:
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

        # Snowflake won't accept list of files it has to be tuple only.
        # but in python tuple([1]) = (1,) => which is invalid for snowflake
        files = str(self.s3_keys)
        files = files.replace('[', '(')
        files = files.replace(']', ')')

        # we can extend this based on stage
        base_sql = """
                    FROM @{stage}/
                    files={files}
                    file_format={file_format}
                """.format(
            stage=self.stage, files=files, file_format=self.file_format
        )

        if self.columns_array:
            copy_query = """
                COPY INTO {schema}.{table}({columns}) {base_sql}
            """.format(
                schema=self.schema, table=self.table, columns=",".join(self.columns_array), base_sql=base_sql
            )
        else:
            copy_query = """
                COPY INTO {schema}.{table} {base_sql}
            """.format(
                schema=self.schema, table=self.table, base_sql=base_sql
            )

        self.log.info('Executing COPY command...')
        snowflake_hook.run(copy_query, self.autocommit)
        self.log.info("COPY command completed")
