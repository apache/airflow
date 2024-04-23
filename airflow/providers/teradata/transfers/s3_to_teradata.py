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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToTeradataOperator(BaseOperator):
    """
    Loads CSV, JSON and Parquet format data from Amazon S3 to Teradata.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToTeradataOperator`

    :param s3_source_key: The URI format specifying the location of the S3 object store.(templated)
        The URI format is /s3/YOUR-BUCKET.s3.amazonaws.com/YOUR-BUCKET-NAME.
        Refer to
        https://docs.teradata.com/search/documents?query=native+object+store&sort=last_update&virtual-field=title_only&content-lang=en-US
    :param teradata_table: The name of the teradata table to which the data is transferred.(templated)
    :param aws_conn_id: The Airflow AWS connection used for AWS credentials.
    :param teradata_conn_id: The connection ID used to connect to Teradata
        :ref:`Teradata connection <howto/connection:Teradata>`.

    Note that ``s3_source_key`` and ``teradata_table`` are
    templated, so you can use variables in them if you wish.
    """

    template_fields: Sequence[str] = ("s3_source_key", "teradata_table")
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        s3_source_key: str,
        teradata_table: str,
        aws_conn_id: str = "aws_default",
        teradata_conn_id: str = "teradata_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_source_key = s3_source_key
        self.teradata_table = teradata_table
        self.aws_conn_id = aws_conn_id
        self.teradata_conn_id = teradata_conn_id

    def execute(self, context: Context) -> None:
        self.log.info(
            "transferring data from %s to teradata table %s...", self.s3_source_key, self.teradata_table
        )

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        access_key = (
            s3_hook.conn_config.aws_access_key_id if s3_hook.conn_config.aws_access_key_id is not None else ""
        )
        access_secret = (
            s3_hook.conn_config.aws_secret_access_key
            if s3_hook.conn_config.aws_secret_access_key is not None
            else ""
        )

        teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        sql = f"""
                        CREATE MULTISET TABLE {self.teradata_table} AS
                        (
                            SELECT * FROM (
                                LOCATION = '{self.s3_source_key}'
                                ACCESS_ID= '{access_key}'
                                ACCESS_KEY= '{access_secret}'
                            ) AS d
                        ) WITH DATA
                        """
        try:
            teradata_hook.run(sql, True)
        except Exception as ex:
            # Handling permission issue errors
            if "Error 3524" in str(ex):
                self.log.error("The user does not have CREATE TABLE access in teradata")
                raise
            if "Error 9134" in str(ex):
                self.log.error(
                    "There is an issue with the transfer operation. Please validate s3 and "
                    "teradata connection details."
                )
                raise
            self.log.error("Issue occurred at Teradata: %s", str(ex))
            raise
        self.log.info("The transfer of data from S3 to Teradata was successful")
