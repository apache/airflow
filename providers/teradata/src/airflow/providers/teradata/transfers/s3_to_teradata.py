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

from collections.abc import Sequence
from textwrap import dedent
from typing import TYPE_CHECKING

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ModuleNotFoundError as e:
    from airflow.exceptions import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class S3ToTeradataOperator(BaseOperator):
    """
    Loads CSV, JSON and Parquet format data from Amazon S3 to Teradata.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToTeradataOperator`

    :param s3_source_key: The URI format specifying the location of the S3 bucket.(templated)
        The URI format is /s3/YOUR-BUCKET.s3.amazonaws.com/YOUR-BUCKET-NAME.
        Refer to
        https://docs.teradata.com/search/documents?query=native+object+store&sort=last_update&virtual-field=title_only&content-lang=en-US
    :param public_bucket: Specifies whether the provided S3 bucket is public. If the bucket is public,
        it means that anyone can access the objects within it via a URL without requiring authentication.
        If the bucket is private and authentication is not provided, the operator will throw an exception.
    :param teradata_table: The name of the teradata table to which the data is transferred.(templated)
    :param aws_conn_id: The Airflow AWS connection used for AWS credentials.
    :param teradata_conn_id: The connection ID used to connect to Teradata
        :ref:`Teradata connection <howto/connection:Teradata>`.
    :param teradata_authorization_name: The name of Teradata Authorization Database Object,
        is used to control who can access an S3 object store.
        Refer to
        https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Setting-Up-Access/Controlling-Foreign-Table-Access-with-an-AUTHORIZATION-Object

    Note that ``s3_source_key`` and ``teradata_table`` are
    templated, so you can use variables in them if you wish.
    """

    template_fields: Sequence[str] = ("s3_source_key", "teradata_table")
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        s3_source_key: str,
        public_bucket: bool = False,
        teradata_table: str,
        aws_conn_id: str = "aws_default",
        teradata_conn_id: str = "teradata_default",
        teradata_authorization_name: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_source_key = s3_source_key
        self.public_bucket = public_bucket
        self.teradata_table = teradata_table
        self.aws_conn_id = aws_conn_id
        self.teradata_conn_id = teradata_conn_id
        self.teradata_authorization_name = teradata_authorization_name

    def execute(self, context: Context) -> None:
        self.log.info(
            "transferring data from %s to teradata table %s...", self.s3_source_key, self.teradata_table
        )

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        credentials_part = "ACCESS_ID= '' ACCESS_KEY= ''"
        if not self.public_bucket:
            # Accessing data directly from the S3 bucket and creating permanent table inside the database
            if self.teradata_authorization_name:
                credentials_part = f"AUTHORIZATION={self.teradata_authorization_name}"
            else:
                credentials = s3_hook.get_credentials()
                access_key = credentials.access_key
                access_secret = credentials.secret_key
                credentials_part = f"ACCESS_ID= '{access_key}' ACCESS_KEY= '{access_secret}'"
                token = credentials.token
                if token:
                    credentials_part = credentials_part + f" SESSION_TOKEN = '{token}'"
        sql = dedent(f"""
                        CREATE MULTISET TABLE {self.teradata_table} AS
                        (
                            SELECT * FROM (
                                LOCATION = '{self.s3_source_key}'
                                {credentials_part}
                            ) AS d
                        ) WITH DATA
                        """).rstrip()
        try:
            teradata_hook.run(sql, True)
        except Exception as ex:
            self.log.error(str(ex))
            raise
        self.log.info("The transfer of data from S3 to Teradata was successful")
