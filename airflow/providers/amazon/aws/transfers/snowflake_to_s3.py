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
"""Transfers data from Snowflake into a S3 Bucket."""
from typing import List, Optional

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults


class SnowflakeToS3Operator(BaseOperator):
    """
    UNLOAD a query or a table from Snowflake to S3. It is not necessary a conn to S3 because
    Snowflake handles it. You have to follow one of the setups in:
    https://docs.snowflake.com/en/user-guide/data-load-s3-config.html

    You can either pass the parameters table and schema or the parameter sql to unload an entire
    table or a custom query.

    :param s3_path: reference to a specific S3 path within a bucket or stage to download the data
    :type s3_path: str
    :param snowflake_conn_id: reference to a specific snowflake connection
    :type snowflake_conn_id: str
    :param schema: reference to a specific schema in snowflake database
    :type schema: str
    :param table: reference to a specific table in snowflake database
    :type table: str
    :param sql: optional parameter to unload a customized query instead an entire table
    :type sql: str
    :param stage: Reference to a specific Snowflake stage to copy the data into it. This allows you to use
        the method 'Configuring AWS IAM User Credentials' for unloading data to s3. If this is passed,
        s3_bucket can't be used and file_format is not necessary
    :type stage: str
    :param s3_bucket: reference to a specific S3 bucket where the data will be saved. For using it, you
        should have done the one-time setup 'Configuring a Snowflake Storage Integration'
    :type s3_bucket: str
    :param file_format: can be either a previous file format created in Snowflake
        or hardcoded one like ``type = csv field_delimiter = ',' skip_header = 1``
    :type file_format: str
    :param unload_options: reference to a list of UNLOAD options (SINGLE, MAX_FILE_SIZE,
        OVERWRITE etc). Each element of the list has to be a string
    :type unload_options: list
    :param include_header: whether or not to include the header columns in the output file(s)
        if possible
    :type include_header: bool
    :param warehouse: reference to a specific snowflake warehouse to override the one in the conn
    :type warehouse: str
    :param database: reference to a specific snowflake database to override the one in the conn
    :type warehouse: str
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :type role: str
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :type authenticator: str
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: dict
    """

    template_fields = (
        's3_path',
        's3_bucket',
        'sql',
        'snowflake_conn_id',
    )
    template_ext = ('.sql',)
    template_fields_renderers = {"sql": "sql"}

    @apply_defaults
    def __init__(
        self,
        *,
        s3_path: str,
        snowflake_conn_id: str = "snowflake_default",
        schema: Optional[str] = None,
        table: Optional[str] = None,
        sql: Optional[str] = None,
        stage: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        file_format: Optional[str] = None,
        unload_options: Optional[list] = None,
        include_header: Optional[bool] = False,
        database: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_path = s3_path
        self.snowflake_conn_id = snowflake_conn_id
        self.schema = schema
        self.table = table
        self.sql = sql
        self.stage = stage
        self.s3_bucket = s3_bucket
        self.file_format = file_format
        self.unload_options: List = unload_options or []
        self.include_header = include_header
        self.database = database
        self.warehouse = warehouse
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters

    def execute(self, context):
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )
        unload_options = '\n\t\t\t'.join(self.unload_options)

        if self.sql:
            self.sql = f"({self.sql})"
        else:
            self.sql = self.schema + '.' + self.table

        if self.stage:
            unload_query = f"""
            COPY INTO @{self.stage}/{self.s3_path}
            FROM {self.sql}
            """
        else:
            unload_query = f"""
            COPY INTO 's3://{self.s3_bucket}/{self.s3_path}'
            FROM {self.sql}
            STORAGE_INTEGRATION = S3
            """

        if self.file_format:
            unload_query += f' FILE_FORMAT = ({self.file_format})'

        unload_query += f' {unload_options}'

        if self.include_header:
            unload_query += f' HEADER = {self.include_header};'

        self.log.info('Executing UNLOAD command... %s', unload_query)
        snowflake_hook.run(unload_query)
        self.log.info("UNLOAD command complete...")
