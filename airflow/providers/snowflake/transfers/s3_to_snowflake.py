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
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :type schema: str
    :param stage: reference to a specific snowflake stage. If the stage's schema is not the same as the
        table one, it must be specified
    :type stage: str
    :param prefix: cloud storage location specified to limit the set of files to load
    :type prefix: str
    :param file_format: reference to a specific file format
    :type file_format: str
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :type warehouse: str
    :param database: reference to a specific database in Snowflake connection
    :type database: str
    :param columns_array: reference to a specific columns array in snowflake database
    :type columns_array: list
    :param snowflake_conn_id: reference to a specific snowflake connection
    :type snowflake_conn_id: str
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

    @apply_defaults
    def __init__(
        self,
        *,
        s3_keys: Optional[list] = None,
        table: str,
        stage: str,
        prefix: Optional[str] = None,
        file_format: str,
        schema: str,  # TODO: shouldn't be required, rely on session/user defaults
        columns_array: Optional[list] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        autocommit: bool = True,
        snowflake_conn_id: str = 'snowflake_default',
        role: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_keys = s3_keys
        self.table = table
        self.warehouse = warehouse
        self.database = database
        self.stage = stage
        self.prefix = prefix
        self.file_format = file_format
        self.schema = schema
        self.columns_array = columns_array
        self.autocommit = autocommit
        self.snowflake_conn_id = snowflake_conn_id
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters

    def execute(self, context: Any) -> None:
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )

        files = ""
        if self.s3_keys:
            files = "files=({})".format(", ".join(f"'{key}'" for key in self.s3_keys))

        # we can extend this based on stage
        base_sql = """
                    FROM @{stage}/{prefix}
                    {files}
                    file_format={file_format}
                """.format(
            stage=self.stage,
            prefix=(self.prefix if self.prefix else ""),
            files=files,
            file_format=self.file_format,
        )

        if self.columns_array:
            copy_query = """
                COPY INTO {schema}.{table}({columns}) {base_sql}
            """.format(
                schema=self.schema, table=self.table, columns=",".join(self.columns_array), base_sql=base_sql
            )
        else:
            copy_query = f"""
                COPY INTO {self.schema}.{self.table} {base_sql}
            """
        copy_query = "\n".join(line.strip() for line in copy_query.splitlines())

        self.log.info('Executing COPY command...')
        snowflake_hook.run(copy_query, self.autocommit)
        self.log.info("COPY command completed")
