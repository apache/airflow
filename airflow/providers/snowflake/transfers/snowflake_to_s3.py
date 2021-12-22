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

"""This module contains Snowflake to AWS S3 operator."""
from typing import Any, Optional

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeToS3Operator(BaseOperator):
    """
    Executes an COPY command to unload files Snowflake to s3

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SnowflakeToS3Operator`

    :param stage: reference to a specific snowflake stage. If the stage's schema is not the same as the
        table one, it must be specified
    :type stage: str
    :param prefix: cloud storage location specified to limit the set of files to load
    :type prefix: str
    :param file_format: reference to a specific file format
    :type file_format: str
    :param on_error: action to be taken in case of any error, possible options are { CONTINUE | SKIP_FILE | 
        SKIP_FILE_<num> | SKIP_FILE_<num>% | ABORT_STATEMENT }
    :type on_error: str
    :param unload_sql: sql that would be used for unloading data
    :type unload_sql: str
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :type warehouse: str
    :param database: reference to a specific database in Snowflake connection
    :type database: str
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
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

    def __init__(
        self,
        *,
        stage: str,
        prefix: Optional[str] = None,
        file_format: str,
        on_error: Optional[str] = None,
        unload_sql: str,
        header: bool = None,
        single: bool = None,
        overwrite: bool = None,
        schema: Optional[str] = None,
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
        self.warehouse = warehouse
        self.database = database
        self.stage = stage
        self.prefix = prefix
        self.file_format = file_format
        self.on_error = on_error
        self.unload_sql = unload_sql
        self.header = header
        self.single = single
        self.overwrite = overwrite
        self.schema = schema
        self.autocommit = autocommit
        self.snowflake_conn_id = snowflake_conn_id
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.query_ids = []

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

        sql_parts = [
            f"COPY INTO @{self.stage}/{self.prefix or ''}",
            f"FROM ({self.unload_sql})",
        ]

        sql_parts.append(f"file_format={self.file_format}")

        if self.on_error:
            sql_parts.append(f"on_error={self.on_error}")

        if self.header:
            sql_parts.append(f"header={self.header}")

        if self.overwrite:
            sql_parts.append(f"overwrite={self.overwrite}")
        
        if self.single:
            sql_parts.append(f"single={self.single}")

        copy_query = "\n".join(sql_parts)

        self.log.info('Executing UNLOAD command...')
        execution_info = snowflake_hook.run(copy_query, self.autocommit)
        self.query_ids = snowflake_hook.query_ids
        self.log.info("UNLOAD command completed")
        return execution_info