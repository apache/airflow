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
import warnings
from typing import Any, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.common import enclose_param


class S3ToSnowflakeOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Snowflake

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToSnowflakeOperator`

    :param s3_keys: reference to a list of S3 keys
    :param table: reference to a specific table in snowflake database
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param stage: reference to a specific snowflake stage. If the stage's schema is not the same as the
        table one, it must be specified
    :param prefix: cloud storage location specified to limit the set of files to load
    :param file_format: reference to a specific file format
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: reference to a specific database in Snowflake connection
    :param columns_array: reference to a specific columns array in snowflake database
    :param pattern: regular expression pattern string specifying the file names and/or paths to match.
        Note: regular expression will be automatically enclose in single quotes
        and all single quotes in expression will replace by two single quotes.
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    """

    template_fields: Sequence[str] = ("s3_keys",)
    template_fields_renderers = {"s3_keys": "json"}

    def __init__(
        self,
        *,
        s3_keys: Optional[list] = None,
        table: str,
        stage: str,
        prefix: Optional[str] = None,
        file_format: str,
        schema: Optional[str] = None,
        columns_array: Optional[list] = None,
        pattern: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        autocommit: bool = True,
        snowflake_conn_id: str = 'snowflake_default',
        role: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        warnings.warn(
            """
            S3ToSnowflakeOperator is deprecated.
            Please use
            `airflow.providers.snowflake.transfers.copy_into_snowflake.CopyFromExternalStageToSnowflakeOperator`.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
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
        self.pattern = pattern
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

        if self.schema:
            into = f"{self.schema}.{self.table}"
        else:
            into = self.table
        if self.columns_array:
            into = f"{into}({','.join(self.columns_array)})"

        sql_parts = [
            f"COPY INTO {into}",
            f"FROM @{self.stage}/{self.prefix or ''}",
        ]
        if self.s3_keys:
            files = ", ".join(map(enclose_param, self.s3_keys))
            sql_parts.append(f"files=({files})")
        sql_parts.append(f"file_format={self.file_format}")
        if self.pattern:
            sql_parts.append(f"pattern={enclose_param(self.pattern)}")
        copy_query = "\n".join(sql_parts)

        self.log.info('Executing COPY command...')
        snowflake_hook.run(copy_query, self.autocommit)
        self.log.info("COPY command completed")
