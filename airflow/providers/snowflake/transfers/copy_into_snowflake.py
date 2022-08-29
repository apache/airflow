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

"""This module contains abstract operator that child classes implements
COPY INTO <TABLE> SQL in Snowflake
"""


from typing import Any, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.common import enclose_param


class CopyFromExternalStageToSnowflakeOperator(BaseOperator):
    """
    Executes a COPY INTO command to load files from an external stage from clouds to Snowflake

    This operator requires the snowflake_conn_id connection. The snowflake host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation.

    :param namespace: snowflake namespace
    :param table: snowflake table
    :param file_format: file format name i.e. CSV, AVRO, etc
    :param stage: reference to a specific snowflake stage. If the stage's schema is not the same as the
        table one, it must be specified
    :param prefix: cloud storage location specified to limit the set of files to load
    :param files: files to load into table
    :param pattern: pattern to load files from external location to table
    :param copy_into_postifx: optional sql postfix for INSERT INTO query
           such as `formatTypeOptions` and `copyOptions`
    :param snowflake_conn_id:  Reference to :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param account: snowflake account name
    :param warehouse: name of snowflake warehouse
    :param database: name of snowflake database
    :param region: name of snowflake region
    :param role: name of snowflake role
    :param schema: name of snowflake schema
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        ``https://<your_okta_account_name>.okta.com`` to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param copy_options: snowflake COPY INTO syntax copy options
    :param validation_mode: snowflake COPY INTO syntax validation mode

    """

    template_fields: Sequence[str] = ("files",)
    template_fields_renderers = {"files": "json"}

    def __init__(
        self,
        *,
        files: Optional[list] = None,
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
        copy_options: Optional[str] = None,
        validation_mode: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.files = files
        self.table = table
        self.stage = stage
        self.prefix = prefix
        self.file_format = file_format
        self.schema = schema
        self.columns_array = columns_array
        self.pattern = pattern
        self.warehouse = warehouse
        self.database = database
        self.autocommit = autocommit
        self.snowflake_conn_id = snowflake_conn_id
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.copy_options = copy_options
        self.validation_mode = validation_mode

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
            into = f"{into}({', '.join(self.columns_array)})"

        sql = f"""
        COPY INTO {into}
             FROM  @{self.stage}/{self.prefix or ""}
        {"FILES=" + ",".join(map(enclose_param ,self.files)) if self.files else ""}
        {"PATTERN=" + enclose_param(self.pattern) if self.pattern else ""}
        FILE_FORMAT={self.file_format}
        {self.copy_options or ""}
        {self.validation_mode or ""}
        """
        self.log.info('Executing COPY command...')
        snowflake_hook.run(sql=sql, autocommit=self.autocommit)
        self.log.info("COPY command completed")
