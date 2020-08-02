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
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults


class SnowflakeOperator(BaseOperator):
    """
    Executes sql code in a Snowflake database

    :param snowflake_conn_id: reference to specific snowflake connection id
    :type snowflake_conn_id: str
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :type warehouse: str
    :param database: name of database (will overwrite database defined
        in connection)
    :type database: str
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :type schema: str
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
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql, snowflake_conn_id='snowflake_default', parameters=None,
            autocommit=True, warehouse=None, database=None, role=None,
            schema=None, authenticator=None, **kwargs):
        super().__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator

    def get_hook(self):
        """
        Create and return SnowflakeHook.
        :return: a SnowflakeHook instance.
        :rtype: SnowflakeHook
        """
        return SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id,
                             warehouse=self.warehouse, database=self.database,
                             role=self.role, schema=self.schema, authenticator=self.authenticator)

    def execute(self, context):
        """
        Run query on snowflake
        """
        self.log.info('Executing: %s', self.sql)
        hook = self.get_hook()
        hook.run(
            self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters)
