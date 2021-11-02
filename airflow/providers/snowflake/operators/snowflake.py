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
from typing import Any, Optional, SupportsAbs

from airflow.models import BaseOperator
from airflow.operators.sql import SQLCheckOperator, SQLIntervalCheckOperator, SQLValueCheckOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_db_hook(self) -> SnowflakeHook:
    """
    Create and return SnowflakeHook.

    :return: a SnowflakeHook instance.
    :rtype: SnowflakeHook
    """
    return SnowflakeHook(
        snowflake_conn_id=self.snowflake_conn_id,
        warehouse=self.warehouse,
        database=self.database,
        role=self.role,
        schema=self.schema,
        authenticator=self.authenticator,
        session_parameters=self.session_parameters,
    )


class SnowflakeOperator(BaseOperator):
    """
    Executes SQL code in a Snowflake database

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SnowflakeOperator`

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
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
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: dict
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: Any,
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.do_xcom_push = do_xcom_push
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.query_ids = []

    def get_db_hook(self) -> SnowflakeHook:
        return get_db_hook(self)

    def execute(self, context: Any) -> None:
        """Run query on snowflake"""
        self.log.info('Executing: %s', self.sql)
        hook = self.get_db_hook()
        execution_info = hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)
        self.query_ids = hook.query_ids

        if self.do_xcom_push:
            return execution_info


class SnowflakeCheckOperator(SQLCheckOperator):
    """
    Performs a check against Snowflake. The ``SnowflakeCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :type snowflake_conn_id: str
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
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: dict
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: Any,
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.do_xcom_push = do_xcom_push
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.query_ids = []

    def get_db_hook(self) -> SnowflakeHook:
        return get_db_hook(self)


class SnowflakeValueCheckOperator(SQLValueCheckOperator):
    """
    Performs a simple check using sql code against a specified value, within a
    certain level of tolerance.

    :param sql: the sql to be executed
    :type sql: str
    :param pass_value: the value to check against
    :type pass_value: Any
    :param tolerance: (optional) the tolerance allowed to accept the query as
        passing
    :type tolerance: Any
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :type snowflake_conn_id: str
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
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: dict
    """

    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(sql=sql, pass_value=pass_value, tolerance=tolerance, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.do_xcom_push = do_xcom_push
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.query_ids = []

    def get_db_hook(self) -> SnowflakeHook:
        return get_db_hook(self)


class SnowflakeIntervalCheckOperator(SQLIntervalCheckOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    This method constructs a query like so ::

        SELECT {metrics_threshold_dict_key} FROM {table}
        WHERE {date_filter_column}=<date>

    :param table: the table name
    :type table: str
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :type days_back: int
    :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
        example 'COUNT(*)': 1.5 would require a 50 percent or less difference
        between the current day, and the prior days_back.
    :type metrics_thresholds: dict
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :type snowflake_conn_id: str
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
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: dict
    """

    def __init__(
        self,
        *,
        table: str,
        metrics_thresholds: dict,
        date_filter_column: str = 'ds',
        days_back: SupportsAbs[int] = -7,
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            table=table,
            metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column,
            days_back=days_back,
            **kwargs,
        )
        self.snowflake_conn_id = snowflake_conn_id
        self.autocommit = autocommit
        self.do_xcom_push = do_xcom_push
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.query_ids = []

    def get_db_hook(self) -> SnowflakeHook:
        return get_db_hook(self)
