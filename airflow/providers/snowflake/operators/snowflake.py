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
from typing import Any, Iterable, List, Mapping, Optional, Sequence, SupportsAbs, Union

from airflow.models import BaseOperator
from airflow.operators.sql import SQLCheckOperator, SQLIntervalCheckOperator, SQLValueCheckOperator
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
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
    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
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

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: Union[str, Iterable[str]],
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[Union[Iterable, Mapping]] = None,
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
        self.query_ids: List[str] = []

    def get_db_hook(self) -> SnowflakeHook:
        return get_db_hook(self)

    def execute(self, context: Any):
        """Run query on snowflake"""
        self.log.info('Executing: %s', self.sql)
        hook = self.get_db_hook()
        execution_info = hook.run(self.sql, self.autocommit, self.parameters, fetch_all_handler)
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

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
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

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: str,
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[Union[Iterable, Mapping]] = None,
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
        self.query_ids: List[str] = []

    def get_db_hook(self) -> SnowflakeHook:
        return get_db_hook(self)


class SnowflakeValueCheckOperator(SQLValueCheckOperator):
    """
    Performs a simple check using sql code against a specified value, within a
    certain level of tolerance.

    :param sql: the sql to be executed
    :param pass_value: the value to check against
    :param tolerance: (optional) the tolerance allowed to accept the query as
        passing
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
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

    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[Union[Iterable, Mapping]] = None,
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
        self.query_ids: List[str] = []

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
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
        example 'COUNT(*)': 1.5 would require a 50 percent or less difference
        between the current day, and the prior days_back.
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
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

    def __init__(
        self,
        *,
        table: str,
        metrics_thresholds: dict,
        date_filter_column: str = 'ds',
        days_back: SupportsAbs[int] = -7,
        snowflake_conn_id: str = 'snowflake_default',
        parameters: Optional[Union[Iterable, Mapping]] = None,
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
        self.query_ids: List[str] = []

    def get_db_hook(self) -> SnowflakeHook:
        return get_db_hook(self)
