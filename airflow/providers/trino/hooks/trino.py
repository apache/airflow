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
import json
import os
import warnings
from typing import Any, Callable, Iterable, Optional, overload

import trino
from trino.exceptions import DatabaseError
from trino.transaction import IsolationLevel

from airflow import AirflowException
from airflow.configuration import conf
from airflow.hooks.dbapi import DbApiHook
from airflow.models import Connection
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING

try:
    from airflow.utils.operator_helpers import DEFAULT_FORMAT_PREFIX
except ImportError:
    # This is from airflow.utils.operator_helpers,
    # For the sake of provider backward compatibility, this is hardcoded if import fails
    # https://github.com/apache/airflow/pull/22416#issuecomment-1075531290
    DEFAULT_FORMAT_PREFIX = 'airflow.ctx.'


def generate_trino_client_info() -> str:
    """Return json string with dag_id, task_id, execution_date and try_number"""
    context_var = {
        format_map['default'].replace(DEFAULT_FORMAT_PREFIX, ''): os.environ.get(
            format_map['env_var_format'], ''
        )
        for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
    }
    # try_number isn't available in context for airflow < 2.2.5
    # https://github.com/apache/airflow/issues/23059
    try_number = context_var.get('try_number', '')
    task_info = {
        'dag_id': context_var['dag_id'],
        'task_id': context_var['task_id'],
        'execution_date': context_var['execution_date'],
        'try_number': try_number,
        'dag_run_id': context_var['dag_run_id'],
        'dag_owner': context_var['dag_owner'],
    }
    return json.dumps(task_info, sort_keys=True)


class TrinoException(Exception):
    """Trino exception"""


def _boolify(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() == 'false':
            return False
        elif value.lower() == 'true':
            return True
    return value


class TrinoHook(DbApiHook):
    """
    Interact with Trino through trino package.

    >>> ph = TrinoHook()
    >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
    >>> ph.get_records(sql)
    [[340698]]
    """

    conn_name_attr = 'trino_conn_id'
    default_conn_name = 'trino_default'
    conn_type = 'trino'
    hook_name = 'Trino'

    def get_conn(self) -> Connection:
        """Returns a connection object"""
        db = self.get_connection(self.trino_conn_id)  # type: ignore[attr-defined]
        extra = db.extra_dejson
        auth = None
        user = db.login
        if db.password and extra.get('auth') == 'kerberos':
            raise AirflowException("Kerberos authorization doesn't support password.")
        elif db.password:
            auth = trino.auth.BasicAuthentication(db.login, db.password)  # type: ignore[attr-defined]
        elif extra.get('auth') == 'jwt':
            auth = trino.auth.JWTAuthentication(token=extra.get('jwt__token'))
        elif extra.get('auth') == 'kerberos':
            auth = trino.auth.KerberosAuthentication(  # type: ignore[attr-defined]
                config=extra.get('kerberos__config', os.environ.get('KRB5_CONFIG')),
                service_name=extra.get('kerberos__service_name'),
                mutual_authentication=_boolify(extra.get('kerberos__mutual_authentication', False)),
                force_preemptive=_boolify(extra.get('kerberos__force_preemptive', False)),
                hostname_override=extra.get('kerberos__hostname_override'),
                sanitize_mutual_error_response=_boolify(
                    extra.get('kerberos__sanitize_mutual_error_response', True)
                ),
                principal=extra.get('kerberos__principal', conf.get('kerberos', 'principal')),
                delegate=_boolify(extra.get('kerberos__delegate', False)),
                ca_bundle=extra.get('kerberos__ca_bundle'),
            )

        if _boolify(extra.get('impersonate_as_owner', False)):
            user = os.getenv('AIRFLOW_CTX_DAG_OWNER', None)
            if user is None:
                user = db.login
        http_headers = {"X-Trino-Client-Info": generate_trino_client_info()}
        trino_conn = trino.dbapi.connect(
            host=db.host,
            port=db.port,
            user=user,
            source=extra.get('source', 'airflow'),
            http_scheme=extra.get('protocol', 'http'),
            http_headers=http_headers,
            catalog=extra.get('catalog', 'hive'),
            schema=db.schema,
            auth=auth,
            # type: ignore[func-returns-value]
            isolation_level=self.get_isolation_level(),
            verify=_boolify(extra.get('verify', True)),
        )

        return trino_conn

    def get_isolation_level(self) -> Any:
        """Returns an isolation level"""
        db = self.get_connection(self.trino_conn_id)  # type: ignore[attr-defined]
        isolation_level = db.extra_dejson.get('isolation_level', 'AUTOCOMMIT').upper()
        return getattr(IsolationLevel, isolation_level, IsolationLevel.AUTOCOMMIT)

    @staticmethod
    def _strip_sql(sql: str) -> str:
        return sql.strip().rstrip(';')

    @overload
    def get_records(self, sql: str = "", parameters: Optional[dict] = None):
        """Get a set of records from Trino

        :param sql: SQL statement to be executed.
        :param parameters: The parameters to render the SQL query with.
        """

    @overload
    def get_records(self, sql: str = "", parameters: Optional[dict] = None, hql: str = ""):
        """:sphinx-autoapi-skip:"""

    def get_records(self, sql: str = "", parameters: Optional[dict] = None, hql: str = ""):
        """:sphinx-autoapi-skip:"""
        if hql:
            warnings.warn(
                "The hql parameter has been deprecated. You should pass the sql parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            sql = hql

        try:
            return super().get_records(self._strip_sql(sql), parameters)
        except DatabaseError as e:
            raise TrinoException(e)

    @overload
    def get_first(self, sql: str = "", parameters: Optional[dict] = None) -> Any:
        """Returns only the first row, regardless of how many rows the query returns.

        :param sql: SQL statement to be executed.
        :param parameters: The parameters to render the SQL query with.
        """

    @overload
    def get_first(self, sql: str = "", parameters: Optional[dict] = None, hql: str = "") -> Any:
        """:sphinx-autoapi-skip:"""

    def get_first(self, sql: str = "", parameters: Optional[dict] = None, hql: str = "") -> Any:
        """:sphinx-autoapi-skip:"""
        if hql:
            warnings.warn(
                "The hql parameter has been deprecated. You should pass the sql parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            sql = hql

        try:
            return super().get_first(self._strip_sql(sql), parameters)
        except DatabaseError as e:
            raise TrinoException(e)

    @overload
    def get_pandas_df(
        self, sql: str = "", parameters: Optional[dict] = None, **kwargs
    ):  # type: ignore[override]
        """Get a pandas dataframe from a sql query.

        :param sql: SQL statement to be executed.
        :param parameters: The parameters to render the SQL query with.
        """

    @overload
    def get_pandas_df(
        self, sql: str = "", parameters: Optional[dict] = None, hql: str = "", **kwargs
    ):  # type: ignore[override]
        """:sphinx-autoapi-skip:"""

    def get_pandas_df(
        self, sql: str = "", parameters: Optional[dict] = None, hql: str = "", **kwargs
    ):  # type: ignore[override]
        """:sphinx-autoapi-skip:"""
        if hql:
            warnings.warn(
                "The hql parameter has been deprecated. You should pass the sql parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            sql = hql

        import pandas

        cursor = self.get_cursor()
        try:
            cursor.execute(self._strip_sql(sql), parameters)
            data = cursor.fetchall()
        except DatabaseError as e:
            raise TrinoException(e)
        column_descriptions = cursor.description
        if data:
            df = pandas.DataFrame(data, **kwargs)
            df.columns = [c[0] for c in column_descriptions]
        else:
            df = pandas.DataFrame(**kwargs)
        return df

    @overload
    def run(
        self,
        sql: str = "",
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Callable] = None,
    ) -> None:
        """Execute the statement against Trino. Can be used to create views."""

    @overload
    def run(
        self,
        sql: str = "",
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Callable] = None,
        hql: str = "",
    ) -> None:
        """:sphinx-autoapi-skip:"""

    def run(
        self,
        sql: str = "",
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Callable] = None,
        hql: str = "",
    ) -> None:
        """:sphinx-autoapi-skip:"""
        if hql:
            warnings.warn(
                "The hql parameter has been deprecated. You should pass the sql parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            sql = hql

        return super().run(
            sql=self._strip_sql(sql), autocommit=autocommit, parameters=parameters, handler=handler
        )

    def insert_rows(
        self,
        table: str,
        rows: Iterable[tuple],
        target_fields: Optional[Iterable[str]] = None,
        commit_every: int = 0,
        replace: bool = False,
        **kwargs,
    ) -> None:
        """
        A generic way to insert a set of tuples into a table.

        :param table: Name of the target table
        :param rows: The rows to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert
        """
        if self.get_isolation_level() == IsolationLevel.AUTOCOMMIT:
            self.log.info(
                'Transactions are not enable in trino connection. '
                'Please use the isolation_level property to enable it. '
                'Falling back to insert all rows in one transaction.'
            )
            commit_every = 0

        super().insert_rows(table, rows, target_fields, commit_every, replace)
