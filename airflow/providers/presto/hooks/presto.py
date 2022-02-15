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
import os
from typing import Any, Callable, Iterable, Optional

import prestodb
from prestodb.exceptions import DatabaseError
from prestodb.transaction import IsolationLevel

from airflow import AirflowException
from airflow.configuration import conf
from airflow.hooks.dbapi import DbApiHook
from airflow.models import Connection


class PrestoException(Exception):
    """Presto exception"""


def _boolify(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() == 'false':
            return False
        elif value.lower() == 'true':
            return True
    return value


class PrestoHook(DbApiHook):
    """
    Interact with Presto through prestodb.

    >>> ph = PrestoHook()
    >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
    >>> ph.get_records(sql)
    [[340698]]
    """

    conn_name_attr = 'presto_conn_id'
    default_conn_name = 'presto_default'
    conn_type = 'presto'
    hook_name = 'Presto'

    def get_conn(self) -> Connection:
        """Returns a connection object"""
        db = self.get_connection(self.presto_conn_id)  # type: ignore[attr-defined]
        extra = db.extra_dejson
        auth = None
        if db.password and extra.get('auth') == 'kerberos':
            raise AirflowException("Kerberos authorization doesn't support password.")
        elif db.password:
            auth = prestodb.auth.BasicAuthentication(db.login, db.password)
        elif extra.get('auth') == 'kerberos':
            auth = prestodb.auth.KerberosAuthentication(
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

        presto_conn = prestodb.dbapi.connect(
            host=db.host,
            port=db.port,
            user=db.login,
            source=db.extra_dejson.get('source', 'airflow'),
            http_scheme=db.extra_dejson.get('protocol', 'http'),
            catalog=db.extra_dejson.get('catalog', 'hive'),
            schema=db.schema,
            auth=auth,
            isolation_level=self.get_isolation_level(),  # type: ignore[func-returns-value]
        )
        if extra.get('verify') is not None:
            # Unfortunately verify parameter is available via public API.
            # The PR is merged in the presto library, but has not been released.
            # See: https://github.com/prestosql/presto-python-client/pull/31
            presto_conn._http_session.verify = _boolify(extra['verify'])

        return presto_conn

    def get_isolation_level(self) -> Any:
        """Returns an isolation level"""
        db = self.get_connection(self.presto_conn_id)  # type: ignore[attr-defined]
        isolation_level = db.extra_dejson.get('isolation_level', 'AUTOCOMMIT').upper()
        return getattr(IsolationLevel, isolation_level, IsolationLevel.AUTOCOMMIT)

    @staticmethod
    def _strip_sql(sql: str) -> str:
        return sql.strip().rstrip(';')

    def get_records(self, hql, parameters: Optional[dict] = None):
        """Get a set of records from Presto"""
        try:
            return super().get_records(self._strip_sql(hql), parameters)
        except DatabaseError as e:
            raise PrestoException(e)

    def get_first(self, hql: str, parameters: Optional[dict] = None) -> Any:
        """Returns only the first row, regardless of how many rows the query returns."""
        try:
            return super().get_first(self._strip_sql(hql), parameters)
        except DatabaseError as e:
            raise PrestoException(e)

    def get_pandas_df(self, hql, parameters=None, **kwargs):
        """Get a pandas dataframe from a sql query."""
        import pandas

        cursor = self.get_cursor()
        try:
            cursor.execute(self._strip_sql(hql), parameters)
            data = cursor.fetchall()
        except DatabaseError as e:
            raise PrestoException(e)
        column_descriptions = cursor.description
        if data:
            df = pandas.DataFrame(data, **kwargs)
            df.columns = [c[0] for c in column_descriptions]
        else:
            df = pandas.DataFrame(**kwargs)
        return df

    def run(
        self,
        hql,
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Callable] = None,
    ) -> None:
        """Execute the statement against Presto. Can be used to create views."""
        return super().run(sql=self._strip_sql(hql), parameters=parameters, handler=handler)

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
                'Transactions are not enable in presto connection. '
                'Please use the isolation_level property to enable it. '
                'Falling back to insert all rows in one transaction.'
            )
            commit_every = 0

        super().insert_rows(table, rows, target_fields, commit_every)
