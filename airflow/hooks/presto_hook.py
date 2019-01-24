# -*- coding: utf-8 -*-
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

import time
from builtins import str

from pyhive import presto
from pyhive.exc import DatabaseError
from requests.auth import HTTPBasicAuth

from airflow.hooks.dbapi_hook import DbApiHook


class PrestoException(Exception):
    pass


class PrestoHook(DbApiHook):
    """
    Interact with Presto through PyHive!

    >>> ph = PrestoHook()
    >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
    >>> ph.get_records(sql)
    [[340698]]
    """

    conn_name_attr = 'presto_conn_id'
    default_conn_name = 'presto_default'

    def get_conn(self):
        """Returns a connection object"""
        db = self.get_connection(self.presto_conn_id)
        reqkwargs = None
        if db.password is not None:
            reqkwargs = {'auth': HTTPBasicAuth(db.login, db.password)}
        return presto.connect(
            host=db.host,
            port=db.port,
            username=db.login,
            source=db.extra_dejson.get('source', 'airflow'),
            protocol=db.extra_dejson.get('protocol', 'http'),
            catalog=db.extra_dejson.get('catalog', 'hive'),
            requests_kwargs=reqkwargs,
            schema=db.schema)

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    @staticmethod
    def _get_pretty_exception_message(e):
        """
        Parses some DatabaseError to provide a better error message
        """
        if (hasattr(e, 'message') and
            'errorName' in e.message and
                'message' in e.message):
            return ('{name}: {message}'.format(
                    name=e.message['errorName'],
                    message=e.message['message']))
        else:
            return str(e)

    def get_records(self, hql, parameters=None):
        """
        Get a set of records from Presto
        """
        try:
            return super(PrestoHook, self).get_records(
                self._strip_sql(hql), parameters)
        except DatabaseError as e:
            raise PrestoException(self._get_pretty_exception_message(e))

    def get_first(self, hql, parameters=None):
        """
        Returns only the first row, regardless of how many rows the query
        returns.
        """
        try:
            return super(PrestoHook, self).get_first(
                self._strip_sql(hql), parameters)
        except DatabaseError as e:
            raise PrestoException(self._get_pretty_exception_message(e))

    def get_pandas_df(self, hql, parameters=None):
        """
        Get a pandas dataframe from a sql query.
        """
        import pandas
        cursor = self.get_cursor()
        try:
            cursor.execute(self._strip_sql(hql), parameters)
            data = cursor.fetchall()
        except DatabaseError as e:
            raise PrestoException(self._get_pretty_exception_message(e))
        column_descriptions = cursor.description
        if data:
            df = pandas.DataFrame(data)
            df.columns = [c[0] for c in column_descriptions]
        else:
            df = pandas.DataFrame()
        return df

    def run(self, sql, parameters=None, poll_interval=None):
        """
        Execute statement(s) against Presto. By default, statements are
        executed asynchronously. To execute each synchronously, pass a non-None
        poll_interval.

        :param sql: the statement(s) to be executed
        :type sql: str or iterable
        :param parameters: the parameters to render the statement(s) with
        :type parameters: mapping or iterable
        :param poll_interval: how often, in seconds, to check the execution
            status of each statement; set to None
        :type poll_interval: int or float
        """
        if isinstance(sql, str):
            sql = [sql]

        cursor = self.get_conn().cursor()

        for stmt in sql:
            stmt = self._strip_sql(stmt)
            self.log.info("{} with parameters {}".format(stmt, parameters))
            cursor.execute(stmt, parameters)

            if poll_interval is not None:
                while not self.execution_finished(cursor):
                    time.sleep(poll_interval)

    @classmethod
    def execution_finished(cls, cursor):
        """
        Return a bool indicating whether the latest statement executed by
        cursor has finished executing.

        :param cursor: a cursor
        :type cursor: presto.Cursor
        """
        return cursor.poll() is None

    # TODO Enable commit_every once PyHive supports transaction.
    # Unfortunately, PyHive 0.5.1 doesn't support transaction for now,
    # whereas Presto 0.132+ does.
    def insert_rows(self, table, rows, target_fields=None):
        """
        A generic way to insert a set of tuples into a table.

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        """
        super(PrestoHook, self).insert_rows(table, rows, target_fields, 0)
