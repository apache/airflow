# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from builtins import str

from pyhive import presto
from pyhive.exc import DatabaseError

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
        return presto.connect(
            host=db.host,
            port=db.port,
            username=db.login,
            catalog=db.extra_dejson.get('catalog', 'hive'),
            schema=db.schema)

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def _get_pretty_exception_message(self, e):
        """
        Parses some DatabaseError to provide a better error message
        """
        if (hasattr(e, 'message')
                and 'errorName' in e.message
                and 'message' in e.message):
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
            raise PrestoException(self._parse_exception_message(e))

    def get_first(self, hql, parameters=None):
        """
        Returns only the first row, regardless of how many rows the query
        returns.
        """
        try:
            return super(PrestoHook, self).get_first(
                self._strip_sql(hql), parameters)
        except DatabaseError as e:
            raise PrestoException(self._parse_exception_message(e))

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
            raise PrestoException(self._parse_exception_message(e))
        column_descriptions = cursor.description
        if data:
            df = pandas.DataFrame(data)
            df.columns = [c[0] for c in column_descriptions]
        else:
            df = pandas.DataFrame()
        return df

    def run(self, hql, parameters=None):
        """
        Execute the statement against Presto. Can be used to create views.
        """
        return super(PrestoHook, self).run(self._strip_sql(hql), parameters)

    def insert_rows(self):
        raise NotImplementedError()
