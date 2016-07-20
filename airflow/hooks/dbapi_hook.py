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
from past.builtins import basestring
from datetime import datetime
import numpy
import logging
import sys

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class DbApiHook(BaseHook):
    """
    Abstract base class for sql hooks.
    """
    # Override to provide the connection name.
    conn_name_attr = None
    # Override to have a default connection id for a particular dbHook
    default_conn_name = 'default_conn_id'
    # Override if this db supports autocommit.
    supports_autocommit = False
    # Override with the object that exposes the connect method
    connector = None

    def __init__(self, *args, **kwargs):
        if not self.conn_name_attr:
            raise AirflowException("conn_name_attr is not defined")
        elif len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])

    def get_conn(self):
        """Returns a connection object
        """
        db = self.get_connection(getattr(self, self.conn_name_attr))
        return self.connector.connect(
            host=db.host,
            port=db.port,
            username=db.login,
            schema=db.schema)

    def get_pandas_df(self, sql, parameters=None):
        """
        Executes the sql and returns a pandas dataframe

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        import pandas.io.sql as psql
        conn = self.get_conn()
        df = psql.read_sql(sql, con=conn, params=parameters)
        conn.close()
        return df

    def get_records(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        conn = self.get_conn()
        cur = self.get_cursor()
        if parameters is not None:
            cur.execute(sql, parameters)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    def get_first(self, sql, parameters=None):
        """
        Executes the sql and returns the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        conn = self.get_conn()
        cur = conn.cursor()
        if parameters is not None:
            cur.execute(sql, parameters)
        else:
            cur.execute(sql)
        rows = cur.fetchone()
        cur.close()
        conn.close()
        return rows

    def run(self, sql, autocommit=False, parameters=None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        conn = self.get_conn()
        if isinstance(sql, basestring):
            sql = [sql]

        if self.supports_autocommit:
            self.set_autocommit(conn, autocommit)

        cur = conn.cursor()
        for s in sql:
            if sys.version_info[0] < 3:
                s = s.encode('utf-8')
            logging.info(s)
            if parameters is not None:
                cur.execute(s, parameters)
            else:
                cur.execute(s)
        cur.close()
        conn.commit()
        conn.close()

    def set_autocommit(self, conn, autocommit):
        conn.autocommit = autocommit

    def get_cursor(self):
        """
        Returns a cursor
        """
        return self.get_conn().cursor()

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        """
        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''
        conn = self.get_conn()
        cur = conn.cursor()
        if self.supports_autocommit:
            cur.execute('SET autocommit = 0')
        conn.commit()
        i = 0
        for row in rows:
            i += 1
            l = []
            for cell in row:
                l.append(self._serialize_cell(cell))
            values = tuple(l)
            sql = "INSERT INTO {0} {1} VALUES ({2});".format(
                table,
                target_fields,
                ",".join(values))
            cur.execute(sql)
            if commit_every and i % commit_every == 0:
                conn.commit()
                logging.info(
                    "Loaded {i} into {table} rows so far".format(**locals()))
        conn.commit()
        cur.close()
        conn.close()
        logging.info(
            "Done loading. Loaded a total of {i} rows".format(**locals()))

    @staticmethod
    def _serialize_cell(cell):
        if isinstance(cell, basestring):
            return "'" + str(cell).replace("'", "''") + "'"
        elif cell is None:
            return 'NULL'
        elif isinstance(cell, numpy.datetime64):
            return "'" + str(cell) + "'"
        elif isinstance(cell, datetime):
            return "'" + cell.isoformat() + "'"
        else:
            return str(cell)

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file

        :param table: The name of the source table
        :type table: str
        :param tmp_file: The path of the target file
        :type tmp_file: str
        """
        raise NotImplementedError()

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table

        :param table: The name of the target table
        :type table: str
        :param tmp_file: The path of the file to load into the table
        :type tmp_file: str
        """
        raise NotImplementedError()
