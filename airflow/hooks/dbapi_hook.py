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

from contextlib import closing
from datetime import datetime
from typing import Optional

from sqlalchemy import create_engine

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.typing_compat import Protocol


class ConnectorProtocol(Protocol):
    def connect(host, port, username, schema):
        ...


class DbApiHook(BaseHook):
    """
    Abstract base class for sql hooks.
    """
    # Override to provide the connection name.
    conn_name_attr = None  # type: Optional[str]
    # Override to have a default connection id for a particular dbHook
    default_conn_name = 'default_conn_id'
    # Override if this db supports autocommit.
    supports_autocommit = False
    # Override with the object that exposes the connect method
    connector = None  # type: Optional[ConnectorProtocol]

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

    def get_uri(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        login = ''
        if conn.login:
            login = '{conn.login}:{conn.password}@'.format(conn=conn)
        host = conn.host
        if conn.port is not None:
            host += ':{port}'.format(port=conn.port)
        uri = '{conn.conn_type}://{login}{host}/'.format(
            conn=conn, login=login, host=host)
        if conn.schema:
            uri += conn.schema
        return uri

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        if engine_kwargs is None:
            engine_kwargs = {}
        return create_engine(self.get_uri(), **engine_kwargs)

    def get_pandas_df(self, sql, parameters=None):
        """
        Executes the sql and returns a pandas dataframe

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        import pandas.io.sql as psql

        with closing(self.get_conn()) as conn:
            return psql.read_sql(sql, con=conn, params=parameters)

    def get_records(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchall()

    def get_first(self, sql, parameters=None):
        """
        Executes the sql and returns the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchone()

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
        if isinstance(sql, str):
            sql = [sql]

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, autocommit)

            with closing(conn.cursor()) as cur:
                for s in sql:
                    if parameters is not None:
                        self.log.info("{} with parameters {}".format(s, parameters))
                        cur.execute(s, parameters)
                    else:
                        self.log.info(s)
                        cur.execute(s)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()

    def set_autocommit(self, conn, autocommit):
        """
        Sets the autocommit flag on the connection
        """
        if not self.supports_autocommit and autocommit:
            self.log.warning(
                "%s connection doesn't support autocommit but autocommit activated.",
                getattr(self, self.conn_name_attr)
            )
        conn.autocommit = autocommit

    def get_autocommit(self, conn):
        """
        Get autocommit setting for the provided connection.
        Return True if conn.autocommit is set to True.
        Return False if conn.autocommit is not set or set to False or conn
        does not support autocommit.

        :param conn: Connection to get autocommit setting from.
        :type conn: connection object.
        :return: connection autocommit setting.
        :rtype: bool
        """

        return getattr(conn, 'autocommit', False) and self.supports_autocommit

    def get_cursor(self):
        """
        Returns a cursor
        """
        return self.get_conn().cursor()

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000,
                    replace=False):
        """
        A generic way to insert a set of tuples into a table,
        a new transaction is created every commit_every rows

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        :param replace: Whether to replace instead of insert
        :type replace: bool
        """
        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ''
        i = 0
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(conn.cursor()) as cur:
                for i, row in enumerate(rows, 1):
                    lst = []
                    for cell in row:
                        lst.append(self._serialize_cell(cell, conn))
                    values = tuple(lst)
                    placeholders = ["%s", ] * len(values)
                    if not replace:
                        sql = "INSERT INTO "
                    else:
                        sql = "REPLACE INTO "
                    sql += "{0} {1} VALUES ({2})".format(
                        table,
                        target_fields,
                        ",".join(placeholders))
                    cur.execute(sql, values)
                    if commit_every and i % commit_every == 0:
                        conn.commit()
                        self.log.info(
                            "Loaded %s into %s rows so far", i, table
                        )

            conn.commit()
        self.log.info("Done loading. Loaded a total of %s rows", i)

    @staticmethod
    def _serialize_cell(cell, conn=None):
        """
        Returns the SQL literal of the cell as a string.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The serialized cell
        :rtype: str
        """

        if cell is None:
            return None
        if isinstance(cell, datetime):
            return cell.isoformat()
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
