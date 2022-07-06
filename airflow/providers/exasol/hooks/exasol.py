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
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pyexasol
from pyexasol import ExaConnection

from airflow.providers.common.sql.hooks.sql import DbApiHook


class ExasolHook(DbApiHook):
    """
    Interact with Exasol.
    You can specify the pyexasol ``compression``, ``encryption``, ``json_lib``
    and ``client_name``  parameters in the extra field of your connection
    as ``{"compression": True, "json_lib": "rapidjson", etc}``.
    See `pyexasol reference
    <https://github.com/badoo/pyexasol/blob/master/docs/REFERENCE.md#connect>`_
    for more details.
    """

    conn_name_attr = 'exasol_conn_id'
    default_conn_name = 'exasol_default'
    conn_type = 'exasol'
    hook_name = 'Exasol'
    supports_autocommit = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self) -> ExaConnection:
        conn_id = getattr(self, self.conn_name_attr)
        conn = self.get_connection(conn_id)
        conn_args = dict(
            dsn=f'{conn.host}:{conn.port}',
            user=conn.login,
            password=conn.password,
            schema=self.schema or conn.schema,
        )
        # check for parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['compression', 'encryption', 'json_lib', 'client_name']:
                conn_args[arg_name] = arg_val

        conn = pyexasol.connect(**conn_args)
        return conn

    def get_pandas_df(
        self, sql: Union[str, list], parameters: Optional[dict] = None, **kwargs
    ) -> pd.DataFrame:
        """
        Executes the sql and returns a pandas dataframe

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param kwargs: (optional) passed into pyexasol.ExaConnection.export_to_pandas method
        """
        with closing(self.get_conn()) as conn:
            df = conn.export_to_pandas(sql, query_params=parameters, **kwargs)
            return df

    def get_records(
        self, sql: Union[str, list], parameters: Optional[dict] = None
    ) -> List[Union[dict, Tuple[Any, ...]]]:
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        with closing(self.get_conn()) as conn:
            with closing(conn.execute(sql, parameters)) as cur:
                return cur.fetchall()

    def get_first(self, sql: Union[str, list], parameters: Optional[dict] = None) -> Optional[Any]:
        """
        Executes the sql and returns the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        with closing(self.get_conn()) as conn:
            with closing(conn.execute(sql, parameters)) as cur:
                return cur.fetchone()

    def export_to_file(
        self,
        filename: str,
        query_or_table: str,
        query_params: Optional[Dict] = None,
        export_params: Optional[Dict] = None,
    ) -> None:
        """
        Exports data to a file.

        :param filename: Path to the file to which the data has to be exported
        :param query_or_table: the sql statement to be executed or table name to export
        :param query_params: Query parameters passed to underlying ``export_to_file``
            method of :class:`~pyexasol.connection.ExaConnection`.
        :param export_params: Extra parameters passed to underlying ``export_to_file``
            method of :class:`~pyexasol.connection.ExaConnection`.
        """
        self.log.info("Getting data from exasol")
        with closing(self.get_conn()) as conn:
            conn.export_to_file(
                dst=filename,
                query_or_table=query_or_table,
                query_params=query_params,
                export_params=export_params,
            )
        self.log.info("Data saved to %s", filename)

    def run(
        self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = None, handler=None
    ) -> Optional[list]:
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of each statement.
        """
        if isinstance(sql, str):
            sql = [sql]

        if sql:
            self.log.debug("Executing %d statements against Exasol DB", len(sql))
        else:
            raise ValueError("List of SQL statements is empty")

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, autocommit)

            for query in sql:
                self.log.info(query)
                with closing(conn.execute(query, parameters)) as cur:
                    results = []

                    if handler is not None:
                        cur = handler(cur)

                    for row in cur:
                        self.log.info("Statement execution info - %s", row)
                        results.append(row)

                    self.log.info(cur.row_count)
            # If autocommit was set to False for db that supports autocommit,
            # or if db does not support autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()

        return results

    def set_autocommit(self, conn, autocommit: bool) -> None:
        """
        Sets the autocommit flag on the connection

        :param conn: Connection to set autocommit setting to.
        :param autocommit: The autocommit setting to set.
        """
        if not self.supports_autocommit and autocommit:
            self.log.warning(
                "%s connection doesn't support autocommit but autocommit activated.",
                getattr(self, self.conn_name_attr),
            )
        conn.set_autocommit(autocommit)

    def get_autocommit(self, conn) -> bool:
        """
        Get autocommit setting for the provided connection.
        Return True if autocommit is set.
        Return False if autocommit is not set or set to False or conn
        does not support autocommit.

        :param conn: Connection to get autocommit setting from.
        :return: connection autocommit setting.
        :rtype: bool
        """
        autocommit = conn.attr.get('autocommit')
        if autocommit is None:
            autocommit = super().get_autocommit(conn)
        return autocommit

    @staticmethod
    def _serialize_cell(cell, conn=None) -> object:
        """
        Exasol will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The cell
        :rtype: object
        """
        return cell
