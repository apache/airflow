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

import math
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Union

import oracledb

try:
    import numpy
except ImportError:
    numpy = None  # type: ignore

from airflow.hooks.dbapi import DbApiHook

PARAM_TYPES = {bool, float, int, str}


def _map_param(value):
    if value in PARAM_TYPES:
        # In this branch, value is a Python type; calling it produces
        # an instance of the type which is understood by the Oracle driver
        # in the out parameter mapping mechanism.
        value = value()
    return value


class OracleHook(DbApiHook):
    """
    Interact with Oracle SQL.

    :param oracle_conn_id: The :ref:`Oracle connection id <howto/connection:oracle>`
        used for Oracle credentials.
    """

    conn_name_attr = 'oracle_conn_id'
    default_conn_name = 'oracle_default'
    conn_type = 'oracle'
    hook_name = 'Oracle'

    supports_autocommit = True

    def get_conn(self) -> oracledb.Connection:
        """
        Returns a oracle connection object
        Optional parameters for using a custom DSN connection
        (instead of using a server alias from tnsnames.ora)
        The dsn (data source name) is the TNS entry
        (from the Oracle names server or tnsnames.ora file)
        or is a string like the one returned from makedsn().

        :param dsn: the data source name for the Oracle server
        :param service_name: the db_unique_name of the database
              that you are connecting to (CONNECT_DATA part of TNS)
        :param sid: Oracle System ID that identifies a particular
              database on a system

        You can set these parameters in the extra fields of your connection
        as in

        .. code-block:: python

           {"dsn": ("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=1521))(CONNECT_DATA=(SID=sid)))")}

        see more param detail in `oracledb.connect
        <https://python-oracledb.readthedocs.io/en/latest/api_manual/module.html#oracledb.connect>`_


        """
        conn = self.get_connection(self.oracle_conn_id)  # type: ignore[attr-defined]
        conn_config = {'user': conn.login, 'password': conn.password}
        sid = conn.extra_dejson.get('sid')
        mod = conn.extra_dejson.get('module')
        schema = conn.schema

        service_name = conn.extra_dejson.get('service_name')
        port = conn.port if conn.port else 1521
        if conn.host and sid and not service_name:
            conn_config['dsn'] = oracledb.makedsn(conn.host, port, sid)
        elif conn.host and service_name and not sid:
            conn_config['dsn'] = oracledb.makedsn(conn.host, port, service_name=service_name)
        else:
            dsn = conn.extra_dejson.get('dsn')
            if dsn is None:
                dsn = conn.host
                if conn.port is not None:
                    dsn += ":" + str(conn.port)
                if service_name:
                    dsn += "/" + service_name
                elif conn.schema:
                    warnings.warn(
                        """Using conn.schema to pass the Oracle Service Name is deprecated.
                        Please use conn.extra.service_name instead.""",
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    dsn += "/" + conn.schema
            conn_config['dsn'] = dsn

        if 'events' in conn.extra_dejson:
            conn_config['events'] = conn.extra_dejson.get('events')

        mode = conn.extra_dejson.get('mode', '').lower()
        if mode == 'sysdba':
            conn_config['mode'] = oracledb.AUTH_MODE_SYSDBA
        elif mode == 'sysasm':
            conn_config['mode'] = oracledb.AUTH_MODE_SYSASM
        elif mode == 'sysoper':
            conn_config['mode'] = oracledb.AUTH_MODE_SYSOPER
        elif mode == 'sysbkp':
            conn_config['mode'] = oracledb.AUTH_MODE_SYSBKP
        elif mode == 'sysdgd':
            conn_config['mode'] = oracledb.AUTH_MODE_SYSDGD
        elif mode == 'syskmt':
            conn_config['mode'] = oracledb.AUTH_MODE_SYSKMT
        elif mode == 'sysrac':
            conn_config['mode'] = oracledb.AUTH_MODE_SYSRAC

        purity = conn.extra_dejson.get('purity', '').lower()
        if purity == 'new':
            conn_config['purity'] = oracledb.PURITY_NEW
        elif purity == 'self':
            conn_config['purity'] = oracledb.PURITY_SELF
        elif purity == 'default':
            conn_config['purity'] = oracledb.PURITY_DEFAULT

        conn = oracledb.connect(**conn_config)
        if mod is not None:
            conn.module = mod

        # if Connection.schema is defined, set schema after connecting successfully
        # cannot be part of conn_config
        # https://python-oracledb.readthedocs.io/en/latest/api_manual/connection.html?highlight=schema#Connection.current_schema
        # Only set schema when not using conn.schema as Service Name
        if schema and service_name:
            conn.current_schema = schema

        return conn

    def insert_rows(
        self,
        table: str,
        rows: List[tuple],
        target_fields=None,
        commit_every: int = 1000,
        replace: Optional[bool] = False,
        **kwargs,
    ) -> None:
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        Changes from standard DbApiHook implementation:

        - Oracle SQL queries in oracledb can not be terminated with a semicolon (`;`)
        - Replace NaN values with NULL using `numpy.nan_to_num` (not using
          `is_nan()` because of input types error for strings)
        - Coerce datetime cells to Oracle DATETIME format during insert

        :param table: target Oracle table, use dot notation to target a
            specific database
        :param rows: the rows to insert into the table
        :param target_fields: the names of the columns to fill in the table
        :param commit_every: the maximum number of rows to insert in one transaction
            Default 1000, Set greater than 0.
            Set 1 to insert each row in each single transaction
        :param replace: Whether to replace instead of insert
        """
        if target_fields:
            target_fields = ', '.join(target_fields)
            target_fields = f'({target_fields})'
        else:
            target_fields = ''
        conn = self.get_conn()
        if self.supports_autocommit:
            self.set_autocommit(conn, False)
        cur = conn.cursor()  # type: ignore[attr-defined]
        i = 0
        for row in rows:
            i += 1
            lst = []
            for cell in row:
                if isinstance(cell, str):
                    lst.append("'" + str(cell).replace("'", "''") + "'")
                elif cell is None:
                    lst.append('NULL')
                elif isinstance(cell, float) and math.isnan(cell):  # coerce numpy NaN to NULL
                    lst.append('NULL')
                elif numpy and isinstance(cell, numpy.datetime64):
                    lst.append("'" + str(cell) + "'")
                elif isinstance(cell, datetime):
                    lst.append(
                        "to_date('" + cell.strftime('%Y-%m-%d %H:%M:%S') + "','YYYY-MM-DD HH24:MI:SS')"
                    )
                else:
                    lst.append(str(cell))
            values = tuple(lst)
            sql = f"INSERT /*+ APPEND */ INTO {table} {target_fields} VALUES ({','.join(values)})"
            cur.execute(sql)
            if i % commit_every == 0:
                conn.commit()  # type: ignore[attr-defined]
                self.log.info('Loaded %s into %s rows so far', i, table)
        conn.commit()  # type: ignore[attr-defined]
        cur.close()
        conn.close()  # type: ignore[attr-defined]
        self.log.info('Done loading. Loaded a total of %s rows', i)

    def bulk_insert_rows(
        self,
        table: str,
        rows: List[tuple],
        target_fields: Optional[List[str]] = None,
        commit_every: int = 5000,
    ):
        """
        A performant bulk insert for oracledb
        that uses prepared statements via `executemany()`.
        For best performance, pass in `rows` as an iterator.

        :param table: target Oracle table, use dot notation to target a
            specific database
        :param rows: the rows to insert into the table
        :param target_fields: the names of the columns to fill in the table, default None.
            If None, each rows should have some order as table columns name
        :param commit_every: the maximum number of rows to insert in one transaction
            Default 5000. Set greater than 0. Set 1 to insert each row in each transaction
        """
        if not rows:
            raise ValueError("parameter rows could not be None or empty iterable")
        conn = self.get_conn()
        if self.supports_autocommit:
            self.set_autocommit(conn, False)
        cursor = conn.cursor()  # type: ignore[attr-defined]
        values_base = target_fields if target_fields else rows[0]
        prepared_stm = 'insert into {tablename} {columns} values ({values})'.format(
            tablename=table,
            columns='({})'.format(', '.join(target_fields)) if target_fields else '',
            values=', '.join(':%s' % i for i in range(1, len(values_base) + 1)),
        )
        row_count = 0
        # Chunk the rows
        row_chunk = []
        for row in rows:
            row_chunk.append(row)
            row_count += 1
            if row_count % commit_every == 0:
                cursor.prepare(prepared_stm)
                cursor.executemany(None, row_chunk)
                conn.commit()  # type: ignore[attr-defined]
                self.log.info('[%s] inserted %s rows', table, row_count)
                # Empty chunk
                row_chunk = []
        # Commit the leftover chunk
        cursor.prepare(prepared_stm)
        cursor.executemany(None, row_chunk)
        conn.commit()  # type: ignore[attr-defined]
        self.log.info('[%s] inserted %s rows', table, row_count)
        cursor.close()
        conn.close()  # type: ignore[attr-defined]

    def callproc(
        self,
        identifier: str,
        autocommit: bool = False,
        parameters: Optional[Union[List, Dict]] = None,
    ) -> Optional[Union[List, Dict]]:
        """
        Call the stored procedure identified by the provided string.

        Any 'OUT parameters' must be provided with a value of either the
        expected Python type (e.g., `int`) or an instance of that type.

        The return value is a list or mapping that includes parameters in
        both directions; the actual return type depends on the type of the
        provided `parameters` argument.

        See
        https://python-oracledb.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.var
        for further reference.
        """
        if parameters is None:
            parameters = []

        args = ",".join(
            f":{name}"
            for name in (parameters if isinstance(parameters, dict) else range(1, len(parameters) + 1))
        )

        sql = f"BEGIN {identifier}({args}); END;"

        def handler(cursor):
            if cursor.bindvars is None:
                return

            if isinstance(cursor.bindvars, list):
                return [v.getvalue() for v in cursor.bindvars]

            if isinstance(cursor.bindvars, dict):
                return {n: v.getvalue() for (n, v) in cursor.bindvars.items()}

            raise TypeError(f"Unexpected bindvars: {cursor.bindvars!r}")

        result = self.run(
            sql,
            autocommit=autocommit,
            parameters=(
                {name: _map_param(value) for (name, value) in parameters.items()}
                if isinstance(parameters, dict)
                else [_map_param(value) for value in parameters]
            ),
            handler=handler,
        )

        return result

    # TODO: Merge this implementation back to DbApiHook when dropping
    # support for Airflow 2.2.
    def test_connection(self):
        """Tests the connection by executing a select 1 from dual query"""
        status, message = False, ''
        try:
            if self.get_first("select 1 from dual"):
                status = True
                message = 'Connection successfully tested'
        except Exception as e:
            status = False
            message = str(e)

        return status, message
