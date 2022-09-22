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
from __future__ import annotations

import math
import warnings
from datetime import datetime
from typing import Any

import oracledb

try:
    import numpy
except ImportError:
    numpy = None  # type: ignore

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook

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

    conn_name_attr = 'oracle_conn_id'
    default_conn_name = 'oracle_default'
    conn_type = 'oracle'
    hook_name = 'Oracle'

    supports_autocommit = True

    def __init__(
        self,
        oracle_conn_id: str | None = 'oracle_default',
        host: str | None = None,
        schema: str | None = None,
        login: str | None = None,
        password: str | None = None,
        sid: str | None = None,
        mod: str | None = None,
        service_name: str | None = None,
        port: int = 1521,
        dsn: str | None = None,
        events: bool = False,
        mode: int = oracledb.AUTH_MODE_DEFAULT,
        purity: int = oracledb.PURITY_DEFAULT,
        thick_mode: bool = False,
        thick_mode_lib_dir: str | None = None,
        thick_mode_config_dir: str | None = None,
        fetch_decimals: bool = False,
        fetch_lobs: bool = True,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.oracle_conn_id = oracle_conn_id
        self.host = host
        self.schema = schema
        self.login = login
        self.password = password
        self.sid = sid
        self.mod = mod
        self.service_name = service_name
        self.port = port
        self.dsn = dsn
        self.events = events
        self.mode = mode
        self.purity = purity
        self.thick_mode = thick_mode
        self.thick_mode_lib_dir = thick_mode_lib_dir
        self.thick_mode_config_dir = thick_mode_config_dir
        self.fetch_decimals = fetch_decimals
        self.fetch_lobs = fetch_lobs

        self.conn_config: dict[str, Any] = {}

        # Use connection to override defaults
        if self.oracle_conn_id is not None:
            conn = self.get_connection(self.oracle_conn_id)
            self.host = conn.host if conn.host else self.host
            self.login = conn.login if conn.login else self.login
            self.password = conn.password if conn.password else self.password
            self.port = conn.port if conn.port else self.port
            self.schema = conn.schema if conn.schema else self.schema

            if conn.extra is not None:
                extra_options = conn.extra_dejson

                sid = extra_options.get('sid')
                self.sid = sid if sid else self.sid

                mod = extra_options.get('mod')
                self.mod = mod if mod else self.mod

                service_name = extra_options.get('service_name')
                self.service_name = service_name if service_name else self.service_name

                dsn = extra_options.get('dsn')
                self.dsn = dsn if dsn else self.dsn

                events = extra_options.get('events')
                self.events = events if events is not None else self.events

                mode = extra_options.get('mode', '').lower()
                if mode == 'sysdba':
                    self.mode = oracledb.AUTH_MODE_SYSDBA
                elif mode == 'sysasm':
                    self.mode = oracledb.AUTH_MODE_SYSASM
                elif mode == 'sysoper':
                    self.mode = oracledb.AUTH_MODE_SYSOPER
                elif mode == 'sysbkp':
                    self.mode = oracledb.AUTH_MODE_SYSBKP
                elif mode == 'sysdgd':
                    self.mode = oracledb.AUTH_MODE_SYSDGD
                elif mode == 'syskmt':
                    self.mode = oracledb.AUTH_MODE_SYSKMT
                elif mode == 'sysrac':
                    self.mode = oracledb.AUTH_MODE_SYSRAC

                purity = extra_options.get('purity', '').lower()
                if purity == 'new':
                    self.purity = oracledb.PURITY_NEW
                elif purity == 'self':
                    self.purity = oracledb.PURITY_SELF
                elif purity == 'default':
                    self.purity = oracledb.PURITY_DEFAULT

                # Check if thick_mode should be enabled in python-oracledb
                thick_mode = extra_options.get('thick_mode')
                self.thick_mode = thick_mode if thick_mode is not None else self.thick_mode
                if not isinstance(self.thick_mode, bool):
                    raise AirflowException(
                        f'thick_mode should be a boolean but type was {type(self.thick_mode)}'
                    )
                if self.thick_mode:
                    thick_mode_lib_dir = extra_options.get('thick_mode_lib_dir')
                    thick_mode_config_dir = extra_options.get('thick_mode_config_dir')
                    oracledb.init_oracle_client(lib_dir=thick_mode_lib_dir, config_dir=thick_mode_config_dir)

                # Check if python-oracledb Defaults attributes should be set
                # Values default to the initial values used by python-oracledb
                fetch_decimals = extra_options.get('fetch_decimals')
                self.fetch_decimals = fetch_decimals if fetch_decimals is not None else self.fetch_decimals
                oracledb.defaults.fetch_decimals = self.fetch_decimals
                fetch_lobs = extra_options.get('fetch_lobs')
                self.fetch_lobs = fetch_lobs if fetch_lobs is not None else self.fetch_lobs
                oracledb.defaults.fetch_lobs = self.fetch_lobs

        self.conn_config['user'] = self.login
        self.conn_config['password'] = self.password
        self.conn_config['events'] = self.events
        self.conn_config['mode'] = self.mode
        self.conn_config['purity'] = self.purity

        if self.host and self.sid and not self.service_name:
            self.conn_config['dsn'] = oracledb.makedsn(self.host, self.port, self.sid)
        elif self.host and self.service_name and not self.sid:
            self.conn_config['dsn'] = oracledb.makedsn(self.host, self.port, service_name=self.service_name)
        else:
            if self.dsn is None:
                dsn = str(self.host) if self.host is not None else ''
                if self.port is not None:
                    dsn += ":" + str(self.port)
                if self.service_name:
                    dsn += "/" + str(service_name)
                elif self.schema:
                    warnings.warn(
                        """Using conn.schema to pass the Oracle Service Name is deprecated.
                        Please use conn.extra.service_name instead.""",
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    dsn += "/" + self.schema
                self.dsn = dsn if dsn else self.dsn
            self.conn_config['dsn'] = self.dsn

    def get_conn(self) -> oracledb.Connection:
        """Returns a oracle connection object"""
        conn = oracledb.connect(**self.conn_config)
        if self.mod is not None:
            conn.module = self.mod

        # if Connection.schema is defined, set schema after connecting successfully
        # cannot be part of conn_config
        # https://python-oracledb.readthedocs.io/en/latest/api_manual/connection.html?highlight=schema#Connection.current_schema
        # Only set schema when not using conn.schema as Service Name
        if self.schema and self.service_name:
            conn.current_schema = self.schema

        return conn

    def insert_rows(
        self,
        table: str,
        rows: list[tuple],
        target_fields=None,
        commit_every: int = 1000,
        replace: bool | None = False,
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
        rows: list[tuple],
        target_fields: list[str] | None = None,
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
        parameters: list | dict | None = None,
    ) -> list | dict | None:
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
