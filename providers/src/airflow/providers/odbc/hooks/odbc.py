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
"""This module contains ODBC hook."""

from __future__ import annotations

from collections import namedtuple
from typing import Any, List, Sequence, cast
from urllib.parse import quote_plus

from pyodbc import Connection, Row, connect

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.utils.helpers import merge_dicts


class OdbcHook(DbApiHook):
    """
    Interact with odbc data sources using pyodbc.

    To configure driver, in addition to supplying as constructor arg, the following are also supported:
        * set ``driver`` parameter in ``hook_params`` dictionary when instantiating hook by SQL operators.
        * set ``driver`` extra in the connection and set ``allow_driver_in_extra`` to True in
          section ``providers.odbc`` section of airflow config.
        * patch ``OdbcHook.default_driver`` in ``local_settings.py`` file.

    See :doc:`/connections/odbc` for full documentation.

    :param args: passed to DbApiHook
    :param database: database to use -- overrides connection ``schema``
    :param driver: name of driver or path to driver. see above for more info
    :param dsn: name of DSN to use.  overrides DSN supplied in connection ``extra``
    :param connect_kwargs: keyword arguments passed to ``pyodbc.connect``
    :param sqlalchemy_scheme: Scheme sqlalchemy connection.  Default is ``mssql+pyodbc`` Only used for
        ``get_sqlalchemy_engine`` and ``get_sqlalchemy_connection`` methods.
    :param kwargs: passed to DbApiHook
    """

    DEFAULT_SQLALCHEMY_SCHEME = "mssql+pyodbc"
    conn_name_attr = "odbc_conn_id"
    default_conn_name = "odbc_default"
    conn_type = "odbc"
    hook_name = "ODBC"
    supports_autocommit = True
    supports_executemany = True

    default_driver: str | None = None

    def __init__(
        self,
        *args,
        database: str | None = None,
        driver: str | None = None,
        dsn: str | None = None,
        connect_kwargs: dict | None = None,
        sqlalchemy_scheme: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._database = database
        self._driver = driver
        self._dsn = dsn
        self._conn_str = None
        self._sqlalchemy_scheme = sqlalchemy_scheme
        self._connection = None
        self._connect_kwargs = connect_kwargs

    @property
    def database(self) -> str | None:
        """Database provided in init if exists; otherwise, ``schema`` from ``Connection`` object."""
        return self._database or self.connection.schema

    @property
    def sqlalchemy_scheme(self) -> str:
        """SQLAlchemy scheme either from constructor, connection extras or default."""
        extra_scheme = self.connection_extra_lower.get("sqlalchemy_scheme")
        if not self._sqlalchemy_scheme and extra_scheme and (":" in extra_scheme or "/" in extra_scheme):
            raise RuntimeError("sqlalchemy_scheme in connection extra should not contain : or / characters")
        return self._sqlalchemy_scheme or extra_scheme or self.DEFAULT_SQLALCHEMY_SCHEME

    @property
    def driver(self) -> str | None:
        """Driver from init param if given; else try to find one in connection extra."""
        extra_driver = self.connection_extra_lower.get("driver")
        from airflow.configuration import conf

        if extra_driver and conf.getboolean("providers.odbc", "allow_driver_in_extra", fallback=False):
            self._driver = extra_driver
        else:
            self.log.warning(
                "You have supplied 'driver' via connection extra but it will not be used. In order to "
                "use 'driver' from extra you must set airflow config setting `allow_driver_in_extra = True` "
                "in section `providers.odbc`. Alternatively you may specify driver via 'driver' parameter of "
                "the hook constructor or via 'hook_params' dictionary with key 'driver' if using SQL "
                "operators."
            )
        if not self._driver:
            self._driver = self.default_driver
        return self._driver.strip().lstrip("{").rstrip("}").strip() if self._driver else None

    @property
    def dsn(self) -> str | None:
        """DSN from init param if given; else try to find one in connection extra."""
        if not self._dsn:
            dsn = self.connection_extra_lower.get("dsn")
            if dsn:
                self._dsn = dsn.strip()
        return self._dsn

    @property
    def odbc_connection_string(self):
        """
        ODBC connection string.

        We build connection string instead of using ``pyodbc.connect`` params
        because, for example, there is no param representing
        ``ApplicationIntent=ReadOnly``.  Any key-value pairs provided in
        ``Connection.extra`` will be added to the connection string.
        """
        if not self._conn_str:
            conn_str = ""
            if self.driver:
                conn_str += f"DRIVER={{{self.driver}}};"
            if self.dsn:
                conn_str += f"DSN={self.dsn};"
            if self.connection.host:
                conn_str += f"SERVER={self.connection.host};"
            database = self.database or self.connection.schema
            if database:
                conn_str += f"DATABASE={database};"
            if self.connection.login:
                conn_str += f"UID={self.connection.login};"
            if self.connection.password:
                conn_str += f"PWD={self.connection.password};"
            if self.connection.port:
                conn_str += f"PORT={self.connection.port};"

            extra_exclude = {"driver", "dsn", "connect_kwargs", "sqlalchemy_scheme", "placeholder"}
            extra_params = {k: v for k, v in self.connection_extra.items() if k.lower() not in extra_exclude}
            for k, v in extra_params.items():
                conn_str += f"{k}={v};"

            self._conn_str = conn_str
        return self._conn_str

    @property
    def connect_kwargs(self) -> dict:
        """
        Effective kwargs to be passed to ``pyodbc.connect``.

        The kwargs are merged from connection extra, ``connect_kwargs``, and
        the hook's init arguments. Values received to the hook precede those
        from the connection.

        If ``attrs_before`` is provided, keys and values are converted to int,
        as required by pyodbc.
        """
        conn_connect_kwargs = self.connection_extra_lower.get("connect_kwargs", {})
        hook_connect_kwargs = self._connect_kwargs or {}
        merged_connect_kwargs = merge_dicts(conn_connect_kwargs, hook_connect_kwargs)

        if "attrs_before" in merged_connect_kwargs:
            merged_connect_kwargs["attrs_before"] = {
                int(k): int(v) for k, v in merged_connect_kwargs["attrs_before"].items()
            }

        return merged_connect_kwargs

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """
        Get an sqlalchemy_engine object.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        :return: the created engine.
        """
        if engine_kwargs is None:
            engine_kwargs = {}
        engine_kwargs["creator"] = self.get_conn

        return super().get_sqlalchemy_engine(engine_kwargs)

    def get_conn(self) -> Connection:
        """Return ``pyodbc`` connection object."""
        conn = connect(self.odbc_connection_string, **self.connect_kwargs)
        return conn

    def get_uri(self) -> str:
        """URI invoked in :meth:`~airflow.providers.common.sql.hooks.sql.DbApiHook.get_sqlalchemy_engine`."""
        quoted_conn_str = quote_plus(self.odbc_connection_string)
        uri = f"{self.sqlalchemy_scheme}:///?odbc_connect={quoted_conn_str}"
        return uri

    def get_sqlalchemy_connection(
        self, connect_kwargs: dict | None = None, engine_kwargs: dict | None = None
    ) -> Any:
        """SQLAlchemy connection object."""
        engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        cnx = engine.connect(**(connect_kwargs or {}))
        return cnx

    def _make_common_data_structure(self, result: Sequence[Row] | Row) -> list[tuple] | tuple:
        """Transform the pyodbc.Row objects returned from an SQL command into namedtuples."""
        # Below ignored lines respect namedtuple docstring, but mypy do not support dynamically
        # instantiated namedtuple, and will never do: https://github.com/python/mypy/issues/848
        field_names: list[tuple[str, type]] | None = None
        if not result:
            return []
        if isinstance(result, Sequence):
            field_names = [col[0] for col in result[0].cursor_description]
            row_object = namedtuple("Row", field_names, rename=True)  # type: ignore
            return cast(List[tuple], [row_object(*row) for row in result])
        else:
            field_names = [col[0] for col in result.cursor_description]
            return cast(tuple, namedtuple("Row", field_names, rename=True)(*result))  # type: ignore
