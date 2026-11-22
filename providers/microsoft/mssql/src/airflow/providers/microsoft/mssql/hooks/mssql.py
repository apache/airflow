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
"""Microsoft SQLServer hook module."""

from __future__ import annotations

from abc import ABC, abstractmethod
from functools import cached_property
from importlib import import_module
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import pymssql

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.microsoft.mssql.dialects.mssql import MsSqlDialect

if TYPE_CHECKING:
    from pymssql import Connection as PymssqlConnection
    from sqlalchemy.engine import URL

    from airflow.providers.common.compat.sdk import Connection
    from airflow.providers.common.sql.dialects.dialect import Dialect
    from airflow.providers.openlineage.sqlparser import DatabaseInfo

# Connection extras that are handled by the hook itself and must never reach the DBAPI driver.
_AIRFLOW_ONLY_EXTRAS = frozenset({"sqlalchemy_scheme", "dbapi_driver"})


class _DbApiDriver(ABC):
    """
    Behaviour of ``MsSqlHook`` that depends on its ``dbapi_driver``.

    The defaults are the plain DB-API behaviour, a driver only overrides what it does differently.
    To add a driver, subclass this and add an instance to ``_DRIVERS``.
    """

    name: str
    default_sqlalchemy_scheme: str
    placeholder = "%s"

    @abstractmethod
    def connect(self, connection: Connection, database: str | None) -> Any:
        """Return a DB-API connection."""

    def build_sqlalchemy_url(self, connection: Connection, database: str | None, scheme: str) -> URL:
        """Return the SQLAlchemy URL. Without one, the engine is built from ``MsSqlHook.get_uri()``."""
        raise NotImplementedError(f"The {self.name} driver builds no SQLAlchemy URL, use get_uri().")

    def rewrite_uri(self, uri: str, scheme: str) -> str:
        """Turn the URI of the Airflow connection into the one used for the SQLAlchemy engine."""
        raise NotImplementedError(f"The {self.name} driver does not rewrite the connection URI.")

    def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        conn.autocommit = autocommit

    def get_autocommit(self, conn: Any) -> bool:
        return bool(getattr(conn, "autocommit", False))

    def make_common_data_structure(self, result: Any) -> tuple | list[tuple] | None:
        return result


class _PymssqlDriver(_DbApiDriver):
    name = "pymssql"
    default_sqlalchemy_scheme = "mssql+pymssql"

    def connect(self, connection: Connection, database: str | None) -> PymssqlConnection:
        extra_conn_args = {
            key: val
            for key, val in connection.extra_dejson.items()
            if key.lower() not in _AIRFLOW_ONLY_EXTRAS
        }
        return pymssql.connect(
            server=connection.host or "",
            user=connection.login,
            password=connection.password,
            database=database or "",
            port=str(connection.port),
            **extra_conn_args,
        )

    def rewrite_uri(self, uri: str, scheme: str) -> str:
        r = list(urlsplit(uri))
        # change the sqlalchemy driver:
        r[0] = scheme
        # remove query string parameters that are meant for the hook, not for the sqlalchemy dialect:
        qs = parse_qs(r[3], keep_blank_values=True)
        for k in list(qs.keys()):
            if k.lower() in _AIRFLOW_ONLY_EXTRAS:
                qs.pop(k, None)
        r[3] = urlencode(qs, doseq=True)
        return urlunsplit(r)

    def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        conn.autocommit(autocommit)

    def get_autocommit(self, conn: Any) -> bool:
        return conn.autocommit_state


class _MssqlPythonDriver(_DbApiDriver):
    name = "mssql_python"
    default_sqlalchemy_scheme = "mssql+mssqlpython"
    # mssql-python has no plain ``%s`` support.
    placeholder = "?"
    # mssql-python takes every other extra as a connection string keyword, and the string built from a
    # SQLAlchemy URL fails on unknown keywords. So the extras that ``DbApiHook`` reads for itself are
    # filtered out for both the direct connection and the SQLAlchemy URL.
    ignored_extras = _AIRFLOW_ONLY_EXTRAS | frozenset(
        {
            "placeholder",
            "insert_statement_format",
            "replace_statement_format",
            "escape_word_format",
            "escape_column_names",
            "dialect",
        }
    )

    def get_extras(self, connection: Connection) -> dict[str, Any]:
        """Return the connection extras that are passed on to mssql-python, without the empty ones."""
        return {
            key: val
            for key, val in connection.extra_dejson.items()
            if key.lower() not in self.ignored_extras and val not in (None, "")
        }

    def connect(self, connection: Connection, database: str | None) -> Any:
        try:
            mssql_python = import_module("mssql_python")
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(
                "The 'mssql_python' dbapi_driver needs the mssql-python package. Install it with: "
                "pip install 'apache-airflow-providers-microsoft-mssql[mssql-python]'"
            ) from e

        server = connection.host or ""
        if server and connection.port:
            server = f"{server},{connection.port}"
        params: dict[str, Any] = {
            "Server": server,
            "Database": database,
            "UID": connection.login,
            "PWD": connection.password,
        }
        params.update(self.get_extras(connection))
        # The keyword arguments are escaped by mssql-python when it builds the connection string.
        return mssql_python.connect(**{key: val for key, val in params.items() if val not in (None, "")})

    def build_sqlalchemy_url(self, connection: Connection, database: str | None, scheme: str) -> URL:
        """
        Build the SQLAlchemy URL from the connection fields and extras.

        ``Connection.get_uri()`` packs extras that do not survive a round trip (a ``true`` for example)
        into one ``__extra__`` parameter, which mssql-python rejects as an unknown keyword.
        """
        try:
            from sqlalchemy.engine import URL
        except (ImportError, ModuleNotFoundError) as err:
            raise AirflowOptionalProviderFeatureException(
                "SQLAlchemy is not installed. Install it with: "
                "pip install 'apache-airflow-providers-common-sql[sqlalchemy]'"
            ) from err

        return URL.create(
            drivername=scheme,
            username=connection.login or None,
            password=connection.password or None,
            host=connection.host or None,
            port=connection.port,
            database=database or None,
            query={key: str(val) for key, val in self.get_extras(connection).items()},
        )

    def make_common_data_structure(self, result: Any) -> tuple | list[tuple] | None:
        """Turn the ``Row`` objects of mssql_python into plain tuples and leave other results alone."""
        row_class = import_module("mssql_python").Row
        if isinstance(result, row_class):
            return tuple(result)
        if isinstance(result, list):
            return [tuple(row) if isinstance(row, row_class) else row for row in result]
        return result


_DRIVERS: dict[str, _DbApiDriver] = {
    driver.name: driver for driver in (_PymssqlDriver(), _MssqlPythonDriver())
}


class MsSqlHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.

    The hook uses ``pymssql`` by default. Set the ``dbapi_driver`` connection extra to ``mssql_python``
    to use Microsoft's ``mssql-python`` driver instead (requires the ``mssql-python`` extra).

    :param args: passed to DBApiHook
    :param sqlalchemy_scheme: Scheme sqlalchemy connection.  Default is ``mssql+pymssql`` (or
      ``mssql+mssqlpython`` when ``dbapi_driver`` is ``mssql_python``). Only used for
      ``get_sqlalchemy_engine`` and ``get_sqlalchemy_connection`` methods.
    :param kwargs: passed to DbApiHook
    """

    conn_name_attr = "mssql_conn_id"
    default_conn_name = "mssql_default"
    conn_type = "mssql"
    hook_name = "Microsoft SQL Server"
    supports_autocommit = True
    DEFAULT_SQLALCHEMY_SCHEME = _PymssqlDriver.default_sqlalchemy_scheme
    MSSQL_PYTHON_SQLALCHEMY_SCHEME = _MssqlPythonDriver.default_sqlalchemy_scheme
    DBAPI_DRIVER_PYMSSQL = _PymssqlDriver.name
    DBAPI_DRIVER_MSSQL_PYTHON = _MssqlPythonDriver.name
    SUPPORTED_DBAPI_DRIVERS = tuple(_DRIVERS)

    def __init__(
        self,
        *args,
        sqlalchemy_scheme: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **{**kwargs, **{"escape_word_format": "[{}]"}})
        self.schema = kwargs.pop("schema", None)
        self._sqlalchemy_scheme = sqlalchemy_scheme

    @property
    def dbapi_driver(self) -> str:
        """DBAPI driver, either ``pymssql`` (default) or ``mssql_python``, from the connection extra."""
        driver = self.connection_extra_lower.get("dbapi_driver")
        if driver is None:
            return self.DBAPI_DRIVER_PYMSSQL

        if not isinstance(driver, str) or driver.lower() not in self.SUPPORTED_DBAPI_DRIVERS:
            raise ValueError(
                f"Unsupported dbapi_driver {driver!r} in the connection extra. "
                f"Supported values: {', '.join(sorted(self.SUPPORTED_DBAPI_DRIVERS))}"
            )

        return driver.lower()

    @cached_property
    def _driver(self) -> _DbApiDriver:
        return _DRIVERS[self.dbapi_driver]

    @property
    def sqlalchemy_scheme(self) -> str:
        """Sqlalchemy scheme either from constructor, connection extras or the default of the driver."""
        extra_scheme = self.connection_extra_lower.get("sqlalchemy_scheme")
        if not self._sqlalchemy_scheme and extra_scheme and (":" in extra_scheme or "/" in extra_scheme):
            raise RuntimeError("sqlalchemy_scheme in connection extra should not contain : or / characters")
        return self._sqlalchemy_scheme or extra_scheme or self._driver.default_sqlalchemy_scheme

    @cached_property
    def placeholder(self) -> str:
        """Return SQL placeholder, ``?`` for mssql_python (it has no plain ``%s`` support)."""
        if self.connection_extra.get("placeholder"):
            return super().placeholder
        return self._driver.placeholder

    @property
    def dialect_name(self) -> str:
        return "mssql"

    @cached_property
    def dialect(self) -> Dialect:
        return MsSqlDialect(self)

    @property
    def sqlalchemy_url(self) -> URL:
        """Return the SQLAlchemy URL, for the drivers that build one (``mssql_python``)."""
        return self._driver.build_sqlalchemy_url(
            self.connection, self.schema or self.connection.schema, self.sqlalchemy_scheme
        )

    def get_uri(self) -> str:
        try:
            url = self.sqlalchemy_url
        except NotImplementedError:
            # Same fallback as ``get_sqlalchemy_engine``: the driver has no URL of its own.
            return self._driver.rewrite_uri(super().get_uri(), self.sqlalchemy_scheme)
        return url.render_as_string(hide_password=False)

    def get_sqlalchemy_connection(
        self, connect_kwargs: dict | None = None, engine_kwargs: dict | None = None
    ) -> Any:
        """Sqlalchemy connection object."""
        engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        return engine.connect(**(connect_kwargs or {}))

    def get_conn(self) -> Any:
        """Return a ``pymssql`` or ``mssql_python`` connection object, depending on ``dbapi_driver``."""
        return self._driver.connect(self.connection, self.schema or self.connection.schema)

    def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        self._driver.set_autocommit(conn, autocommit)

    def get_autocommit(self, conn: Any) -> bool:
        return self._driver.get_autocommit(conn)

    def _make_common_data_structure(self, result: Any) -> tuple | list[tuple] | None:
        return self._driver.make_common_data_structure(result)

    def get_openlineage_database_info(self, connection: Connection) -> DatabaseInfo:
        """Return MSSQL specific information for OpenLineage."""
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        return DatabaseInfo(
            scheme=self.get_openlineage_database_dialect(connection),
            authority=DbApiHook.get_openlineage_authority_part(connection, default_port=1433),
            information_schema_columns=[
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "data_type",
                "table_catalog",
            ],
            database=self.schema or self.connection.schema,
            is_information_schema_cross_db=True,
        )

    def get_openlineage_database_dialect(self, connection) -> str:
        """Return database dialect."""
        return "mssql"

    def get_openlineage_default_schema(self) -> str | None:
        """Return current schema."""
        return self.get_first("SELECT SCHEMA_NAME();")[0]
