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

from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import closing
from typing import TYPE_CHECKING, Any, TypeVar, overload

import pyexasol
from deprecated import deprecated
from pyexasol import ExaConnection, ExaStatement

try:
    from sqlalchemy.engine import URL
except ImportError:
    URL = None  # type: ignore[assignment,misc]

from airflow.exceptions import AirflowOptionalProviderFeatureException, AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.handlers import return_single_query_results
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    import pandas as pd

T = TypeVar("T")


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

    conn_name_attr = "exasol_conn_id"
    default_conn_name = "exasol_default"
    conn_type = "exasol"
    hook_name = "Exasol"
    supports_autocommit = True
    DEFAULT_SQLALCHEMY_SCHEME = "exa+websocket"  # sqlalchemy-exasol dialect

    def __init__(self, *args, sqlalchemy_scheme: str | None = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self._sqlalchemy_scheme = sqlalchemy_scheme

    def get_conn(self) -> ExaConnection:
        conn = self.get_connection(self.get_conn_id())
        conn_args = {
            "dsn": f"{conn.host}:{conn.port}",
            "user": conn.login,
            "password": conn.password,
            "schema": self.schema or conn.schema,
        }
        # check for parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ["compression", "encryption", "json_lib", "client_name"]:
                conn_args[arg_name] = arg_val

        conn = pyexasol.connect(**conn_args)
        return conn

    @property
    def sqlalchemy_scheme(self) -> str:
        """Sqlalchemy scheme either from constructor, connection extras or default."""
        extra_scheme = self.connection is not None and self.connection_extra_lower.get("sqlalchemy_scheme")
        sqlalchemy_scheme = self._sqlalchemy_scheme or extra_scheme or self.DEFAULT_SQLALCHEMY_SCHEME
        if sqlalchemy_scheme not in ["exa+websocket", "exa+pyodbc", "exa+turbodbc"]:
            raise ValueError(
                f"sqlalchemy_scheme in connection extra should be one of 'exa+websocket', 'exa+pyodbc' or 'exa+turbodbc', "
                f"but got '{sqlalchemy_scheme}'. See https://github.com/exasol/sqlalchemy-exasol?tab=readme-ov-file#using-sqlalchemy-with-exasol-db for more details."
            )
        return sqlalchemy_scheme

    @property
    def sqlalchemy_url(self) -> URL:
        """
        Return a Sqlalchemy.engine.URL object from the connection.

        :return: the extracted sqlalchemy.engine.URL object.
        """
        if URL is None:
            raise AirflowOptionalProviderFeatureException(
                "The 'sqlalchemy' library is required to use 'sqlalchemy_url'."
            )
        connection = self.connection
        query = connection.extra_dejson
        query = {k: v for k, v in query.items() if k.lower() != "sqlalchemy_scheme"}
        return URL.create(
            drivername=self.sqlalchemy_scheme,
            username=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port,
            database=self.schema or connection.schema,
            query=query,
        )

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.

        :return: the extracted uri.
        """
        if URL is None:
            raise AirflowOptionalProviderFeatureException(
                "The 'sqlalchemy' library is required to render the connection URI."
            )
        return self.sqlalchemy_url.render_as_string(hide_password=False)

    def _get_pandas_df(
        self, sql, parameters: Iterable | Mapping[str, Any] | None = None, **kwargs
    ) -> pd.DataFrame:
        """
        Execute the SQL and return a Pandas dataframe.

        :param sql: The sql statement to be executed (str) or a list of
            sql statements to execute.
        :param parameters: The parameters to render the SQL query with.

        Other keyword arguments are all forwarded into
        ``pyexasol.ExaConnection.export_to_pandas``.
        """
        with closing(self.get_conn()) as conn:
            df = conn.export_to_pandas(sql, query_params=parameters, **kwargs)
            return df

    @deprecated(
        reason="Replaced by function `get_df`.",
        category=AirflowProviderDeprecationWarning,
        action="ignore",
    )
    def get_pandas_df(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Execute the sql and returns a pandas dataframe.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param kwargs: (optional) passed into pandas.io.sql.read_sql method
        """
        return self._get_pandas_df(sql, parameters, **kwargs)

    def _get_polars_df(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs,
    ):
        raise NotImplementedError("Polars is not supported for Exasol")

    def get_records(
        self,
        sql: str | list[str],
        parameters: Iterable | Mapping[str, Any] | None = None,
    ) -> list[dict | tuple[Any, ...]]:
        """
        Execute the SQL and return a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        with closing(self.get_conn()) as conn, closing(conn.execute(sql, parameters)) as cur:
            return cur.fetchall()

    def get_first(self, sql: str | list[str], parameters: Iterable | Mapping[str, Any] | None = None) -> Any:
        """
        Execute the SQL and return the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        with closing(self.get_conn()) as conn, closing(conn.execute(sql, parameters)) as cur:
            return cur.fetchone()

    def export_to_file(
        self,
        filename: str,
        query_or_table: str,
        query_params: dict | None = None,
        export_params: dict | None = None,
    ) -> None:
        """
        Export data to a file.

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

    @staticmethod
    def get_description(statement: ExaStatement) -> Sequence[Sequence]:
        """
        Get description; copied implementation from DB2-API wrapper.

        For more info, see
        https://github.com/exasol/pyexasol/blob/master/docs/DBAPI_COMPAT.md#db-api-20-wrapper

        :param statement: Exasol statement
        :return: description sequence of t
        """
        cols = []
        for k, v in statement.columns().items():
            cols.append(
                (
                    k,
                    v.get("type", None),
                    v.get("size", None),
                    v.get("size", None),
                    v.get("precision", None),
                    v.get("scale", None),
                    True,
                )
            )
        return cols

    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: None = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> None: ...

    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: Callable[[Any], T] = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None: ...

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler: Callable[[Any], T] | None = None,
        split_statements: bool = False,
        return_last: bool = True,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None:
        """
        Run a command or a list of commands.

        Pass a list of SQL statements to the SQL parameter to get them to
        execute sequentially.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of each statement.
        :param split_statements: Whether to split a single SQL string into statements and run separately
        :param return_last: Whether to return result for only last statement or for all after split
        :return: return only result of the LAST SQL expression if handler was provided.
        """
        self.descriptions = []
        if isinstance(sql, str):
            if split_statements:
                sql_list: Iterable[str] = self.split_sql_string(sql)
            else:
                statement = self.strip_sql_string(sql)
                sql_list = [statement] if statement.strip() else []
        else:
            sql_list = sql

        if sql_list:
            self.log.debug("Executing following statements against Exasol DB: %s", list(sql_list))
        else:
            raise ValueError("List of SQL statements is empty")
        _last_result = None
        with closing(self.get_conn()) as conn:
            self.set_autocommit(conn, autocommit)
            results = []
            for sql_statement in sql_list:
                self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
                with closing(conn.execute(sql_statement, parameters)) as exa_statement:
                    if handler is not None:
                        result = self._make_common_data_structure(handler(exa_statement))

                        if return_single_query_results(sql, return_last, split_statements):
                            _last_result = result
                            _last_columns = self.get_description(exa_statement)
                        else:
                            results.append(result)
                            self.descriptions.append(self.get_description(exa_statement))
                    self.log.info("Rows affected: %s", exa_statement.rowcount())

            # If autocommit was set to False or db does not support autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()

        if handler is None:
            return None
        if return_single_query_results(sql, return_last, split_statements):
            self.descriptions = [_last_columns]
            return _last_result
        return results

    def set_autocommit(self, conn, autocommit: bool) -> None:
        """
        Set the autocommit flag on the connection.

        :param conn: Connection to set autocommit setting to.
        :param autocommit: The autocommit setting to set.
        """
        if not self.supports_autocommit and autocommit:
            self.log.warning(
                "%s connection doesn't support autocommit but autocommit activated.",
                self.get_conn_id(),
            )
        conn.set_autocommit(autocommit)

    def get_autocommit(self, conn) -> bool:
        """
        Get autocommit setting for the provided connection.

        :param conn: Connection to get autocommit setting from.
        :return: connection autocommit setting. True if ``autocommit`` is set
            to True on the connection. False if it is either not set, set to
            False, or the connection does not support auto-commit.
        """
        autocommit = conn.attr.get("autocommit")
        if autocommit is None:
            autocommit = super().get_autocommit(conn)
        return autocommit

    @staticmethod
    def _serialize_cell(cell, conn=None) -> Any:
        """
        Override to disable cell serialization.

        Exasol will adapt all arguments to the ``execute()`` method internally,
        hence we return cell without any conversion.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The cell
        """
        return cell


def exasol_fetch_all_handler(statement: ExaStatement) -> list[tuple] | None:
    if statement.result_type == "resultSet":
        return statement.fetchall()
    return None


def exasol_fetch_one_handler(statement: ExaStatement) -> list[tuple] | None:
    if statement.result_type == "resultSet":
        return statement.fetchone()
    return None
