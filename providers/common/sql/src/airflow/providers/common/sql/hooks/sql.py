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

import contextlib
import warnings
from collections.abc import Callable, Generator, Iterable, Mapping, MutableMapping, Sequence
from contextlib import closing, contextmanager, suppress
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeVar, cast, overload
from urllib.parse import urlparse

import sqlparse
from deprecated import deprecated
from methodtools import lru_cache
from more_itertools import chunked
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, NoSuchModuleError

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowOptionalProviderFeatureException,
    AirflowProviderDeprecationWarning,
)
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.sql.dialects.dialect import Dialect
from airflow.providers.common.sql.hooks import handlers
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame
    from sqlalchemy.engine import URL, Engine, Inspector

    from airflow.providers.common.compat.sdk import Connection
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.providers.openlineage.sqlparser import DatabaseInfo


T = TypeVar("T")
SQL_PLACEHOLDERS = frozenset({"%s", "?"})
WARNING_MESSAGE = """Import of {} from the 'airflow.providers.common.sql.hooks' module is deprecated and will
be removed in the future. Please import it from 'airflow.providers.common.sql.hooks.handlers'."""


def return_single_query_results(
    sql: str | Iterable[str], return_last: bool, split_statements: bool | None
) -> bool:
    warnings.warn(WARNING_MESSAGE.format("return_single_query_results"), DeprecationWarning, stacklevel=2)

    return handlers.return_single_query_results(sql, return_last, split_statements)


def fetch_all_handler(cursor) -> list[tuple] | None:
    warnings.warn(WARNING_MESSAGE.format("fetch_all_handler"), DeprecationWarning, stacklevel=2)

    return handlers.fetch_all_handler(cursor)


def fetch_one_handler(cursor) -> list[tuple] | None:
    warnings.warn(WARNING_MESSAGE.format("fetch_one_handler"), DeprecationWarning, stacklevel=2)

    return handlers.fetch_one_handler(cursor)


def resolve_dialects() -> MutableMapping[str, MutableMapping]:
    from airflow.providers_manager import ProvidersManager

    providers_manager = ProvidersManager()

    # TODO: this check can be removed once common sql provider depends on Airflow 3.0 or higher,
    #       we could then also use DialectInfo and won't need to convert it to a dict.
    if hasattr(providers_manager, "dialects"):
        return {key: dict(value._asdict()) for key, value in providers_manager.dialects.items()}

    # TODO: this can be removed once common sql provider depends on Airflow 3.0 or higher
    return {
        "default": dict(
            name="default",
            dialect_class_name="airflow.providers.common.sql.dialects.dialect.Dialect",
            provider_name="apache-airflow-providers-common-sql",
        ),
        "mssql": dict(
            name="mssql",
            dialect_class_name="airflow.providers.microsoft.mssql.dialects.mssql.MsSqlDialect",
            provider_name="apache-airflow-providers-microsoft-mssql",
        ),
        "postgresql": dict(
            name="postgresql",
            dialect_class_name="airflow.providers.postgres.dialects.postgres.PostgresDialect",
            provider_name="apache-airflow-providers-postgres",
        ),
    }


class ConnectorProtocol(Protocol):
    """Database connection protocol."""

    def connect(self, host: str, port: int, username: str, schema: str) -> Any:
        """
        Connect to a database.

        :param host: The database host to connect to.
        :param port: The database port to connect to.
        :param username: The database username used for the authentication.
        :param schema: The database schema to connect to.
        :return: the authorized connection object.
        """


class DbApiHook(BaseHook):
    """
    Abstract base class for sql hooks.

    When subclassing, maintainers can override the `_make_common_data_structure` method:
    This method transforms the result of the handler method (typically `cursor.fetchall()`) into
    objects common across all Hooks derived from this class (tuples). Most of the time, the underlying SQL
    library already returns tuples from its cursor, and the `_make_common_data_structure` method can be ignored.

    :param schema: Optional DB schema that overrides the schema specified in the connection. Make sure that
        if you change the schema parameter value in the constructor of the derived Hook, such change
        should be done before calling the ``DBApiHook.__init__()``.
    :param log_sql: Whether to log SQL query when it's executed. Defaults to *True*.
    """

    # Override to provide the connection name.
    conn_name_attr: str
    # Override to have a default connection id for a particular dbHook
    default_conn_name = "default_conn_id"
    # Override if this db doesn't support semicolons in SQL queries
    strip_semicolon = False
    # Override if this db supports autocommit.
    supports_autocommit = False
    # Override if this db supports executemany.
    supports_executemany = False
    # Override with the object that exposes the connect method
    connector: ConnectorProtocol | None = None
    # Override with db-specific query to check connection
    _test_connection_sql = "select 1"
    # Default SQL placeholder
    _placeholder: str = "%s"
    _dialects: MutableMapping[str, MutableMapping] = resolve_dialects()
    _resolve_target_fields = conf.getboolean("core", "dbapihook_resolve_target_fields", fallback=False)

    def __init__(self, *args, schema: str | None = None, log_sql: bool = True, **kwargs):
        super().__init__()
        if not self.conn_name_attr:
            raise AirflowException("conn_name_attr is not defined")
        if len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])
        # We should not make schema available in deriving hooks for backwards compatibility
        # If a hook deriving from DBApiHook has a need to access schema, then it should retrieve it
        # from kwargs and store it on its own. We do not run "pop" here as we want to give the
        # Hook deriving from the DBApiHook to still have access to the field in its constructor
        self.__schema = schema
        self.log_sql = log_sql
        self.descriptions: list[Sequence[Sequence] | None] = []
        self._insert_statement_format: str | None = kwargs.get("insert_statement_format")
        self._replace_statement_format: str | None = kwargs.get("replace_statement_format")
        self._escape_word_format: str | None = kwargs.get("escape_word_format")
        self._escape_column_names: bool | None = kwargs.get("escape_column_names")
        self._connection: Connection | None = kwargs.pop("connection", None)

    def get_conn_id(self) -> str:
        return getattr(self, self.conn_name_attr)

    @cached_property
    def placeholder(self) -> str:
        """Return SQL placeholder."""
        placeholder = self.connection_extra.get("placeholder")
        if placeholder:
            if placeholder in SQL_PLACEHOLDERS:
                return placeholder
            self.log.warning(
                "Placeholder '%s' defined in Connection '%s' is not listed in 'DEFAULT_SQL_PLACEHOLDERS' "
                "and got ignored. Falling back to the default placeholder '%s'.",
                placeholder,
                self.get_conn_id(),
                self._placeholder,
            )
        return self._placeholder

    @property
    def insert_statement_format(self) -> str:
        """Return the insert statement format."""
        if self._insert_statement_format is None:
            self._insert_statement_format = self.connection_extra.get(
                "insert_statement_format", "INSERT INTO {} {} VALUES ({})"
            )
        return self._insert_statement_format

    @property
    def replace_statement_format(self) -> str:
        """Return the replacement statement format."""
        if self._replace_statement_format is None:
            self._replace_statement_format = self.connection_extra.get(
                "replace_statement_format", "REPLACE INTO {} {} VALUES ({})"
            )
        return self._replace_statement_format

    @property
    def escape_word_format(self) -> str:
        """Return the escape word format."""
        if self._escape_word_format is None:
            self._escape_word_format = self.connection_extra.get("escape_word_format", '"{}"')
        return self._escape_word_format

    @property
    def escape_column_names(self) -> bool:
        """Return the escape column names flag."""
        if self._escape_column_names is None:
            self._escape_column_names = self.connection_extra.get("escape_column_names", False)
        return self._escape_column_names

    @property
    def connection(self) -> Connection:
        if self._connection is None:
            self._connection = self.get_connection(self.get_conn_id())
        return self._connection

    @connection.setter
    def connection(self, value: Any) -> None:
        if value != self.connection:
            self.log.warning(
                "This setter is for backward compatibility and should not be used.\n"
                "Since the introduction of connection property, the providers listed below "
                "breaks due to assigning value to self.connection in their __init__ method.\n"
                "* apache-airflow-providers-mysql<5.7.1\n"
                "* apache-airflow-providers-elasticsearch<5.5.1\n"
                "* apache-airflow-providers-postgres<5.13.0"
            )

    @property
    def connection_extra(self) -> dict:
        return self.connection.extra_dejson

    @cached_property
    def connection_extra_lower(self) -> dict:
        """
        ``connection.extra_dejson`` but where keys are converted to lower case.

        This is used internally for case-insensitive access of extra params.
        """
        return {k.lower(): v for k, v in self.connection_extra.items()}

    def get_conn(self) -> Any:
        """Return a connection object."""
        db = self.connection
        if self.connector is None:
            raise RuntimeError(f"{type(self).__name__} didn't have `self.connector` set!")
        host = db.host or ""
        login = db.login or ""
        schema = db.schema or ""
        return self.connector.connect(host=host, port=cast("int", db.port), username=login, schema=schema)

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.

        :return: the extracted uri.
        """
        conn = self.connection
        if self.__schema:
            conn.schema = self.__schema
        return conn.get_uri()

    @property
    def sqlalchemy_url(self) -> URL:
        """
        Return a Sqlalchemy.engine.URL object from the connection.

        Needs to be implemented in the provider subclass to return the sqlalchemy.engine.URL object.

        :return: the extracted sqlalchemy.engine.URL object.
        """
        qualname = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        if qualname != "airflow.providers.common.sql.hooks.sql.DbApiHook":
            msg = f"{qualname!r} does not implement/support built SQLAlchemy URL."
        else:
            msg = "`sqlalchemy_url` property should be implemented in the provider subclass."
        raise NotImplementedError(msg)

    def get_sqlalchemy_engine(self, engine_kwargs=None) -> Engine:
        """
        Get an sqlalchemy_engine object.

        :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
        :return: the created engine.
        """
        if engine_kwargs is None:
            engine_kwargs = {}

        try:
            url: URL | str = self.sqlalchemy_url
        except NotImplementedError:
            url = self.get_uri()

        self.log.debug("url: %s", url)
        self.log.debug("engine_kwargs: %s", engine_kwargs)
        return create_engine(url=url, **engine_kwargs)

    @property
    def inspector(self) -> Inspector:
        return inspect(self.get_sqlalchemy_engine())

    @cached_property
    def dialect_name(self) -> str:
        try:
            return make_url(self.get_uri()).get_dialect().name
        except (ArgumentError, NoSuchModuleError, ValueError):
            config = self.connection_extra
            sqlalchemy_scheme = config.get("sqlalchemy_scheme")
            if sqlalchemy_scheme:
                return sqlalchemy_scheme.split("+")[0] if "+" in sqlalchemy_scheme else sqlalchemy_scheme
            return config.get("dialect", "default")

    @cached_property
    def dialect(self) -> Dialect:
        from airflow.utils.module_loading import import_string

        dialect_info = self._dialects.get(self.dialect_name)

        self.log.debug("dialect_info: %s", dialect_info)

        if dialect_info:
            try:
                return import_string(dialect_info["dialect_class_name"])(self)
            except ImportError:
                raise AirflowOptionalProviderFeatureException(
                    f"{dialect_info['dialect_class_name']} not found, run: pip install "
                    f"'{dialect_info['provider_name']}'."
                )
        return Dialect(self)

    @property
    def reserved_words(self) -> set[str]:
        return self.get_reserved_words(self.dialect_name)

    @lru_cache(maxsize=None)
    def get_reserved_words(self, dialect_name: str) -> set[str]:
        result = set()
        with suppress(ImportError, ModuleNotFoundError, NoSuchModuleError):
            dialect_module = import_string(f"sqlalchemy.dialects.{dialect_name}.base")

            if hasattr(dialect_module, "RESERVED_WORDS"):
                result = set(dialect_module.RESERVED_WORDS)
            else:
                dialect_module = import_string(f"sqlalchemy.dialects.{dialect_name}.reserved_words")
                reserved_words_attr = f"RESERVED_WORDS_{dialect_name.upper()}"

                if hasattr(dialect_module, reserved_words_attr):
                    result = set(getattr(dialect_module, reserved_words_attr))

        self.log.debug("reserved words for '%s': %s", dialect_name, result)
        return result

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
    ) -> PandasDataFrame:
        """
        Execute the sql and returns a pandas dataframe.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param kwargs: (optional) passed into pandas.io.sql.read_sql method
        """
        return self.get_df(sql, parameters, df_type="pandas", **kwargs)

    @deprecated(
        reason="Replaced by function `get_df_by_chunks`.",
        category=AirflowProviderDeprecationWarning,
        action="ignore",
    )
    def get_pandas_df_by_chunks(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        **kwargs,
    ) -> Generator[PandasDataFrame, None, None]:
        return self.get_df_by_chunks(sql, parameters, chunksize=chunksize, df_type="pandas", **kwargs)

    @overload
    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["pandas"] = "pandas",
        **kwargs: Any,
    ) -> PandasDataFrame: ...
    @overload
    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["polars"],
        **kwargs: Any,
    ) -> PolarsDataFrame: ...

    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["pandas", "polars"] = "pandas",
        **kwargs,
    ) -> PandasDataFrame | PolarsDataFrame:
        """
        Execute the sql and returns a dataframe.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param df_type: Type of dataframe to return, either "pandas" or "polars"
        :param kwargs: (optional) passed into `pandas.io.sql.read_sql` or `polars.read_database` method
        """
        if df_type == "pandas":
            return self._get_pandas_df(sql, parameters, **kwargs)
        if df_type == "polars":
            return self._get_polars_df(sql, parameters, **kwargs)

    def _get_pandas_df(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs,
    ) -> PandasDataFrame:
        """
        Execute the sql and returns a pandas dataframe.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param kwargs: (optional) passed into pandas.io.sql.read_sql method
        """
        try:
            from pandas.io import sql as psql
        except ImportError:
            raise AirflowOptionalProviderFeatureException(
                "pandas library not installed, run: pip install "
                "'apache-airflow-providers-common-sql[pandas]'."
            )

        with closing(self.get_conn()) as conn:
            return psql.read_sql(sql, con=conn, params=parameters, **kwargs)

    def _get_polars_df(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs,
    ) -> PolarsDataFrame:
        """
        Execute the sql and returns a polars dataframe.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param kwargs: (optional) passed into polars.read_database method
        """
        try:
            import polars as pl
        except ImportError:
            raise AirflowOptionalProviderFeatureException(
                "polars library not installed, run: pip install "
                "'apache-airflow-providers-common-sql[polars]'."
            )

        with closing(self.get_conn()) as conn:
            execute_options: dict[str, Any] | None = None
            if parameters is not None:
                if isinstance(parameters, Mapping):
                    execute_options = dict(parameters)
                else:
                    execute_options = {}

            return pl.read_database(sql, connection=conn, execute_options=execute_options, **kwargs)

    @overload
    def get_df_by_chunks(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        df_type: Literal["pandas"] = "pandas",
        **kwargs,
    ) -> Generator[PandasDataFrame, None, None]: ...
    @overload
    def get_df_by_chunks(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        df_type: Literal["polars"],
        **kwargs,
    ) -> Generator[PolarsDataFrame, None, None]: ...

    def get_df_by_chunks(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        df_type: Literal["pandas", "polars"] = "pandas",
        **kwargs,
    ) -> Generator[PandasDataFrame | PolarsDataFrame, None, None]:
        """
        Execute the sql and return a generator.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with
        :param chunksize: number of rows to include in each chunk
        :param df_type: Type of dataframe to return, either "pandas" or "polars"
        :param kwargs: (optional) passed into `pandas.io.sql.read_sql` or `polars.read_database` method
        """
        if df_type == "pandas":
            return self._get_pandas_df_by_chunks(sql, parameters, chunksize=chunksize, **kwargs)
        if df_type == "polars":
            return self._get_polars_df_by_chunks(sql, parameters, chunksize=chunksize, **kwargs)

    def _get_pandas_df_by_chunks(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        **kwargs,
    ) -> Generator[PandasDataFrame, None, None]:
        """
        Execute the sql and return a generator.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with
        :param chunksize: number of rows to include in each chunk
        :param kwargs: (optional) passed into pandas.io.sql.read_sql method
        """
        try:
            from pandas.io import sql as psql
        except ImportError:
            raise AirflowOptionalProviderFeatureException(
                "pandas library not installed, run: pip install "
                "'apache-airflow-providers-common-sql[pandas]'."
            )
        with closing(self.get_conn()) as conn:
            yield from psql.read_sql(sql, con=conn, params=parameters, chunksize=chunksize, **kwargs)

    def _get_polars_df_by_chunks(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        **kwargs,
    ) -> Generator[PolarsDataFrame, None, None]:
        """
        Execute the sql and return a generator.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param chunksize: number of rows to include in each chunk
        :param kwargs: (optional) passed into pandas.io.sql.read_sql method
        """
        try:
            import polars as pl
        except ImportError:
            raise AirflowOptionalProviderFeatureException(
                "polars library not installed, run: pip install "
                "'apache-airflow-providers-common-sql[polars]'."
            )

        with closing(self.get_conn()) as conn:
            execute_options = None
            if parameters is not None:
                if isinstance(parameters, Mapping):
                    execute_options = dict(parameters)

            yield from pl.read_database(
                sql, connection=conn, execute_options=execute_options, batch_size=chunksize, **kwargs
            )

    def get_records(
        self,
        sql: str | list[str],
        parameters: Iterable | Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Execute the sql and return a set of records.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        return self.run(sql=sql, parameters=parameters, handler=handlers.fetch_all_handler)

    def get_first(self, sql: str | list[str], parameters: Iterable | Mapping[str, Any] | None = None) -> Any:
        """
        Execute the sql and return the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        return self.run(sql=sql, parameters=parameters, handler=handlers.fetch_one_handler)

    @staticmethod
    def strip_sql_string(sql: str) -> str:
        return sql.strip().rstrip(";")

    @staticmethod
    def split_sql_string(sql: str, strip_semicolon: bool = False) -> list[str]:
        """
        Split string into multiple SQL expressions.

        :param sql: SQL string potentially consisting of multiple expressions
        :param strip_semicolon: whether to strip semicolon from SQL string
        :return: list of individual expressions
        """
        splits = sqlparse.split(
            sql=sqlparse.format(sql, strip_comments=True),
            strip_semicolon=strip_semicolon,
        )
        return [s for s in splits if s]

    @property
    def last_description(self) -> Sequence[Sequence] | None:
        if not self.descriptions:
            return None
        return self.descriptions[-1]

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
    ) -> tuple | list | list[tuple] | list[list[tuple] | tuple] | None: ...

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler: Callable[[Any], T] | None = None,
        split_statements: bool = False,
        return_last: bool = True,
    ) -> tuple | list | list[tuple] | list[list[tuple] | tuple] | None:
        """
        Run a command or a list of commands.

        Pass a list of SQL statements to the sql parameter to get them to
        execute sequentially.

        The method will return either single query results (typically list of rows) or list of those results
        where each element in the list are results of one of the queries (typically list of list of rows :D)

        For compatibility reasons, the behaviour of the DBAPIHook is somewhat confusing.
        In some cases, when multiple queries are run, the return value will be an iterable (list) of results
        -- one for each query. However, in other cases, when single query is run, the return value will
        be the result of that single query without wrapping the results in a list.

        The cases when single query results are returned without wrapping them in a list are as follows:

        a) sql is string and ``return_last`` is True (regardless what ``split_statements`` value is)
        b) sql is string and ``split_statements`` is False

        In all other cases, the results are wrapped in a list, even if there is only one statement to process.
        In particular, the return value will be a list of query results in the following circumstances:

        a) when ``sql`` is an iterable of string statements (regardless what ``return_last`` value is)
        b) when ``sql`` is string, ``split_statements`` is True and ``return_last`` is False

        After ``run`` is called, you may access the following properties on the hook object:

        * ``descriptions``: an array of cursor descriptions. If ``return_last`` is True, this will be
          a one-element array containing the cursor ``description`` for the last statement.
          Otherwise, it will contain the cursor description for each statement executed.
        * ``last_description``: the description for the last statement executed

        Note that query result will ONLY be actually returned when a handler is provided; if
        ``handler`` is None, this method will return None.

        Handler is a way to process the rows from cursor (Iterator) into a value that is suitable to be
        returned to XCom and generally fit in memory.

        You can use pre-defined handles (``fetch_all_handler``, ``fetch_one_handler``) or implement your
        own handler.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of each statement.
        :param split_statements: Whether to split a single SQL string into statements and run separately
        :param return_last: Whether to return result for only last statement or for all after split
        :return: if handler provided, returns query results (may be list of results depending on params)
        """
        self.descriptions = []

        if isinstance(sql, str):
            if split_statements:
                sql_list: Iterable[str] = self.split_sql_string(
                    sql=sql,
                    strip_semicolon=self.strip_semicolon,
                )
            else:
                sql_list = [sql] if sql.strip() else []
        else:
            sql_list = sql

        if sql_list:
            self.log.debug("Executing following statements against DB: %s", sql_list)
        else:
            raise ValueError("List of SQL statements is empty")
        _last_result = None
        with self._create_autocommit_connection(autocommit) as conn:
            with closing(conn.cursor()) as cur:
                results = []
                for sql_statement in sql_list:
                    self._run_command(cur, sql_statement, parameters)

                    if handler is not None:
                        result = self._make_common_data_structure(handler(cur))
                        if handlers.return_single_query_results(sql, return_last, split_statements):
                            _last_result = result
                            _last_description = cur.description
                        else:
                            results.append(result)
                            self.descriptions.append(cur.description)

            # If autocommit was set to False or db does not support autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()
            # Logs all database messages or errors sent to the client
            self.get_db_log_messages(conn)

        if handler is None:
            return None
        if handlers.return_single_query_results(sql, return_last, split_statements):
            self.descriptions = [_last_description]
            return _last_result
        return results

    def _make_common_data_structure(self, result: T | Sequence[T]) -> tuple | list[tuple]:
        """
        Ensure the data returned from an SQL command is a standard tuple or list[tuple].

        This method is intended to be overridden by subclasses of the `DbApiHook`. Its purpose is to
        transform the result of an SQL command (typically returned by cursor methods) into a common
        data structure (a tuple or list[tuple]) across all DBApiHook derived Hooks, as defined in the
        ADR-0002 of the sql provider.

        If this method is not overridden, the result data is returned as-is. If the output of the cursor
        is already a common data structure, this method should be ignored.
        """
        if isinstance(result, Sequence):
            return cast("list[tuple]", result)
        return cast("tuple", result)

    def _run_command(self, cur, sql_statement, parameters):
        """Run a statement using an already open cursor."""
        if self.log_sql:
            self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)

        if parameters:
            # If we're using psycopg3, we might need to handle parameters differently
            if hasattr(cur, "__module__") and "psycopg" in cur.__module__ and isinstance(parameters, list):
                parameters = tuple(parameters)
            cur.execute(sql_statement, parameters)
        else:
            cur.execute(sql_statement)

        # According to PEP 249, this is -1 when query result is not applicable.
        if cur.rowcount >= 0:
            self.log.info("Rows affected: %s", cur.rowcount)

    def set_autocommit(self, conn, autocommit):
        """Set the autocommit flag on the connection."""
        if not self.supports_autocommit and autocommit:
            self.log.warning(
                "%s connection doesn't support autocommit but autocommit activated.",
                self.get_conn_id(),
            )
        conn.autocommit = autocommit

    def get_autocommit(self, conn) -> bool:
        """
        Get autocommit setting for the provided connection.

        :param conn: Connection to get autocommit setting from.
        :return: connection autocommit setting. True if ``autocommit`` is set
            to True on the connection. False if it is either not set, set to
            False, or the connection does not support auto-commit.
        """
        return getattr(conn, "autocommit", False) and self.supports_autocommit

    def get_cursor(self) -> Any:
        """Return a cursor."""
        return self.get_conn().cursor()

    def _generate_insert_sql(self, table, values, target_fields=None, replace: bool = False, **kwargs) -> str:
        """
        Generate the INSERT SQL statement.

        The REPLACE variant is specific to MySQL syntax, the UPSERT variant is specific to SAP Hana syntax

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table. If no target fields are
            specified, they will be determined dynamically from the table's metadata.
        :param replace: Whether to replace/upsert instead of insert
        :return: The generated INSERT or REPLACE/UPSERT SQL statement
        """
        if not target_fields and self._resolve_target_fields:
            with suppress(Exception):
                target_fields = self.dialect.get_target_fields(table)

        if replace:
            return self.dialect.generate_replace_sql(table, values, target_fields, **kwargs)

        return self.dialect.generate_insert_sql(table, values, target_fields, **kwargs)

    @contextmanager
    def _create_autocommit_connection(self, autocommit: bool = False):
        """Context manager that closes the connection after use and detects if autocommit is supported."""
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, autocommit)
            yield conn

    def insert_rows(
        self,
        table,
        rows,
        target_fields=None,
        commit_every=1000,
        replace=False,
        *,
        executemany=False,
        fast_executemany=False,
        autocommit=False,
        **kwargs,
    ):
        """
        Insert a collection of tuples into a table.

        Rows are inserted in chunks, each chunk (of size ``commit_every``) is
        done in a new transaction.

        :param table: Name of the target table
        :param rows: The rows to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert
        :param executemany: If True, all rows are inserted at once in
            chunks defined by the commit_every parameter. This only works if all rows
            have same number of column names, but leads to better performance.
        :param fast_executemany: If True, the `fast_executemany` parameter will be set on the
            cursor used by `executemany` which leads to better performance, if supported by driver.
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        """
        nb_rows = 0
        with self._create_autocommit_connection(autocommit) as conn:
            conn.commit()
            with closing(conn.cursor()) as cur:
                if self.supports_executemany or executemany:
                    if fast_executemany:
                        with contextlib.suppress(AttributeError):
                            # Try to set the fast_executemany attribute
                            cur.fast_executemany = True
                            self.log.info(
                                "Fast_executemany is enabled for conn_id '%s'!",
                                self.get_conn_id(),
                            )

                    for chunked_rows in chunked(rows, commit_every):
                        values = list(
                            map(
                                lambda row: self._serialize_cells(row, conn),
                                chunked_rows,
                            )
                        )
                        sql = self._generate_insert_sql(table, values[0], target_fields, replace, **kwargs)
                        self.log.debug("Generated sql: %s", sql)

                        try:
                            cur.executemany(sql, values)
                        except Exception as e:
                            self.log.error("Generated sql: %s", sql)
                            self.log.error("Parameters: %s", values)
                            raise e

                        conn.commit()
                        nb_rows += len(chunked_rows)
                        self.log.info("Loaded %s rows into %s so far", nb_rows, table)
                else:
                    for i, row in enumerate(rows, 1):
                        values = self._serialize_cells(row, conn)
                        sql = self._generate_insert_sql(table, values, target_fields, replace, **kwargs)
                        self.log.debug("Generated sql: %s", sql)

                        try:
                            cur.execute(sql, values)
                        except Exception as e:
                            self.log.error("Generated sql: %s", sql)
                            self.log.error("Parameters: %s", values)
                            raise e

                        if commit_every and i % commit_every == 0:
                            conn.commit()
                            self.log.info("Loaded %s rows into %s so far", i, table)
                        nb_rows += 1
                    conn.commit()
        self.log.info("Done loading. Loaded a total of %s rows into %s", nb_rows, table)

    @classmethod
    def _serialize_cells(cls, row, conn=None):
        return tuple(cls._serialize_cell(cell, conn) for cell in row)

    @staticmethod
    def _serialize_cell(cell, conn=None) -> str | None:
        """
        Return the SQL literal of the cell as a string.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The serialized cell
        """
        if cell is None:
            return None
        if isinstance(cell, datetime):
            return cell.isoformat()
        return str(cell)

    def bulk_dump(self, table, tmp_file):
        """
        Dump a database table into a tab-delimited file.

        :param table: The name of the source table
        :param tmp_file: The path of the target file
        """
        raise NotImplementedError()

    def bulk_load(self, table, tmp_file):
        """
        Load a tab-delimited file into a database table.

        :param table: The name of the target table
        :param tmp_file: The path of the file to load into the table
        """
        raise NotImplementedError()

    def test_connection(self):
        """Tests the connection using db-specific query."""
        status, message = False, ""
        try:
            if self.get_first(self._test_connection_sql):
                status = True
                message = "Connection successfully tested"
        except Exception as e:
            status = False
            message = str(e)

        return status, message

    def get_openlineage_database_info(self, connection) -> DatabaseInfo | None:
        """
        Return database specific information needed to generate and parse lineage metadata.

        This includes information helpful for constructing information schema query
        and creating correct namespace.

        :param connection: Airflow connection to reduce calls of `get_connection` method
        """

    def get_openlineage_database_dialect(self, connection) -> str:
        """
        Return database dialect used for SQL parsing.

        For a list of supported dialects check: https://openlineage.io/docs/development/sql#sql-dialects
        """
        return "generic"

    def get_openlineage_default_schema(self) -> str | None:
        """
        Return default schema specific to database.

        .. seealso::
            - :class:`airflow.providers.openlineage.sqlparser.SQLParser`
        """
        return self.__schema or "public"

    def get_openlineage_database_specific_lineage(self, task_instance) -> OperatorLineage | None:
        """
        Return additional database specific lineage, e.g. query execution information.

        This method is called only on completion of the task.

        :param task_instance: this may be used to retrieve additional information
            that is collected during runtime of the task
        """

    @staticmethod
    def get_openlineage_authority_part(connection, default_port: int | None = None) -> str:
        """
        Get authority part from Airflow Connection.

        The authority represents the hostname and port of the connection
        and conforms OpenLineage naming convention for a number of databases (e.g. MySQL, Postgres, Trino).

        :param default_port: (optional) used if no port parsed from connection URI
        """
        parsed = urlparse(connection.get_uri())
        port = parsed.port or default_port
        if port:
            authority = f"{parsed.hostname}:{port}"
        else:
            authority = parsed.hostname
        return authority

    def get_db_log_messages(self, conn) -> None:
        """
        Log all database messages sent to the client during the session.

        :param conn: Connection object
        """
