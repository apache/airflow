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

import re
from functools import cached_property
from typing import TYPE_CHECKING, Any

import duckdb

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.duckdb.version_compat import AirflowNotFoundException

if TYPE_CHECKING:
    from collections.abc import Sequence

    from duckdb import DuckDBPyConnection

    try:
        from airflow.sdk import Connection
    except ImportError:
        from airflow.models.connection import Connection  # type: ignore[assignment]

IN_MEMORY_DATABASE = ":memory:"

# DuckDB has no bind-parameter form for extension names or PRAGMA-style identifiers, so anything
# interpolated into those statements is validated against this instead.
_IDENTIFIER = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")


class DuckDBHook(DbApiHook):
    """
    Interact with an in-process `DuckDB <https://duckdb.org/>`__ database.

    The hook opens a DuckDB database — in memory, backed by a local file, or hosted by MotherDuck —
    applies resource limits, and loads the requested extensions, so a Dag author only supplies SQL.

    The Airflow connection is optional. With no connection configured the hook opens an in-memory
    database, which is the right default for a stateless, task-scoped analytical query.

    Extensions are loaded with ``LOAD`` first and only installed when that fails. DuckDB downloads
    extensions from its extension repository on first use, so deployments without outbound internet
    access should pre-populate an extension directory, point ``extension_directory`` at it and set
    ``autoinstall_extensions=False`` to turn the network access off entirely.

    :param duckdb_conn_id: reference to a :ref:`DuckDB connection <howto/connection:duckdb>`. The
        connection need not exist; when it is missing an in-memory database is used.
    :param database: database to open. Overrides the connection. ``:memory:`` (the default) opens a
        transient database that is discarded when the task finishes.
    :param extensions: extensions to load on connect, for example ``["httpfs", "iceberg"]``.
    :param extension_directory: directory DuckDB loads extensions from and installs them into.
        Point this at a pre-populated directory to avoid downloading extensions at task runtime.
    :param autoinstall_extensions: whether an extension that is not installed locally may be
        downloaded and installed. Set to ``False`` in environments without outbound internet access
        so a missing extension fails loudly instead of hanging on a network call.
    :param allow_community_extensions: whether DuckDB may load community (third-party) extensions.
        Community extensions are native code from outside the DuckDB project, so this defaults to
        ``False``.
    :param memory_limit: memory DuckDB may use, for example ``"2GB"``. DuckDB otherwise sizes itself
        from the memory it detects on the host, which over-commits inside a container that has a
        smaller limit than the host it runs on.
    :param threads: number of threads DuckDB may use. Defaults to the cores DuckDB detects, which is
        subject to the same container caveat as ``memory_limit``.
    :param temp_directory: directory DuckDB spills to when a query exceeds ``memory_limit``.
    :param read_only: open the database read-only. Not valid for an in-memory database.
    :param settings: additional DuckDB configuration options, passed through verbatim.

    Every parameter above except ``duckdb_conn_id`` may also be set in the connection ``extra``, in
    which case an explicit argument wins. This mirrors what
    :class:`~airflow.providers.common.sql.operators.sql.BaseSQLOperator` does when it merges connection
    extras into hook keyword arguments — a path the connection-optional operator has to bypass.
    """

    conn_name_attr = "duckdb_conn_id"
    default_conn_name = "duckdb_default"
    conn_type = "duckdb"
    hook_name = "DuckDB"
    placeholder = "?"
    supports_autocommit = False

    #: Extensions every connection opened by this hook class loads, regardless of configuration.
    #: Subclasses that integrate a specific backend declare their requirements here.
    required_extensions: tuple[str, ...] = ()

    def __init__(
        self,
        *args,
        duckdb_conn_id: str = default_conn_name,
        database: str | None = None,
        extensions: Sequence[str] | None = None,
        extension_directory: str | None = None,
        autoinstall_extensions: bool | None = None,
        allow_community_extensions: bool | None = None,
        memory_limit: str | None = None,
        threads: int | None = None,
        temp_directory: str | None = None,
        read_only: bool | None = None,
        settings: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        kwargs[self.conn_name_attr] = duckdb_conn_id
        super().__init__(*args, **kwargs)
        self.database = database
        # Stored unresolved: ``None`` means "not set explicitly", so the connection extra may supply
        # it. Reading these through the properties below is what makes the two sources consistent.
        self._extensions = list(extensions) if extensions is not None else None
        self._extension_directory = extension_directory
        self._autoinstall_extensions = autoinstall_extensions
        self._allow_community_extensions = allow_community_extensions
        self._memory_limit = memory_limit
        self._threads = threads
        self._temp_directory = temp_directory
        self._read_only = read_only
        self._settings = settings

    def resolve_parameter(self, name: str, explicit: Any, default: Any = None) -> Any:
        """Return the explicit argument if given, else the connection extra, else the default."""
        if explicit is not None:
            return explicit
        from_extra = self.connection_extra.get(name)
        return default if from_extra is None else from_extra

    @property
    def extension_directory(self) -> str | None:
        return self.resolve_parameter("extension_directory", self._extension_directory)

    @property
    def autoinstall_extensions(self) -> bool:
        return bool(self.resolve_parameter("autoinstall_extensions", self._autoinstall_extensions, True))

    @property
    def allow_community_extensions(self) -> bool:
        return bool(
            self.resolve_parameter("allow_community_extensions", self._allow_community_extensions, False)
        )

    @property
    def memory_limit(self) -> str | None:
        return self.resolve_parameter("memory_limit", self._memory_limit)

    @property
    def threads(self) -> int | None:
        return self.resolve_parameter("threads", self._threads)

    @property
    def temp_directory(self) -> str | None:
        return self.resolve_parameter("temp_directory", self._temp_directory)

    @property
    def read_only(self) -> bool:
        return bool(self.resolve_parameter("read_only", self._read_only, False))

    @property
    def settings(self) -> dict[str, Any]:
        merged = dict(self.connection_extra.get("settings") or {})
        merged.update(self._settings or {})
        return merged

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for the DuckDB connection."""
        return {
            "hidden_fields": ["login", "port"],
            "relabeling": {
                "host": "Database path",
                "schema": "MotherDuck database",
                "password": "MotherDuck token",
            },
            "placeholders": {
                "host": "/tmp/analytics.duckdb (leave empty for an in-memory database)",
                "extra": '{"extensions": ["httpfs"], "memory_limit": "2GB", "threads": 4}',
            },
        }

    @cached_property
    def airflow_connection(self) -> Connection | None:
        """Return the configured Airflow connection, or ``None`` when it does not exist."""
        try:
            return self.get_connection(self.get_conn_id())
        except AirflowNotFoundException:
            self.log.debug(
                "No Airflow connection %r; falling back to an in-memory DuckDB database.",
                self.get_conn_id(),
            )
            return None

    @cached_property
    def connection_extra(self) -> dict[str, Any]:
        """Return the connection's ``extra``, or an empty mapping when there is no connection."""
        connection = self.airflow_connection
        return connection.extra_dejson if connection else {}

    def get_database(self) -> str:
        """
        Return the DuckDB database to open.

        Precedence: the ``database`` argument, the connection extra ``database``, a MotherDuck
        database built from the connection's token, the connection host, then an in-memory database.
        """
        if self.database:
            return self.database
        extra_database = self.connection_extra.get("database")
        if extra_database:
            return extra_database
        connection = self.airflow_connection
        if connection is None:
            return IN_MEMORY_DATABASE
        token = self.connection_extra.get("motherduck_token") or connection.password
        if token:
            return f"md:{connection.schema or ''}?motherduck_token={token}"
        if connection.host:
            return connection.host
        return IN_MEMORY_DATABASE

    def get_extensions(self) -> list[str]:
        """Return the extensions to load, in order, without duplicates."""
        configured = self.resolve_parameter("extensions", self._extensions, [])
        ordered: list[str] = []
        for extension in (*self.required_extensions, *configured):
            if extension not in ordered:
                ordered.append(extension)
        return ordered

    def get_connect_config(self) -> dict[str, Any]:
        """
        Return the DuckDB configuration applied when the database is opened.

        These are passed to ``duckdb.connect(config=...)`` rather than issued as ``SET`` statements
        so that options which are only settable at startup are honored.
        """
        config: dict[str, Any] = {
            "autoinstall_known_extensions": self.autoinstall_extensions,
            "autoload_known_extensions": self.autoinstall_extensions,
            "allow_community_extensions": self.allow_community_extensions,
        }
        for key, value in (
            ("extension_directory", self.extension_directory),
            ("memory_limit", self.memory_limit),
            ("threads", self.threads),
            ("temp_directory", self.temp_directory),
        ):
            if value is not None:
                config[key] = value
        config.update(self.settings)
        return config

    def get_conn(self) -> DuckDBPyConnection:
        """Return a DuckDB connection with configuration, extensions and secrets applied."""
        database = self.get_database()
        if self.read_only and database == IN_MEMORY_DATABASE:
            raise ValueError("read_only is not supported for an in-memory DuckDB database.")
        self.log.info("Opening DuckDB database %s", self._redact_database(database))
        conn = duckdb.connect(database=database, read_only=self.read_only, config=self.get_connect_config())
        try:
            self.load_extensions(conn)
            self.configure_secrets(conn)
        except Exception:
            conn.close()
            raise
        return conn

    def load_extensions(self, conn: DuckDBPyConnection) -> None:
        """Load every configured extension, installing it only if it is not already available."""
        for extension in self.get_extensions():
            self._load_extension(conn, extension)

    def configure_secrets(self, conn: DuckDBPyConnection) -> None:
        """
        Create DuckDB secrets on a freshly opened connection.

        A no-op for a plain DuckDB database. Subclasses that broker credentials for remote storage
        override this to issue ``CREATE SECRET``.
        """

    def get_uri(self) -> str:
        """Return a SQLAlchemy URI for the database, for ``get_sqlalchemy_engine``."""
        return f"duckdb:///{self.get_database()}"

    @cached_property
    def dialect_name(self) -> str:
        """
        Return the SQL dialect name, without asking SQLAlchemy to resolve it.

        ``DbApiHook`` derives this by handing ``get_uri()`` to SQLAlchemy and asking for a matching
        dialect, which only resolves when the third-party ``duckdb-engine`` package happens to be
        installed. Leaving it inferred would make dialect selection — and so the SQL generated for
        things like ``insert_rows`` — differ between deployments that are otherwise identical. Naming
        it here also keeps the MotherDuck token that ``get_uri()`` embeds out of SQLAlchemy's URL
        parser.

        Note this does not by itself populate :attr:`reserved_words`: that looks for a
        ``sqlalchemy.dialects.duckdb`` module, which does not exist. Choosing a keyword source is a
        separate decision.
        """
        return self.connection_extra.get("dialect", "duckdb")

    def _load_extension(self, conn: DuckDBPyConnection, extension: str) -> None:
        self._validate_identifier(extension, "extension")
        try:
            conn.execute(f"LOAD {extension};")
            return
        except duckdb.Error as load_error:
            if not self.autoinstall_extensions:
                raise ValueError(
                    f"DuckDB extension {extension!r} is not installed and autoinstall_extensions is "
                    f"disabled. Pre-install it into extension_directory, or enable "
                    f"autoinstall_extensions to download it at runtime."
                ) from load_error
            self.log.info("DuckDB extension %r is not installed locally; installing it.", extension)
        conn.execute(f"INSTALL {extension};")
        conn.execute(f"LOAD {extension};")

    @staticmethod
    def _validate_identifier(value: str, kind: str) -> None:
        if not _IDENTIFIER.match(value):
            raise ValueError(f"Invalid DuckDB {kind} name: {value!r}")

    @staticmethod
    def _redact_database(database: str) -> str:
        """Strip a MotherDuck token out of a database string so it is safe to log."""
        return re.sub(r"motherduck_token=[^&]*", "motherduck_token=***", database)
