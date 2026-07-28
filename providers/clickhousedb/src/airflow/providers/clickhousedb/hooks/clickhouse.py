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

import json
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus

from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    import clickhouse_connect.driver.client
    from clickhouse_connect.dbapi.cursor import Cursor as ClickHouseDbApiCursor

# Optional scalar parameters forwarded verbatim to clickhouse_connect.get_client()
# Note: client_name is handled separately by _build_client_name() so Airflow version
# info is always embedded in the HTTP User-Agent / system.query_log entry.
_OPTIONAL_CLIENT_KWARGS = (
    "connect_timeout",
    "send_receive_timeout",
    "compress",
)

# kwargs that the hook always manages itself.  User-supplied client_kwargs that
# overlap with these keys are dropped and logged at DEBUG so the hook's own values always win.
_HOOK_MANAGED_KWARGS: frozenset[str] = frozenset(
    {
        "host",
        "port",
        "username",
        "password",
        "database",
        "secure",
        "verify",
        "client_name",
        "settings",
    }
)


def _build_client_name(custom: str | None = None) -> str:
    """
    Return the ``client_name`` string passed to ``clickhouse_connect.get_client()``.

    The value is prepended to the HTTP ``User-Agent`` header that ClickHouse records
    in ``system.query_log``, so every query is traceable back to its Airflow source.

    Format (no custom label)::

        apache-airflow/<airflow_version> apache-airflow-providers-clickhousedb/<provider_version>

    Format (with the ``client_name`` extra field set to ``"my-pipeline"``)::

        apache-airflow/<airflow_version> apache-airflow-providers-clickhousedb/<provider_version> (my-pipeline)

    The ``clickhouse_connect`` library appends its own token and OS information, so
    the full ``User-Agent`` looks like::

        apache-airflow/X.Y.Z apache-airflow-providers-clickhousedb/X.Y.Z
        clickhouse-connect/X.Y.Z (lv:py/X.Y.Z; mode:sync; os:linux)
    """
    import airflow
    from airflow.providers.clickhousedb import __version__ as provider_version

    name = f"apache-airflow/{airflow.__version__} apache-airflow-providers-clickhousedb/{provider_version}"
    if custom:
        name = f"{name} ({custom.strip()})"
    return name


class ClickHouseConnection:
    """
    Minimal DB-API 2.0 connection adapter wrapping a ``clickhouse_connect`` Client.

    SQL execution is delegated to ``clickhouse_connect.dbapi.cursor.Cursor``, which
    routes each statement to ``client.query()`` or ``client.command()`` automatically
    by inspecting the SQL keyword after stripping comments — the same logic used by
    the ``clickhouse-connect`` SQLAlchemy dialect.

    ClickHouse has no multi-statement transactions.  Every statement is effectively
    auto-committed, so ``commit()`` and ``rollback()`` are intentional no-ops and
    the ``autocommit`` attribute is always ``True``.  ``DbApiHook.run()`` checks
    ``conn.autocommit`` via :meth:`ClickHouseHook.get_autocommit` and skips the
    ``conn.commit()`` call when it is truthy.
    """

    # Signals to DbApiHook.get_autocommit() that no explicit commit is needed.
    autocommit: bool = True

    def __init__(self, client: clickhouse_connect.driver.client.Client) -> None:
        self._client = client

    def cursor(self) -> ClickHouseDbApiCursor:
        from clickhouse_connect.dbapi.cursor import Cursor

        return Cursor(self._client)

    def close(self) -> None:
        self._client.close()

    def commit(self) -> None:
        pass  # ClickHouse has no multi-statement transactions

    def rollback(self) -> None:
        pass  # ClickHouse has no multi-statement transactions


class ClickHouseHook(DbApiHook):
    """
    Interact with ClickHouse via the HTTP interface (``clickhouse-connect``).

    This hook wraps ``clickhouse_connect.get_client()`` behind a thin DB-API 2.0
    adapter (:class:`ClickHouseConnection`), so all standard
    :class:`~airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`
    features work out of the box (templating, ``handler``, ``split_statements``,
    etc.).

    :param database: Optional database name.  Overrides the ``schema`` field of the
        Airflow connection.  Useful when one connection points to a ClickHouse cluster
        and individual tasks need to target different databases.
    :param session_settings: Optional dict of ClickHouse session-level settings
        (e.g. ``{"max_execution_time": 60, "max_threads": 4}``).  Values supplied
        here are **merged on top of** any ``session_settings`` dict already present
        in the connection's ``extra`` JSON field, with the constructor argument
        taking precedence. For a full list of available session settings visit https://clickhouse.com/docs/operations/settings/settings
    :param client_kwargs: Optional dict of additional keyword arguments forwarded
        verbatim to ``clickhouse_connect.get_client()``.  Use this to pass any
        parameter not otherwise exposed by the hook (e.g. ``http_proxy``,
        ``pool_mgr_params``).  Values supplied here are **merged on top of** any
        ``client_kwargs`` dict already present in the connection's ``extra`` JSON
        field, with the constructor argument taking precedence on conflicting keys.
        Keys that the hook manages itself (``host``, ``port``, ``username``,
        ``password``, ``database``, ``secure``, ``verify``, ``client_name``,
        ``settings``) are silently ignored so that hook-managed values always
        take precedence.

    .. seealso::
        - `clickhouse-connect documentation <https://clickhouse.com/docs/en/integrations/python>`_
        - `clickhouse session settings documentation <https://clickhouse.com/docs/operations/settings/settings>`_
        - :ref:`howto/connection:clickhouse`

    """

    conn_name_attr = "clickhouse_conn_id"
    default_conn_name = "clickhouse_default"
    conn_type = "clickhouse"
    hook_name = "ClickHouse"

    # ClickHouse has no multi-statement transactions; every statement is auto-committed.
    supports_autocommit = True
    _test_connection_sql = "SELECT 1"

    def set_autocommit(self, conn: ClickHouseConnection, autocommit: bool) -> None:
        """No-op: ClickHouse has no transaction support."""

    def get_autocommit(self, conn: ClickHouseConnection) -> bool:
        """Return ``True``: ClickHouse auto-commits every statement."""
        return True

    def __init__(
        self,
        *args,
        database: str | None = None,
        session_settings: dict[str, Any] | None = None,
        client_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.database = database
        self.session_settings: dict[str, Any] = session_settings or {}
        self.client_kwargs: dict[str, Any] = client_kwargs or {}

    def _get_client_kwargs(self) -> dict[str, Any]:
        """
        Build the keyword-argument dict passed to ``clickhouse_connect.get_client()``.

        Construction order (last write wins):

        1. ``client_kwargs`` from the connection ``extra`` JSON — connection-level
           passthrough kwargs.
        2. ``client_kwargs`` constructor argument — task-level overrides merged on top.
        3. Hook-managed keys (``host``, ``port``, ``username``, ``password``,
           ``database``, ``secure``, ``verify``, ``client_name``, ``settings``) —
           always override anything in ``client_kwargs``.

        Optional tuning parameters (``connect_timeout``, ``send_receive_timeout``,
        ``compress``) are forwarded only when explicitly set in the ``extra`` JSON
        field so that the driver's own defaults apply otherwise.

        ``client_name`` is always set to a string that identifies the Airflow
        installation (see :func:`_build_client_name`).  Any ``client_name`` value in
        ``extra`` is treated as a human-readable label and appended as a comment so
        it appears alongside the Airflow version info in ``system.query_log``.

        ``session_settings`` from ``extra`` and from the constructor ``session_settings``
        argument are **merged**, with the constructor argument taking precedence on
        conflicting keys.
        """
        conn = self.get_connection(self.get_conn_id())
        extra: dict[str, Any] = conn.extra_dejson

        # Merge client_kwargs: extra values are the base, constructor values override.
        raw_client_kwargs = extra.get("client_kwargs")
        if isinstance(raw_client_kwargs, str):
            try:
                raw_client_kwargs = json.loads(raw_client_kwargs)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in extra.client_kwargs: {e}") from e
        merged_client_kwargs: dict[str, Any] = {**(raw_client_kwargs or {}), **self.client_kwargs}

        # Strip hook-managed keys so they cannot overwrite connection credentials
        # or versioning info.  Log dropped keys at DEBUG to aid troubleshooting.
        dropped = _HOOK_MANAGED_KWARGS.intersection(merged_client_kwargs)
        if dropped:
            self.log.debug(
                "Ignoring hook-managed keys in client_kwargs: %s",
                sorted(dropped),
            )
        kwargs: dict[str, Any] = {
            k: v for k, v in merged_client_kwargs.items() if k not in _HOOK_MANAGED_KWARGS
        }

        # Hook-managed connection parameters always take precedence.
        kwargs.update(
            {
                "host": conn.host or "localhost",
                "port": int(conn.port) if conn.port else 8123,
                "username": conn.login or "default",
                "password": conn.password or "",
                "database": self.database or conn.schema or "default",
                "secure": bool(extra.get("secure", False)),
                "verify": bool(extra.get("verify", True)),
            }
        )

        for key in _OPTIONAL_CLIENT_KWARGS:
            if key in extra and extra[key] is not None:
                kwargs[key] = extra[key]

        # Always embed Airflow + provider version; user's 'client_name' extra is appended to the User-Agent header.
        kwargs["client_name"] = _build_client_name(extra.get("client_name"))

        # Merge session_settings: extra values are the base, constructor values override.
        raw = extra.get("session_settings")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in extra.session_settings: {e}") from e
        merged: dict[str, Any] = {**(raw or {}), **self.session_settings}
        if merged:
            kwargs["settings"] = merged

        return kwargs

    def get_conn(self) -> ClickHouseConnection:
        """Return a DB-API 2.0 compatible connection backed by ``clickhouse_connect``."""
        import clickhouse_connect

        client = clickhouse_connect.get_client(**self._get_client_kwargs())
        return ClickHouseConnection(client)

    def get_client(self) -> clickhouse_connect.driver.client.Client:
        """
        Return the raw ``clickhouse_connect`` Client for ClickHouse-specific operations.

        Use this for bulk inserts, streaming queries, or any operation that
        benefits from the native ``clickhouse_connect`` API rather than DB-API
        cursors.  The caller is responsible for closing the client.
        """
        import clickhouse_connect

        return clickhouse_connect.get_client(**self._get_client_kwargs())

    def bulk_insert_rows(
        self,
        table: str,
        rows: list[tuple],
        column_names: list[str],
        batch_size: int | None = None,
    ) -> None:
        """
        Insert rows into a ClickHouse table using the native columnar insert.

        Uses ``clickhouse_connect``'s optimized insert path, which is
        significantly faster than row-by-row cursor inserts for large datasets.

        By default (``batch_size=None``) all rows are sent in a single call.
        Set ``batch_size`` to bound peak memory usage on very large inputs; an
        insert context is created once and reused across chunks to avoid a
        repeated ``DESCRIBE TABLE`` round-trip per batch.

        :param table: Target table name.
        :param rows: List of row tuples to insert.
        :param column_names: Column names matching each position in the row tuples.
        :param batch_size: Number of rows per insert chunk.  ``None`` (default)
            sends all rows in one request.
        """
        if not rows:
            self.log.warning(
                "bulk_insert_rows called with an empty rows list — nothing was inserted into %s.", table
            )
            return

        client = self.get_client()
        try:
            if batch_size is None:
                client.insert(table, rows, column_names=column_names)
            else:
                ctx = client.create_insert_context(table, column_names=column_names)
                for i in range(0, len(rows), batch_size):
                    client.insert(data=rows[i : i + batch_size], context=ctx)
            self.log.info("Inserted %d rows into %s", len(rows), table)
        finally:
            client.close()

    def get_uri(self) -> str:
        """
        Return a SQLAlchemy-compatible URI for ``get_sqlalchemy_engine()``.

        Always uses the ``clickhousedb://`` scheme registered by ``clickhouse-connect``.
        TLS and tuning parameters are forwarded as query-string arguments so that
        SQLAlchemy-path users get the same settings as DB-API-path users:

        * ``secure`` — enables HTTPS/TLS (``?secure=true``)
        * ``verify`` — TLS certificate verification (``?verify=false`` to disable)
        * ``connect_timeout``, ``send_receive_timeout``, ``compress`` — forwarded when
          explicitly set in the connection's ``extra`` JSON
        """
        conn = self.get_connection(self.get_conn_id())
        extra: dict[str, Any] = conn.extra_dejson
        username = conn.login or "default"
        password = quote_plus(conn.password) if conn.password else ""
        host = conn.host or "localhost"
        port = int(conn.port) if conn.port else 8123
        database = self.database or conn.schema or "default"

        params: dict[str, str] = {}
        if extra.get("secure", False):
            params["secure"] = "true"
        if "verify" in extra and not extra["verify"]:
            params["verify"] = "false"
        for key in _OPTIONAL_CLIENT_KWARGS:
            if key in extra:
                value = extra[key]
                params[key] = str(value).lower() if isinstance(value, bool) else str(value)

        base = (
            f"clickhousedb://{username}:{password}@{host}:{port}/{database}"
            if password
            else f"clickhousedb://{username}@{host}:{port}/{database}"
        )
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            return f"{base}?{query_string}"
        return base
