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
"""SAS hook: run ``PROC SQL`` on a SAS server over SSH, through the ``DbApiHook`` contract."""

from __future__ import annotations

import base64
from collections.abc import Mapping
from contextlib import aclosing, contextmanager, suppress
from functools import cached_property
from typing import TYPE_CHECKING, Any

import paramiko

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.sas.dialects.sas import SASDialect
from airflow.providers.sas.driver.connection import SASAsyncConnection, SASConnection
from airflow.providers.sas.driver.script import SASSettings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from pandas import DataFrame

    from airflow.providers.common.sql.dialects.dialect import Dialect


def _apply_host_key_extras(ssh_hook: Any, extra: Mapping[str, Any]) -> None:
    """
    Apply the ``host_key`` / ``no_host_key_check`` extras of the SAS connection to ``ssh_hook``.

    ``SSHHook`` reads them only from an SSH connection's extra, and direct mode
    builds it from host, login and password.  The semantics follow
    ``SSHHook``: a ``host_key`` turns the check on, and asking for both a key
    and no check is an error.
    """
    no_host_key_check = extra.get("no_host_key_check")
    skip_check = no_host_key_check is not None and str(no_host_key_check).lower() == "true"
    host_key = extra.get("host_key")
    if host_key:
        if skip_check:
            raise ValueError("Must check host key when provided")
        parts = str(host_key).split()
        key_type, data = (parts[0], parts[1]) if len(parts) >= 2 else ("ssh-rsa", parts[0])
        ssh_hook.host_key = paramiko.PKey.from_type_string(key_type, base64.decodebytes(data.encode("utf-8")))
        ssh_hook.no_host_key_check = False
    elif no_host_key_check is not None:
        ssh_hook.no_host_key_check = skip_check


class SASHook(DbApiHook):
    """
    Interact with SAS data over SSH.

    Opens an SSH connection to the SAS server, uploads a ``PROC SQL``
    script via SFTP, executes it through the SAS binary, and reads
    the resulting ``.sas7bdat`` file back with ``pandas.read_sas()``.

    This approach requires **no SAS-specific Python driver** — only
    ``paramiko`` (SSH) and ``pandas`` (for ``.sas7bdat`` parsing).

    **One SAS run per statement.**  Every ``execute()`` uploads a script,
    starts the SAS binary and downloads the whole result: there is no
    server-side cursor.  Do not page a query at DAG level with
    ``FIRSTOBS=``/``OBS=`` — that multiplies SAS runs.  Use the
    ``DbApiHook`` DataFrame API instead: ``get_df(sql)`` runs SAS once
    and returns the typed result, ``get_df_by_chunks(sql, chunksize=N)``
    runs SAS once and reads it in chunks.  A ``limit`` (``PROC SQL
    OUTOBS=``) can be set on the hook or per call (``get_df(sql,
    limit=1000)``) for smoke tests.

    **Paging (``GenericTransfer``).**  A query ending in ``LIMIT n OFFSET
    m``, the operator's default ``paginated_sql_statement_clause``, is not
    sent to SAS (which has no LIMIT / OFFSET): the first page starts one
    SAS run of the query without the clause and reads the result in chunks
    of ``n`` rows, and every page that follows in sequence is the next
    chunk, so a paged transfer costs one SAS run and one download (see
    :class:`~airflow.providers.sas.driver.job.SASPagedRead`).  Use ``page_size`` alone, without a
    ``paginated_sql_statement_clause``, and not in deferrable mode: the
    trigger pages from a new hook each time, which starts a run per page.
    An ``ORDER BY`` in the query sorts the whole result once in SAS.

    **Typed DataFrames.**  ``get_df()`` returns one stable dtype per SAS
    column kind, derived from the column metadata in the ``.sas7bdat``
    file (see :meth:`~airflow.providers.sas.driver.metadata.SASMetadata.normalize_dataframe`): character →
    ``string``, numeric → ``float64``, DATE-formatted numeric →
    ``date32[pyarrow]``, DATETIME-formatted numeric → ``datetime64``.
    All-missing columns keep their type and every chunk has the dtypes
    of the whole result, so ``DataFrame.to_parquet`` (and therefore
    ``SqlToS3Operator(file_format="parquet")``) yields a stable schema.
    The DB-API path (``get_records()``, ``GenericTransfer``) still
    returns plain Python values with ``None`` for missing.

    **SSH authentication** can work in two modes:

    1. **Direct mode** (default) — credentials come from the SAS
       connection itself (``host``, ``login``, ``password``).
    2. **Delegated mode** — pass ``ssh_conn_id`` to delegate SSH
       authentication to Airflow's ``SSHHook``.  This supports
       private keys stored in Vault, connection extras, key files,
       etc.  When ``ssh_conn_id`` is set, the SSH-related fields on
       the SAS connection are ignored; only the SAS-specific extras
       (``sas_binary``, ``sas_libraries``, etc.) are read from it.
       ``ssh_conn_id`` can also be an extra of the SAS connection, so
       operators that only take a ``conn_id`` can use this mode.

    Connection fields are mapped as follows (direct mode only):

    =========  =============================
    Airflow    Meaning
    =========  =============================
    host       SAS server hostname (SSH)
    port       SSH port (default ``22``)
    login      SSH username
    password   SSH password
    =========  =============================

    Configurable keys in the connection *extra* JSON:

    * ``ssh_conn_id``        – SSH connection to delegate SSH authentication
      to (delegated mode); the constructor argument wins.
    * ``sas_binary``         – path to the SAS binary on the remote server
      (default ``/opt/sas/SASFoundation/9.4/sas``)
    * ``sas_options``        – JSON list of command-line options passed to SAS
      (default ``["-linesize 90", "-noterminal", "-nodate", "-nonews"]``)
    * ``sas_libraries``      – JSON object mapping SAS library names to their
      **full** directory paths on the remote server.  The library name
      is extracted automatically from the SQL (e.g. ``MYLIB`` in
      ``SELECT * FROM MYLIB.TABLE``) and looked up in this dict; a
      query over several libraries gets one ``LIBNAME`` per library.
    * ``sas_temp_dir``       – remote directory for temporary script/output
      files (default ``/tmp``)
    * ``sas_work_dir``       – remote directory used as the SAS ``WORK``
      library root (``-work``).  Defaults to ``sas_temp_dir``.  Override
      when the SAS site default ``WORK`` path is not writable by the
      Airflow account — the SAS kernel refuses to start in that case.
      Ignored when ``sas_options`` already contains a ``-work`` option.
    * ``encoding``           – encoding for reading ``.sas7bdat`` files
      (default ``iso-8859-15``)
    * ``keepalive_interval`` – SSH keepalive interval in seconds. Set to
      ``0`` to disable keepalive (default ``30``).
    * ``timeout``            – seconds to wait for one SAS run before the
      statement fails with :class:`~airflow.providers.sas.driver.script.SASScriptError` (default: no limit).
    * ``sas_compress``       – ``COMPRESS=`` dataset option for query
      results: ``yes`` (default, run-length encoding of the padded
      character columns SAS writes), ``binary`` or ``no``.  SAS writes
      and the hook downloads a much smaller file; pandas decompresses
      natively.
    * ``ssh_compress``       – zlib on the SSH transport (direct mode only).
      On by default: the run-length encoded result still shrinks a lot on
      the wire, and the download is the slowest step (``get_df`` 5-57%
      faster than without in our measurements); ``false`` turns it off.
    * ``host_key``           – the server's public host key, as in
      ``known_hosts`` without the host name (``ssh-ed25519 AAAA...``; a bare
      base64 key is read as RSA).  The SSH session then only accepts that
      key (direct mode only; in delegated mode the SSH connection carries it).
    * ``no_host_key_check``  – ``true`` to accept any host key (direct mode
      only).  This is a stopgap: it leaves the password open to a
      man-in-the-middle.  Without either key the ssh provider's default
      applies: only hosts in ``known_hosts`` are accepted.

    :param sas_conn_id: The Airflow connection id for SAS credentials.
    :param ssh_conn_id: Optional Airflow connection id for SSH (delegates
        to ``SSHHook``).
    :param sas_binary: Path to the SAS binary on the remote server.
        Overrides the connection extra.
    :param sas_options: Command-line options passed to SAS.
        Overrides the connection extra.
    :param sas_libraries: Mapping of SAS library names to directory paths.
        Overrides the connection extra.
    :param sas_temp_dir: Remote directory for temporary script/output files.
        Overrides the connection extra.
    :param sas_work_dir: Remote directory used as the SAS ``WORK`` library
        root. Defaults to ``sas_temp_dir``. Overrides the connection extra.
    :param encoding: Encoding for reading ``.sas7bdat`` files.
        Overrides the connection extra.
    :param keepalive_interval: SSH keepalive interval in seconds.
        Overrides the connection extra.
    :param timeout: Seconds to wait for one SAS run.  Overrides the
        connection extra.  The SAS process keeps running on the server
        when the wait is given up.
    :param sas_compress: ``COMPRESS=`` dataset option for query results
        (``yes``, ``char``, ``binary`` or ``no``).  Overrides the
        connection extra; default ``yes``.
    :param limit: Default server-side row limit (``PROC SQL OUTOBS=``)
        applied to every data query of this hook.  Meant for smoke
        tests; ``None`` (default) means no limit.

    Example connection setup (direct mode)::

        Connection Id : sas_default
        Host          : sas-server.example.com
        Login         : my_user
        Password      : ****
        Extra         : {"sas_binary": "/opt/sas/SASFoundation/9.4/sas",
                         "sas_libraries": {
                             "sales": "/data/sas/sales",
                             "finance": "/data/sas/finance"
                         },
                         "sas_temp_dir": "/data/sas/work"}

    Example connection setup (delegated mode)::

        Connection Id : sas_default
        Extra         : {"sas_binary": "/opt/sas/SASFoundation/9.4/sas",
                         "sas_libraries": {
                             "sales": "/data/sas/sales"
                         },
                         "sas_temp_dir": "/data/sas/work"}

        # In the Dag:
        hook = SASHook(sas_conn_id="sas_default", ssh_conn_id="ssh_sas_server")

    A single shared connection can serve multiple libraries — the
    library is resolved automatically from the SQL.
    """

    conn_name_attr = "sas_conn_id"
    default_conn_name = "sas_default"
    conn_type = "sas"
    hook_name = "SAS"
    supports_autocommit = False

    _DEFAULT_KEEPALIVE_INTERVAL = 30

    def __init__(
        self,
        *args: Any,
        ssh_conn_id: str | None = None,
        sas_binary: str | None = None,
        sas_options: list[str] | None = None,
        sas_libraries: dict[str, str] | None = None,
        sas_temp_dir: str | None = None,
        sas_work_dir: str | None = None,
        encoding: str | None = None,
        keepalive_interval: int | None = None,
        limit: int | None = None,
        timeout: float | None = None,
        sas_compress: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._ssh_conn_id = ssh_conn_id
        self._limit = limit
        self._keepalive_interval = keepalive_interval
        # The SASSettings fields a connection extra may also set, kept as
        # given (``None`` = not passed) so that ``_resolve_settings`` can
        # apply the documented precedence: constructor argument, then
        # connection extra, then the SASSettings default.
        self._settings_arguments: dict[str, Any] = {
            "sas_binary": sas_binary,
            "sas_options": sas_options,
            "sas_libraries": sas_libraries,
            "sas_temp_dir": sas_temp_dir,
            "sas_work_dir": sas_work_dir,
            "encoding": encoding,
            "timeout": timeout,
            "sas_compress": sas_compress,
        }
        self._cached_conn: SASConnection | None = None
        self._cached_async_conn: SASAsyncConnection | None = None

    @property
    def dialect_name(self) -> str:
        return "sas"

    @cached_property
    def dialect(self) -> Dialect:
        # Cached like ``DbApiHook.dialect``: ``SASDialect.get_column_names``
        # memoises per instance, and a new dialect per access would run the
        # DICTIONARY query (one SAS job) on every call.
        return SASDialect(self)

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------
    def get_conn(self) -> SASConnection:
        """
        Return a cached SSH connection, creating one on first call.

        Always delegates SSH session creation to Airflow's ``SSHHook``.
        The SAS parameters (``sas_binary``, ``sas_libraries``, etc.) come
        from the ``sas_conn_id`` connection extra in both modes.

        * When ``ssh_conn_id`` is provided (constructor argument or
          connection extra), SSH authentication is handled entirely by
          that connection.
        * Otherwise, the ``sas_conn_id`` connection also provides the SSH
          credentials (host/port/login/password).

        Constructor arguments always override connection extras when
        both are available.

        The connection is reused across multiple calls (e.g. paginated
        queries in ``GenericTransfer``) instead of opening a new SSH
        session each time.  A cached connection whose session paramiko saw
        end (see :meth:`~airflow.providers.sas.driver.connection.SASConnection.is_active`) is replaced by a new one,
        so a long task survives a dropped session between two statements.
        """
        if self._cached_conn is not None:
            if self._cached_conn.is_active():
                return self._cached_conn
            self.log.warning("The SAS SSH session was closed; opening a new one.")
            # The session is gone already; closing it only releases what
            # paramiko still holds and may fail on the dead channel.
            with suppress(OSError, EOFError, paramiko.SSHException):
                self._cached_conn.close()
            self._cached_conn = None

        from airflow.providers.ssh.hooks.ssh import SSHHook

        conn = self.connection
        extra = conn.extra_dejson
        settings = self._resolve_settings(extra)
        ssh_conn_id = self._ssh_conn_id or extra.get("ssh_conn_id")
        if ssh_conn_id:
            # Delegated mode: SSH from the SSH connection, SAS params from
            # the SAS connection as in direct mode.
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
        else:
            # Direct mode: SSH credentials from the SAS connection too.
            if not conn.host:
                raise ValueError(
                    f"The SAS connection {conn.conn_id!r} has no host: set it, "
                    "or set ssh_conn_id to delegate SSH to an SSH connection."
                )
            ssh_hook = SSHHook(
                remote_host=conn.host,
                port=int(conn.port) if conn.port else None,
                username=conn.login,
                password=conn.password,
                keepalive_interval=self._resolve_keepalive_interval(extra),
            )
            # SSHHook only reads its ``compress`` extra when built from an
            # ssh_conn_id, so direct mode applies the SAS connection's own.
            ssh_hook.compress = self._ssh_compress(extra)
            # The same holds for the host key extras, and from ssh provider 7.0
            # a hook without them refuses any host that is not in known_hosts.
            _apply_host_key_extras(ssh_hook, extra)

        ssh_client = ssh_hook.get_conn()
        sftp_client = ssh_client.open_sftp()

        self._cached_conn = SASConnection(ssh_client=ssh_client, sftp_client=sftp_client, settings=settings)
        return self._cached_conn

    async def aget_conn(self) -> SASAsyncConnection:
        """
        Return a cached asyncssh connection, the async twin of :meth:`get_conn`.

        Safe in an async task running items concurrently (``.iterate()``): the
        SAS connection is read with common.compat's ``get_async_connection`` and
        ``get_async_extra_dejson``, never with ``self.connection`` / ``extra_dejson``
        (synchronous SDK calls, ``DeadlockImminentError`` there).  SSH goes
        through the ssh provider's ``SSHHookAsync``, which reads its extra the
        same way and compresses as ``SSHHook``: in
        delegated mode on the SSH connection, in direct mode on the SAS
        connection itself, whose host, login, password and host-key extras it
        reads as an SSH connection's.  The settings, ``ssh_compress`` and
        ``keepalive_interval`` resolve as in :meth:`get_conn`.  A cached
        connection that was closed is replaced.
        """
        if self._cached_async_conn is not None:
            if self._cached_async_conn.is_active():
                return self._cached_async_conn
            self.log.warning("The SAS SSH session was closed; opening a new one.")
            with suppress(Exception):
                # Releasing a dead session; whatever it raises changes nothing.
                await self._cached_async_conn.close()
            self._cached_async_conn = None

        from airflow.providers.common.compat.connection import (
            get_async_connection,
            get_async_extra_dejson,
        )
        from airflow.providers.ssh.hooks.ssh import SSHHookAsync

        conn = await get_async_connection(self.get_conn_id())
        extra = await get_async_extra_dejson(conn)
        settings = self._resolve_settings(extra)
        ssh_conn_id = self._ssh_conn_id or extra.get("ssh_conn_id")
        if ssh_conn_id:
            ssh_hook = SSHHookAsync(ssh_conn_id=ssh_conn_id)
        else:
            ssh_hook = SSHHookAsync(
                ssh_conn_id=self.get_conn_id(),
                keepalive_interval=self._resolve_keepalive_interval(extra),
            )
            ssh_hook.compress = self._ssh_compress(extra)

        ssh_connection = await ssh_hook.get_conn()
        sftp_client = await ssh_connection.start_sftp_client()
        self._cached_async_conn = SASAsyncConnection(ssh_connection, sftp_client, settings)
        return self._cached_async_conn

    async def aget_records(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Async ``get_records`` for a query: plain Python rows, ``None`` for missing."""
        statement, bind_params = self._split_statement_and_parameters(sql, parameters)
        conn = await self.aget_conn()
        return await conn.fetch_records(statement, bind_params)

    async def aget_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        limit: int | None = None,
        dtype_backend: str | None = None,
    ) -> DataFrame:
        """Async ``get_df``: the typed DataFrame of one SAS run (see :meth:`_get_pandas_df`)."""
        statement, bind_params = self._split_statement_and_parameters(sql, parameters)
        conn = await self.aget_conn()
        return await conn.fetch_dataframe(statement, bind_params, limit=limit, dtype_backend=dtype_backend)

    async def aget_df_by_chunks(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        limit: int | None = None,
        dtype_backend: str | None = None,
    ) -> AsyncGenerator[DataFrame, None]:
        """Async ``get_df_by_chunks``: one SAS run, typed chunks of *chunksize* rows."""
        statement, bind_params = self._split_statement_and_parameters(sql, parameters)
        conn = await self.aget_conn()
        async with aclosing(
            conn.fetch_dataframe_chunks(
                statement,
                bind_params,
                chunksize=chunksize,
                limit=limit,
                dtype_backend=dtype_backend,
            )
        ) as chunks:
            async for chunk in chunks:
                yield chunk

    def close(self) -> None:
        """Close the cached SSH connection, if any; the next statement opens a new one."""
        if self._cached_conn is not None:
            conn, self._cached_conn = self._cached_conn, None
            conn.close()

    async def aclose(self) -> None:
        """Close the cached asyncssh connection, if any."""
        if self._cached_async_conn is not None:
            conn, self._cached_async_conn = self._cached_async_conn, None
            await conn.close()

    def _resolve_keepalive_interval(self, extra: Mapping[str, Any]) -> int:
        """Resolve the keepalive: constructor argument, then extra, then 30 s; ``0`` disables it."""
        if self._keepalive_interval is not None:
            return int(self._keepalive_interval)
        return int(extra.get("keepalive_interval", self._DEFAULT_KEEPALIVE_INTERVAL))

    def _resolve_settings(self, extra: Mapping[str, Any]) -> SASSettings:
        """
        Resolve the SAS settings with the documented precedence.

        A constructor argument wins, then the connection *extra*, then the
        :class:`~airflow.providers.sas.driver.script.SASSettings` default.  The checks are on ``None``, not on
        truthiness, so an empty or zero value is still a value.  ``limit``
        is a constructor argument only.
        """
        values = {
            name: extra.get(name) if argument is None else argument
            for name, argument in self._settings_arguments.items()
        }
        return SASSettings(
            limit=self._limit,
            **{name: value for name, value in values.items() if value is not None},
        )

    @staticmethod
    def _ssh_compress(extra: Mapping[str, Any]) -> bool:
        """Return the ``ssh_compress`` extra, on when absent."""
        value = extra.get("ssh_compress")
        return value is None or str(value).lower() != "false"

    @contextmanager
    def _create_autocommit_connection(self, autocommit: bool = False):
        """
        Yield the cached connection **without** closing it afterwards.

        The base class wraps ``get_conn()`` with ``closing()``, which
        would tear down the SSH session after every single query.
        SAS-over-SSH sessions are expensive to establish, so we keep
        the connection alive and reuse it for subsequent calls.
        """
        yield self.get_conn()

    # ------------------------------------------------------------------
    # DataFrame — DbApiHook extension points (no tuple round-trip)
    # ------------------------------------------------------------------
    @staticmethod
    def _split_statement_and_parameters(sql, parameters):
        statement = sql[-1] if isinstance(sql, list) else sql
        bind_params: list | tuple | dict[str, Any] | None = (
            dict(parameters) if isinstance(parameters, Mapping) else parameters
        )
        return statement, bind_params

    def _warn_unsupported_kwargs(self, kwargs: Mapping[str, Any]) -> None:
        if kwargs:
            self.log.warning(
                "SASHook ignores DataFrame arguments %s: results come from a "
                ".sas7bdat file, not from pandas.read_sql / polars.read_database.",
                sorted(kwargs),
            )

    def _get_pandas_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        limit: int | None = None,
        dtype_backend: str | None = None,
        **kwargs: Any,
    ) -> DataFrame:
        """
        Return a typed DataFrame directly from the SAS cursor.

        Overrides the ``DbApiHook`` extension point behind ``get_df()`` to
        avoid the ``DataFrame → tuples → DataFrame`` round-trip that
        ``pandas.io.sql.read_sql`` would cause.  SAS runs exactly once.

        :param limit: Server-side row limit (``PROC SQL OUTOBS=``) for this
            call; overrides the hook-level ``limit``, ``0`` disables it.
        :param dtype_backend: ``"pyarrow"`` or ``"numpy_nullable"`` to
            type the result with that nullable backend, as
            ``pandas.read_sql`` would (see
            :meth:`~airflow.providers.sas.driver.metadata.SASMetadata.normalize_dataframe`).  ``SqlToS3Operator``
            skips its value-dependent dtype fixing when ``read_kwargs``
            carries ``dtype_backend="pyarrow"``, which keeps the Parquet
            schema stable.
        """
        self._warn_unsupported_kwargs(kwargs)
        statement, bind_params = self._split_statement_and_parameters(sql, parameters)
        conn = self.get_conn()
        with conn.cursor() as cursor:
            df = cursor.fetch_dataframe(statement, bind_params, limit=limit, dtype_backend=dtype_backend)
        self.log.info(
            "SAS returned %d rows, %d columns: %s",
            len(df),
            len(df.columns),
            ", ".join(f"{col} ({dtype})" for col, dtype in df.dtypes.items()),
        )
        return df

    def _get_pandas_df_by_chunks(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        limit: int | None = None,
        dtype_backend: str | None = None,
        **kwargs: Any,
    ) -> Generator[DataFrame, None, None]:
        """
        Yield typed DataFrame chunks directly from the SAS cursor.

        Overrides the ``DbApiHook`` extension point behind
        ``get_df_by_chunks()``.  SAS runs once and writes the whole result
        to one ``.sas7bdat`` file; the file is parsed while it is read, so
        only one *chunksize*-row chunk is held in memory.  Every chunk
        carries the dtypes of the whole result, even when all its values
        are missing.

        :param limit: as for :meth:`_get_pandas_df`.
        :param dtype_backend: as for :meth:`_get_pandas_df`.
        """
        self._warn_unsupported_kwargs(kwargs)
        statement, bind_params = self._split_statement_and_parameters(sql, parameters)
        with self.get_conn().cursor() as cursor:
            yield from cursor.fetch_dataframe_chunks(
                statement,
                bind_params,
                chunksize=chunksize,
                limit=limit,
                dtype_backend=dtype_backend,
            )

    def _get_polars_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ):
        """Return a polars DataFrame (``get_df(df_type="polars")``) from the typed pandas result."""
        try:
            import polars as pl
        except ImportError:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "polars library not installed, run: pip install "
                "'apache-airflow-providers-common-sql[polars]'."
            ) from None
        return pl.from_pandas(self._get_pandas_df(sql, parameters, **kwargs))

    def _get_polars_df_by_chunks(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        **kwargs: Any,
    ):
        """Yield polars DataFrame chunks (``get_df_by_chunks(df_type="polars")``)."""
        try:
            import polars as pl
        except ImportError:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "polars library not installed, run: pip install "
                "'apache-airflow-providers-common-sql[polars]'."
            ) from None
        for chunk in self._get_pandas_df_by_chunks(sql, parameters, chunksize=chunksize, **kwargs):
            yield pl.from_pandas(chunk)

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------
    def test_connection(self) -> tuple[bool, str]:
        """Test the SSH connection to the SAS server."""
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 AS test FROM DICTIONARY.TABLES(OBS=1)")
                cursor.fetchone()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
