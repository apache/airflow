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
"""DB-API objects of the SAS hook: the cursor and the synchronous and asynchronous connections."""

from __future__ import annotations

from contextlib import aclosing
from typing import TYPE_CHECKING, Any

from airflow.providers.sas.driver.job import SASAsyncJob, SASJob, SASPagedRead
from airflow.providers.sas.driver.metadata import SASMetadata
from airflow.providers.sas.driver.script import SASScript, SASSettings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Mapping

    import asyncssh
    import paramiko
    from pandas import DataFrame


class SASCursor:
    """
    DB-API 2.0 cursor that executes SQL on a remote SAS server over SSH.

    SELECT / WITH queries are wrapped in a ``PROC SQL … CREATE TABLE``
    script by :class:`~airflow.providers.sas.driver.script.SASScript` and run by the cursor's :class:`~airflow.providers.sas.driver.job.SASJob`,
    which uploads it via SFTP, executes it through the SAS binary and reads
    the resulting ``.sas7bdat`` file back with ``pandas.read_sas()``.

    Non-SELECT statements (INSERT, UPDATE, DELETE, …) are executed
    directly via ``PROC SQL`` and produce no result set.
    """

    arraysize = 1

    def __init__(
        self,
        ssh_client: paramiko.SSHClient,
        sftp_client: paramiko.SFTPClient,
        settings: SASSettings,
        metadata: SASMetadata | None = None,
        script: SASScript | None = None,
        pages: SASPagedRead | None = None,
    ) -> None:
        self._settings = settings
        self._metadata = metadata or SASMetadata()
        self._script = script or SASScript(settings.sas_libraries, settings.sas_compress)
        self._job = SASJob(ssh_client, sftp_client, settings, self._metadata)
        # The connection's, so that a paged read outlives this cursor; a
        # standalone cursor pages on its own.
        self._pages = pages or SASPagedRead(
            SASJob(ssh_client, sftp_client, settings, self._metadata),
            self._script,
            settings.limit,
        )

        self.description: list[tuple[str, str, None, None, None, None, None]] | None = None
        self._rows: list[tuple[Any, ...]] = []
        self._index = 0
        self.rowcount = -1

    # ------------------------------------------------------------------
    # DB-API 2.0
    # ------------------------------------------------------------------
    def execute(self, sql: str, parameters: list | tuple | dict[str, Any] | None = None) -> None:
        """
        Execute a single SQL statement on the remote SAS server.

        The SAS libraries (``LIBNAME``) are resolved automatically from
        qualified table references in the SQL (e.g. ``FROM MYLIB.TABLE``)
        and looked up in ``sas_libraries``.

        :param sql: SQL statement (without ``PROC SQL / QUIT`` wrapping).
        :param parameters: Optional bind parameters (``%s`` or
            ``%(name)s``), written into the SQL as SAS literals.
        """
        sql_clean = self._script.bind(sql, parameters)

        if self._script.is_query(sql_clean):
            df = self._execute_query(sql_clean)
            self.description = self._metadata.build_description(df)
            # DB-API rows are plain Python values with ``None`` for missing,
            # whatever the (typed) dtype of the DataFrame column.
            self._rows = self._metadata.to_records(df)
            self.rowcount = len(self._rows)
            self._index = 0
        else:
            self._execute_statement(sql_clean)
            self.description = None
            self._rows = []
            self.rowcount = -1
            self._index = 0

    def fetch_dataframe(
        self,
        sql: str,
        parameters: list | tuple | dict[str, Any] | None = None,
        *,
        limit: int | None = None,
        dtype_backend: str | None = None,
    ) -> DataFrame:
        """
        Execute a SELECT query and return the result as a typed DataFrame.

        This bypasses the DB-API 2.0 tuple conversion and returns the
        DataFrame from :meth:`~airflow.providers.sas.driver.metadata.SASMetadata.normalize_dataframe` directly —
        useful when the caller already needs a DataFrame (e.g.
        :meth:`~airflow.providers.sas.hooks.sas.SASHook._get_pandas_df`).

        :param sql: SQL SELECT/WITH statement.
        :param parameters: Optional bind parameters (``%s`` style).
        :param limit: Server-side row limit (``PROC SQL OUTOBS=``) for
            this query; overrides the cursor default, ``0`` disables it.
        :param dtype_backend: see :meth:`~airflow.providers.sas.driver.metadata.SASMetadata.normalize_dataframe`.
        :raises ValueError: If *sql* is not a data-returning query.
        """
        bound = self._script.bind(sql, parameters)
        if not self._script.is_query(bound):
            raise ValueError("Only SELECT / WITH queries return a DataFrame")
        return self._query(bound, limit, dtype_backend)

    def fetch_dataframe_chunks(
        self,
        sql: str,
        parameters: list | tuple | dict[str, Any] | None = None,
        *,
        chunksize: int,
        limit: int | None = None,
        dtype_backend: str | None = None,
    ) -> Generator[DataFrame, None, None]:
        """
        Execute a SELECT query and yield the result in typed chunks.

        SAS runs once, as for :meth:`fetch_dataframe`; the result file is
        parsed while it is read, so only one chunk of *chunksize* rows is
        held in memory.  Parameters as for :meth:`fetch_dataframe`.
        """
        return self._job.run_query_chunks(
            self._query_script(sql, parameters, limit),
            chunksize,
            dtype_backend=dtype_backend,
        )

    def _query_script(
        self,
        sql: str,
        parameters: list | tuple | dict[str, Any] | None,
        limit: int | None,
    ) -> str:
        """Bind *parameters*, check that *sql* is a query and write its script."""
        outobs = self._settings.limit if limit is None else limit
        return self._script.bound_query(sql, parameters, self._job.out_dir, outobs)

    def _execute_query(self, sql: str) -> DataFrame:
        """Execute a data-returning query and return the typed DataFrame."""
        return self._query(sql, None, None)

    def _query(self, sql: str, limit: int | None, dtype_backend: str | None) -> DataFrame:
        """
        Run a bound query: one SAS run, or one page of the connection's paged read.

        A query ending in ``LIMIT n OFFSET m`` (what ``GenericTransfer``
        sends per page) is served by :class:`~airflow.providers.sas.driver.job.SASPagedRead`; *limit* then
        does not apply, the read caps the query with the cursor default.
        """
        page = self._script.page(sql)
        if page is not None:
            return self._pages.read(page, dtype_backend=dtype_backend)
        outobs = self._settings.limit if limit is None else limit
        return self._job.run_query(
            self._script.query(sql, self._job.out_dir, outobs=outobs),
            dtype_backend=dtype_backend,
        )

    def _execute_statement(self, sql: str) -> None:
        """Execute a non-data-returning statement (INSERT / UPDATE / …)."""
        self._job.run_statement(self._script.statement(sql))

    def executemany(self, sql: str, seq_of_parameters: list[list | tuple | dict[str, Any]]) -> None:
        """
        Execute *sql* once for each parameter set in *seq_of_parameters*.

        Every parameter set is a SAS run of its own (a script upload and a
        SAS start), so this is meant for a handful of statements, not for
        bulk inserts: no DAG writes to SAS.
        """
        for params in seq_of_parameters:
            self.execute(sql, params)

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row, or ``None`` when exhausted."""
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows."""
        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to *size* rows (defaults to ``arraysize``)."""
        if size is None:
            size = self.arraysize
        rows = self._rows[self._index : self._index + size]
        self._index = min(self._index + size, len(self._rows))
        return rows

    def close(self) -> None:
        """Reset cursor state and release the in-memory result set."""
        self.description = None
        self._rows = []
        self._index = 0
        self.rowcount = -1

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class SASConnection:
    """
    DB-API 2.0 connection that delegates to a remote SAS server via SSH.

    Holds a ``paramiko.SSHClient`` and creates :class:`SASCursor`
    instances that share the same SSH/SFTP session, settings, typing and
    script templates.
    """

    def __init__(
        self,
        ssh_client: paramiko.SSHClient,
        sftp_client: paramiko.SFTPClient,
        settings: SASSettings,
        date_columns: set[str] | None = None,
    ) -> None:
        self._ssh = ssh_client
        self._sftp = sftp_client
        self.settings = settings
        self._metadata = SASMetadata(date_columns=date_columns)
        self._script = SASScript(settings.sas_libraries, settings.sas_compress)
        self._pages = SASPagedRead(
            SASJob(ssh_client, sftp_client, settings, self._metadata),
            self._script,
            settings.limit,
        )

    def cursor(self) -> SASCursor:
        """Return a new :class:`SASCursor` bound to this SSH session."""
        return SASCursor(
            ssh_client=self._ssh,
            sftp_client=self._sftp,
            settings=self.settings,
            metadata=self._metadata,
            script=self._script,
            pages=self._pages,
        )

    @property
    def date_columns(self) -> set[str] | None:
        """
        Column names treated as SAS date floats (days-since-1960).

        Set this after querying ``DICTIONARY.COLUMNS`` to override the
        column-name-suffix heuristic.  Values are upper-cased internally.
        """
        return self._metadata.date_columns

    @date_columns.setter
    def date_columns(self, value: set[str] | None) -> None:
        self._metadata.date_columns = {c.upper() for c in value} if value is not None else None

    def is_active(self) -> bool:
        """
        Whether the SSH session and its SFTP channel are still open.

        Reads the state paramiko keeps, without a round trip to the server:
        the transport turns inactive once paramiko saw the session end (an
        EOF, a failed keepalive), the SFTP channel once it was closed.  A
        session a firewall dropped without a word still looks active until
        the next read or write on it fails.
        """
        transport = self._ssh.get_transport()
        return transport is not None and transport.is_active() and not self._sftp.sock.closed

    def close(self) -> None:
        """Close an open paged read, the SFTP channel and the SSH connection."""
        try:
            self._pages.close()
        finally:
            try:
                self._sftp.close()
            finally:
                self._ssh.close()

    def commit(self) -> None:
        """No-op – SAS does not support transactions."""

    def rollback(self) -> None:
        """No-op – SAS does not support transactions."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class SASAsyncConnection:
    """
    The async twin of :class:`SASConnection`: one asyncssh connection and SFTP session.

    No DB-API cursor (there is no async DB-API): the queries are methods of
    the connection, and each call runs on a :class:`~airflow.providers.sas.driver.job.SASAsyncJob` of its own,
    so several calls may run concurrently on the one SSH connection.
    """

    def __init__(
        self,
        ssh_connection: asyncssh.SSHClientConnection,
        sftp_client: asyncssh.SFTPClient,
        settings: SASSettings,
        date_columns: set[str] | None = None,
    ) -> None:
        self._ssh = ssh_connection
        self._sftp = sftp_client
        self.settings = settings
        self._metadata = SASMetadata(date_columns=date_columns)
        self._script = SASScript(settings.sas_libraries, settings.sas_compress)

    def is_active(self) -> bool:
        """Whether the SSH connection is still open (no round trip)."""
        return not self._ssh.is_closed()

    def _job(self) -> SASAsyncJob:
        return SASAsyncJob(self._ssh, self._sftp, self.settings, self._metadata)

    async def fetch_dataframe(
        self,
        sql: str,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        limit: int | None = None,
        dtype_backend: str | None = None,
    ) -> DataFrame:
        """Execute a SELECT query and return the typed DataFrame (see :meth:`SASCursor.fetch_dataframe`)."""
        job = self._job()
        return await job.run_query(
            self._query_script(job, sql, parameters, limit), dtype_backend=dtype_backend
        )

    async def fetch_dataframe_chunks(
        self,
        sql: str,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        limit: int | None = None,
        dtype_backend: str | None = None,
    ) -> AsyncGenerator[DataFrame, None]:
        """Execute a SELECT query once and yield the result in typed chunks."""
        job = self._job()
        # aclosing: an async generator has no ``yield from``, so closing this
        # one early must close the job's too, or its cleanup waits for the GC.
        async with aclosing(
            job.run_query_chunks(
                self._query_script(job, sql, parameters, limit),
                chunksize,
                dtype_backend=dtype_backend,
            )
        ) as chunks:
            async for chunk in chunks:
                yield chunk

    async def fetch_records(
        self, sql: str, parameters: list | tuple | Mapping[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a SELECT query and return DB-API rows: plain Python values, ``None`` for missing."""
        df = await self.fetch_dataframe(sql, parameters)
        return self._metadata.to_records(df)

    def _query_script(
        self,
        job: SASAsyncJob,
        sql: str,
        parameters: list | tuple | Mapping[str, Any] | None,
        limit: int | None,
    ) -> str:
        outobs = self.settings.limit if limit is None else limit
        return self._script.bound_query(sql, parameters, job.out_dir, outobs)

    async def close(self) -> None:
        """Close the SFTP session and the SSH connection."""
        try:
            self._sftp.exit()
        finally:
            self._ssh.close()
            await self._ssh.wait_closed()
