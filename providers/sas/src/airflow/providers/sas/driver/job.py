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
"""One SAS run over SSH: upload the program, run SAS, download the result, clean up, and paging from one run."""

from __future__ import annotations

import asyncio
import io
import logging
import shlex
import time
import uuid
from contextlib import suppress
from typing import TYPE_CHECKING, cast

from airflow.providers.sas.driver.script import _RESULT_TABLE, SASPage, SASScript, SASScriptError, SASSettings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    import asyncssh
    import paramiko
    from pandas import DataFrame

    from airflow.providers.sas.driver.metadata import SASMetadata


log = logging.getLogger(__name__)

# SAS return codes: 0 success, 1 warnings, from 2 on errors.  The shell line
# and _run_script both test it.
_SAS_ERROR_RETURN_CODE = 2

# Read requests a chunked download keeps in flight (32 KiB each in paramiko:
# 2 MiB).  Unbounded, ``prefetch`` asks for the whole file at once, and a
# chunked read that is paused (a paged read between two pages) leaves the
# responses queued on the SFTP channel the connection shares: the next SFTP
# call drains them all first, which can take minutes behind a large result.
_CHUNK_PREFETCH_REQUESTS = 64


class SASPagedRead:
    """
    The pages of one query, served from one SAS run.

    ``GenericTransfer`` pages a source with ``SELECT ... LIMIT n OFFSET m``
    and one ``get_records`` per page.  Run literally, every page would be a
    SAS run that reads the table up to the page: on a large table each
    page then costs seconds, where a streamed run costs a fraction of that.
    Instead the first page starts one run of the query without the clause
    and reads its result in chunks (:meth:`SASJob.run_query_chunks`); each
    page that follows in sequence is served from that stream, with the
    chunks cut to the page size.  A page out of sequence, or of another
    query or size, closes the read and starts a new run that skips to the
    offset.  The run is released when a page comes up short (the end of the
    result) or on :meth:`close`; until then the result file stays on the
    server.

    One instance per :class:`~airflow.providers.sas.driver.connection.SASConnection`, so the read outlives the cursor
    of each page: ``DbApiHook.get_records`` closes its cursor after every
    call and keeps the connection.
    """

    def __init__(self, job: SASJob, script: SASScript, limit: int | None) -> None:
        self._job = job
        self._script = script
        self._limit = limit
        self._page: SASPage | None = None
        self._dtype_backend: str | None = None
        self._chunks: Generator[DataFrame, None, None] | None = None
        self._rest: DataFrame | None = None
        self._columns: DataFrame | None = None

    def read(self, page: SASPage, dtype_backend: str | None = None) -> DataFrame:
        """Return the rows of *page*, from the open run when it follows the last page."""
        if not self._follows(page, dtype_backend):
            self.close()
            self._start(page, dtype_backend)
        df = self._rows(page.size)
        self._page = page
        if len(df) < page.size:
            # The end of the result: release the run and its files now rather
            # than on the next page or on close().
            self.close()
        return df

    def close(self) -> None:
        """Close the open run, if any; its remote files are removed."""
        if self._chunks is not None:
            self._chunks.close()
        self._chunks = self._rest = self._columns = self._page = None

    def _follows(self, page: SASPage, dtype_backend: str | None) -> bool:
        return (
            self._chunks is not None
            and self._page is not None
            and page.sql == self._page.sql
            and page.size == self._page.size
            and page.offset == self._page.end
            and dtype_backend == self._dtype_backend
        )

    def _start(self, page: SASPage, dtype_backend: str | None) -> None:
        script = self._script.query(page.sql, self._job.out_dir, outobs=self._limit)
        self._chunks = self._job.run_query_chunks(script, page.size, dtype_backend=dtype_backend)
        self._dtype_backend = dtype_backend
        self._skip(page.offset)

    def _take(self) -> DataFrame | None:
        """Return the next piece of the stream: the rows left over from a cut, else a chunk."""
        if self._rest is not None:
            part, self._rest = self._rest, None
            return part
        if self._chunks is None:
            return None
        chunk = next(self._chunks, None)
        if chunk is not None and self._columns is None:
            self._columns = chunk.iloc[:0]
        return chunk

    def _skip(self, rows: int) -> None:
        while rows:
            part = self._take()
            if part is None:
                return
            if len(part) > rows:
                self._rest = part.iloc[rows:].reset_index(drop=True)
                return
            rows -= len(part)

    def _rows(self, count: int) -> DataFrame:
        """Return the next *count* rows of the stream, fewer at its end, typed like the chunks."""
        from pandas import DataFrame, concat

        parts: list[DataFrame] = []
        have = 0
        while have < count:
            part = self._take()
            if part is None:
                break
            parts.append(part)
            have += len(part)
        if not parts:
            return DataFrame() if self._columns is None else self._columns.copy()
        frame = parts[0] if len(parts) == 1 else concat(parts, ignore_index=True)
        if len(frame) > count:
            self._rest = frame.iloc[count:].reset_index(drop=True)
            frame = frame.iloc[:count].reset_index(drop=True)
        return frame


class _SASJobFiles:
    """
    The remote files and the shell line of one SAS job, for :class:`SASJob` and :class:`SASAsyncJob`.

    A unique prefix under ``sas_temp_dir`` (one per job, reused by its runs)
    names the script, the log and the output directory; the shell line runs
    SAS on them.  The two jobs differ only in how they move bytes (paramiko
    or asyncssh), so what they must agree on is written here once.
    """

    def __init__(self, settings: SASSettings) -> None:
        self._settings = settings
        prefix = f"{settings.sas_temp_dir}/airflow_sas_{uuid.uuid4().hex[:12]}"
        self.script_path = f"{prefix}.sas"
        self.log_path = f"{prefix}.log"
        self.out_dir = f"{prefix}_out"
        self.result_path = f"{self.out_dir}/{_RESULT_TABLE}.sas7bdat"

        # The SAS kernel refuses to start when it cannot create its WORK
        # library, and the site default may point at a tree the Airflow
        # account cannot write to.  Anchor WORK on the temp dir we already
        # know is writable, unless the caller supplied its own -work option.
        options = list(settings.sas_options)
        has_work_option = any(option.lstrip("-").lower().startswith("work") for option in options)
        if not has_work_option:
            work_dir = settings.sas_work_dir or settings.sas_temp_dir
            options.append(f"-work {shlex.quote(work_dir)}")
        # Paths are shell-quoted; the options are passed as written, since
        # each one is an option and its value (``-linesize 90``).
        self.command = (
            f"{shlex.quote(settings.sas_binary)} {' '.join(options)} "
            f"-sysin {shlex.quote(self.script_path)} "
            f"-log {shlex.quote(self.log_path)}"
        )

    def shell_command(self, with_output: bool) -> str:
        """
        Build the remote shell line for one SAS run.

        The line creates the output directory when a result is expected,
        runs SAS, and removes the script and the log itself when SAS did
        not fail (return code < 2).  That saves the SFTP round trips a
        ``mkdir`` and the removal of those two files would cost; on failure
        the log stays behind so the error can quote it.
        """
        setup = f"mkdir -p {shlex.quote(self.out_dir)} && " if with_output else ""
        script, log = shlex.quote(self.script_path), shlex.quote(self.log_path)
        return (
            f"{setup}{self.command}; rc=$?; "
            f"[ $rc -lt {_SAS_ERROR_RETURN_CODE} ] && rm -f {script} {log}; exit $rc"
        )

    def removals(self, failed: bool, with_output: bool) -> tuple[list[str], list[str]]:
        """
        Return the remote files and directories the shell line left behind.

        The result file and its directory after a query; after a failure the
        script and the log as well (the shell line keeps them so the error
        can be reported).
        """
        files = [self.script_path, self.log_path] if failed else []
        if with_output:
            return [*files, self.result_path], [self.out_dir]
        return files, []

    def error_message(
        self,
        return_code: int,
        log: str,
        stderr: str,
        stdout: str = "",
        script: str = "",
    ) -> str:
        """Build a human-readable error message from SAS execution output."""
        parts = [f"SAS returned code {return_code}."]
        if return_code == 127:
            parts.append(
                f"The SAS binary was not found at '{self._settings.sas_binary}'. "
                "Set the correct path via the 'sas_binary' key in the connection extra."
            )
        parts.append(f"command: {self.command}")
        if stderr:
            parts.append(f"stderr: {stderr}")
        if stdout:
            parts.append(f"stdout: {stdout}")
        if script:
            parts.append(f"SAS script:\n{script}")
        if log:
            parts.append(f"SAS log:\n{log}")
        return "\n".join(parts)

    def timeout_message(self, script: str) -> str:
        """Return the error message when SAS did not finish within the settings' ``timeout``."""
        return (
            f"SAS did not finish within {self._settings.timeout}s "
            f"(the process keeps running on the server).\n"
            f"command: {self.command}\nSAS script:\n{script}"
        )


class SASJob:
    """
    Run SAS scripts on the server over paramiko, for one cursor.

    Owns the I/O of a SAS run on its :class:`_SASJobFiles`: the upload, the
    run with its timeout and error report, the download and typing of the
    result, and the cleanup.  It runs scripts :class:`~airflow.providers.sas.driver.script.SASScript` wrote and
    never looks at SQL.
    """

    def __init__(
        self,
        ssh_client: paramiko.SSHClient,
        sftp_client: paramiko.SFTPClient,
        settings: SASSettings,
        metadata: SASMetadata,
    ) -> None:
        self._ssh = ssh_client
        self._sftp = sftp_client
        self._settings = settings
        self._metadata = metadata
        self._files = _SASJobFiles(settings)

    @property
    def out_dir(self) -> str:
        """Remote directory a query script stores its result in."""
        return self._files.out_dir

    def run_query(self, sas_script: str, dtype_backend: str | None = None) -> DataFrame:
        """Run a query script and return its result as a typed DataFrame."""
        failed = True
        try:
            self._run_script(sas_script, with_output=True)
            df = self._read_sas7bdat(self._files.result_path, dtype_backend=dtype_backend)
            failed = False
            return df
        finally:
            self._cleanup(failed, with_output=True)

    def run_query_chunks(
        self, sas_script: str, chunksize: int, dtype_backend: str | None = None
    ) -> Generator[DataFrame, None, None]:
        """
        Run a query script once and yield its result in typed chunks.

        The result is parsed while it is read from the server, one chunk at
        a time.  The remote files are removed when the last chunk is read,
        and also when the caller stops early (the generator is closed).
        """
        failed = True
        try:
            self._run_script(sas_script, with_output=True)
            yield from self._read_sas7bdat_chunks(
                self._files.result_path, chunksize, dtype_backend=dtype_backend
            )
            failed = False
        finally:
            self._cleanup(failed, with_output=True)

    def run_statement(self, sas_script: str) -> None:
        """Run a script that produces no result (INSERT / UPDATE / …)."""
        failed = True
        try:
            self._run_script(sas_script)
            failed = False
        finally:
            self._cleanup(failed, with_output=False)

    def _cleanup(self, failed: bool, with_output: bool) -> None:
        """Remove the remote files :meth:`_SASJobFiles.removals` names, ignoring errors."""
        files, directories = self._files.removals(failed, with_output)
        for remove, paths in (
            (self._sftp.remove, files),
            (self._sftp.rmdir, directories),
        ):
            for path in paths:
                with suppress(OSError):
                    remove(path)

    def _read_remote_log(self, log_path: str) -> str:
        """
        Read and return a remote log file as a string.

        The log is decoded with the connection ``encoding`` (SAS writes
        it in the session encoding, not necessarily UTF-8) and never
        raises: a decoding or transfer problem must not mask the
        original SAS failure.
        """
        try:
            with self._sftp.file(log_path, "rb") as file:
                raw = file.read()
        except OSError as error:
            return f"<no SAS log at '{log_path}': {error}>"
        if isinstance(raw, bytes):
            return raw.decode(self._settings.encoding, errors="replace")
        return raw

    def _read_sas7bdat(self, path: str, dtype_backend: str | None = None) -> DataFrame:
        """
        Download a remote ``.sas7bdat`` file and parse it into a typed DataFrame.

        ``pandas.read_sas()`` accepts a file-like object, so we read
        directly from the SFTP handle — no temp file or intermediate
        copy needed.  The ``.prefetch()`` call tells paramiko to fetch
        all file blocks in parallel for faster sequential reads.  Parsing
        and typing are :meth:`~airflow.providers.sas.driver.metadata.SASMetadata.parse_sas7bdat`.
        """
        started = time.perf_counter()
        size = self._sftp.stat(path).st_size
        with self._sftp.file(path, "rb") as file:
            file.prefetch(size)
            df = self._metadata.parse_sas7bdat(file, self._settings.encoding, dtype_backend=dtype_backend)
        log.info(
            "Downloaded, parsed and typed %s bytes (%d rows) in %.2fs",
            f"{size:,}",
            len(df),
            time.perf_counter() - started,
        )
        return df

    def _read_sas7bdat_chunks(
        self, path: str, chunksize: int, dtype_backend: str | None = None
    ) -> Generator[DataFrame, None, None]:
        """
        Download a remote ``.sas7bdat`` file and yield it in typed chunks.

        The prefetch is bounded (``_CHUNK_PREFETCH_REQUESTS``): the generator
        may be paused between chunks, and what it has not consumed must not
        sit on the shared SFTP channel.
        """
        started = time.perf_counter()
        size = self._sftp.stat(path).st_size
        rows = 0
        with self._sftp.file(path, "rb") as file:
            file.prefetch(size, max_concurrent_requests=_CHUNK_PREFETCH_REQUESTS)
            for chunk in self._metadata.parse_sas7bdat_chunks(
                file, self._settings.encoding, chunksize, dtype_backend=dtype_backend
            ):
                rows += len(chunk)
                yield chunk
        log.info(
            "Downloaded, parsed and typed %s bytes (%d rows) in chunks of %d in %.2fs",
            f"{size:,}",
            rows,
            chunksize,
            time.perf_counter() - started,
        )

    def _run_script(self, sas_script: str, with_output: bool = False) -> None:
        """
        Upload *sas_script*, run it through SAS, and raise on failure.

        SAS return codes are ``0`` (success), ``1`` (warnings) and
        ``>= 2`` (errors).  On failure the SAS log, stdout and stderr
        are collected together with the generated script so that the
        exception is actionable without shelling into the SAS server.

        When the settings have a ``timeout`` the SSH channel stops waiting
        after that many seconds and a :class:`~airflow.providers.sas.driver.script.SASScriptError` is raised;
        the SAS process itself keeps running on the server.
        """
        with self._sftp.file(self._files.script_path, "w") as file:
            file.write(sas_script)

        started = time.perf_counter()
        _stdin, stdout, stderr = self._ssh.exec_command(
            self._files.shell_command(with_output), timeout=self._settings.timeout
        )
        try:
            # Drain both streams *before* waiting for the exit status: a
            # chatty SAS run can fill the channel window and deadlock.
            stdout_output = stdout.read().decode("utf-8", errors="replace").strip()
            stderr_output = stderr.read().decode("utf-8", errors="replace").strip()
            return_code = stdout.channel.recv_exit_status()
        except TimeoutError as error:
            raise SASScriptError(self._files.timeout_message(sas_script)) from error
        log.info(
            "SAS run finished in %.2fs (return code %d)",
            time.perf_counter() - started,
            return_code,
        )

        if return_code >= _SAS_ERROR_RETURN_CODE:
            raise SASScriptError(
                self._files.error_message(
                    return_code,
                    self._read_remote_log(self._files.log_path),
                    stderr_output,
                    stdout_output,
                    sas_script,
                )
            )


class SASAsyncJob:
    """
    Run SAS scripts on the server over asyncssh: the async twin of :class:`SASJob`.

    Same :class:`_SASJobFiles`, same steps (upload, run with its timeout and
    error report, download, typing, cleanup); the I/O awaits instead of
    blocking.  pandas has no async parser, so the result is downloaded into
    memory (compressed, a few MiB) and parsed in a worker thread, which keeps
    the event loop free for the other items of an ``.iterate()``.
    :class:`~airflow.providers.sas.driver.connection.SASAsyncConnection` makes one job per call, so calls on the same
    connection may run at the same time without sharing remote files.
    """

    def __init__(
        self,
        ssh_connection: asyncssh.SSHClientConnection,
        sftp_client: asyncssh.SFTPClient,
        settings: SASSettings,
        metadata: SASMetadata,
    ) -> None:
        self._ssh = ssh_connection
        self._sftp = sftp_client
        self._settings = settings
        self._metadata = metadata
        self._files = _SASJobFiles(settings)

    @property
    def out_dir(self) -> str:
        """Remote directory a query script stores its result in."""
        return self._files.out_dir

    async def run_query(self, sas_script: str, dtype_backend: str | None = None) -> DataFrame:
        """Run a query script and return its result as a typed DataFrame."""
        failed = True
        try:
            await self._run_script(sas_script, with_output=True)
            data = await self._download(self._files.result_path)
            df = await asyncio.to_thread(
                self._metadata.parse_sas7bdat,
                io.BytesIO(data),
                self._settings.encoding,
                dtype_backend=dtype_backend,
            )
            failed = False
            return df
        finally:
            await self._cleanup(failed, with_output=True)

    async def run_query_chunks(
        self, sas_script: str, chunksize: int, dtype_backend: str | None = None
    ) -> AsyncGenerator[DataFrame, None]:
        """
        Run a query script once and yield its result in typed chunks.

        Each chunk is parsed in a worker thread; the remote files are removed
        when the last chunk is read, and also when the caller stops early.
        """
        failed = True
        try:
            await self._run_script(sas_script, with_output=True)
            data = await self._download(self._files.result_path)
            chunks = self._metadata.parse_sas7bdat_chunks(
                io.BytesIO(data),
                self._settings.encoding,
                chunksize,
                dtype_backend=dtype_backend,
            )
            try:
                while (chunk := await asyncio.to_thread(next, chunks, None)) is not None:
                    yield chunk
            finally:
                chunks.close()
            failed = False
        finally:
            await self._cleanup(failed, with_output=True)

    async def run_statement(self, sas_script: str) -> None:
        """Run a script that produces no result (INSERT / UPDATE / …)."""
        failed = True
        try:
            await self._run_script(sas_script)
            failed = False
        finally:
            await self._cleanup(failed, with_output=False)

    async def _run_script(self, sas_script: str, with_output: bool = False) -> None:
        """Upload *sas_script*, run it through SAS, and raise on failure (see :meth:`SASJob._run_script`)."""
        async with self._sftp.open(self._files.script_path, "w") as file:
            await file.write(sas_script)

        started = time.perf_counter()
        try:
            # run() collects stdout and stderr while it waits, so a chatty SAS
            # run cannot fill the channel window as with paramiko.
            result = await self._ssh.run(
                self._files.shell_command(with_output),
                check=False,
                timeout=self._settings.timeout,
                encoding="utf-8",
                errors="replace",
            )
        except TimeoutError as error:
            raise SASScriptError(self._files.timeout_message(sas_script)) from error
        return_code = result.returncode
        log.info(
            "SAS run finished in %.2fs (return code %s)",
            time.perf_counter() - started,
            return_code,
        )
        if return_code is None or return_code >= _SAS_ERROR_RETURN_CODE:
            raise SASScriptError(
                self._files.error_message(
                    -1 if return_code is None else return_code,
                    await self._read_remote_log(self._files.log_path),
                    # run() decodes the output: it was given an encoding.
                    cast("str", result.stderr or "").strip(),
                    cast("str", result.stdout or "").strip(),
                    sas_script,
                )
            )

    async def _download(self, path: str) -> bytes:
        """Read a remote file completely; asyncssh reads it with parallel requests."""
        started = time.perf_counter()
        async with self._sftp.open(path, "rb") as file:
            data = cast("bytes", await file.read())
        log.info(
            "Downloaded %s bytes in %.2fs",
            f"{len(data):,}",
            time.perf_counter() - started,
        )
        return data

    async def _read_remote_log(self, log_path: str) -> str:
        """Read a remote log as text; never raises (see :meth:`SASJob._read_remote_log`)."""
        import asyncssh

        try:
            async with self._sftp.open(log_path, "rb") as file:
                raw = cast("bytes", await file.read())
        except (OSError, asyncssh.SFTPError) as error:
            return f"<no SAS log at '{log_path}': {error}>"
        return raw.decode(self._settings.encoding, errors="replace")

    async def _cleanup(self, failed: bool, with_output: bool) -> None:
        """Remove the remote files :meth:`_SASJobFiles.removals` names, ignoring errors."""
        import asyncssh

        files, directories = self._files.removals(failed, with_output)
        for remove, paths in (
            (self._sftp.remove, files),
            (self._sftp.rmdir, directories),
        ):
            for path in paths:
                with suppress(OSError, asyncssh.SFTPError):
                    await remove(path)
