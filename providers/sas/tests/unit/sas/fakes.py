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
"""Fakes and helpers shared by the SAS provider tests: no SAS server, no network."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock, MagicMock, PropertyMock, create_autospec, patch

import numpy as np
import pandas as pd
import paramiko
import pyarrow as pa

from airflow.providers.sas.driver.connection import SASConnection, SASCursor
from airflow.providers.sas.driver.job import SASJob
from airflow.providers.sas.driver.script import SASSettings
from airflow.providers.sas.hooks.sas import SASHook

if TYPE_CHECKING:
    import asyncssh


DATE32 = pd.ArrowDtype(pa.date32())


EPOCH_DAYS_2020_01_01 = 21915.0  # SAS date value for 2020-01-01


def _raw_frame() -> pd.DataFrame:
    """What ``pandas.read_sas`` hands back for a small SAS table."""
    return pd.DataFrame(
        {
            "NAME": ["Alice   ", "Bob", "     .", ".", ""],
            "AMOUNT": [1.5, np.nan, 3.0, 4.0, 5.0],
            "DATDEP": [EPOCH_DAYS_2020_01_01, np.nan, 0.0, 1.0, 2.0],
            "TS": [0.0, np.nan, 86400.5, 3600.0, 60.0],
            "EMPTY_DATE": [np.nan] * 5,
        }
    )


COLUMN_TYPES = {"NAME": "s", "AMOUNT": "d", "DATDEP": "d", "TS": "d", "EMPTY_DATE": "d"}


COLUMN_FORMATS = {
    "NAME": "",
    "AMOUNT": "COMMA12.2",
    "DATDEP": "DATE9.",
    "TS": "DATETIME21.",
    "EMPTY_DATE": "YYMMDD10.",
}


def _cursor(**settings) -> SASCursor:
    """A cursor on autospecced paramiko clients; *settings* override SASSettings."""
    return SASCursor(
        ssh_client=create_autospec(paramiko.SSHClient, instance=True),
        sftp_client=create_autospec(paramiko.SFTPClient, instance=True),
        settings=SASSettings(
            **{
                "sas_binary": "/opt/sas/sas",
                "sas_options": ("-noterminal",),
                "sas_libraries": {"mylib": "/data/mylib"},
                "sas_compress": "no",
                **settings,
            }
        ),
    )


# ---------------------------------------------------------------------------
# SASCursor: reading the .sas7bdat and the DB-API tuple path
# ---------------------------------------------------------------------------
class _FakeReader:
    """
    Stand-in for ``pandas.io.sas.sas7bdat.SAS7BDATReader``.

    It implements the reader's protocol itself (iterator, context manager that
    closes it) rather than subclassing a pandas base, whose name and module
    differ between the pandas versions the provider supports.
    """

    def __init__(self, frame: pd.DataFrame, chunksize: int | None = None):
        self._frame = frame
        self._chunksize = chunksize
        self._position = 0
        self.column_names = list(frame.columns)
        self.column_formats = [COLUMN_FORMATS[c] for c in frame.columns]
        self.closed = False

    def column_types(self):
        """A method, not a property, exactly like pandas' SAS7BDATReader."""
        return np.asarray([COLUMN_TYPES[c].encode() for c in self.column_names], dtype="S1")

    def read(self, nrows=None):
        """The next *nrows* rows (all of them without), indexed as pandas does."""
        end = len(self._frame) if nrows is None else self._position + nrows
        rows = self._frame.iloc[self._position : end].copy()
        self._position += len(rows)
        return rows

    def __iter__(self):
        return self

    def __next__(self):
        rows = self.read(self._chunksize)
        if rows.empty:
            raise StopIteration
        return rows

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# SASHook: DataFrame API
# ---------------------------------------------------------------------------
def _hook_with_frame(frame: pd.DataFrame, **hook_kwargs) -> tuple[SASHook, SASCursor]:
    hook = SASHook(sas_conn_id="sas.test", **hook_kwargs)
    cursor = create_autospec(SASCursor, instance=True)
    cursor.__enter__.return_value = cursor
    cursor.fetch_dataframe.return_value = frame
    conn = create_autospec(SASConnection, instance=True)
    conn.cursor.return_value = cursor
    hook.get_conn = create_autospec(hook.get_conn, return_value=conn)  # type: ignore[method-assign]
    return hook, cursor


# ---------------------------------------------------------------------------
# SASHook.get_conn: parameter precedence (constructor > extra > default)
# ---------------------------------------------------------------------------
def _sas_connection(extra: dict | None = None):
    import json

    from airflow.models import Connection

    return Connection(
        conn_id="sas.test",
        conn_type="sas",
        host="sas.example",
        login="user",
        password="secret",
        port=22,
        extra=json.dumps(extra or {}),
    )


def _connect(hook: SASHook):
    """Run get_conn() with SSHHook replaced; return (SASConnection, SSHHook mock)."""
    with patch("airflow.providers.ssh.hooks.ssh.SSHHook") as ssh_hook_cls:
        conn = hook.get_conn()
    return conn, ssh_hook_cls


# GitHub's published Ed25519 host key: any well-formed public key will do.
_ED25519_HOST_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"


def _live_connection(*, transport_active=True, has_transport=True, sftp_closed=False):
    ssh = create_autospec(paramiko.SSHClient, instance=True)
    transport = create_autospec(paramiko.Transport, instance=True)
    transport.is_active.return_value = transport_active
    ssh.get_transport.return_value = transport if has_transport else None
    sftp = create_autospec(paramiko.SFTPClient, instance=True)
    # ``sock`` is set in SFTPClient.__init__, so autospec cannot see it.
    sftp.sock = create_autospec(paramiko.Channel, instance=True)
    sftp.sock.closed = sftp_closed
    return SASConnection(ssh, sftp, SASSettings())


# ---------------------------------------------------------------------------
# SASCursor: one shell line per run, cleanup and timeout
# ---------------------------------------------------------------------------
def _fake_ssh_run(cursor: SASCursor, rc: int = 0, stdout: bytes = b"", stderr: bytes = b""):
    def channel_file(output: bytes):
        file = create_autospec(paramiko.ChannelFile, instance=True)
        # ``channel`` is set in ChannelFile.__init__, so autospec cannot see it.
        file.channel = create_autospec(paramiko.Channel, instance=True)
        file.read.return_value = output
        return file

    out, err = channel_file(stdout), channel_file(stderr)
    out.channel.recv_exit_status.return_value = rc
    stdin = channel_file(b"")
    cast("MagicMock", cursor._job._ssh).exec_command.return_value = (stdin, out, err)
    return out


def _paged_connection(chunks: list[list[float]], **settings):
    """A connection whose SAS runs yield *chunks* (one frame per list), with a run log."""
    conn = SASConnection(
        create_autospec(paramiko.SSHClient, instance=True),
        create_autospec(paramiko.SFTPClient, instance=True),
        SASSettings(
            sas_binary="/opt/sas/sas",
            sas_options=("-noterminal",),
            sas_libraries={"mylib": "/data/mylib"},
            sas_compress="no",
            **settings,
        ),
    )
    runs: list[str] = []

    def run_script(self, script, with_output=False):
        runs.append(script)

    def read_chunks(self, path, chunksize, dtype_backend=None):
        for values in chunks:
            yield pd.DataFrame({"A": values})

    patches = (
        patch.object(SASJob, "_run_script", autospec=True, side_effect=run_script),
        patch.object(SASJob, "_read_sas7bdat_chunks", autospec=True, side_effect=read_chunks),
    )
    return conn, runs, patches


def _records(conn: SASConnection, sql: str) -> list[tuple]:
    """What ``DbApiHook.get_records`` does per page: a new cursor, closed after the fetch."""
    from contextlib import closing

    with closing(conn.cursor()) as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


class _FakeSFTPFile:
    def __init__(self, sftp: _FakeSFTP, path: str, mode: str):
        self._sftp, self._path, self._mode = sftp, path, mode

    async def __aenter__(self):
        if "r" in self._mode and self._path not in self._sftp.files:
            raise OSError(f"no such file: {self._path}")
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def write(self, data):
        self._sftp.files[self._path] = data

    async def read(self):
        return self._sftp.files[self._path]


class _FakeSFTP:
    """Stand-in for asyncssh.SFTPClient: ``open`` is an async context manager there."""

    def __init__(self, files: dict[str, object] | None = None):
        self.files = dict(files or {})
        self.removed: list[str] = []
        self.removed_directories: list[str] = []
        self.exited = False

    def open(self, path, mode="r"):
        return _FakeSFTPFile(self, path, mode)

    async def remove(self, path):
        self.removed.append(path)

    async def rmdir(self, path):
        self.removed_directories.append(path)

    def exit(self):
        self.exited = True


def _async_ssh(returncode: int = 0, stdout: str = "", stderr: str = ""):
    import asyncssh

    ssh = create_autospec(asyncssh.SSHClientConnection, instance=True)
    ssh.run.return_value = asyncssh.SSHCompletedProcess(
        env={},
        command="",
        subsystem=None,
        exit_status=returncode,
        exit_signal=None,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )
    ssh.is_closed.return_value = False
    return ssh


def _async_connection(sftp: _FakeSFTP | None = None, **ssh_result):
    from airflow.providers.sas.driver.connection import SASAsyncConnection

    sftp = sftp or _FakeSFTP()
    ssh = _async_ssh(**ssh_result)
    settings = SASSettings(sas_libraries={"mylib": "/data/mylib"}, sas_compress="no", timeout=60)
    return SASAsyncConnection(ssh, cast("asyncssh.SFTPClient", sftp), settings), ssh, sftp


def _result_on_any_path(sftp: _FakeSFTP):
    """Serve the same bytes for every .sas7bdat the job asks for."""
    real_open = sftp.open

    def open_(path, mode="r"):
        if path.endswith(".sas7bdat"):
            sftp.files.setdefault(path, b"sas7bdat bytes")
        return real_open(path, mode)

    sftp.open = open_  # type: ignore[method-assign]


def _patch_async_lookup(extra: dict, ssh_connection):
    """Patch the async connection lookup and SSHHookAsync; any sync SDK path fails the test."""
    import json

    from airflow.models import Connection

    conn = Connection(
        conn_id="sas.test",
        conn_type="sas",
        host="sas.example",
        login="user",
        password="secret",
        extra=json.dumps(extra),
    )
    from airflow.providers.ssh.hooks.ssh import SSHHookAsync

    return (
        patch(
            "airflow.providers.common.compat.connection.get_async_connection",
            AsyncMock(return_value=conn),
        ),
        patch.object(
            Connection,
            "extra_dejson",
            new_callable=PropertyMock,
            side_effect=AssertionError("sync extra_dejson"),
        ),
        patch.object(Connection, "aextra_dejson", AsyncMock(return_value=extra), create=True),
        patch.object(
            SASHook,
            "connection",
            new_callable=PropertyMock,
            side_effect=AssertionError("sync self.connection"),
        ),
        patch.object(SSHHookAsync, "get_conn", AsyncMock(return_value=ssh_connection)),
        patch.object(SSHHookAsync, "__init__", autospec=True, return_value=None),
    )
