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
"""
End-to-end tests of the SAS hook against an SSH/SFTP server in this process.

The server (asyncssh) runs on 127.0.0.1 in a thread of its own, executes the
shell line the hook sends, and serves the local file system over SFTP. A stub
``sas`` executable stands in for SAS: it reads the program the hook uploaded
and copies the matching ``.sas7bdat`` from ``data/`` (written by SAS 9.4 with
dummy data, see ``data/fixtures.sas``) to where the program asked for its
result. Everything else is the real code path: SSH exec, SFTP upload and
download, the shell line, cleanup, parsing, typing, chunking and paging.
No SAS server and no network are needed.
"""

from __future__ import annotations

import asyncio
import datetime
import io
import json
import shlex
import stat
import sys
import threading
from pathlib import Path

import asyncssh
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from packaging.version import Version

from airflow.providers.sas.driver.script import SASScriptError
from airflow.providers.sas.hooks.sas import SASHook

# The hook itself needs asyncssh 2.15 (``is_closed()``) and talks to OpenSSH servers. The SFTP
# server of the asyncssh releases before 2.18, which these tests run in-process, ends paramiko
# 5's SFTP session with an EOF, so the tests only run from 2.18 on.
pytestmark = pytest.mark.skipif(
    Version(asyncssh.__version__) < Version("2.18.0"),
    reason="the in-process SFTP server of asyncssh < 2.18 ends paramiko's SFTP session",
)

DATA_DIR = Path(__file__).parent / "data"
STUB = Path(__file__).parent / "sas_stub.py"
CONN_ID = "sas_end_to_end"
PASSWORD = "secret"


class _PasswordServer(asyncssh.SSHServer):
    def begin_auth(self, username: str) -> bool:
        return True

    def password_auth_supported(self) -> bool:
        return True

    def validate_password(self, username: str, password: str) -> bool:
        return password == PASSWORD


async def _run_shell(process: asyncssh.SSHServerProcess) -> None:
    if process.command is None:
        # The hook only runs commands; an interactive shell is not something it asks for.
        process.exit(1)
        return
    shell = await asyncio.create_subprocess_shell(
        process.command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await shell.communicate()
    process.stdout.write(stdout.decode(errors="replace"))
    process.stderr.write(stderr.decode(errors="replace"))
    process.exit(shell.returncode or 0)


async def _listen() -> asyncssh.SSHAcceptor:
    # asyncssh.listen() returns an awaitable, not a coroutine run_coroutine_threadsafe accepts.
    return await asyncssh.listen(
        "127.0.0.1",
        0,
        server_factory=_PasswordServer,
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        process_factory=_run_shell,
        sftp_factory=True,
    )


@pytest.fixture
def ssh_port():
    """Run an SSH/SFTP server on 127.0.0.1 in a thread of its own; yield its port."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    listener = asyncio.run_coroutine_threadsafe(_listen(), loop).result(timeout=30)
    try:
        yield listener.get_port()
    finally:
        # close() shuts the listening socket at once, and every test closes its hooks.
        # wait_closed() is not awaited: on Python 3.12 asyncio.Server can call _wakeup()
        # twice while closing, and the second call iterates waiters already set to None.
        loop.call_soon_threadsafe(listener.close)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=30)
        loop.close()


@pytest.fixture
def server_dir(tmp_path: Path) -> Path:
    """The server's side: the stub SAS binary, its record of runs, the work directory."""
    (tmp_path / "work").mkdir()
    stub = tmp_path / "sas"
    stub.write_text(
        f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(STUB))} "
        f'{shlex.quote(str(DATA_DIR))} {shlex.quote(str(tmp_path / "runs.txt"))} "$@"\n'
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    (tmp_path / "runs.txt").touch()
    return tmp_path


@pytest.fixture
def sas_connection(monkeypatch, ssh_port: int, server_dir: Path) -> str:
    """Point the SAS connection at the in-process server; return its id."""
    # Password authentication only: no key from an ssh-agent or ~/.ssh of the machine running
    # the tests (paramiko and asyncssh offer those first, and the test server only takes passwords).
    monkeypatch.delenv("SSH_AUTH_SOCK", raising=False)
    monkeypatch.setenv("HOME", str(server_dir))
    monkeypatch.setenv(
        f"AIRFLOW_CONN_{CONN_ID.upper()}",
        json.dumps(
            {
                "conn_type": "sas",
                "host": "127.0.0.1",
                "port": ssh_port,
                "login": "airflow",
                "password": PASSWORD,
                "extra": {
                    "sas_binary": str(server_dir / "sas"),
                    "sas_temp_dir": str(server_dir / "work"),
                    "no_host_key_check": "true",
                },
            }
        ),
    )
    return CONN_ID


@pytest.fixture
def make_hook(sas_connection: str):
    """Build hooks on the test connection and close their SSH sessions afterwards."""
    hooks: list[SASHook] = []

    def make(**kwargs) -> SASHook:
        hooks.append(SASHook(sas_conn_id=sas_connection, **kwargs))
        return hooks[-1]

    yield make
    for hook in hooks:
        hook.close()


def _sas_runs(server_dir: Path) -> int:
    return (server_dir / "runs.txt").read_text().count("<end of run>")


def _leftovers(server_dir: Path) -> list[str]:
    return sorted(path.name for path in (server_dir / "work").iterdir())


def test_get_df_types_every_column_kind_from_the_sas_metadata(make_hook, server_dir):
    hook = make_hook()

    df = hook.get_df("select * from fx.all_types", dtype_backend="pyarrow")

    assert {column: str(dtype) for column, dtype in df.dtypes.items()} == {
        "id": "double[pyarrow]",
        "small_int": "double[pyarrow]",
        "amount": "double[pyarrow]",
        "c_short": "string[pyarrow]",
        "c_long": "string[pyarrow]",
        "c_padded": "string[pyarrow]",
        "c_quotes": "string[pyarrow]",
        "c_latin": "string[pyarrow]",
        "d_date": "date32[day][pyarrow]",
        "d_ymd": "date32[day][pyarrow]",
        "dt_sec": "timestamp[us][pyarrow]",
        "dt_us": "timestamp[us][pyarrow]",
        "dt_iso": "timestamp[us][pyarrow]",
        "t_time": "double[pyarrow]",
    }
    first, missing = df.iloc[0], df.iloc[1]
    assert first["c_latin"] == "éàüß€"
    assert first["c_quotes"] == 'single \' and "double"'
    assert first["c_padded"] == "  pad", "leading blanks are data, trailing padding is not"
    assert first["d_date"] == datetime.date(1960, 1, 1)
    assert first["d_ymd"] == datetime.date(2099, 12, 31)
    assert first["dt_us"] == pd.Timestamp("2024-03-15 13:45:30.123456")
    assert first["t_time"] == 86399.0, "a TIME format stays seconds since midnight"
    assert missing.drop("id").isna().all(), "missing and special missing values (.A, .Z, ._) read as missing"


@pytest.mark.parametrize("sas_compress", ["no", "char", "binary", "yes"])
def test_every_compression_of_the_result_reads_the_same(make_hook, sas_compress):
    expected = make_hook(sas_compress="no").get_df("select * from fx.all_types")

    df = make_hook(sas_compress=sas_compress).get_df("select * from fx.all_types")

    pd.testing.assert_frame_equal(df, expected)


def test_get_df_by_chunks_streams_one_sas_run_with_the_same_schema(make_hook, server_dir):
    hook = make_hook()

    chunks = list(
        hook.get_df_by_chunks("select * from fx.many_rows", chunksize=3000, dtype_backend="pyarrow")
    )

    assert [len(chunk) for chunk in chunks] == [3000, 3000, 3000, 1000]
    assert pd.concat(chunks)["id"].tolist() == [float(i) for i in range(1, 10_001)]
    schemas = {pa.Schema.from_pandas(chunk, preserve_index=False) for chunk in chunks}
    assert len(schemas) == 1, "every chunk has the dtypes of the whole result"
    assert _sas_runs(server_dir) == 1
    assert _leftovers(server_dir) == [], "the result and its directory are removed"


def test_limit_offset_pages_are_served_from_one_sas_run(make_hook, server_dir):
    hook = make_hook()

    rows = []
    offset = 0
    while page := hook.get_records(f"SELECT * FROM fx.many_rows LIMIT 2500 OFFSET {offset}"):
        rows.extend(page)
        offset += len(page)

    assert len(rows) == 10_000
    assert [row[0] for row in rows] == [float(i) for i in range(1, 10_001)]
    assert _sas_runs(server_dir) == 1, "GenericTransfer's pages do not start a SAS run each"


def test_get_records_returns_plain_python_with_none_for_missing(make_hook):
    rows = make_hook().get_records("select * from fx.all_types")

    assert len(rows) == 4
    assert rows[0][3] == "abc"
    assert rows[0][8] == datetime.date(1960, 1, 1)
    assert all(value is None for value in rows[1][1:])


def test_parquet_from_the_typed_frame_keeps_the_sas_types(make_hook):
    df = make_hook().get_df("select * from fx.all_types", dtype_backend="pyarrow")
    buffer = io.BytesIO()

    df.to_parquet(buffer, index=False)

    schema = pq.read_schema(io.BytesIO(buffer.getvalue()))
    assert schema.field("d_date").type == pa.date32()
    assert schema.field("dt_us").type == pa.timestamp("us")
    assert schema.field("c_short").type in (pa.string(), pa.large_string())


def test_a_failed_run_reports_the_sas_log_and_cleans_up(make_hook, server_dir):
    hook = make_hook()

    with pytest.raises(SASScriptError, match=r"FX\.MISSING\.DATA does not exist"):
        hook.get_df("select * from fx.missing")

    assert _leftovers(server_dir) == []


@pytest.mark.asyncio
async def test_aget_df_reads_what_get_df_reads(make_hook, server_dir):
    expected = make_hook().get_df("select * from fx.all_types")
    hook = make_hook()

    try:
        df = await hook.aget_df("select * from fx.all_types")
        chunks = [
            chunk async for chunk in hook.aget_df_by_chunks("select * from fx.many_rows", chunksize=4000)
        ]
    finally:
        await hook.aclose()

    pd.testing.assert_frame_equal(df, expected)
    assert [len(chunk) for chunk in chunks] == [4000, 4000, 2000]
    assert _leftovers(server_dir) == []
