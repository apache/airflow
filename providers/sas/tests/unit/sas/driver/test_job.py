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
"""Tests for ``SASJob``, ``SASAsyncJob`` and ``SASPagedRead``: one SAS run over SSH, its files and paging."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pytest

from airflow.providers.sas.driver.metadata import SASMetadata
from airflow.providers.sas.driver.script import SASScriptError

from unit.sas.fakes import (
    COLUMN_FORMATS,
    COLUMN_TYPES,
    DATE32,
    _async_connection,
    _cursor,
    _fake_ssh_run,
    _FakeReader,
    _FakeSFTP,
    _paged_connection,
    _raw_frame,
    _records,
    _result_on_any_path,
)


def test_read_sas7bdat_passes_file_metadata_to_normalize():
    cursor = _cursor()
    cursor._job._sftp.stat.return_value.st_size = 12345
    reader = _FakeReader(_raw_frame())
    with patch("pandas.io.sas.sas7bdat.SAS7BDATReader", return_value=reader) as read_sas:
        df = cursor._job._read_sas7bdat("/tmp/out/result.sas7bdat", dtype_backend="pyarrow")
    assert df["NAME"].dtype == pd.ArrowDtype(pa.string())

    assert read_sas.call_args.kwargs["convert_dates"] is False, "pandas would cut datetimes to ms"
    remote_file = cursor._job._sftp.file.return_value.__enter__.return_value
    remote_file.prefetch.assert_called_once_with(12345)
    assert read_sas.call_args.kwargs["encoding"] == "iso-8859-15"
    assert reader.closed
    assert df["DATDEP"].dtype == DATE32


def test_read_sas7bdat_chunks_bounds_the_prefetch():
    from airflow.providers.sas.driver.job import _CHUNK_PREFETCH_REQUESTS

    cursor = _cursor()
    cursor._job._sftp.stat.return_value.st_size = 12345
    with patch.object(
        SASMetadata,
        "parse_sas7bdat_chunks",
        return_value=iter([pd.DataFrame({"A": [1.0]})]),
    ):
        chunks = list(cursor._job._read_sas7bdat_chunks("/tmp/out/result.sas7bdat", 10))

    assert len(chunks) == 1
    remote_file = cursor._job._sftp.file.return_value.__enter__.return_value
    remote_file.prefetch.assert_called_once_with(12345, max_concurrent_requests=_CHUNK_PREFETCH_REQUESTS)


def test_run_query_chunks_cleans_up_when_the_caller_stops_early():
    cursor = _cursor()
    job = cursor._job

    def three_chunks(path, chunksize, dtype_backend=None):
        for value in (1.0, 2.0, 3.0):
            yield pd.DataFrame({"A": [value]})

    with (
        patch.object(job, "_run_script"),
        patch.object(job, "_read_sas7bdat_chunks", side_effect=three_chunks),
    ):
        chunks = job.run_query_chunks("script", chunksize=1)
        assert next(chunks)["A"].tolist() == [1.0]
        chunks.close()

    removed = [call.args[0] for call in job._sftp.remove.call_args_list]
    assert job._files.result_path in removed
    job._sftp.rmdir.assert_called_once_with(job._files.out_dir)


def test_shell_command_creates_output_dir_and_removes_script_and_log_on_success():
    cursor = _cursor()

    line = cursor._job._files.shell_command(with_output=True)

    assert line.startswith(f"mkdir -p {cursor._job._files.out_dir} && /opt/sas/sas ")
    assert f"-sysin {cursor._job._files.script_path} -log {cursor._job._files.log_path}" in line
    assert (
        f"[ $rc -lt 2 ] && rm -f {cursor._job._files.script_path} {cursor._job._files.log_path}; exit $rc"
        in line
    )
    assert "-nonews" not in line, "options come from the caller, not the cursor"
    assert not cursor._job._files.shell_command(with_output=False).startswith("mkdir")


def test_successful_query_cleans_up_with_two_sftp_calls_and_no_listdir():
    cursor = _cursor(timeout=None)
    _fake_ssh_run(cursor, rc=0)
    frame = pd.DataFrame({"A": [1.0]})
    with patch.object(cursor._job, "_read_sas7bdat", return_value=frame):
        cursor.execute("select * from mylib.orders")

    cursor._job._ssh.exec_command.assert_called_once_with(
        cursor._job._files.shell_command(with_output=True), timeout=None
    )
    cursor._job._sftp.mkdir.assert_not_called()
    cursor._job._sftp.listdir.assert_not_called()
    cursor._job._sftp.remove.assert_called_once_with(cursor._job._files.result_path)
    cursor._job._sftp.rmdir.assert_called_once_with(cursor._job._files.out_dir)
    assert cursor.fetchall() == [(1.0,)]


def test_failed_query_reports_sas_log_and_removes_script_log_and_result():
    cursor = _cursor()
    _fake_ssh_run(cursor, rc=2, stderr=b"boom")
    remote_file = cursor._job._sftp.file.return_value.__enter__.return_value
    remote_file.read.return_value = b"ERROR: File MYLIB.X.DATA does not exist."

    with pytest.raises(SASScriptError) as raised:
        cursor.execute("select * from mylib.x")

    message = str(raised.value)
    assert "SAS returned code 2" in message
    assert "ERROR: File MYLIB.X.DATA does not exist." in message
    assert "stderr: boom" in message
    removed = [call.args[0] for call in cursor._job._sftp.remove.call_args_list]
    assert removed == [
        cursor._job._files.script_path,
        cursor._job._files.log_path,
        cursor._job._files.result_path,
    ]
    cursor._job._sftp.rmdir.assert_called_once_with(cursor._job._files.out_dir)


def test_timeout_is_passed_to_the_channel_and_turns_into_sas_script_error():
    cursor = _cursor(timeout=5)
    out = _fake_ssh_run(cursor)
    out.read.side_effect = TimeoutError("timed out")

    with pytest.raises(SASScriptError, match="did not finish within 5"):
        cursor.execute("select * from mylib.x")

    assert cursor._job._ssh.exec_command.call_args.kwargs["timeout"] == 5


def test_non_select_statement_runs_without_output_dir():
    cursor = _cursor()
    _fake_ssh_run(cursor, rc=0)

    cursor.execute("delete from mylib.x where id = 1")

    line = cursor._job._ssh.exec_command.call_args.args[0]
    assert not line.startswith("mkdir")
    cursor._job._sftp.remove.assert_not_called()
    cursor._job._sftp.rmdir.assert_not_called()
    assert cursor.description is None
    assert cursor.rowcount == -1


def test_shell_line_quotes_paths_with_spaces():
    cursor = _cursor(
        sas_binary="/opt/my sas/sas",
        sas_options=("-linesize 90",),
        sas_temp_dir="/tmp/sas work",
    )

    line = cursor._job._files.shell_command(with_output=True)

    assert line.startswith(f"mkdir -p '{cursor._job._files.out_dir}' && '/opt/my sas/sas' ")
    assert "-linesize 90 -work '/tmp/sas work' " in line
    assert f"-sysin '{cursor._job._files.script_path}' -log '{cursor._job._files.log_path}'" in line
    assert f"rm -f '{cursor._job._files.script_path}' '{cursor._job._files.log_path}'; exit $rc" in line


def test_pages_in_sequence_come_from_one_sas_run():
    conn, runs, patches = _paged_connection([[1.0, 2.0], [3.0, 4.0], [5.0]])
    sql = "select * from mylib.t LIMIT 2 OFFSET {}"

    with patches[0], patches[1]:
        pages = [_records(conn, sql.format(offset)) for offset in (0, 2, 4)]

    assert pages == [[(1.0,), (2.0,)], [(3.0,), (4.0,)], [(5.0,)]]
    assert len(runs) == 1
    assert "LIMIT" not in runs[0]
    assert "select * from mylib.t" in runs[0]
    # The short last page released the run: its result directory is gone.
    assert conn._sftp.rmdir.call_count == 1


def test_pages_cut_chunks_that_do_not_align_and_end_with_an_empty_page():
    # 8 rows in pages of 4: the operator sees two full pages and asks a third,
    # which is empty, without another SAS run.
    conn, runs, patches = _paged_connection([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0]])
    sql = "select * from mylib.t LIMIT 4 OFFSET {}"

    with patches[0], patches[1]:
        pages = [_records(conn, sql.format(offset)) for offset in (0, 4, 8)]

    assert pages == [
        [(1.0,), (2.0,), (3.0,), (4.0,)],
        [(5.0,), (6.0,), (7.0,), (8.0,)],
        [],
    ]
    assert len(runs) == 1
    assert conn._sftp.rmdir.call_count == 1


def test_a_page_out_of_sequence_starts_a_new_run_at_its_offset():
    conn, runs, patches = _paged_connection([[1.0, 2.0], [3.0, 4.0], [5.0]])
    sql = "select * from mylib.t LIMIT 2 OFFSET {}"

    with patches[0], patches[1]:
        third = _records(conn, sql.format(3))
        assert conn._sftp.rmdir.call_count == 0, "the run stays open for the next page"
        first = _records(conn, sql.format(0))

    assert third == [(4.0,), (5.0,)]
    assert first == [(1.0,), (2.0,)]
    assert len(runs) == 2
    assert conn._sftp.rmdir.call_count == 1, "the first run was released by the restart"


@pytest.mark.parametrize(
    "next_sql",
    [
        "select * from mylib.t LIMIT 3 OFFSET 2",
        "select * from mylib.u LIMIT 2 OFFSET 2",
    ],
)
def test_a_page_of_another_size_or_query_starts_a_new_run(next_sql):
    conn, runs, patches = _paged_connection([[1.0, 2.0], [3.0, 4.0], [5.0]])

    with patches[0], patches[1]:
        _records(conn, "select * from mylib.t LIMIT 2 OFFSET 0")
        page = _records(conn, next_sql)

    assert page[0] == (3.0,)
    assert len(runs) == 2


def test_closing_the_connection_releases_an_open_paged_read():
    conn, _runs, patches = _paged_connection([[1.0, 2.0], [3.0, 4.0]])

    with patches[0], patches[1]:
        _records(conn, "select * from mylib.t LIMIT 2 OFFSET 0")
        assert conn._sftp.rmdir.call_count == 0
        conn.close()

    assert conn._sftp.rmdir.call_count == 1
    conn._sftp.close.assert_called_once()
    conn._ssh.close.assert_called_once()


def test_fetch_dataframe_pages_too_and_the_hook_limit_caps_the_run():
    conn, runs, patches = _paged_connection([[1.0, 2.0], [3.0]], limit=100)

    with patches[0], patches[1]:
        df = conn.cursor().fetch_dataframe("select * from mylib.t LIMIT 2 OFFSET 0")

    assert df["A"].tolist() == [1.0, 2.0]
    assert "proc sql outobs=100;" in runs[0]
    assert "LIMIT" not in runs[0]


@pytest.mark.asyncio
async def test_async_fetch_dataframe_runs_sas_once_and_parses_off_the_loop():
    conn, ssh, sftp = _async_connection()
    _result_on_any_path(sftp)
    typed = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )
    loop_thread = __import__("threading").get_ident()
    parse_threads = []

    def parse(file, encoding, dtype_backend=None):
        parse_threads.append(__import__("threading").get_ident())
        assert file.read() == b"sas7bdat bytes"
        return typed

    with patch.object(SASMetadata, "parse_sas7bdat", side_effect=parse):
        df = await conn.fetch_dataframe("select * from mylib.orders where name = %s", ("O'Brien",), limit=5)

    assert df is typed
    assert parse_threads, "parsed once"
    assert parse_threads[0] != loop_thread, "parsed in a worker thread"
    (command,), kwargs = ssh.run.call_args
    assert command.startswith("mkdir -p ")
    assert kwargs["timeout"] == 60.0
    script = next(v for k, v in sftp.files.items() if k.endswith(".sas"))
    assert "libname mylib '/data/mylib';" in script
    assert "proc sql outobs=5;" in script
    assert "where name = 'O''Brien';" in script
    assert [p.endswith(".sas7bdat") for p in sftp.removed] == [True]
    assert len(sftp.removed_directories) == 1


@pytest.mark.asyncio
async def test_async_failure_reports_the_sas_log():
    sftp = _FakeSFTP()
    conn, ssh, _ = _async_connection(sftp, returncode=2, stderr="boom")
    real_run = ssh.run.return_value

    async def run_and_write_log(command, **kwargs):
        log_path = command.split(" -log ", 1)[1].split(";", 1)[0]
        sftp.files[log_path] = b"ERROR: Table MYLIB.NOPE does not exist."
        return real_run

    ssh.run.side_effect = run_and_write_log

    with pytest.raises(SASScriptError, match=r"Table MYLIB\.NOPE does not exist") as info:
        await conn.fetch_dataframe("select * from mylib.nope")

    assert "stderr: boom" in str(info.value)
    assert len(sftp.removed) == 3, "script, log and result removed after a failure"


@pytest.mark.asyncio
async def test_async_timeout_raises_sas_script_error():
    conn, ssh, _ = _async_connection()
    ssh.run.side_effect = TimeoutError()

    with pytest.raises(SASScriptError, match=r"did not finish within 60\.0s"):
        await conn.fetch_dataframe("select * from mylib.x")


@pytest.mark.asyncio
async def test_async_chunks_clean_up_when_the_caller_stops_early():
    conn, _, sftp = _async_connection()
    _result_on_any_path(sftp)

    def three_chunks(file, encoding, chunksize, dtype_backend=None):
        for value in (1.0, 2.0, 3.0):
            yield pd.DataFrame({"A": [value]})

    with patch.object(SASMetadata, "parse_sas7bdat_chunks", side_effect=three_chunks):
        chunks = conn.fetch_dataframe_chunks("select * from mylib.x", chunksize=1)
        assert (await anext(chunks))["A"].tolist() == [1.0]
        await chunks.aclose()

    assert any(p.endswith(".sas7bdat") for p in sftp.removed)
    assert len(sftp.removed_directories) == 1


@pytest.mark.asyncio
async def test_async_calls_run_concurrently_on_distinct_remote_files():
    conn, ssh, sftp = _async_connection()
    _result_on_any_path(sftp)
    started = asyncio.Event()
    running = 0
    overlapped = False
    real_result = ssh.run.return_value

    async def run(command, **kwargs):
        nonlocal running, overlapped
        running += 1
        overlapped = overlapped or running > 1
        started.set()
        await asyncio.sleep(0.01)
        running -= 1
        return real_result

    ssh.run.side_effect = run
    with patch.object(SASMetadata, "parse_sas7bdat", return_value=pd.DataFrame()):
        await asyncio.gather(
            conn.fetch_dataframe("select * from mylib.a"),
            conn.fetch_dataframe("select * from mylib.b"),
        )

    assert overlapped, "the two SAS runs overlapped"
    scripts = [path for path in sftp.files if path.endswith(".sas")]
    assert len(set(scripts)) == 2, "each call has its own remote files"
