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

import datetime
import functools
import json
from unittest import mock

import pytest
import requests
from sqlalchemy.orm import Session

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job, JobState
from airflow.models.callback import Callback
from airflow.models.taskinstance import TaskInstance
from airflow.models.trigger import Trigger, TriggerKind
from airflow.utils.log import trigger_log_reader
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.trigger_log import get_trigger_log_dir
from airflow.utils.log.trigger_log_reader import (
    _get_last_dead_jobs_on_live_hosts,
    _get_triggerer_job_ends,
    _get_triggerer_job_ids,
    read_trigger_logs,
)

from tests_common.test_utils.config import conf_vars

TRY_LOG = "dag_id=d/run_id=r/task_id=t/attempt=2.log"


def _line(second: int, event: str, trigger_id: int = 7) -> str:
    return json.dumps(
        {
            "timestamp": f"2026-10-21T12:00:{second:02d}Z",
            "event": event,
            "level": "info",
            "trigger_id": trigger_id,
        }
    )


def _task_line(second: int, event: str) -> str:
    return json.dumps({"timestamp": f"2026-10-21T12:00:{second:02d}Z", "event": event, "ti_id": "ti"})


def _at(second):
    return datetime.datetime(2026, 10, 21, 12, 0, second, tzinfo=datetime.UTC)


def _dir(job_id: int | None = None, trigger_id: int = 7) -> str:
    return get_trigger_log_dir(trigger_id, job_id)


def _url(path: str) -> str:
    return f"http://trig-a:8794/log/{path}"


def _write(path, *lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{line}\n" for line in lines))


def _trigger(trigger_id, owner, kind=TriggerKind.ASSET_WATCHER, **attrs):
    """A trigger of ``kind`` on the live triggerer ``owner`` (job id, hostname), or on none."""
    trigger = mock.Mock(spec=Trigger, id=trigger_id, kind=kind, **attrs)
    trigger.triggerer_job = mock.Mock(spec=Job, id=owner[0], hostname=owner[1]) if owner else None
    trigger.is_owner_alive.return_value = owner is not None
    return trigger


def _read(owner=None, tail_lines=10, trigger=None):
    return read_trigger_logs(
        trigger or _trigger(7, owner), tail_lines=tail_lines, session=mock.Mock(spec=Session)
    )


def _events(messages):
    return [m.event for m in messages if not m.event.startswith("::")]


def _response(status_code, text, content_range=None):
    return mock.Mock(
        spec=requests.Response,
        status_code=status_code,
        text=text,
        headers={"content-range": content_range} if content_range else {},
    )


def _live(*lines):
    return _response(200, "".join(f"{line}\n" for line in lines))


def _remote(files, read=None):
    """Answer a remote path with the ``files`` (path: lines) it starts, in order, as a store lists them."""

    def stream(path):
        if read is not None:
            read.append(path)
        yield from files[path]

    def answer(prefix, tail_lines):
        paths = [path for path in files if path.startswith(prefix)]
        return [f"s3://b/{path}" for path in paths], [stream(path) for path in paths]

    return answer


@pytest.fixture
def base_log_folder(tmp_path):
    with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
        yield tmp_path


@pytest.fixture(autouse=True)
def triggerer_jobs():
    with mock.patch.object(
        trigger_log_reader, "_get_triggerer_job_ids", autospec=True, return_value=[]
    ) as jobs:
        yield jobs


@pytest.fixture(autouse=True)
def triggerer_job_ends():
    # No end known: the walk stops once it has tail_lines lines.
    with mock.patch.object(
        trigger_log_reader, "_get_triggerer_job_ends", autospec=True, return_value={}
    ) as ends:
        yield ends


@pytest.fixture(autouse=True)
def dead_jobs_on_live_hosts():
    with mock.patch.object(
        trigger_log_reader, "_get_last_dead_jobs_on_live_hosts", autospec=True, return_value={}
    ) as jobs:
        yield jobs


@pytest.fixture
def fetch():
    with mock.patch.object(trigger_log_reader, "fetch_logs_from_service", autospec=True) as fetch:
        yield fetch


@pytest.fixture
def no_remote():
    with mock.patch.object(
        trigger_log_reader, "_read_remote_streams", autospec=True, return_value=([], [])
    ) as remote:
        yield remote


@pytest.fixture
def task_trigger():
    ti = mock.Mock(spec=TaskInstance, id="ti", try_number=2)
    with (
        mock.patch.object(
            trigger_log_reader, "prepare_task_log_contexts", autospec=True, return_value={"ti": "ctx"}
        ),
        mock.patch.object(
            trigger_log_reader, "render_task_log_filename", autospec=True, return_value=TRY_LOG
        ),
    ):
        yield lambda owner: _trigger(5, owner, TriggerKind.TASK, task_instance=ti)


@pytest.mark.usefixtures("no_remote")
def test_merges_every_triggerers_local_files_in_time_order_and_tails(base_log_folder):
    _write(base_log_folder / f"{_dir(3)}/rotated/trigger.log", _line(1, "a1"), _line(3, "a3"))
    _write(base_log_folder / f"{_dir(3)}/trigger.log", _line(5, "a5"))
    _write(base_log_folder / f"{_dir(4)}/trigger.log", _line(2, "b2"), _line(4, "b4"), _line(6, "b6"))
    _write(base_log_folder / f"{_dir(4, trigger_id=70)}/trigger.log", _line(0, "another trigger"))

    messages = _read(None, 4)

    assert _events(messages) == ["a3", "b4", "a5", "b6"]
    assert messages[0].sources == [
        f"{_dir(4)}/trigger.log",
        f"{_dir(3)}/rotated/trigger.log",
        f"{_dir(3)}/trigger.log",
    ]


@pytest.mark.parametrize(
    ("head", "content_range"),
    [
        pytest.param("ial line\n", "bytes 9000-9999/10000", id="drops-the-partial-first-line"),
        pytest.param("", "bytes 0-99/100", id="keeps-the-first-line-of-a-range-from-the-start"),
    ],
)
@pytest.mark.usefixtures("no_remote", "base_log_folder")
def test_live_tail_asks_the_owner_for_a_range(fetch, head, content_range):
    fetch.return_value = _response(206, f"{head}{_line(1, 'x')}\n{_line(2, 'y')}\n", content_range)

    messages = _read((3, "trig-a"), 2)

    assert fetch.call_args_list[0] == mock.call(
        _url(f"{_dir(3)}/trigger.log"), f"{_dir(3)}/trigger.log", headers={"Range": "bytes=-4096"}
    )
    assert _events(messages) == ["x", "y"]
    assert messages[0].sources == [_url(f"{_dir(3)}/trigger.log")]


@pytest.mark.usefixtures("base_log_folder")
def test_a_live_log_server_that_stops_mid_body_is_a_source_note_not_an_error(fetch, no_remote):
    response = mock.Mock(spec=requests.Response, status_code=206, headers={"content-range": "bytes 0-99/100"})
    type(response).text = mock.PropertyMock(
        side_effect=requests.exceptions.ConnectionError("Read timed out.")
    )
    fetch.return_value = response
    no_remote.return_value = ([f"s3://b/{_dir(3)}/trigger.log"], [iter([_line(1, "stored")])])

    messages = _read((3, "trig-a"), 2)

    assert _events(messages) == ["stored"]
    assert any(s.startswith(f"Could not read {_url(f'{_dir(3)}/trigger.log')}") for s in messages[0].sources)


@pytest.mark.usefixtures("no_remote", "base_log_folder")
@conf_vars({("logging", "trigger_log_server_port"): None, ("logging", "triggerer_log_server_port"): "9002"})
def test_live_tail_uses_the_deprecated_port_option_as_task_logs_do(fetch):
    fetch.return_value = _live(_line(1, "x"))

    _read((3, "trig-a"), 1)

    assert fetch.call_args_list[0].args[0] == f"http://trig-a:9002/log/{_dir(3)}/trigger.log"


@pytest.mark.usefixtures("no_remote", "base_log_folder")
def test_a_short_live_file_is_completed_from_the_owners_held_and_rotated_files(fetch):
    fetch.side_effect = [
        _live(_line(6, "new")),
        _live(_line(5, "held")),
        _response(206, f"tial\n{_line(3, 'old3')}\n{_line(4, 'old4')}\n", "bytes 10-99/100"),
    ]

    messages = _read((3, "trig-a"), 3)

    assert fetch.call_args_list[2] == mock.call(
        _url(f"{_dir(3)}/rotated/trigger.log"),
        f"{_dir(3)}/rotated/trigger.log",
        headers={"Range": "bytes=-2048"},
    )
    assert _events(messages) == ["old4", "held", "new"]
    assert messages[0].sources == [
        _url(f"{_dir(3)}/trigger.log"),
        _url(f"{_dir(3)}/trigger.held.log"),
        _url(f"{_dir(3)}/rotated/trigger.log"),
    ]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
def test_no_rotated_file_on_the_owner_is_not_reported(fetch):
    fetch.side_effect = [_live(_line(5, "new")), _response(404, ""), _response(404, "")]

    messages = _read((3, "trig-a"), 3)

    assert _events(messages) == ["new"]
    assert messages[0].sources == [_url(f"{_dir(3)}/trigger.log")]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
def test_an_empty_live_file_is_not_an_error(fetch):
    fetch.side_effect = [_response(416, ""), _response(404, ""), _live(_line(1, "older"))]

    messages = _read((3, "trig-a"), 2)

    assert _events(messages) == ["older"]
    assert messages[0].sources == [_url(f"{_dir(3)}/trigger.log"), _url(f"{_dir(3)}/rotated/trigger.log")]


@pytest.mark.usefixtures("no_remote")
def test_an_unreachable_owner_is_reported_and_the_rest_still_read(fetch, base_log_folder):
    fetch.side_effect = ConnectionError("refused")
    _write(base_log_folder / f"{_dir(2)}/trigger.log", _line(1, "older"))

    messages = _read((3, "trig-a"))

    assert _events(messages) == ["older"]
    assert messages[0].sources[0] == f"Could not read {_url(f'{_dir(3)}/trigger.log')}: refused"


@pytest.mark.usefixtures("base_log_folder")
def test_lines_also_streamed_with_their_time_to_the_millisecond_appear_once(fetch, no_remote):
    def line(stamp, event):
        return json.dumps({"timestamp": f"2026-10-21T12:00:{stamp}", "event": event, "trigger_id": 7})

    fetch.return_value = _live(line("01.123456Z", "x"), line("02.000999Z", "y"))
    no_remote.return_value = (
        ["stream"],
        [iter([line("01.123000+00:00", "x"), line("02.001000+00:00", "y")])],
    )

    assert _events(_read((3, "trig-a"))) == ["x", "y"]


@pytest.mark.usefixtures("base_log_folder")
def test_remote_logs_are_found_by_the_triggers_prefix(no_remote):
    no_remote.return_value = ([f"s3://bucket/{_dir(3)}/trigger.log"], [iter([_line(1, "uploaded")])])

    messages = _read()

    no_remote.assert_called_once_with(f"{_dir()}/", tail_lines=10)
    assert _events(messages) == ["uploaded"]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
def test_no_logs():
    assert [m.event for m in _read()] == ["No logs found."]


@pytest.mark.usefixtures("base_log_folder")
def test_no_file_yet_on_a_new_owner_is_said_plainly_and_older_lines_still_read(fetch, no_remote):
    fetch.return_value = _response(404, "")
    no_remote.return_value = ([f"s3://bucket/{_dir(3)}/trigger.log"], [iter([_line(1, "older")])])

    messages = _read((4, "trig-b"))

    assert _events(messages) == ["older"]
    assert messages[0].sources == [
        f"http://trig-b:8794/log/{_dir(4)}/trigger.log: no file yet, the trigger has not logged on this triggerer",
        f"s3://bucket/{_dir(3)}/trigger.log",
    ]


def test_a_task_trigger_reads_only_the_trigger_part_of_its_task_try_log(
    fetch, task_trigger, base_log_folder, no_remote
):
    fetch.return_value = _live(_line(9, "live on job 4"))
    _write(base_log_folder / TRY_LOG, _line(1, "the worker's lines"))
    _write(base_log_folder / f"{TRY_LOG}.trigger.3.log", _line(2, "on job 3"))
    _write(base_log_folder / "dag_id=d/run_id=r/task_id=t/attempt=1.log.trigger.3.log", _line(0, "try 1"))

    messages = _read(trigger=task_trigger((4, "trig-b")))

    assert _events(messages) == ["on job 3", "live on job 4"]
    assert fetch.call_args.args[1] == f"{TRY_LOG}.trigger.4.log"
    no_remote.assert_called_once_with(f"{TRY_LOG}.trigger.", tail_lines=10)


@mock.patch.object(
    FileTaskHandler,
    "add_triggerer_suffix",
    autospec=True,
    side_effect=lambda full_path, job_id=None: f"{full_path}.trg" + (f".{job_id}.log" if job_id else ""),
)
def test_a_task_trigger_log_is_named_as_the_task_log_handler_names_it(
    _, fetch, task_trigger, base_log_folder, no_remote
):
    fetch.return_value = _live(_line(9, "live on job 4"))
    _write(base_log_folder / f"{TRY_LOG}.trg.3.log", _line(2, "on job 3"))

    messages = _read(trigger=task_trigger((4, "trig-b")))

    assert _events(messages) == ["on job 3", "live on job 4"]
    assert fetch.call_args.args[1] == f"{TRY_LOG}.trg.4.log"
    no_remote.assert_called_once_with(f"{TRY_LOG}.trg.", tail_lines=10)


def test_a_callback_trigger_reads_the_callback_log(fetch, base_log_folder, no_remote):
    data = {"dag_id": "d", "kwargs": {"context": {"dag_run": {"dag_run_id": "r"}}}}
    callback = mock.Mock(spec=Callback, id="0199", data=data)
    _write(base_log_folder / "triggerer_callbacks/d/r/0199/callback.log", _line(1, "callback ran"))

    messages = _read(trigger=_trigger(6, None, TriggerKind.CALLBACK, callback=callback))

    assert _events(messages) == ["callback ran"]
    fetch.assert_not_called()
    no_remote.assert_called_once_with("triggerer_callbacks/d/r/0199/callback.log", tail_lines=10)


@pytest.mark.usefixtures("base_log_folder")
def test_a_callback_log_path_outside_the_log_folder_is_not_read(no_remote):
    callback = mock.Mock(spec=Callback, id="0199", data={"dag_id": "..", "kwargs": {}})

    messages = _read(trigger=_trigger(6, None, TriggerKind.CALLBACK, callback=callback))

    assert _events(messages) == [
        "Not reading a log outside the log folder: triggerer_callbacks/../unknown/0199/callback.log"
    ]
    no_remote.assert_not_called()


@pytest.mark.usefixtures("base_log_folder")
@pytest.mark.parametrize("streams", [False, True], ids=["read-only-handler", "stream-handler"])
# The merge sorts only within a buffer, here two lines, as it would past HEAP_DUMP_SIZE lines of a long log.
@mock.patch("airflow.utils.log.file_task_handler.HALF_HEAP_DUMP_SIZE", 1)
@mock.patch("airflow.utils.log.file_task_handler.HEAP_DUMP_SIZE", 2)
@mock.patch.object(trigger_log_reader, "get_remote_task_log", autospec=True)
def test_remote_logs_of_several_triggerers_merge_by_time(get_remote_task_log, streams):
    # Key order puts job 10's file before job 9's, though job 9 ran first.
    job_10 = f"{_line(5, 'job 10, first')}\n{_line(6, 'job 10, last')}\n"
    job_9 = f"{_line(1, 'job 9, first')}\n{_line(2, 'job 9, last')}\n"
    remote_io = mock.Mock(spec=["read", "stream"] if streams else ["read"])
    remote_io.read.return_value = ([f"s3://b/{_dir(10)}/", f"s3://b/{_dir(9)}/"], [job_10, job_9])
    if streams:
        remote_io.stream.return_value = (
            [f"gs://b/{_dir(10)}/", f"gs://b/{_dir(9)}/"],
            [iter(job_10.splitlines()), iter(job_9.splitlines())],
        )
    get_remote_task_log.return_value = remote_io

    assert _events(_read(None, 3)) == ["job 9, last", "job 10, first", "job 10, last"]


@pytest.mark.usefixtures("base_log_folder")
def test_a_watcher_is_read_newest_triggerer_first_until_enough_lines(no_remote, triggerer_jobs):
    # Job 11 is alive but never ran the trigger; job 3 ran it long ago.
    triggerer_jobs.return_value = [11, 10, 9, 3]
    no_remote.side_effect = _remote(
        {
            f"{_dir(10)}/trigger.log": [_line(5, "job 10, a"), _line(6, "job 10, b")],
            f"{_dir(9)}/trigger.log": [_line(1, "job 9, a"), _line(2, "job 9, b")],
            f"{_dir(3)}/trigger.log": [_line(0, "job 3")],
        }
    )

    messages = _read(None, 3)

    assert [c.args[0] for c in no_remote.call_args_list] == [f"{_dir(11)}/", f"{_dir(10)}/", f"{_dir(9)}/"]
    assert _events(messages) == ["job 9, b", "job 10, a", "job 10, b"]
    assert messages[0].sources == [
        f"s3://b/{_dir(10)}/trigger.log",
        f"s3://b/{_dir(9)}/trigger.log",
        "Older logs, of triggerer job 3, were not read.",
    ]


@pytest.mark.usefixtures("base_log_folder")
def test_a_watcher_moved_to_an_older_triggerer_is_read_there_first(fetch, no_remote, triggerer_jobs):
    # Job 5 ran the watcher until it stopped; job 3, started before it, runs it now.
    triggerer_jobs.return_value = [3, 5]
    no_remote.side_effect = _remote({f"{_dir(5)}/trigger.log": [_line(1, "job 5, a"), _line(2, "job 5, b")]})
    fetch.return_value = _live(_line(5, "job 3, a"), _line(6, "job 3, b"))

    messages = _read((3, "trig-a"), 2)

    assert _events(messages) == ["job 3, a", "job 3, b"]
    assert messages[0].sources[-1] == "Older logs, of triggerer job 5, were not read."


@pytest.mark.parametrize(
    ("job_5", "live", "expected"),
    [
        pytest.param(
            {"trigger.log": [_line(4, "job 5, a"), _line(5, "job 5, b"), _line(6, "job 5, c")]},
            [_line(1, "job 3, a"), _line(2, "job 3, b"), _line(7, "job 3, c"), _line(8, "job 3, d")],
            ["job 5, b", "job 5, c", "job 3, c", "job 3, d"],
            id="keeps-the-lines-logged-in-between",
        ),
        pytest.param(
            {
                "rotated/1.log": [_line(4, "job 5, a")],
                "rotated/2.log": [_line(5, "job 5, b")],
                "trigger.log": [_line(6, "job 5, c")],
            },
            [_line(1, "job 3, a"), _line(8, "job 3, b")],
            ["job 5, a", "job 5, b", "job 5, c", "job 3, b"],
            id="reads-all-newer-lines-of-a-later-job-not-only-those-missing",
        ),
    ],
)
@pytest.mark.usefixtures("base_log_folder")
def test_a_watcher_back_on_a_triggerer_that_ran_it_before(
    fetch, no_remote, triggerer_jobs, triggerer_job_ends, job_5, live, expected
):
    # Job 3 ran the watcher, job 5 took it over and stopped at 12:00:06, and job 3 runs it again.
    triggerer_jobs.return_value = [3, 5]
    triggerer_job_ends.return_value = {5: _at(6)}
    no_remote.side_effect = _remote({f"{_dir(5)}/{name}": lines for name, lines in job_5.items()})
    fetch.return_value = _live(*live)

    assert _events(_read((3, "trig-a"), 4)) == expected


@pytest.mark.usefixtures("base_log_folder")
def test_a_job_that_ended_before_the_oldest_line_kept_is_not_read(
    fetch, no_remote, triggerer_jobs, triggerer_job_ends
):
    triggerer_jobs.return_value = [3, 5, 2]
    triggerer_job_ends.return_value = {5: _at(4), 2: _at(1)}
    no_remote.side_effect = _remote({f"{_dir(5)}/trigger.log": [_line(3, "job 5, a"), _line(4, "job 5, b")]})
    fetch.return_value = _live(_line(5, "job 3, a"), _line(6, "job 3, b"))

    messages = _read((3, "trig-a"), 2)

    assert _events(messages) == ["job 3, a", "job 3, b"]
    assert [c.args[0] for c in no_remote.call_args_list] == [f"{_dir(3)}/"]
    assert messages[0].sources[-1] == "Older logs, of triggerer jobs 2 to 5, were not read."


@pytest.mark.usefixtures("base_log_folder")
@pytest.mark.parametrize("uploaded", [False, True])
def test_a_killed_triggerers_files_are_read_through_the_triggerer_restarted_on_its_host(
    fetch, no_remote, triggerer_jobs, dead_jobs_on_live_hosts, uploaded
):
    # Job 3 was killed on trig-a and restarted there as job 4, which has not uploaded job 3's files yet unless
    # ``uploaded``; the watcher moved to job 5 on trig-b.
    triggerer_jobs.return_value = [5, 4, 3]
    dead_jobs_on_live_hosts.return_value = {3: "trig-a"}
    no_remote.side_effect = _remote({f"{_dir(3)}/trigger.log": [_line(1, "job 3, a")]})
    served = {
        f"http://trig-b:8794/log/{_dir(5)}/trigger.log": [_line(5, "job 5")],
        _url(f"{_dir(3)}/trigger.log"): [] if uploaded else [_line(1, "job 3, a"), _line(2, "job 3, b")],
    }
    fetch.side_effect = lambda url, relative_path, headers: (
        _live(*served[url]) if served.get(url) else _response(404, "")
    )

    messages = _read((5, "trig-b"))

    assert not any(_dir(4) in c.args[0] for c in fetch.call_args_list)
    if uploaded:
        assert _events(messages) == ["job 3, a", "job 5"]
        assert messages[0].sources == [
            f"http://trig-b:8794/log/{_dir(5)}/trigger.log",
            f"s3://b/{_dir(3)}/trigger.log",
        ]
    else:
        assert _events(messages) == ["job 3, a", "job 3, b", "job 5"]
        assert messages[0].sources == [
            f"http://trig-b:8794/log/{_dir(5)}/trigger.log",
            _url(f"{_dir(3)}/trigger.log"),
            f"s3://b/{_dir(3)}/trigger.log",
        ]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "READ_MAX_BYTES", 2 * (len(_line(3, "job 10, 3")) + 1))
def test_a_request_reads_at_most_the_byte_cap_newest_lines_first(no_remote, triggerer_jobs):
    triggerer_jobs.return_value = [10, 9]
    no_remote.side_effect = _remote(
        {
            f"{_dir(10)}/trigger.log": [_line(s, f"job 10, {s}") for s in range(3, 6)],
            f"{_dir(9)}/trigger.log": [_line(1, "job 9")],
        }
    )

    messages = _read()

    assert _events(messages) == ["job 10, 4", "job 10, 5"]
    assert [c.args[0] for c in no_remote.call_args_list] == [f"{_dir(10)}/"]
    assert messages[0].sources[-1] == trigger_log_reader._NOT_READ_PAST_CAP


@pytest.mark.usefixtures("no_remote")
def test_triggerers_of_a_watcher_are_also_found_from_its_local_directories(base_log_folder, triggerer_jobs):
    triggerer_jobs.return_value = [4]
    _write(base_log_folder / f"{_dir(4)}/trigger.log", _line(5, "job 4"))
    _write(base_log_folder / f"{_dir(2)}/rotated/trigger.log", _line(1, "job 2, rotated"))
    _write(base_log_folder / f"{_dir(2)}/trigger.log", _line(2, "job 2"))

    assert _events(_read(None, 2)) == ["job 2", "job 4"]
    assert _events(_read(None, 1)) == ["job 4"]


@pytest.mark.db_test
def test_the_triggerers_of_a_watcher_ran_since_it_was_created_the_last_to_heartbeat_first(session):
    created = timezone.utcnow()

    def add_job(seconds_after, job_type="TriggererJob"):
        job = Job(job_type=job_type, latest_heartbeat=created + datetime.timedelta(seconds=seconds_after))
        session.add(job)
        session.flush()
        return job

    before, scheduler = add_job(-60), add_job(2, job_type="SchedulerJob")
    # Added first, so with the lower id, but heartbeating last.
    older_alive, newer_stopped = add_job(2), add_job(1)

    job_ids = _get_triggerer_job_ids(mock.Mock(spec=Trigger, created_date=created), session=session)

    ours = {before.id, scheduler.id, older_alive.id, newer_stopped.id}
    assert [job_id for job_id in job_ids if job_id in ours] == [older_alive.id, newer_stopped.id]
    session.rollback()


@pytest.mark.db_test
@conf_vars({("triggerer", "triggerer_health_check_threshold"): "30"})
def test_a_triggerer_job_ended_at_its_end_else_its_last_heartbeat_and_the_health_threshold(session):
    now = timezone.utcnow()
    stopped = Job(job_type="TriggererJob", latest_heartbeat=now, end_date=now + datetime.timedelta(seconds=1))
    killed = Job(job_type="TriggererJob", latest_heartbeat=now)
    session.add_all([stopped, killed])
    session.flush()

    ends = _get_triggerer_job_ends([stopped.id, killed.id], session=session)

    assert ends == {
        stopped.id: now + datetime.timedelta(seconds=1),
        killed.id: now + datetime.timedelta(seconds=30),
    }
    session.rollback()


@pytest.mark.db_test
def test_the_last_job_to_stop_on_each_host_a_live_triggerer_runs_on_is_found(session):
    now = timezone.utcnow()

    def add_job(hostname, seconds_ago, state=JobState.RUNNING):
        job = Job(job_type="TriggererJob", hostname=hostname, state=state)
        job.latest_heartbeat = now - datetime.timedelta(seconds=seconds_ago)
        session.add(job)
        session.flush()
        return job

    restarted = add_job("trig-a", 0)
    killed = add_job("trig-a", 60)
    killed_before = add_job("trig-a", 120, state=JobState.SUCCESS)
    alive_elsewhere = add_job("trig-b", 0)
    killed_alone = add_job("trig-c", 60)

    found = _get_last_dead_jobs_on_live_hosts(
        [job.id for job in (restarted, killed, killed_before, alive_elsewhere, killed_alone)], session=session
    )

    assert found == {killed.id: "trig-a"}
    session.rollback()


@pytest.mark.usefixtures("base_log_folder")
def test_lines_that_share_only_their_text_or_repeat_in_one_stream_are_all_kept(no_remote):
    def line(n, stamp="01.000100"):
        return json.dumps({"timestamp": f"2026-10-21T12:00:{stamp}Z", "event": "received", "n": n})

    repeats = [line(3, stamp) for stamp in ("01.000900", "01.001500", "01.002900")]
    no_remote.return_value = (
        [f"s3://b/{_dir(3)}/trigger.log"],
        [iter([line(1), line(2), line(3), *repeats])],
    )

    assert [m.model_dump().get("n") for m in _read() if m.event == "received"] == [1, 2, 3, 3, 3, 3]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
def test_long_live_lines_are_read_by_asking_for_a_larger_range(fetch):
    text = "".join(f"{_line(s, 'x' * 3000)}\n" for s in range(1, 4))
    fetch.side_effect = [
        # 4 KiB: the tail of one line only.
        _response(206, text[-4096:], content_range=f"bytes {len(text) - 4096}-{len(text) - 1}/{len(text)}"),
        _response(206, text, content_range=f"bytes 0-{len(text) - 1}/{len(text)}"),
    ]

    messages = _read((3, "trig-a"), 2)

    assert [len(e) for e in _events(messages)] == [3000, 3000]
    assert fetch.call_args_list[1].kwargs["headers"] == {"Range": f"bytes=-{4096 * 8}"}


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "READ_MAX_JOBS", 3)
def test_a_watcher_is_looked_for_on_at_most_the_newest_triggerer_jobs(no_remote, triggerer_jobs):
    triggerer_jobs.return_value = list(range(100, 96, -1))
    no_remote.side_effect = _remote({f"{_dir(98)}/trigger.log": [_line(1, "job 98")]})

    messages = _read()

    assert triggerer_jobs.call_args.kwargs["limit"] == 4
    assert [c.args[0] for c in no_remote.call_args_list] == [f"{_dir(100)}/", f"{_dir(99)}/", f"{_dir(98)}/"]
    assert _events(messages) == ["job 98"]
    assert messages[0].sources[-1] == "Logs of triggerer jobs before 98 were not looked for."


def test_after_a_remote_read_fails_older_jobs_are_read_only_locally(
    no_remote, triggerer_jobs, base_log_folder
):
    triggerer_jobs.return_value = [10, 9, 8]
    no_remote.side_effect = TimeoutError("connect timeout")
    _write(base_log_folder / f"{_dir(8)}/trigger.log", _line(1, "job 8"))

    messages = _read()

    assert [c.args[0] for c in no_remote.call_args_list] == [f"{_dir(10)}/"]
    assert _events(messages) == ["job 8"]
    assert messages[0].sources == [
        "Could not read remote logs: connect timeout",
        trigger_log_reader._REMOTE_NOT_READ_AFTER_FAILURE,
        f"{_dir(8)}/trigger.log",
    ]


@pytest.mark.usefixtures("base_log_folder")
def test_a_remote_stream_that_fails_while_read_is_reported_and_the_live_lines_kept(fetch, no_remote):
    def throttled():
        raise RuntimeError("ThrottlingException")
        yield

    fetch.return_value = _live(_line(5, "live"))
    no_remote.return_value = (["stream"], [throttled()])

    messages = _read((3, "trig-a"))

    assert _events(messages) == ["live"]
    assert "Could not read remote logs: ThrottlingException" in messages[0].sources


@pytest.mark.usefixtures("no_remote", "base_log_folder")
def test_a_live_line_with_unicode_line_separators_stays_whole(fetch):
    event = "ls\u2028ps\u2029nel\x85end"
    fetch.return_value = _live(
        json.dumps({"timestamp": "2026-10-21T12:00:01Z", "event": event, "level": "info"}, ensure_ascii=False)
    )

    assert _events(_read((3, "trig-a"))) == [event]


@pytest.mark.usefixtures("base_log_folder")
def test_remote_lines_without_the_watchers_trigger_id_are_dropped(no_remote, triggerer_jobs):
    # A notice stamped now, one per triggerer job, would push the watcher's own lines out.
    triggerer_jobs.return_value = [4, 3]
    noise = [
        _line(3, "another trigger", trigger_id=70),
        json.dumps({"timestamp": "2026-10-21T12:59:59Z", "event": "No log stream found."}),
    ]
    no_remote.side_effect = _remote(
        {
            f"{_dir(4)}/trigger.log": [_line(2, "job 4"), *noise],
            f"{_dir(3)}/trigger.log": [_line(1, "job 3"), *noise],
        }
    )

    assert _events(_read(None, 2)) == ["job 3", "job 4"]


@pytest.mark.usefixtures("base_log_folder")
def test_a_watchers_remote_files_are_read_newest_first_until_enough_lines(no_remote, triggerer_jobs):
    triggerer_jobs.return_value = [3]
    files = {
        f"{_dir(3)}/rotated/trigger.000001.log": [_line(1, "part 1")],
        f"{_dir(3)}/rotated/trigger.000002.log": [_line(2, "part 2"), _line(3, "part 2, b")],
        f"{_dir(3)}/rotated/trigger.000003.log": [_line(4, "part 3")],
        f"{_dir(3)}/trigger.log": [_line(5, "uploaded when it left")],
    }
    read: list[str] = []
    no_remote.side_effect = _remote(files, read)

    assert _events(_read()) == ["part 1", "part 2", "part 2, b", "part 3", "uploaded when it left"]
    read.clear()
    assert _events(_read(None, 3)) == ["part 2, b", "part 3", "uploaded when it left"]
    assert read == [f"{_dir(3)}/trigger.log", *(f"{_dir(3)}/rotated/trigger.00000{n}.log" for n in (3, 2))]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "READ_MAX_BYTES", 2 * (len(_line(1, "part 1")) + 1))
def test_the_byte_cap_holds_across_a_watchers_remote_files(no_remote, triggerer_jobs):
    triggerer_jobs.return_value = [3]
    no_remote.side_effect = _remote(
        {f"{_dir(3)}/rotated/trigger.00000{n}.log": [_line(n, f"part {n}")] for n in range(1, 5)}
    )

    messages = _read()

    assert _events(messages) == ["part 3", "part 4"]
    assert messages[0].sources[-1] == trigger_log_reader._NOT_READ_PAST_CAP


@pytest.mark.usefixtures("no_remote")
def test_a_triggerer_directory_with_only_numbered_rotated_files_is_read_from_a_shared_disk(base_log_folder):
    _write(base_log_folder / f"{_dir(3)}/rotated/trigger.000002.log", _line(1, "job 3, rotated"))
    _write(base_log_folder / f"{_dir(4)}/trigger.log", _line(2, "job 4"))

    messages = _read()

    assert _events(messages) == ["job 3, rotated", "job 4"]
    assert messages[0].sources == [f"{_dir(4)}/trigger.log", f"{_dir(3)}/rotated/trigger.000002.log"]


@pytest.mark.usefixtures("no_remote")
def test_a_watchers_file_linked_from_outside_the_log_folder_is_not_read(base_log_folder, tmp_path_factory):
    outside = tmp_path_factory.mktemp("outside") / "secret.log"
    _write(outside, _line(1, "not a log"))
    (base_log_folder / f"{_dir(3)}/rotated").mkdir(parents=True)
    (base_log_folder / f"{_dir(3)}/rotated/trigger.000001.log").symlink_to(outside)
    _write(base_log_folder / f"{_dir(3)}/trigger.log", _line(2, "live"))

    messages = _read()

    assert _events(messages) == ["live"]
    assert messages[0].sources == [f"{_dir(3)}/trigger.log"]


@pytest.mark.usefixtures("base_log_folder")
def test_lines_of_the_same_millisecond_stay_in_order_across_a_watchers_files(no_remote, triggerer_jobs):
    triggerer_jobs.return_value = [3]
    no_remote.side_effect = _remote(
        {
            f"{_dir(3)}/rotated/trigger.000001.log": [_line(1, "a"), _line(2, "b")],
            f"{_dir(3)}/rotated/trigger.000002.log": [_line(2, "c"), _line(3, "d")],
            f"{_dir(3)}/trigger.log": [_line(3, "e")],
        }
    )

    assert _events(_read()) == ["a", "b", "c", "d", "e"]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_remote_handler_streams", autospec=True, return_value=True)
def test_a_streaming_handlers_watcher_streams_are_read_by_name_not_by_listing_a_directory(
    _, no_remote, triggerer_jobs
):
    triggerer_jobs.return_value = [4, 3]
    no_remote.side_effect = _remote(
        {f"{_dir(4)}/trigger.log": [_line(2, "job 4")], f"{_dir(3)}/trigger.log": [_line(1, "job 3")]}
    )

    messages = _read()

    assert [c.args[0] for c in no_remote.call_args_list] == [
        f"{_dir(4)}/trigger.log",
        f"{_dir(3)}/trigger.log",
    ]
    assert _events(messages) == ["job 3", "job 4"]


class _LazyProcessorsIO:
    @functools.cached_property
    def processors(self):
        raise ConnectionError("the log store is unreachable")


@pytest.mark.parametrize(
    ("remote_io", "streaming"),
    [
        pytest.param(None, False, id="no-remote-logging"),
        pytest.param(mock.Mock(spec=["read", "processors"], processors=()), False, id="stores-files"),
        pytest.param(mock.Mock(spec=["read", "processors"], processors=(print,)), True, id="streams-lines"),
        pytest.param(_LazyProcessorsIO(), True, id="not-built-to-tell"),
    ],
)
@mock.patch.object(trigger_log_reader, "get_remote_task_log", autospec=True)
def test_a_handler_streams_when_it_has_processors(get_remote_task_log, remote_io, streaming):
    get_remote_task_log.return_value = remote_io

    assert trigger_log_reader._remote_handler_streams() is streaming


class _TailingIO:
    def stream(self, relative_path, ti, *, tail_lines=None):
        return [f"{relative_path} {tail_lines}"], [iter([_line(1, "last")])]


class _OlderIO:
    def stream(self, relative_path, ti):
        return [relative_path], [iter([_line(1, "last")])]


@pytest.mark.parametrize(
    ("remote_io", "source"),
    [
        pytest.param(_TailingIO(), f"{_dir(3)}/trigger.log 25", id="asked-for-the-end-of-each-log"),
        pytest.param(_OlderIO(), f"{_dir(3)}/trigger.log", id="without-tail-lines-asked-as-before"),
    ],
)
@mock.patch.object(trigger_log_reader, "get_remote_task_log", autospec=True)
def test_a_stream_is_asked_for_tail_lines_if_it_takes_them(get_remote_task_log, remote_io, source):
    get_remote_task_log.return_value = remote_io

    sources, streams = trigger_log_reader._read_remote_streams(f"{_dir(3)}/trigger.log", tail_lines=25)

    assert sources == [source]
    assert [list(stream) for stream in streams] == [[_line(1, "last")]]


def test_the_tail_of_several_triggerers_files_is_their_newest_lines_in_time_order():
    # Newest triggerer first, as the reader passes them; more lines than the task log merge holds.
    def lines(minute: int, count: int, tag: str) -> list[str]:
        start = datetime.datetime(2026, 10, 21, 12, minute, tzinfo=datetime.UTC)
        return [
            json.dumps(
                {
                    "timestamp": (start + datetime.timedelta(milliseconds=20 * i)).isoformat(),
                    "event": f"{tag} {i}",
                }
            )
            for i in range(count)
        ]

    oldest, older, newest = lines(0, 3940, "a"), lines(2, 1088, "b"), lines(4, 7502, "c")
    every = [json.loads(line)["event"] for line in oldest + older + newest]

    tail = trigger_log_reader._build_tail([], [newest, older, oldest], tail_lines=10_000)

    assert [message.event for message in tail[2:]] == every[-10_000:]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_remote_handler_streams", autospec=True, return_value=True)
def test_a_streaming_handlers_task_trigger_streams_are_read_by_name_one_per_triggerer(
    _, task_trigger, no_remote, triggerer_jobs
):
    # Job 2 never ran the trigger: its stream name is answered with a notice.
    triggerer_jobs.return_value = [4, 3, 2]
    no_remote.side_effect = _remote(
        {
            f"{TRY_LOG}.trigger.4.log": [_task_line(3, "on job 4")],
            # A stream holding lines of the try is kept whole, a line without the id too.
            f"{TRY_LOG}.trigger.3.log": [
                _task_line(1, "on job 3"),
                json.dumps({"timestamp": _at(2).isoformat(), "event": "no id"}),
            ],
            f"{TRY_LOG}.trigger.2.log": [
                json.dumps({"timestamp": "2026-10-21T12:59:59Z", "event": "No log stream found."})
            ],
        }
    )

    messages = _read(trigger=task_trigger(None))

    assert [c.args[0] for c in no_remote.call_args_list] == [f"{TRY_LOG}.trigger.{j}.log" for j in (4, 3, 2)]
    assert _events(messages) == ["on job 3", "no id", "on job 4"]
    assert messages[0].sources == [f"s3://b/{TRY_LOG}.trigger.4.log", f"s3://b/{TRY_LOG}.trigger.3.log"]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_remote_handler_streams", autospec=True, return_value=True)
def test_a_task_triggers_older_streams_are_not_read_once_the_remote_store_failed(
    _, task_trigger, no_remote, triggerer_jobs
):
    triggerer_jobs.return_value = [4, 3, 2]
    no_remote.side_effect = OSError("connect timeout")

    messages = _read(trigger=task_trigger(None))

    assert no_remote.call_count == 1
    assert _events(messages) == ["No logs found."]
    assert messages[0].sources == [
        "Could not read remote logs: connect timeout",
        trigger_log_reader._REMOTE_NOT_READ_AFTER_FAILURE,
    ]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_remote_handler_streams", autospec=True, return_value=True)
def test_a_task_triggers_streams_are_read_until_enough_lines(_, task_trigger, no_remote, triggerer_jobs):
    triggerer_jobs.return_value = [4, 3]
    no_remote.side_effect = _remote({f"{TRY_LOG}.trigger.4.log": [_task_line(1, "a"), _task_line(2, "b")]})

    messages = _read(trigger=task_trigger(None), tail_lines=2)

    assert [c.args[0] for c in no_remote.call_args_list] == [f"{TRY_LOG}.trigger.4.log"]
    assert _events(messages) == ["a", "b"]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_remote_handler_streams", autospec=True, return_value=True)
def test_a_task_trigger_back_on_a_triggerer_that_ran_it_before(
    _, task_trigger, no_remote, triggerer_jobs, triggerer_job_ends
):
    # Job 4 ran the trigger, job 5 took it over and stopped at 12:00:06, and job 4 runs it again.
    triggerer_jobs.return_value = [4, 5]
    triggerer_job_ends.return_value = {5: _at(6)}
    no_remote.side_effect = _remote(
        {
            f"{TRY_LOG}.trigger.4.log": [
                _task_line(1, "job 4, a"),
                _task_line(2, "job 4, b"),
                _task_line(7, "job 4, c"),
                _task_line(8, "job 4, d"),
            ],
            f"{TRY_LOG}.trigger.5.log": [_task_line(4, "job 5, a"), _task_line(5, "job 5, b")],
        }
    )

    messages = _read(trigger=task_trigger(None), tail_lines=4)

    assert _events(messages) == ["job 5, a", "job 5, b", "job 4, c", "job 4, d"]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "READ_MAX_BYTES", 100)
@mock.patch.object(trigger_log_reader, "_remote_handler_streams", autospec=True, return_value=True)
def test_a_task_triggers_streams_share_one_byte_budget(_, task_trigger, no_remote, triggerer_jobs):
    triggerer_jobs.return_value = [4, 3]
    no_remote.return_value = (["stream"], [iter([_task_line(1, "older"), _task_line(2, "newer")])])

    messages = _read(trigger=task_trigger(None))

    # Job 4's stream alone is over the budget: job 3's is not read.
    assert no_remote.call_count == 1
    assert _events(messages) == ["newer"]
    assert messages[0].sources[-1] == trigger_log_reader._NOT_READ_PAST_CAP


def test_keep_newest_keeps_at_most_max_lines():
    assert trigger_log_reader._keep_newest(["a", "bb", "ccc"], 100, max_lines=2) == (["bb", "ccc"], 9)
