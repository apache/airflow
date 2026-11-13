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
import json
from unittest import mock

import pytest

from airflow.models.trigger import Trigger, TriggerKind
from airflow.utils.log import trigger_log_reader
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.trigger_log import get_trigger_log_dir
from airflow.utils.log.trigger_log_reader import (
    _get_triggerer_job_ends,
    _get_triggerer_job_ids,
    read_trigger_logs,
)

from tests_common.test_utils.config import conf_vars


def _line(second: int, event: str, trigger_id: int = 7) -> str:
    # Every line of a watcher's own file carries its trigger_id.
    return json.dumps(
        {
            "timestamp": f"2026-10-21T12:00:{second:02d}Z",
            "event": event,
            "level": "info",
            "trigger_id": trigger_id,
        }
    )


def _dir(job_id: int | None = None, trigger_id: int = 7) -> str:
    """The watcher's log directory, or with ``job_id`` its directory on that triggerer job."""
    return get_trigger_log_dir(trigger_id, job_id)


def _write(path, *lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{line}\n" for line in lines))


def _trigger(trigger_id, owner, kind=TriggerKind.ASSET_WATCHER, **attrs):
    """A trigger of ``kind`` on the live triggerer ``owner`` (job id, hostname), or on none."""
    trigger = mock.Mock(spec=Trigger, id=trigger_id, kind=kind, **attrs)
    trigger.triggerer_job = mock.Mock(id=owner[0], hostname=owner[1]) if owner else None
    trigger.is_owner_alive.return_value = owner is not None
    return trigger


def _read(trigger_id, owner, tail_lines):
    return read_trigger_logs(_trigger(trigger_id, owner), tail_lines=tail_lines, session=mock.Mock())


def _events(messages):
    return [m.event for m in messages if not m.event.startswith("::")]


@pytest.fixture
def base_log_folder(tmp_path):
    with conf_vars({("logging", "base_log_folder"): str(tmp_path)}):
        yield tmp_path


@pytest.fixture(autouse=True)
def triggerer_jobs():
    with mock.patch.object(trigger_log_reader, "_get_triggerer_job_ids", return_value=[]) as jobs:
        yield jobs


@pytest.fixture(autouse=True)
def triggerer_job_ends():
    # No end known: the walk stops once it has tail_lines lines.
    with mock.patch.object(trigger_log_reader, "_get_triggerer_job_ends", return_value={}) as ends:
        yield ends


def _at(second):
    return datetime.datetime(2026, 10, 21, 12, 0, second, tzinfo=datetime.UTC)


@pytest.fixture
def no_remote():
    with mock.patch.object(trigger_log_reader, "_read_remote_streams", return_value=([], [])) as remote:
        yield remote


def _response(status_code, text, content_range=None):
    return mock.Mock(
        status_code=status_code, text=text, headers={"content-range": content_range} if content_range else {}
    )


@pytest.mark.usefixtures("no_remote")
def test_merges_every_triggerers_local_files_in_time_order_and_tails(base_log_folder):
    _write(base_log_folder / f"{_dir(3)}/rotated/trigger.log", _line(1, "a1"), _line(3, "a3"))
    _write(base_log_folder / f"{_dir(3)}/trigger.log", _line(5, "a5"))
    _write(base_log_folder / f"{_dir(4)}/trigger.log", _line(2, "b2"), _line(4, "b4"), _line(6, "b6"))
    _write(base_log_folder / f"{_dir(4, trigger_id=70)}/trigger.log", _line(0, "another trigger"))

    messages = _read(7, None, 4)

    assert _events(messages) == ["a3", "b4", "a5", "b6"]
    assert messages[0].sources == [
        f"{_dir(4)}/trigger.log",
        f"{_dir(3)}/rotated/trigger.log",
        f"{_dir(3)}/trigger.log",
    ]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_live_tail_asks_the_owner_for_a_range_and_drops_the_partial_first_line(fetch):
    fetch.side_effect = [
        _response(
            206,
            "ial line\n" + _line(1, "x") + "\n" + _line(2, "y") + "\n",
            content_range="bytes 9000-9999/10000",
        ),
        _response(404, ""),
    ]

    messages = _read(7, (3, "trig-a"), 2)

    assert fetch.call_args_list[0] == mock.call(
        f"http://trig-a:8794/log/{_dir(3)}/trigger.log",
        f"{_dir(3)}/trigger.log",
        headers={"Range": "bytes=-4096"},
    )
    assert _events(messages) == ["x", "y"]
    assert messages[0].sources == [f"http://trig-a:8794/log/{_dir(3)}/trigger.log"]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_live_log_server_that_stops_mid_body_is_a_source_note_not_an_error(fetch, no_remote):
    import requests

    response = mock.Mock(status_code=206, headers={"content-range": "bytes 0-99/100"})
    type(response).text = mock.PropertyMock(
        side_effect=requests.exceptions.ConnectionError("Read timed out.")
    )
    fetch.return_value = response
    no_remote.return_value = (
        ["s3://b/triggers/trigger_id=7/triggerer_job_id=3/trigger.log"],
        [iter([_line(1, "stored")])],
    )

    messages = _read(7, (3, "trig-a"), 2)

    assert _events(messages) == ["stored"]
    url = "http://trig-a:8794/log/triggers/trigger_id=7/triggerer_job_id=3/trigger.log"
    assert any(source.startswith(f"Could not read {url}") for source in messages[0].sources)


@pytest.mark.usefixtures("no_remote", "base_log_folder")
@conf_vars({("logging", "trigger_log_server_port"): None, ("logging", "triggerer_log_server_port"): "9002"})
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_live_tail_uses_the_deprecated_port_option_as_task_logs_do(fetch):
    fetch.return_value = _response(200, _line(1, "x") + "\n")

    _read(7, (3, "trig-a"), 1)

    assert fetch.call_args_list[0].args[0] == f"http://trig-a:9002/log/{_dir(3)}/trigger.log"


@pytest.mark.usefixtures("no_remote", "base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_range_from_the_start_keeps_its_first_line(fetch):
    fetch.return_value = _response(
        206, _line(1, "x") + "\n" + _line(2, "y") + "\n", content_range="bytes 0-99/100"
    )

    assert _events(_read(7, (3, "trig-a"), 2)) == ["x", "y"]


@pytest.mark.usefixtures("no_remote")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_lines_found_live_and_locally_appear_once(fetch, base_log_folder):
    fetch.return_value = _response(200, _line(1, "x") + "\n" + _line(2, "y") + "\n")
    _write(base_log_folder / f"{_dir(3)}/trigger.log", _line(1, "x"), _line(2, "y"))

    assert _events(_read(7, (3, "trig-a"), 2)) == ["x", "y"]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_short_live_file_is_completed_from_the_owners_rotated_file(fetch):
    # Right after the local cap rotated the file, most of the history is in rotated/ on the owner.
    fetch.side_effect = [
        _response(200, _line(5, "new") + "\n"),
        _response(206, "tial\n" + _line(3, "old3") + "\n" + _line(4, "old4") + "\n", "bytes 10-99/100"),
    ]

    messages = _read(7, (3, "trig-a"), 3)

    assert fetch.call_args_list[1] == mock.call(
        f"http://trig-a:8794/log/{_dir(3)}/rotated/trigger.log",
        f"{_dir(3)}/rotated/trigger.log",
        headers={"Range": "bytes=-4096"},
    )
    assert _events(messages) == ["old3", "old4", "new"]
    assert messages[0].sources == [
        f"http://trig-a:8794/log/{_dir(3)}/trigger.log",
        f"http://trig-a:8794/log/{_dir(3)}/rotated/trigger.log",
    ]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_no_rotated_file_on_the_owner_is_not_reported(fetch):
    fetch.side_effect = [_response(200, _line(5, "new") + "\n"), _response(404, "")]

    messages = _read(7, (3, "trig-a"), 3)

    assert _events(messages) == ["new"]
    assert messages[0].sources == [f"http://trig-a:8794/log/{_dir(3)}/trigger.log"]


@pytest.mark.usefixtures("no_remote")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_an_unreachable_owner_is_reported_and_the_rest_still_read(fetch, base_log_folder):
    fetch.side_effect = ConnectionError("refused")
    _write(base_log_folder / f"{_dir(2)}/trigger.log", _line(1, "older"))

    messages = _read(7, (3, "trig-a"), 10)

    assert _events(messages) == ["older"]
    assert messages[0].sources[0] == f"Could not read http://trig-a:8794/log/{_dir(3)}/trigger.log: refused"


@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_lines_also_streamed_to_cloudwatch_appear_once(fetch, no_remote, base_log_folder):
    def line(stamp, event):
        return json.dumps({"timestamp": stamp, "event": event, "level": "info"})

    # CloudWatch already holds the live file's lines, with their time cut to the millisecond.
    fetch.return_value = _response(
        200, line("2026-10-21T12:00:01.123456Z", "x") + "\n" + line("2026-10-21T12:00:02.000999Z", "y") + "\n"
    )
    no_remote.return_value = (
        [f"Reading remote log from Cloudwatch log_group: watchers log_stream: {_dir(3)}/trigger.log"],
        [
            iter(
                [line("2026-10-21T12:00:01.123000+00:00", "x"), line("2026-10-21T12:00:02.000000+00:00", "y")]
            )
        ],
    )

    assert _events(_read(7, (3, "trig-a"), 10)) == ["x", "y"]


@pytest.mark.usefixtures("base_log_folder")
def test_remote_logs_are_found_by_the_triggers_prefix(no_remote):
    no_remote.return_value = ([f"s3://bucket/{_dir(3)}/trigger.log"], [iter([_line(1, "uploaded")])])

    messages = _read(7, None, 10)

    no_remote.assert_called_once_with(f"{_dir()}/")
    assert _events(messages) == ["uploaded"]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
def test_no_logs():
    assert _events(_read(7, None, 10)) == ["No logs found."]


@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_no_file_yet_on_a_new_owner_is_said_plainly_and_older_lines_still_read(
    fetch, base_log_folder, no_remote
):
    # kill -9 of the old owner: the new one has not written its file yet.
    fetch.return_value = _response(404, "")
    no_remote.return_value = ([f"s3://bucket/{_dir(3)}/trigger.log"], [iter([_line(1, "older")])])

    messages = _read(7, (4, "trig-b"), 10)

    assert _events(messages) == ["older"]
    assert messages[0].sources == [
        f"http://trig-b:8794/log/{_dir(4)}/trigger.log: no file yet, the trigger has not logged on this triggerer",
        f"s3://bucket/{_dir(3)}/trigger.log",
    ]


@pytest.fixture
def task_trigger():
    ti = mock.Mock(id="ti", try_number=2)
    with (
        mock.patch.object(trigger_log_reader, "prepare_task_log_contexts", return_value={"ti": "ctx"}),
        mock.patch.object(
            trigger_log_reader,
            "render_task_log_filename",
            return_value="dag_id=d/run_id=r/task_id=t/attempt=2.log",
        ),
    ):
        yield lambda owner: _trigger(5, owner, TriggerKind.TASK, task_instance=ti)


@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_task_trigger_reads_only_the_trigger_part_of_its_task_try_log(
    fetch, task_trigger, base_log_folder, no_remote
):
    fetch.return_value = _response(200, _line(9, "live on job 4") + "\n")
    _write(base_log_folder / "dag_id=d/run_id=r/task_id=t/attempt=2.log", _line(1, "the worker's lines"))
    _write(base_log_folder / "dag_id=d/run_id=r/task_id=t/attempt=2.log.trigger.3.log", _line(2, "on job 3"))
    _write(base_log_folder / "dag_id=d/run_id=r/task_id=t/attempt=1.log.trigger.3.log", _line(0, "try 1"))

    messages = read_trigger_logs(task_trigger((4, "trig-b")), tail_lines=10, session=mock.Mock())

    assert _events(messages) == ["on job 3", "live on job 4"]
    assert fetch.call_args.args[1] == "dag_id=d/run_id=r/task_id=t/attempt=2.log.trigger.4.log"
    no_remote.assert_called_once_with("dag_id=d/run_id=r/task_id=t/attempt=2.log.trigger.")


def _other_triggerer_suffix(full_path, job_id=None):
    return f"{full_path}.trg" + (f".{job_id}.log" if job_id else "")


@mock.patch.object(FileTaskHandler, "add_triggerer_suffix", side_effect=_other_triggerer_suffix)
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_task_trigger_log_is_named_as_the_task_log_handler_names_it(
    fetch, _, task_trigger, base_log_folder, no_remote
):
    fetch.return_value = _response(200, _line(9, "live on job 4") + "\n")
    _write(base_log_folder / "dag_id=d/run_id=r/task_id=t/attempt=2.log.trg.3.log", _line(2, "on job 3"))

    messages = read_trigger_logs(task_trigger((4, "trig-b")), tail_lines=10, session=mock.Mock())

    assert _events(messages) == ["on job 3", "live on job 4"]
    assert fetch.call_args.args[1] == "dag_id=d/run_id=r/task_id=t/attempt=2.log.trg.4.log"
    no_remote.assert_called_once_with("dag_id=d/run_id=r/task_id=t/attempt=2.log.trg.")


@pytest.mark.parametrize(
    "data",
    [
        pytest.param({"dag_id": "d", "run_id": "r", "kwargs": {}}, id="run-id-stored"),
        pytest.param(
            {"dag_id": "d", "kwargs": {"context": {"dag_run": {"dag_run_id": "r"}}}},
            id="run-in-stored-context",
        ),
    ],
)
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_callback_trigger_reads_the_callback_log(fetch, base_log_folder, no_remote, data):
    callback = mock.Mock(id="0199", data=data)
    _write(base_log_folder / "triggerer_callbacks/d/r/0199/callback.log", _line(1, "callback ran"))

    messages = read_trigger_logs(
        _trigger(6, None, TriggerKind.CALLBACK, callback=callback), tail_lines=10, session=mock.Mock()
    )

    assert _events(messages) == ["callback ran"]
    fetch.assert_not_called()
    no_remote.assert_called_once_with("triggerer_callbacks/d/r/0199/callback.log")


@pytest.mark.usefixtures("base_log_folder")
def test_a_callback_log_path_outside_the_log_folder_is_not_read(no_remote):
    callback = mock.Mock(id="0199", data={"dag_id": "..", "kwargs": {}})

    messages = read_trigger_logs(
        _trigger(6, None, TriggerKind.CALLBACK, callback=callback), tail_lines=10, session=mock.Mock()
    )

    assert _events(messages) == [
        "Not reading a log outside the log folder: triggerer_callbacks/../unknown/0199/callback.log"
    ]
    no_remote.assert_not_called()


@pytest.mark.usefixtures("base_log_folder")
@pytest.mark.parametrize("streams", [False, True], ids=["read-only-handler", "stream-handler"])
# The merge sorts only within a buffer, here two lines, as it would past HEAP_DUMP_SIZE lines of a long log.
@mock.patch("airflow.utils.log.file_task_handler.HALF_HEAP_DUMP_SIZE", 1)
@mock.patch("airflow.utils.log.file_task_handler.HEAP_DUMP_SIZE", 2)
@mock.patch("airflow.logging_config.get_remote_task_log")
def test_remote_logs_of_several_triggerers_merge_by_time(get_remote_task_log, streams):
    # Key order puts job 10's file before job 9's, though job 9 ran first.
    job_10 = _line(5, "job 10, first") + "\n" + _line(6, "job 10, last") + "\n"
    job_9 = _line(1, "job 9, first") + "\n" + _line(2, "job 9, last") + "\n"
    remote_io = mock.Mock(spec=["read", "stream"] if streams else ["read"])
    remote_io.read.return_value = (
        [f"s3://b/{_dir(10)}/trigger.log", f"s3://b/{_dir(9)}/trigger.log"],
        [job_10, job_9],
    )
    if streams:
        remote_io.stream.return_value = (
            [f"gs://b/{_dir(10)}/trigger.log", f"gs://b/{_dir(9)}/trigger.log"],
            [iter(job_10.splitlines()), iter(job_9.splitlines())],
        )
    get_remote_task_log.return_value = remote_io

    assert _events(_read(7, None, 3)) == ["job 9, last", "job 10, first", "job 10, last"]


@pytest.mark.usefixtures("base_log_folder")
def test_a_watcher_is_read_newest_triggerer_first_until_enough_lines(no_remote, triggerer_jobs):
    # Job 11 is alive but never ran the trigger; job 3 ran it long ago.
    triggerer_jobs.return_value = [11, 10, 9, 3]
    remote = {
        f"{_dir(10)}/": [_line(5, "job 10, a"), _line(6, "job 10, b")],
        f"{_dir(9)}/": [_line(1, "job 9, a"), _line(2, "job 9, b")],
        f"{_dir(3)}/": [_line(0, "job 3")],
    }
    no_remote.side_effect = lambda prefix: (
        ([f"s3://b/{prefix}trigger.log"], [iter(remote[prefix])]) if prefix in remote else ([], [])
    )

    messages = _read(7, None, 3)

    assert [c.args[0] for c in no_remote.call_args_list] == [
        f"{_dir(11)}/",
        f"{_dir(10)}/",
        f"{_dir(9)}/",
    ]
    assert _events(messages) == ["job 9, b", "job 10, a", "job 10, b"]
    assert messages[0].sources == [
        f"s3://b/{_dir(10)}/trigger.log",
        f"s3://b/{_dir(9)}/trigger.log",
        "Older logs, of triggerer job 3, were not read.",
    ]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_watcher_moved_to_an_older_triggerer_is_read_there_first(fetch, no_remote, triggerer_jobs):
    # Job 5 ran the watcher until it stopped; job 3, started before it, runs it now.
    triggerer_jobs.return_value = [3, 5]
    no_remote.side_effect = lambda prefix: (
        (
            ["s3://b/triggers/trigger_id=7/triggerer_job_id=5/trigger.log"],
            [iter([_line(1, "job 5, a"), _line(2, "job 5, b")])],
        )
        if prefix == "triggers/trigger_id=7/triggerer_job_id=5/"
        else ([], [])
    )
    live = "".join(f"{line}\n" for line in [_line(5, "job 3, a"), _line(6, "job 3, b")])
    fetch.return_value = _response(206, live, f"bytes 0-{len(live) - 1}/{len(live)}")

    messages = _read(7, (3, "trig-a"), 2)

    assert _events(messages) == ["job 3, a", "job 3, b"]
    assert messages[0].sources[-1] == "Older logs, of triggerer job 5, were not read."


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_watcher_back_on_a_triggerer_that_ran_it_before_keeps_the_lines_logged_in_between(
    fetch, no_remote, triggerer_jobs, triggerer_job_ends
):
    # Job 3 ran the watcher, job 5 took it over and stopped at 12:00:06, and job 3 runs it again: job 3's own file
    # holds tail_lines lines, two of them older than job 5's.
    triggerer_jobs.return_value = [3, 5]
    triggerer_job_ends.return_value = {5: _at(6)}
    no_remote.side_effect = lambda prefix: (
        (
            ["s3://b/triggers/trigger_id=7/triggerer_job_id=5/trigger.log"],
            [iter([_line(4, "job 5, a"), _line(5, "job 5, b"), _line(6, "job 5, c")])],
        )
        if prefix == "triggers/trigger_id=7/triggerer_job_id=5/"
        else ([], [])
    )
    live = "".join(
        f"{line}\n"
        for line in [_line(1, "job 3, a"), _line(2, "job 3, b"), _line(7, "job 3, c"), _line(8, "job 3, d")]
    )
    fetch.return_value = _response(206, live, f"bytes 0-{len(live) - 1}/{len(live)}")

    messages = _read(7, (3, "trig-a"), 4)

    assert _events(messages) == ["job 5, b", "job 5, c", "job 3, c", "job 3, d"]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_job_read_after_the_owner_gives_all_its_newer_lines_not_only_the_lines_still_missing(
    fetch, no_remote, triggerer_jobs, triggerer_job_ends
):
    # The owner, read first, has one line from before job 5 ran the watcher and one after: job 5's three lines,
    # one per uploaded part, are all newer than the first.
    triggerer_jobs.return_value = [3, 5]
    triggerer_job_ends.return_value = {5: _at(6)}
    no_remote.side_effect = lambda prefix: (
        (
            [
                f"s3://b/triggers/trigger_id=7/triggerer_job_id=5/{name}"
                for name in ("rotated/1.log", "rotated/2.log", "trigger.log")
            ],
            [iter([_line(4, "job 5, a")]), iter([_line(5, "job 5, b")]), iter([_line(6, "job 5, c")])],
        )
        if prefix == "triggers/trigger_id=7/triggerer_job_id=5/"
        else ([], [])
    )
    live = "".join(f"{line}\n" for line in [_line(1, "job 3, a"), _line(8, "job 3, b")])
    fetch.return_value = _response(206, live, f"bytes 0-{len(live) - 1}/{len(live)}")

    messages = _read(7, (3, "trig-a"), 4)

    assert _events(messages) == ["job 5, a", "job 5, b", "job 5, c", "job 3, b"]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_job_that_ended_before_the_oldest_line_kept_is_not_read(
    fetch, no_remote, triggerer_jobs, triggerer_job_ends
):
    triggerer_jobs.return_value = [3, 5, 2]
    triggerer_job_ends.return_value = {5: _at(4), 2: _at(1)}
    no_remote.side_effect = lambda prefix: (
        (
            ["s3://b/triggers/trigger_id=7/triggerer_job_id=5/trigger.log"],
            [iter([_line(3, "job 5, a"), _line(4, "job 5, b")])],
        )
        if prefix == "triggers/trigger_id=7/triggerer_job_id=5/"
        else ([], [])
    )
    live = "".join(f"{line}\n" for line in [_line(5, "job 3, a"), _line(6, "job 3, b")])
    fetch.return_value = _response(206, live, f"bytes 0-{len(live) - 1}/{len(live)}")

    messages = _read(7, (3, "trig-a"), 2)

    assert _events(messages) == ["job 3, a", "job 3, b"]
    assert [c.args[0] for c in no_remote.call_args_list] == ["triggers/trigger_id=7/triggerer_job_id=3/"]
    assert messages[0].sources[-1] == "Older logs, of triggerer jobs 2 to 5, were not read."


@pytest.mark.usefixtures("base_log_folder")
def test_a_request_reads_at_most_the_byte_cap_newest_lines_first(no_remote, triggerer_jobs):
    triggerer_jobs.return_value = [10, 9]
    remote = {
        f"{_dir(10)}/": [_line(s, f"job 10, {s}") for s in range(3, 6)],
        f"{_dir(9)}/": [_line(1, "job 9")],
    }
    no_remote.side_effect = lambda prefix: ([f"s3://b/{prefix}trigger.log"], [iter(remote[prefix])])

    with mock.patch.object(trigger_log_reader, "READ_MAX_BYTES", 2 * (len(_line(3, "job 10, 3")) + 1)):
        messages = _read(7, None, 10)

    assert _events(messages) == ["job 10, 4", "job 10, 5"]
    assert [c.args[0] for c in no_remote.call_args_list] == [f"{_dir(10)}/"]
    assert messages[0].sources[-1].startswith("Older lines were not read: a request reads at most")


@pytest.mark.usefixtures("no_remote")
def test_triggerers_of_a_watcher_are_also_found_from_its_local_directories(base_log_folder, triggerer_jobs):
    triggerer_jobs.return_value = [4]
    _write(base_log_folder / f"{_dir(4)}/trigger.log", _line(5, "job 4"))
    _write(base_log_folder / f"{_dir(2)}/rotated/trigger.log", _line(1, "job 2, rotated"))
    _write(base_log_folder / f"{_dir(2)}/trigger.log", _line(2, "job 2"))

    assert _events(_read(7, None, 2)) == ["job 2", "job 4"]
    assert _events(_read(7, None, 1)) == ["job 4"]


@pytest.mark.db_test
def test_the_triggerers_of_a_watcher_are_those_alive_since_it_was_created(session):
    from airflow._shared.timezones import timezone
    from airflow.jobs.job import Job

    created = timezone.utcnow()
    before = Job(job_type="TriggererJob", latest_heartbeat=created - datetime.timedelta(minutes=1))
    old_alive = Job(job_type="TriggererJob", latest_heartbeat=created + datetime.timedelta(seconds=1))
    new = Job(job_type="TriggererJob", latest_heartbeat=created + datetime.timedelta(seconds=2))
    scheduler = Job(job_type="SchedulerJob", latest_heartbeat=created + datetime.timedelta(seconds=2))
    session.add_all([before, old_alive, new, scheduler])
    session.flush()

    job_ids = _get_triggerer_job_ids(mock.Mock(created_date=created), session=session)

    ours = {before.id, old_alive.id, new.id, scheduler.id}
    assert [job_id for job_id in job_ids if job_id in ours] == [new.id, old_alive.id]
    session.rollback()


@pytest.mark.db_test
def test_the_triggerers_of_a_watcher_are_returned_the_last_to_heartbeat_first(session):
    from airflow._shared.timezones import timezone
    from airflow.jobs.job import Job

    created = timezone.utcnow()
    older_alive = Job(job_type="TriggererJob", latest_heartbeat=created + datetime.timedelta(seconds=2))
    newer_stopped = Job(job_type="TriggererJob", latest_heartbeat=created + datetime.timedelta(seconds=1))
    session.add(older_alive)
    session.flush()
    session.add(newer_stopped)
    session.flush()

    job_ids = _get_triggerer_job_ids(mock.Mock(created_date=created), session=session)

    ours = {older_alive.id, newer_stopped.id}
    assert [job_id for job_id in job_ids if job_id in ours] == [older_alive.id, newer_stopped.id]
    session.rollback()


@pytest.mark.db_test
@conf_vars({("triggerer", "triggerer_health_check_threshold"): "30"})
def test_a_triggerer_job_ended_at_its_end_else_its_last_heartbeat_and_the_health_threshold(session):
    from airflow._shared.timezones import timezone
    from airflow.jobs.job import Job

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


@pytest.mark.usefixtures("base_log_folder")
def test_lines_that_share_only_their_text_are_all_kept(no_remote):
    def line(n):
        return json.dumps({"timestamp": "2026-10-21T12:00:01.000100Z", "event": "received", "n": n})

    no_remote.return_value = ([f"s3://b/{_dir(3)}/trigger.log"], [iter([line(1), line(2), line(3)])])

    messages = _read(7, None, 10)

    assert [m.model_dump().get("n") for m in messages if m.event == "received"] == [1, 2, 3]


@pytest.mark.usefixtures("no_remote", "base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_long_live_lines_are_read_by_asking_for_a_larger_range(fetch):
    long = [_line(s, "x" * 3000) for s in range(1, 4)]
    text = "".join(f"{line}\n" for line in long)
    fetch.side_effect = [
        # 4 KiB: the tail of one line only.
        _response(206, text[-4096:], content_range=f"bytes {len(text) - 4096}-{len(text) - 1}/{len(text)}"),
        _response(206, text, content_range=f"bytes 0-{len(text) - 1}/{len(text)}"),
    ]

    messages = _read(7, (3, "trig-a"), 2)

    assert [len(e) for e in _events(messages)] == [3000, 3000]
    assert fetch.call_args_list[1].kwargs["headers"] == {"Range": f"bytes=-{4096 * 8}"}


@pytest.mark.usefixtures("no_remote")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_an_empty_live_file_is_not_an_error(fetch, base_log_folder):
    # Just rotated: no range of an empty file can be served.
    fetch.side_effect = [_response(416, ""), _response(206, _line(1, "older") + "\n", "bytes 0-99/100")]

    messages = _read(7, (3, "trig-a"), 2)

    assert _events(messages) == ["older"]
    assert messages[0].sources == [
        f"http://trig-a:8794/log/{_dir(3)}/trigger.log",
        f"http://trig-a:8794/log/{_dir(3)}/rotated/trigger.log",
    ]


@pytest.mark.usefixtures("base_log_folder")
def test_a_watcher_is_looked_for_on_at_most_the_newest_triggerer_jobs(no_remote, triggerer_jobs):
    # Every triggerer start is a job, and each job looked at is a remote read, even if it never ran the watcher.
    triggerer_jobs.return_value = list(range(100, 0, -1))
    no_remote.side_effect = lambda prefix: (
        ([f"s3://b/{_dir(98)}/trigger.log"], [iter([_line(1, "job 98")])])
        if prefix == f"{_dir(98)}/"
        else ([], [])
    )

    with mock.patch.object(trigger_log_reader, "READ_MAX_JOBS", 3):
        messages = _read(7, None, 10)

    assert triggerer_jobs.call_args.kwargs["limit"] == 4
    assert [c.args[0] for c in no_remote.call_args_list] == [
        f"{_dir(100)}/",
        f"{_dir(99)}/",
        f"{_dir(98)}/",
    ]
    assert _events(messages) == ["job 98"]
    assert messages[0].sources[-1] == "Logs of triggerer jobs before 98 were not looked for."


def test_after_a_remote_read_fails_older_jobs_are_read_only_locally(
    no_remote, triggerer_jobs, base_log_folder
):
    # An unreachable store makes each read wait out its timeout and retries: minutes per job.
    triggerer_jobs.return_value = [10, 9, 8]
    no_remote.side_effect = TimeoutError("connect timeout")
    _write(base_log_folder / f"{_dir(8)}/trigger.log", _line(1, "job 8"))

    messages = _read(7, None, 10)

    assert [c.args[0] for c in no_remote.call_args_list] == [f"{_dir(10)}/"]
    assert _events(messages) == ["job 8"]
    assert messages[0].sources == [
        "Could not read remote logs: connect timeout",
        "Remote logs of older triggerer jobs were not read: the remote log store failed.",
        f"{_dir(8)}/trigger.log",
    ]


@pytest.mark.usefixtures("base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_remote_stream_that_fails_while_read_is_reported_and_the_live_lines_kept(fetch, no_remote):
    # CloudWatch's streams are lazy: a throttled or unreachable endpoint fails only once they are read.
    def throttled():
        raise RuntimeError("ThrottlingException")
        yield

    fetch.return_value = _response(200, _line(5, "live") + "\n")
    no_remote.return_value = (["Reading remote log from Cloudwatch"], [throttled()])

    messages = _read(7, (3, "trig-a"), 10)

    assert _events(messages) == ["live"]
    assert "Could not read remote logs: ThrottlingException" in messages[0].sources


@pytest.mark.usefixtures("no_remote", "base_log_folder")
@mock.patch.object(trigger_log_reader, "_fetch_logs_from_service", autospec=True)
def test_a_live_line_with_unicode_line_separators_stays_whole(fetch):
    event = "ls\u2028ps\u2029nel\x85end"
    line = json.dumps(
        {"timestamp": "2026-10-21T12:00:01Z", "event": event, "level": "info"}, ensure_ascii=False
    )
    fetch.return_value = _response(200, line + "\n")

    assert _events(_read(7, (3, "trig-a"), 10)) == [event]


@pytest.mark.usefixtures("base_log_folder")
def test_remote_lines_without_the_watchers_trigger_id_are_dropped(no_remote, triggerer_jobs):
    # A handler that reads a job directory as one stream name (CloudWatch in older amazon providers) answers
    # with a notice stamped now, one per triggerer job, which would push the watcher's own lines out.
    triggerer_jobs.return_value = [4, 3]
    notice = json.dumps({"timestamp": "2026-10-21T12:59:59Z", "event": "No log stream found."})
    own = {f"{_dir(4)}/": _line(2, "job 4"), f"{_dir(3)}/": _line(1, "job 3")}
    no_remote.side_effect = lambda prefix: (
        [f"cloudwatch {prefix}"],
        [iter([own[prefix], _line(3, "another trigger", trigger_id=70), notice])],
    )

    assert _events(_read(7, None, 2)) == ["job 3", "job 4"]


def _remote_files(files, read=None):
    """Answer a triggerer's prefix with ``files`` (name: lines) as a store lists them, in name order."""

    def stream(name):
        if read is not None:
            read.append(name)
        yield from files[name]

    return lambda prefix: ([f"s3://b/{prefix}{name}" for name in files], [stream(name) for name in files])


@pytest.mark.usefixtures("base_log_folder")
def test_a_watchers_remote_files_are_read_newest_first_until_enough_lines(no_remote, triggerer_jobs):
    # A busy watcher's rotated files are one remote object each; their numbers put them in age order.
    triggerer_jobs.return_value = [3]
    files = {
        "rotated/trigger.000001.log": [_line(1, "part 1")],
        "rotated/trigger.000002.log": [_line(2, "part 2"), _line(3, "part 2, b")],
        "rotated/trigger.000003.log": [_line(4, "part 3")],
        "trigger.log": [_line(5, "uploaded when it left")],
    }
    read: list[str] = []
    no_remote.side_effect = _remote_files(files, read)

    assert _events(_read(7, None, 10)) == ["part 1", "part 2", "part 2, b", "part 3", "uploaded when it left"]
    read.clear()
    assert _events(_read(7, None, 3)) == ["part 2, b", "part 3", "uploaded when it left"]
    assert read == ["trigger.log", "rotated/trigger.000003.log", "rotated/trigger.000002.log"]


@pytest.mark.usefixtures("base_log_folder")
def test_the_byte_cap_holds_across_a_watchers_remote_files(no_remote, triggerer_jobs):
    triggerer_jobs.return_value = [3]
    no_remote.side_effect = _remote_files(
        {f"rotated/trigger.00000{n}.log": [_line(n, f"part {n}")] for n in range(1, 5)}
    )

    with mock.patch.object(trigger_log_reader, "READ_MAX_BYTES", 2 * (len(_line(1, "part 1")) + 1)):
        messages = _read(7, None, 10)

    assert _events(messages) == ["part 3", "part 4"]
    assert messages[0].sources[-1].startswith("Older lines were not read: a request reads at most")


@pytest.mark.usefixtures("no_remote")
def test_a_watchers_numbered_rotated_file_is_read_from_a_shared_disk(base_log_folder):
    # Its upload failed, or the handler keeps the file after uploading it (HDFS).
    _write(base_log_folder / f"{_dir(3)}/rotated/trigger.000002.log", _line(1, "part 2"))
    _write(base_log_folder / f"{_dir(3)}/trigger.log", _line(2, "live"))

    messages = _read(7, None, 10)

    assert _events(messages) == ["part 2", "live"]
    assert messages[0].sources == [f"{_dir(3)}/rotated/trigger.000002.log", f"{_dir(3)}/trigger.log"]


@pytest.mark.usefixtures("no_remote")
def test_a_watchers_file_linked_from_outside_the_log_folder_is_not_read(base_log_folder, tmp_path_factory):
    outside = tmp_path_factory.mktemp("outside") / "secret.log"
    _write(outside, _line(1, "not a log"))
    (base_log_folder / f"{_dir(3)}/rotated").mkdir(parents=True)
    (base_log_folder / f"{_dir(3)}/rotated/trigger.000001.log").symlink_to(outside)
    _write(base_log_folder / f"{_dir(3)}/trigger.log", _line(2, "live"))

    messages = _read(7, None, 10)

    assert _events(messages) == ["live"]
    assert messages[0].sources == [f"{_dir(3)}/trigger.log"]


@pytest.mark.usefixtures("base_log_folder")
def test_lines_of_the_same_millisecond_stay_in_order_across_a_watchers_files(no_remote, triggerer_jobs):
    # A rotation in the middle of a burst: the end of one file and the start of the next share a millisecond.
    triggerer_jobs.return_value = [3]
    no_remote.side_effect = _remote_files(
        {
            "rotated/trigger.000001.log": [_line(1, "a"), _line(2, "b")],
            "rotated/trigger.000002.log": [_line(2, "c"), _line(3, "d")],
            "trigger.log": [_line(3, "e")],
        }
    )

    assert _events(_read(7, None, 10)) == ["a", "b", "c", "d", "e"]
