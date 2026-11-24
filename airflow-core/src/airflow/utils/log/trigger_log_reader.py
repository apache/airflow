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
"""Read a trigger's log lines, whatever its kind, live from its triggerer, from remote logging and locally."""

from __future__ import annotations

import datetime
import functools
import heapq
import inspect
import itertools
import json
import os
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import not_, select

from airflow.configuration import conf
from airflow.jobs.job import Job
from airflow.logging_config import get_remote_task_log
from airflow.models.trigger import TriggerKind
from airflow.utils.log.file_task_handler import (
    DEFAULT_SORT_DATETIME,
    FileTaskHandler,
    LogType,
    StructuredLogMessage,
    _get_compatible_log_stream,
    fetch_logs_from_service,
    get_log_server_url,
    log_stream_to_parsed_log_stream,
)
from airflow.utils.log.path_log_reader import read_local_logs, validate_log_path_component
from airflow.utils.log.task_log_address import prepare_task_log_contexts, render_task_log_filename
from airflow.utils.log.trigger_log import (
    TRIGGERER_JOB_ID_KEY,
    get_callback_trigger_log_relative_path,
    get_trigger_log_dir,
    get_trigger_log_held_path,
    get_trigger_log_relative_path,
    get_trigger_log_rotated_path,
    list_trigger_log_files,
    parse_trigger_log_dir_id,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Sequence

    from sqlalchemy.orm import Session

    from airflow._shared.logging.remote import RawLogStream
    from airflow.models.trigger import Trigger

# Bytes asked of a log server per line; a tail with fewer whole lines is asked again, GROWTH times larger.
LIVE_TAIL_BYTES_PER_LINE = 2048
LIVE_TAIL_GROWTH = 8
LIVE_TAIL_MAX_BYTES = 64 * 1024 * 1024
# Most bytes of log lines one request keeps; past it, older files and triggerer jobs are not read.
READ_MAX_BYTES = 64 * 1024 * 1024
_REMOTE_READ_FAILED = "Could not read remote logs"
_REMOTE_NOT_READ_AFTER_FAILURE = (
    "Remote logs of older triggerer jobs were not read: the remote log store failed."
)
_NOT_READ_PAST_CAP = f"Older lines were not shown: a request keeps at most {READ_MAX_BYTES // 2**20} MiB."
# Most triggerer jobs looked at for a watcher: each costs a remote read, and every triggerer start is a job.
READ_MAX_JOBS = 50
# Recent lines remembered to drop copies read from another source.
_SEEN_MAX = 10_000


def read_trigger_logs(trigger: Trigger, *, tail_lines: int, session: Session) -> list[StructuredLogMessage]:
    """
    Return the last ``tail_lines`` lines a trigger logged, read live, from remote logging and from local disk.

    A task's trigger logs to ``<try log>.trigger.<triggerer job id>.log``, a callback's to the callback's log,
    and an asset watcher to its own files. Needs ``triggerer_job`` and the owner loaded.
    """
    job = trigger.triggerer_job
    owner = (job.id, job.hostname) if job is not None and trigger.is_owner_alive() else None
    kind = trigger.kind
    if kind == TriggerKind.TASK and (ti := trigger.task_instance) is not None:
        context = prepare_task_log_contexts([ti], session=session)[ti.id]
        try_log = render_task_log_filename(ti, ti.try_number, context=context)
        prefix = f"{FileTaskHandler.add_triggerer_suffix(try_log)}."
        live = FileTaskHandler.add_triggerer_suffix(try_log, job_id=owner[0]) if owner else None
        if _remote_handler_streams() and (
            job_ids := _get_triggerer_job_ids(trigger, session=session, limit=READ_MAX_JOBS)
        ):
            return _read_task_trigger_streams(
                str(ti.id),
                try_log,
                prefix,
                job_ids,
                live=live,
                owner=owner,
                tail_lines=tail_lines,
                session=session,
            )
        return _read_tail(stored=prefix, local=[prefix], live=live, owner=owner, tail_lines=tail_lines)
    if kind == TriggerKind.CALLBACK and (callback := trigger.callback) is not None:
        path = get_callback_trigger_log_relative_path(callback)
        try:
            for component in path.split("/"):
                validate_log_path_component(component)
        except ValueError:
            return [StructuredLogMessage(event=f"Not reading a log outside the log folder: {path}")]
        return _read_tail(
            stored=path, local=[path], live=path if owner else None, owner=owner, tail_lines=tail_lines
        )
    return _read_watcher_tail(trigger, owner=owner, tail_lines=tail_lines, session=session)


def _read_tail(
    *,
    stored: str,
    local: list[str],
    live: str | None,
    owner: tuple[int, str | None] | None,
    tail_lines: int,
) -> list[StructuredLogMessage]:
    sources, lines, size = _read_sources(
        stored=stored, local=local, live=live, owner=owner, tail_lines=tail_lines, max_bytes=READ_MAX_BYTES
    )
    if size > READ_MAX_BYTES:
        sources.append(_NOT_READ_PAST_CAP)
    return _build_tail(sources, lines, tail_lines=tail_lines)


def _read_task_trigger_streams(
    ti_id: str,
    try_log: str,
    prefix: str,
    job_ids: list[int],
    *,
    live: str | None,
    owner: tuple[int, str | None] | None,
    tail_lines: int,
    session: Session,
) -> list[StructuredLogMessage]:
    """Read a task trigger's remote streams by name: a streaming handler keeps one per triggerer job."""
    sources, lines, size = _read_sources(
        stored=prefix,
        local=[prefix],
        live=live,
        owner=owner,
        tail_lines=tail_lines,
        max_bytes=READ_MAX_BYTES,
        remote=False,
    )
    found = 0
    ends: dict[int, datetime.datetime] | None = None
    for job_id in job_ids:
        if size >= READ_MAX_BYTES:
            break
        if found >= tail_lines:
            # The trigger can be back on a triggerer that ran it before: read on while a job wrote after them.
            if ends is None:
                ends = _get_triggerer_job_ends(job_ids, session=session)
            if _ended_before_tail(ends.get(job_id), lines, tail_lines=tail_lines):
                break
        job_sources, job_lines, job_size = _read_sources(
            stored=FileTaskHandler.add_triggerer_suffix(try_log, job_id=job_id),
            local=[],
            live=None,
            owner=None,
            tail_lines=tail_lines,
            max_bytes=READ_MAX_BYTES - size,
        )
        # A job that never ran the trigger answers with a notice, not a line of the try.
        job_lines = [stream for stream in job_lines if next(_filter_lines(stream, "ti_id", ti_id), None)]
        failed = any(s.startswith(_REMOTE_READ_FAILED) for s in job_sources)
        if job_lines or failed:
            sources.extend(job_sources)
        lines.extend(job_lines)
        found += sum(map(len, job_lines))
        size += job_size
        if failed:
            sources.append(_REMOTE_NOT_READ_AFTER_FAILURE)
            break
    if size > READ_MAX_BYTES:
        sources.append(_NOT_READ_PAST_CAP)
    return _build_tail(sources, lines, tail_lines=tail_lines)


def _read_watcher_tail(
    trigger: Trigger, *, owner: tuple[int, str | None] | None, tail_lines: int, session: Session
) -> list[StructuredLogMessage]:
    """
    Read an asset watcher's files newest triggerer first, and stop once enough lines are read.

    With no triggerer known, as after ``airflow db clean``, the trigger's whole directory is read.
    """
    base = Path(os.path.realpath(conf.get("logging", "base_log_folder")))
    local_by_job: dict[int, list[str]] = {}
    for job_dir in sorted(base.glob(f"{get_trigger_log_dir(trigger.id)}/{TRIGGERER_JOB_ID_KEY}=*")):
        files = [
            path.relative_to(base).as_posix() for path in list_trigger_log_files(job_dir) if path.is_file()
        ]
        if files and (job_id := parse_trigger_log_dir_id(TRIGGERER_JOB_ID_KEY, job_dir.name)) is not None:
            local_by_job.setdefault(job_id, []).extend(files)
    recent = _get_triggerer_job_ids(trigger, session=session, limit=READ_MAX_JOBS + 1)
    # The owner first, then the last to heartbeat: a watcher moves to an older job when a newer one stops.
    job_ids = list(
        dict.fromkeys(
            [
                *([owner[0]] if owner else []),
                *recent[:READ_MAX_JOBS],
                *sorted(local_by_job.keys() - set(recent[:READ_MAX_JOBS]), reverse=True),
            ]
        )
    )
    more_jobs = len(recent) > READ_MAX_JOBS
    dead_on_live_host = _get_last_dead_jobs_on_live_hosts(recent[:READ_MAX_JOBS], session=session)
    if not job_ids:
        # The trailing slash keeps "trigger_id=1/" from also matching "trigger_id=10/".
        return _read_tail(
            stored=f"{get_trigger_log_dir(trigger.id)}/",
            local=[],
            live=None,
            owner=None,
            tail_lines=tail_lines,
        )
    sources: list[str] = []
    lines: list[list[str]] = []
    found = read = 0
    remote = True
    streaming = _remote_handler_streams()
    ends: dict[int, datetime.datetime] | None = None
    for job_id in job_ids:
        if read >= READ_MAX_BYTES:
            sources.append(_NOT_READ_PAST_CAP)
            break
        if found >= tail_lines:
            # A watcher can be back on a triggerer that ran it before: read on while a job wrote after them.
            if ends is None:
                ends = _get_triggerer_job_ends(job_ids, session=session)
            if _ended_before_tail(ends.get(job_id), lines, tail_lines=tail_lines):
                rest = job_ids[job_ids.index(job_id) :]
                jobs = f"job {job_id}" if len(rest) == 1 else f"jobs {min(rest)} to {max(rest)}"
                sources.append(f"Older logs, of triggerer {jobs}, were not read.")
                break
        is_owner = owner is not None and owner[0] == job_id
        if is_owner:
            live_on = owner
        elif job_id in dead_on_live_host:
            live_on = (job_id, dead_on_live_host[job_id])
        else:
            live_on = None
        live = get_trigger_log_relative_path(trigger.id, job_id) if live_on else None
        job_sources, job_lines, size = _read_sources(
            # A streaming handler keeps one stream per job, named after the live file: read it by name.
            stored=get_trigger_log_relative_path(trigger.id, job_id)
            if streaming
            else f"{get_trigger_log_dir(trigger.id, job_id)}/",
            local=local_by_job.get(job_id, []),
            live=live,
            owner=live_on,
            live_missing_ok=not is_owner,
            # Up to tail_lines from each: jobs are not read in time order.
            tail_lines=tail_lines,
            max_bytes=READ_MAX_BYTES - read,
            remote=remote,
            trigger_id=trigger.id,
        )
        if remote and any(s.startswith(_REMOTE_READ_FAILED) for s in job_sources):
            # Each attempt can take minutes: don't ask an unreachable store again for every older job.
            remote = False
            job_sources.append(_REMOTE_NOT_READ_AFTER_FAILURE)
        if is_owner or any(job_lines) or any(s.startswith("Could not read") for s in job_sources):
            sources.extend(job_sources)
        lines.extend(job_lines)
        found += sum(1 for _ in _merge_lines(job_lines))
        read += size
    else:
        if read > READ_MAX_BYTES:
            sources.append(_NOT_READ_PAST_CAP)
        elif more_jobs:
            sources.append(f"Logs of triggerer jobs before {min(job_ids)} were not looked for.")
    return _build_tail(sources, lines, tail_lines=tail_lines)


def _get_triggerer_job_ids(trigger: Trigger, *, session: Session, limit: int | None = None) -> list[int]:
    """Return the triggerer jobs that ran while the trigger existed, the last to heartbeat first."""
    return list(
        session.scalars(
            select(Job.id)
            .where(Job.job_type == "TriggererJob", Job.latest_heartbeat >= trigger.created_date)
            .order_by(Job.latest_heartbeat.desc(), Job.id.desc())
            .limit(limit)
        )
    )


def _get_last_dead_jobs_on_live_hosts(job_ids: list[int], *, session: Session) -> dict[int, str]:
    """
    Return the last of these triggerer jobs to stop on each host a live triggerer runs on, with that host.

    A triggerer restarted in place uploads the killed job's files a health check threshold after it starts;
    until then its log server serves them.
    """
    if not job_ids:
        return {}
    alive = Job.is_alive_clause(["TriggererJob"])
    rows = session.execute(
        select(Job.id, Job.hostname)
        .where(
            Job.id.in_(job_ids),
            not_(alive),
            Job.hostname.in_(select(Job.hostname).where(Job.job_type == "TriggererJob", alive)),
        )
        .order_by(Job.latest_heartbeat.desc(), Job.id.desc())
    )
    last: dict[str, int] = {}
    for job_id, hostname in rows:
        last.setdefault(hostname, job_id)
    return {job_id: hostname for hostname, job_id in last.items()}


def _get_triggerer_job_ends(job_ids: Iterable[int], *, session: Session) -> dict[int, datetime.datetime]:
    """Return when each of these triggerer jobs last wrote a line: its end, else when its heartbeat lapsed."""
    threshold = datetime.timedelta(seconds=conf.getfloat("triggerer", "triggerer_health_check_threshold"))
    return {
        job_id: end_date or latest_heartbeat + threshold
        for job_id, end_date, latest_heartbeat in session.execute(
            select(Job.id, Job.end_date, Job.latest_heartbeat).where(Job.id.in_(list(job_ids)))
        )
        if end_date or latest_heartbeat
    }


def _ended_before_tail(ended: datetime.datetime | None, lines: list[list[str]], *, tail_lines: int) -> bool:
    """Return whether ``tail_lines`` lines are kept and a job that ended at ``ended`` wrote none of them."""
    tail = deque(_merge_lines(lines), maxlen=tail_lines)
    if len(tail) < tail_lines:
        return False
    oldest = tail[0].timestamp
    return oldest is None or ended is None or ended < oldest


def _read_sources(
    *,
    stored: str,
    local: list[str],
    live: str | None,
    owner: tuple[int, str | None] | None,
    tail_lines: int,
    max_bytes: int,
    remote: bool = True,
    trigger_id: int | None = None,
    live_missing_ok: bool = False,
) -> tuple[list[str], list[list[str]], int]:
    """
    Read every copy of a log, keeping the newest ``max_bytes`` of each; also return the bytes read.

    ``local`` paths are read with the files whose name they start. With ``trigger_id``, a watcher's files on
    one triggerer: its held and rotated files too, and the remote ones newest first until they hold enough.
    """
    sources: list[str] = []
    streams: list[Iterable[str]] = []
    if live is not None and owner is not None and (hostname := owner[1]):
        lines, source = _read_live_tail_lines(
            live, hostname, tail_lines=tail_lines, missing_ok=live_missing_ok
        )
        if source:
            sources.append(source)
        streams.append(lines)
        found = len(lines)
        # Older lines until they are uploaded: a release held them, or the local size cap moved them aside.
        for older_path in (get_trigger_log_held_path(live), get_trigger_log_rotated_path(live)):
            if trigger_id is None or found >= tail_lines:
                break
            older, older_source = _read_live_tail_lines(
                older_path, hostname, tail_lines=tail_lines - found, missing_ok=True
            )
            if older:
                sources.append(older_source)
                streams.append(older)
                found += len(older)
    try:
        remote_sources, remote_streams = (
            _read_remote_streams(stored, tail_lines=tail_lines) if remote else ([], [])
        )
        # Streams can be lazy: read them inside this ``try``, or their errors fail the whole request.
        if trigger_id is None:
            remote_kept = [_keep_newest(stream, max_bytes, max_lines=tail_lines) for stream in remote_streams]
        else:
            # A handler can answer a job it has no stream for with a notice stamped now.
            of_trigger = [_filter_lines(stream, "trigger_id", trigger_id) for stream in remote_streams]
            remote_kept = [_keep_newest_files(of_trigger, tail_lines=tail_lines, max_bytes=max_bytes)]
    except Exception as e:
        remote_sources, remote_kept = [f"{_REMOTE_READ_FAILED}: {e}"], []
    sources.extend(remote_sources)
    if local:
        base = os.path.realpath(conf.get("logging", "base_log_folder"))
        for relative_path in local:
            local_sources, local_streams = read_local_logs(relative_path)
            sources.extend(os.path.relpath(source, base) for source in local_sources)
            streams.extend(local_streams)
    kept = [*remote_kept, *(_keep_newest(stream, max_bytes, max_lines=tail_lines) for stream in streams)]
    return sources, [lines for lines, _ in kept], sum(size for _, size in kept)


def _filter_lines(lines: Iterable[str], key: str, value: object) -> Generator[str, None, None]:
    """Keep the JSON lines whose ``key`` is ``value``."""
    for line in lines:
        if key not in line:
            continue
        try:
            fields = json.loads(line)
        except ValueError:
            continue
        if isinstance(fields, dict) and fields.get(key) == value:
            yield line


def _keep_newest(lines: Iterable[str], max_bytes: int, *, max_lines: int) -> tuple[list[str], int]:
    """
    Return the newest ``max_lines`` lines that fit in ``max_bytes``, and the size of all of them.

    The newest ``tail_lines`` of the merged streams are always among the newest ``tail_lines`` of each.
    """
    kept: deque[str] = deque()
    kept_size = size = 0
    for line in lines:
        if len(kept) == max_lines:
            kept_size -= len(kept.popleft()) + 1
        kept.append(line)
        kept_size += len(line) + 1
        size += len(line) + 1
        while kept_size > max_bytes and kept:
            kept_size -= len(kept.popleft()) + 1
    return list(kept), size


def _keep_newest_files(
    streams: Sequence[Iterable[str]], *, tail_lines: int, max_bytes: int
) -> tuple[list[str], int]:
    """
    Keep the newest lines of a watcher's remote files on one triggerer, given oldest first, read newest first.

    One stream: the merge orders lines of the same millisecond by their line number in their stream.
    """
    kept: list[list[str]] = []
    size = 0
    for stream in reversed(streams):
        if tail_lines <= 0 or max_bytes < size:
            break
        lines, file_size = _keep_newest(stream, max_bytes - size, max_lines=tail_lines)
        kept.append(lines)
        tail_lines, size = tail_lines - len(lines), size + file_size
    return [line for lines in reversed(kept) for line in lines], size


def _build_tail(sources: list[str], lines: list[list[str]], *, tail_lines: int) -> list[StructuredLogMessage]:
    tail = deque(_merge_lines(lines), maxlen=tail_lines)
    if not tail:
        tail.append(StructuredLogMessage(event="No logs found."))
        if not sources:
            return list(tail)
    return [
        StructuredLogMessage(event="::group::Log message source details", sources=sources),  # type: ignore[call-arg]
        StructuredLogMessage(event="::endgroup::"),
        *tail,
    ]


def _merge_lines(lines: list[list[str]]) -> Generator[StructuredLogMessage, None, None]:
    return _drop_copies(_interleave_in_time_order(*map(_iter_lines, lines)))


def _interleave_in_time_order(
    *streams: RawLogStream,
) -> Generator[tuple[int, StructuredLogMessage], None, None]:
    """
    Merge streams, each oldest first, by timestamp, then stream, then line; yield each line with its stream.

    ``_interleave_logs`` sorts only within a 5,000-line buffer: files that don't overlap come out of order.
    """

    def keyed(index: int, stream: RawLogStream):
        for timestamp, line_num, message in log_stream_to_parsed_log_stream(stream):
            yield (timestamp or DEFAULT_SORT_DATETIME, index, line_num), message

    for (_, index, _), message in heapq.merge(
        *(keyed(index, stream) for index, stream in enumerate(streams)), key=lambda record: record[0]
    ):
        yield index, message


def _remote_handler_streams() -> bool:
    """
    Whether the remote log handler streams each line as it is logged, which it does when it has processors.

    Looked up without building them: that can create remote resources, and fail with the store unreachable.
    """
    processors = inspect.getattr_static(get_remote_task_log(), "processors", None)
    if isinstance(processors, (property, functools.cached_property)):
        return True
    return bool(processors)


def _read_remote_streams(relative_path: str, *, tail_lines: int) -> tuple[list[str], list[RawLogStream]]:
    """
    Read the remote logs at ``relative_path``, one stream per log, so they merge by time, not in key order.

    A ``stream`` that takes ``tail_lines`` is asked for the end of each log only.
    """
    if (remote_io := get_remote_task_log()) is None:
        return [], []
    if stream_method := getattr(remote_io, "stream", None):
        kwargs = {"tail_lines": tail_lines} if _takes_tail_lines(stream_method) else {}
        sources, logs = stream_method(relative_path, None, **kwargs)
        return list(sources), list(logs or [])
    sources, logs = remote_io.read(relative_path, None)
    return list(sources), [_get_compatible_log_stream([log]) for log in logs or []]


def _takes_tail_lines(method) -> bool:
    try:
        return "tail_lines" in inspect.signature(method).parameters
    except (TypeError, ValueError):
        return False


def _drop_copies(
    messages: Iterable[tuple[int, StructuredLogMessage]],
) -> Generator[StructuredLogMessage, None, None]:
    """Drop a line already read from another stream, which may have kept its time only to the millisecond."""
    seen: dict[tuple[str, int], int] = {}
    for stream, message in messages:
        if message.timestamp is None:
            yield message
            continue
        # The whole line but its time: lines that share only their text are not copies.
        fields = message.model_dump(mode="json", exclude={"timestamp"}, exclude_none=True)
        event, ms = json.dumps(fields, sort_keys=True, default=str), int(message.timestamp.timestamp() * 1000)
        if any(seen.get((event, ms + d), stream) != stream for d in (0, -1, 1)):
            continue
        seen[(event, ms)] = stream
        if len(seen) > _SEEN_MAX:
            # Lines arrive in time order, so a copy is always of a recent line.
            for key in list(itertools.islice(seen, _SEEN_MAX // 2)):
                del seen[key]
        yield message


def _read_live_tail_lines(
    relative_path: str, hostname: str, *, tail_lines: int, missing_ok: bool = False
) -> tuple[list[str], str]:
    """Read at least ``tail_lines`` whole lines from the end of a live file, or all of it if it has fewer."""
    max_bytes = tail_lines * LIVE_TAIL_BYTES_PER_LINE
    while True:
        lines, source, whole_file = _read_live_tail(
            relative_path, hostname, max_bytes=max_bytes, missing_ok=missing_ok
        )
        if whole_file or len(lines) >= tail_lines or max_bytes >= LIVE_TAIL_MAX_BYTES:
            return lines[-tail_lines:], source
        max_bytes = min(max_bytes * LIVE_TAIL_GROWTH, LIVE_TAIL_MAX_BYTES)


def _read_live_tail(
    relative_path: str, hostname: str, *, max_bytes: int, missing_ok: bool = False
) -> tuple[list[str], str, bool]:
    """Return the lines in the last ``max_bytes`` of a live file, their source, and whether that was all."""
    url = get_log_server_url(hostname, relative_path, LogType.TRIGGER)
    try:
        response = fetch_logs_from_service(url, relative_path, headers={"Range": f"bytes=-{max_bytes}"})
        # The body is streamed: a log server that stops mid-body fails here.
        text = response.text if response.status_code in (200, 206) else ""
    except Exception as e:
        return [], f"Could not read {url}: {e}", True
    if response.status_code == 404 and missing_ok:
        return [], "", True
    if response.status_code == 404:
        return [], f"{url}: no file yet, the trigger has not logged on this triggerer", True
    if response.status_code == 416:
        # An empty file has no range to serve.
        return [], url, True
    if response.status_code not in (200, 206):
        return [], f"{url} returned {response.status_code}", True
    # Not str.splitlines(): it also splits at U+2028 and other characters JSON leaves unescaped in a line.
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    content_range = response.headers.get("content-range", "")
    whole_file = response.status_code == 200 or content_range.startswith("bytes 0-")
    if not whole_file:
        # The range starts in the middle of a line.
        lines = lines[1:]
    return lines, url, whole_file


def _iter_lines(lines: list[str]) -> RawLogStream:
    yield from lines
