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
"""Read a trigger's log lines, whatever its kind, from its triggerer, from remote logging and from the local disk."""

from __future__ import annotations

import datetime
import itertools
import json
import os
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.configuration import conf
from airflow.jobs.job import Job
from airflow.models.trigger import TriggerKind
from airflow.utils.log.file_task_handler import (
    FileTaskHandler,
    LogType,
    StructuredLogMessage,
    _fetch_logs_from_service,
    _get_compatible_log_stream,
    _get_log_server_url,
    _interleave_logs,
)
from airflow.utils.log.path_log_reader import _read_local_logs, validate_log_path_component
from airflow.utils.log.task_log_address import prepare_task_log_contexts, render_task_log_filename
from airflow.utils.log.trigger_log import (
    TRIGGER_LOG_FILE_NAME,
    TRIGGER_LOG_ROTATED_DIR,
    TRIGGERER_JOB_ID_KEY,
    get_callback_trigger_log_relative_path,
    get_trigger_log_dir,
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

# Bytes asked of the triggerer's log server per requested line; trigger lines are JSON, a few hundred bytes.
LIVE_TAIL_BYTES_PER_LINE = 2048
# A tail that came back with fewer whole lines than asked (longer lines: tracebacks, payload dumps) is asked
# again with a range this many times larger, up to LIVE_TAIL_MAX_BYTES.
LIVE_TAIL_GROWTH = 8
LIVE_TAIL_MAX_BYTES = 64 * 1024 * 1024
# Most bytes of log lines one request reads for a trigger; older lines are not read.
READ_MAX_BYTES = 64 * 1024 * 1024
_REMOTE_READ_FAILED = "Could not read remote logs"
_NOT_READ_PAST_CAP = f"Older lines were not read: a request reads at most {READ_MAX_BYTES // 2**20} MiB."
# Most triggerer jobs, newest first, a request looks at for a watcher (besides its owner and local files):
# each costs a remote read even when the job never ran the watcher, and every triggerer start is a job.
READ_MAX_JOBS = 50

# Recent lines remembered to drop copies read from another source.
_SEEN_MAX = 10_000


def read_trigger_logs(trigger: Trigger, *, tail_lines: int, session: Session) -> list[StructuredLogMessage]:
    """
    Return the last ``tail_lines`` lines a trigger logged, wherever they are, whatever its kind.

    A deferred task's trigger logs to its task try's log, ``<try log>.trigger.<triggerer job id>.log``, and a
    callback's trigger to the callback's log; for those only those files are read, as the owner's log reads
    them. An asset watcher, whose owner has no log, has its own files,
    ``triggers/trigger_id=<trigger id>/triggerer_job_id=<triggerer job id>/trigger.log``. Each is read from
    the triggerer running the trigger now, through its log server, from remote logging, and from this
    process's ``base_log_folder`` when it shares the triggerers' disk. Lines found in more than one place are
    returned once. Needs ``triggerer_job`` and the owner loaded.
    """
    job = trigger.triggerer_job
    owner = (job.id, job.hostname) if job is not None and trigger.is_owner_alive() else None
    kind = trigger.kind
    if kind == TriggerKind.TASK and (ti := trigger.task_instance) is not None:
        context = prepare_task_log_contexts([ti], session=session)[ti.id]
        try_log = render_task_log_filename(ti, ti.try_number, context=context)
        prefix = f"{FileTaskHandler.add_triggerer_suffix(try_log)}."
        live = FileTaskHandler.add_triggerer_suffix(try_log, job_id=owner[0]) if owner else None
        return _read_tail(stored=prefix, local=[prefix], live=live, owner=owner, tail_lines=tail_lines)
    if kind == TriggerKind.CALLBACK and (callback := trigger.callback) is not None:
        path = get_callback_trigger_log_relative_path(callback)
        try:
            for component in path.split("/"):
                validate_log_path_component(component)
        except ValueError:
            return [StructuredLogMessage(event=f"Not reading a log outside the log folder: {path}")]
        return _read_tail(
            stored=path,
            local=[path],
            live=path if owner else None,
            owner=owner,
            tail_lines=tail_lines,
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
    return _tail(sources, lines, tail_lines=tail_lines)


def _read_watcher_tail(
    trigger: Trigger, *, owner: tuple[int, str | None] | None, tail_lines: int, session: Session
) -> list[StructuredLogMessage]:
    """
    Read an asset watcher's files newest triggerer first, and stop once enough lines are read.

    A watcher can run for months, on a new triggerer after each restart or move; reading every file it ever
    wrote would make each request slower than the last. The triggerers are the ones that heartbeat since the
    trigger was created, and those that left a local directory for it; if none is known, as after ``airflow db
    clean``, everything under the trigger's directory is read, up to the same byte cap.
    """
    base = Path(os.path.realpath(conf.get("logging", "base_log_folder")))
    local_by_job: dict[int, list[str]] = {}
    for path in _find_local_watcher_files(base, trigger.id):
        job_dir = path.parent.parent if path.parent.name == TRIGGER_LOG_ROTATED_DIR else path.parent
        if (job_id := parse_trigger_log_dir_id(TRIGGERER_JOB_ID_KEY, job_dir.name)) is not None:
            local_by_job.setdefault(job_id, []).append(path.relative_to(base).as_posix())
    recent = _get_triggerer_job_ids(trigger, session=session, limit=READ_MAX_JOBS + 1)
    # Newest lines first: the owner's, then those of the jobs that stopped last. A job id does not tell: a watcher
    # moves to an older triggerer, a lower id, whenever a newer one stops while the older one runs.
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
    ends: dict[int, datetime.datetime] | None = None
    for job_id in job_ids:
        if read >= READ_MAX_BYTES:
            sources.append(_NOT_READ_PAST_CAP)
            break
        if found >= tail_lines:
            # Enough lines, but some can be older than this job's: a watcher back on a triggerer that ran it
            # before has that triggerer's earlier lines too. Read on while the next job wrote after them.
            if ends is None:
                ends = _get_triggerer_job_ends(job_ids, session=session)
            oldest, full = _oldest_kept(lines, tail_lines=tail_lines)
            ended = ends.get(job_id)
            if full and (oldest is None or ended is None or ended < oldest):
                rest = job_ids[job_ids.index(job_id) :]
                jobs = f"job {job_id}" if len(rest) == 1 else f"jobs {min(rest)} to {max(rest)}"
                sources.append(f"Older logs, of triggerer {jobs}, were not read.")
                break
        live = get_trigger_log_relative_path(trigger.id, job_id) if owner and owner[0] == job_id else None
        job_sources, job_lines, size = _read_sources(
            stored=f"{get_trigger_log_dir(trigger.id, job_id)}/",
            local=local_by_job.get(job_id, []),
            live=live,
            owner=owner,
            # Up to tail_lines from each: jobs are not read in time order, a later one can hold newer lines.
            tail_lines=tail_lines,
            max_bytes=READ_MAX_BYTES - read,
            read_rotated=True,
            remote=remote,
            trigger_id=trigger.id,
        )
        if remote and any(s.startswith(_REMOTE_READ_FAILED) for s in job_sources):
            # The remote store did not answer (each attempt can take minutes, e.g. botocore's retries of a
            # connect timeout): don't ask it again for every older triggerer job in the same request.
            remote = False
            job_sources.append(
                "Remote logs of older triggerer jobs were not read: the remote log store failed."
            )
        if live is not None or any(job_lines) or any(s.startswith("Could not read") for s in job_sources):
            # A triggerer that never ran the trigger has nothing to say about it.
            sources.extend(job_sources)
        lines.extend(job_lines)
        found += sum(1 for _ in _drop_copies(_interleave_logs(*map(_stream, job_lines))))
        read += size
    else:
        if read > READ_MAX_BYTES:
            sources.append(_NOT_READ_PAST_CAP)
        elif more_jobs:
            sources.append(f"Logs of triggerer jobs before {min(job_ids)} were not looked for.")
    return _tail(sources, lines, tail_lines=tail_lines)


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


def _get_triggerer_job_ends(job_ids: Iterable[int], *, session: Session) -> dict[int, datetime.datetime]:
    """
    Return when each of these triggerer jobs last wrote a line: its end, else its last heartbeat.

    A triggerer that died has no end; it may have written until its heartbeat counted as missed.
    """
    threshold = datetime.timedelta(seconds=conf.getfloat("triggerer", "triggerer_health_check_threshold"))
    return {
        job_id: end_date or latest_heartbeat + threshold
        for job_id, end_date, latest_heartbeat in session.execute(
            select(Job.id, Job.end_date, Job.latest_heartbeat).where(Job.id.in_(list(job_ids)))
        )
        if end_date or latest_heartbeat
    }


def _oldest_kept(lines: list[list[str]], *, tail_lines: int) -> tuple[datetime.datetime | None, bool]:
    """Return the time of the oldest of the newest ``tail_lines`` lines, and whether there are that many."""
    tail = deque(_drop_copies(_interleave_logs(*map(_stream, lines))), maxlen=tail_lines)
    if len(tail) < tail_lines:
        return None, False
    return tail[0].timestamp, True


def _read_sources(
    *,
    stored: str,
    local: list[str],
    live: str | None,
    owner: tuple[int, str | None] | None,
    tail_lines: int,
    max_bytes: int,
    read_rotated: bool = False,
    remote: bool = True,
    trigger_id: int | None = None,
) -> tuple[list[str], list[list[str]], int]:
    """
    Read every copy of a log, keeping the newest ``max_bytes`` of each; also return the bytes read.

    ``local`` are paths in the log folder, each read with the files whose name it starts, as task logs are.

    With ``read_rotated``, an asset watcher's files on one triggerer: their remote copies are read newest first
    and only until they hold ``tail_lines`` lines or ``max_bytes``.
    """
    sources: list[str] = []
    streams: list[Iterable[str]] = []
    if live is not None and owner is not None and (hostname := owner[1]):
        lines, source = _read_live_tail_lines(live, hostname, tail_lines=tail_lines)
        sources.append(source)
        streams.append(lines)
        if read_rotated and len(lines) < tail_lines:
            # The older lines the local size cap moved aside, until they are uploaded.
            older, older_source = _read_live_tail_lines(
                get_trigger_log_rotated_path(live),
                hostname,
                tail_lines=tail_lines - len(lines),
                missing_ok=True,
            )
            if older:
                sources.append(older_source)
                streams.append(older)
    try:
        remote_sources, remote_streams = _read_remote_streams(stored) if remote else ([], [])
        # Read the remote streams here: some handlers (CloudWatch) return lazy ones, whose errors (an unreachable
        # or throttling endpoint) would otherwise surface later, outside this ``try``, and fail the whole request.
        if trigger_id is not None:
            # A watcher's own lines all carry its trigger_id. A handler that reads a directory as one stream name
            # (CloudWatch in older amazon providers) answers with a notice stamped now, one per triggerer job.
            remote_streams = [_lines_of_trigger(stream, trigger_id) for stream in remote_streams]
        if not read_rotated:
            remote_kept = [_keep_newest(stream, max_bytes) for stream in remote_streams]
        elif remote_streams:
            remote_kept = [_keep_newest_files(remote_streams, tail_lines=tail_lines, max_bytes=max_bytes)]
        else:
            remote_kept = []
    except Exception as e:
        remote_sources, remote_kept = [f"{_REMOTE_READ_FAILED}: {e}"], []
    sources.extend(remote_sources)
    if local:
        base = os.path.realpath(conf.get("logging", "base_log_folder"))
        for relative_path in local:
            local_sources, local_streams = _read_local_logs(relative_path)
            sources.extend(os.path.relpath(source, base) for source in local_sources)
            streams.extend(local_streams)
    kept = [*remote_kept, *(_keep_newest(stream, max_bytes) for stream in streams)]
    return sources, [lines for lines, _ in kept], sum(size for _, size in kept)


def _lines_of_trigger(lines: Iterable[str], trigger_id: int) -> Generator[str, None, None]:
    """Keep the lines that carry ``trigger_id``."""
    for line in lines:
        if "trigger_id" not in line:
            continue
        try:
            fields = json.loads(line)
        except ValueError:
            continue
        if isinstance(fields, dict) and fields.get("trigger_id") == trigger_id:
            yield line


def _keep_newest(lines: Iterable[str], max_bytes: int) -> tuple[list[str], int]:
    """Return the newest lines that fit in ``max_bytes``, and the size of all of them."""
    kept: deque[str] = deque()
    kept_size = size = 0
    for line in lines:
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
    Keep the newest lines of a watcher's remote files on one triggerer, reading the newest file first.

    Remote handlers return a directory's files in name order, which is age order there: the rotated files,
    numbered, then the file uploaded when the watcher left or the triggerer stopped. A busy watcher has one
    per ``watcher_log_max_bytes`` it logged; the older ones are not read once the newer hold enough. They are
    returned as one stream, oldest first: the merge orders lines of the same millisecond by their line number
    in their stream, which would put a file's first lines before the end of the one before it.
    """
    kept: list[list[str]] = []
    size = 0
    for stream in reversed(streams):
        if tail_lines <= 0 or max_bytes < size:
            break
        lines, file_size = _keep_newest(stream, max_bytes - size)
        kept.append(lines)
        tail_lines, size = tail_lines - len(lines), size + file_size
    return [line for lines in reversed(kept) for line in lines], size


def _tail(sources: list[str], lines: list[list[str]], *, tail_lines: int) -> list[StructuredLogMessage]:
    if not lines:
        return [StructuredLogMessage(event="No logs found.")]
    tail = deque(_drop_copies(_interleave_logs(*map(_stream, lines))), maxlen=tail_lines)
    return [
        StructuredLogMessage(event="::group::Log message source details", sources=sources),  # type: ignore[call-arg]
        StructuredLogMessage(event="::endgroup::"),
        *tail,
    ]


def _read_remote_streams(relative_path: str) -> tuple[list[str], list[RawLogStream]]:
    """
    Read the remote logs at ``relative_path``, one stream per log, so lines from several logs merge by time.

    A handler with only ``read()`` (S3) returns every log under a prefix in key order, where
    ``triggerer_job_id=10/`` comes before ``triggerer_job_id=9/``; joined into one stream, as the callback log reader does,
    the newest lines would not be at the end.
    """
    from airflow.logging_config import get_remote_task_log

    if (remote_io := get_remote_task_log()) is None:
        return [], []
    if stream_method := getattr(remote_io, "stream", None):
        sources, logs = stream_method(relative_path, None)
        return list(sources), list(logs or [])
    sources, logs = remote_io.read(relative_path, None)
    return list(sources), [_get_compatible_log_stream([log]) for log in logs or []]


def _drop_copies(messages: Iterable[StructuredLogMessage]) -> Generator[StructuredLogMessage, None, None]:
    """
    Drop a line already read from another source.

    The same file can be read live, from remote logging and from a shared disk, and CloudWatch, which
    receives each line as it is logged, keeps its time only to the millisecond.
    """
    seen: dict[tuple[str, int], None] = {}
    for message in messages:
        if message.timestamp is None:
            yield message
            continue
        # The whole line but its time: lines that only share their text (``log.info("received", id=...)`` in a
        # loop) are not copies of each other.
        fields = message.model_dump(mode="json", exclude={"timestamp"}, exclude_none=True)
        event, ms = json.dumps(fields, sort_keys=True, default=str), int(message.timestamp.timestamp() * 1000)
        if any((event, ms + d) in seen for d in (0, -1, 1)):
            continue
        seen[(event, ms)] = None
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
    """Return the lines in the last ``max_bytes`` of a live file, where they came from, and whether that was all of it."""
    url = _get_log_server_url(hostname, relative_path, LogType.TRIGGER)
    try:
        response = _fetch_logs_from_service(url, relative_path, headers={"Range": f"bytes=-{max_bytes}"})
        # Streamed: the body is read here, and a log server that stops mid-body fails here too.
        text = response.text if response.status_code in (200, 206) else ""
    except Exception as e:
        return [], f"Could not read {url}: {e}", True
    if response.status_code == 404 and missing_ok:
        return [], url, True
    if response.status_code == 404:
        # Normal right after a move: e.g. the new owner has not logged anything for the trigger yet.
        return [], f"{url}: no file yet, the trigger has not logged on this triggerer", True
    if response.status_code == 416:
        # An empty file (just rotated): no range of it can be served.
        return [], url, True
    if response.status_code not in (200, 206):
        return [], f"{url} returned {response.status_code}", True
    # Split on "\n" only, as the task log reader does: str.splitlines() also splits at U+2028, U+2029, U+0085
    # and other characters JSON leaves unescaped inside a line, cutting such a line into unparsable pieces.
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    content_range = response.headers.get("content-range", "")
    whole_file = response.status_code == 200 or content_range.startswith("bytes 0-")
    if not whole_file:
        # The range starts in the middle of a line.
        lines = lines[1:]
    return lines, url, whole_file


def _find_local_watcher_files(base: Path, trigger_id: int) -> list[Path]:
    return [
        path
        for producer in sorted(
            base.glob(f"{get_trigger_log_dir(trigger_id)}/{TRIGGERER_JOB_ID_KEY}=*/{TRIGGER_LOG_FILE_NAME}")
        )
        for path in list_trigger_log_files(producer.parent)
        if path.is_file()
    ]


def _stream(lines: list[str]) -> RawLogStream:
    yield from lines
