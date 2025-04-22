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
"""File logging handler for tasks."""

from __future__ import annotations

import heapq
import io
import logging
import os
from collections.abc import Generator
from contextlib import suppress
from datetime import datetime
from enum import Enum
from itertools import chain
from pathlib import Path
from types import GeneratorType
from typing import IO, TYPE_CHECKING, Any, Callable, Optional, Union, cast
from urllib.parse import urljoin

import pendulum
from pydantic import BaseModel, ConfigDict, ValidationError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.utils.helpers import parse_template_string, render_template
from airflow.utils.log.logging_mixin import SetContextPropagate
from airflow.utils.log.non_caching_file_handler import NonCachingRotatingFileHandler
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from requests import Response

    from airflow.executors.base_executor import BaseExecutor
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.typing_compat import TypeAlias

CHUNK_SIZE = 1024 * 1024 * 5  # 5MB
DEFAULT_SORT_DATETIME = pendulum.datetime(2000, 1, 1)
DEFAULT_SORT_TIMESTAMP = int(DEFAULT_SORT_DATETIME.timestamp() * 1000)
SORT_KEY_OFFSET = 10000000
"""An offset used by the _sort_key utility.

Assuming 50 characters per line, an offset of 10,000,000 can represent approximately 500 MB of file data, which is sufficient for use as a constant.
"""
HEAP_DUMP_SIZE = 500000
HALF_HEAP_DUMP_SIZE = HEAP_DUMP_SIZE // 2
PARALLEL_YIELD_SIZE = HEAP_DUMP_SIZE // 100
FIRST_TIME_READ_KEY = "first_time_read"

# These types are similar, but have distinct names to make processing them less error prone
LogMessages: TypeAlias = list[str]
"""The legacy format of log messages, represented as a single string blob to be parsed later."""
LogSourceInfo: TypeAlias = list[str]
"""Information _about_ the log fetching process for display to a user"""
LogMetadata: TypeAlias = dict[str, Any]
"""Metadata about the log fetching process, including `end_of_log` and `<source>_log_pos`.

Instead of using a single aggregated `log_pos` for all log sources, we use `<source>_log_pos` to track
each source individually. By maintaining separate positions for each source, we can seek to the last
retrieved position per source and read logs from there. This approach allows us to interleave logs
from multiple sources accurately, rather than cutting off logs based on a shared `log_pos`.

Fields:
- `end_of_log`: Boolean. True if the end of the log has been reached; False if more logs may be available.
  This is typically determined by the status of the TaskInstance.
- `<source>_last_log_pos`: Integer. The character position up to which logs have been retrieved for each source.

For reference, in the previous implementation:
- `end_of_log`: Boolean. Indicates if the log has ended.
- `log_pos`: Integer. The absolute character position up to which the log was retrieved across all sources.
"""
RawLogStream: TypeAlias = Generator[str, None, None]
"""Raw log stream, containing unparsed log lines."""
LegacyLogResponse: TypeAlias = tuple[LogSourceInfo, LogMessages]
"""Legacy log response, containing source information and log messages."""
LogResponse: TypeAlias = tuple[LogSourceInfo, list[RawLogStream]]
"""Log response, containing source information, stream of log lines, and total log size."""
LAST_LOG_POS_FORMAT = "{identifier}_last_log_pos"

logger = logging.getLogger(__name__)


class StructuredLogMessage(BaseModel):
    """An individual log message."""

    timestamp: datetime | None = None
    event: str

    # Collisions of _sort_key may occur due to duplicated messages. If this happens, the heap will use the second element,
    # which is the StructuredLogMessage for comparison. Therefore, we need to define a comparator for it.
    def __lt__(self, other: StructuredLogMessage) -> bool:
        return self.sort_key < other.sort_key

    @property
    def sort_key(self) -> datetime:
        return self.timestamp or DEFAULT_SORT_DATETIME

    # We don't need to cache string when parsing in to this, as almost every line will have a different
    # values; `extra=allow` means we'll create extra properties as needed. Only timestamp and event are
    # required, everything else is up to what ever is producing the logs
    model_config = ConfigDict(cache_strings=False, extra="allow")


StructuredLogStream: TypeAlias = Generator[StructuredLogMessage, None, None]
"""Structured log stream, containing structured log messages."""
LogHandlerOutputStream: TypeAlias = Union[StructuredLogStream, chain[StructuredLogMessage]]
"""Output stream, containing structured log messages or a chain of them."""
ParsedLog: TypeAlias = tuple[Optional[datetime], int, StructuredLogMessage]
"""Parsed log record, containing timestamp, line_num and the structured log message."""
ParsedLogStream: TypeAlias = Generator[ParsedLog, None, None]


class LogType(str, Enum):
    """
    Type of service from which we retrieve logs.

    :meta private:
    """

    TRIGGER = "trigger"
    WORKER = "worker"


def _set_task_deferred_context_var():
    """
    Tell task log handler that task exited with deferral.

    This exists for the sole purpose of telling elasticsearch handler not to
    emit end_of_log mark after task deferral.

    Depending on how the task is run, we may need to set this in task command or in local task job.
    Kubernetes executor requires the local task job invocation; local executor requires the task
    command invocation.

    :meta private:
    """
    logger = logging.getLogger()
    with suppress(StopIteration):
        h = next(h for h in logger.handlers if hasattr(h, "ctx_task_deferred"))
        h.ctx_task_deferred = True


def _fetch_logs_from_service(url: str, log_relative_path: str) -> Response:
    # Import occurs in function scope for perf. Ref: https://github.com/apache/airflow/pull/21438
    import requests

    from airflow.api_fastapi.auth.tokens import JWTGenerator, get_signing_key

    timeout = conf.getint("api", "log_fetch_timeout_sec", fallback=None)
    generator = JWTGenerator(
        secret_key=get_signing_key("api", "secret_key"),
        # Since we are using a secret key, we need to be explicit about the algorithm here too
        algorithm="HS512",
        private_key=None,
        issuer=None,
        valid_for=conf.getint("webserver", "log_request_clock_grace", fallback=30),
        audience="task-instance-logs",
    )
    response = requests.get(
        url,
        timeout=timeout,
        headers={"Authorization": generator.generate({"filename": log_relative_path})},
        stream=True,
    )
    response.encoding = "utf-8"
    return response


_parse_timestamp = conf.getimport("logging", "interleave_timestamp_parser", fallback=None)

if not _parse_timestamp:

    def _parse_timestamp(line: str):
        timestamp_str, _ = line.split(" ", 1)
        return pendulum.parse(timestamp_str.strip("[]"))


def _stream_lines_by_chunk(
    log_io: IO[str],
    last_end_log_pos: int = 0,
    current_end_log_pos: int = -1,
) -> RawLogStream:
    """
    Stream lines from a file-like IO object within a specified byte range.

    Reads the content between the provided start (`last_end_log_pos`) and end
    (`current_end_log_pos`) positions to avoid reading newly appended logs outside
    the intended snapshot.

    .. note::
        Since logs can be written to concurrently (e.g. for local file cases), specifying both start and end positions
        ensures a consistent, snapshot-like view of the log content.

    :param log_io: A file-like IO object to read from.
    :param last_end_log_pos: The byte offset to start reading from.
    :param current_end_log_pos: The byte offset to stop reading at (exclusive).
                                If negative, reads until EOF.
    :return: A generator that yields individual lines within the specified range.
    """
    log_io.seek(last_end_log_pos, 0)

    if current_end_log_pos < 0:
        bytes_left = None
    else:
        bytes_left = current_end_log_pos - last_end_log_pos
        if bytes_left <= 0:
            return

    buffer = ""
    while True:
        chunk_size = CHUNK_SIZE if bytes_left is None else min(CHUNK_SIZE, bytes_left)
        chunk = log_io.read(chunk_size)
        if not chunk:
            break

        if bytes_left is not None:
            bytes_left -= len(chunk)

        buffer += chunk
        *lines, buffer = buffer.split("\n")
        yield from lines

        if bytes_left == 0:
            break

    if buffer:
        yield buffer


def _log_stream_to_parsed_log_stream(
    log_stream: RawLogStream,
) -> ParsedLogStream:
    """
    Turn a str log stream into a generator of parsed log lines.

    :param log_stream: The stream to parse.
    :return: A generator of parsed log lines.
    """
    from airflow.utils.timezone import coerce_datetime

    timestamp = None
    next_timestamp = None
    idx = 0
    for line in log_stream:
        if line:
            try:
                log = StructuredLogMessage.model_validate_json(line)
            except ValidationError:
                with suppress(Exception):
                    # If we can't parse the timestamp, don't attach one to the row
                    if isinstance(line, str):
                        next_timestamp = _parse_timestamp(line)
                log = StructuredLogMessage(event=str(line), timestamp=next_timestamp)
            if log.timestamp:
                log.timestamp = coerce_datetime(log.timestamp)
                timestamp = log.timestamp
            yield timestamp, idx, log
        idx += 1


def _sort_key(timestamp: datetime | None, line_num: int) -> int:
    """
    Generate a sort key for log record, to be used in K-way merge.

    :param timestamp: timestamp of the log line
    :param line_num: line number of the log line
    :return: a integer as sort key to avoid overhead of memory usage
    """
    return int((timestamp or DEFAULT_SORT_DATETIME).timestamp() * 1000) * SORT_KEY_OFFSET + line_num


def _is_sort_key_with_default_timestamp(sort_key: int) -> bool:
    """
    Check if the sort key was generated with the DEFAULT_SORT_TIMESTAMP.

    This is used to identify log records that don't have timestamp.

    :param sort_key: The sort key to check
    :return: True if the sort key was generated with DEFAULT_SORT_TIMESTAMP, False otherwise
    """
    # Extract the timestamp part from the sort key (remove the line number part)
    timestamp_part = sort_key // SORT_KEY_OFFSET
    return timestamp_part == DEFAULT_SORT_TIMESTAMP


def _add_log_from_parsed_log_streams_to_heap(
    heap: list[tuple[int, StructuredLogMessage]],
    parsed_log_streams: dict[int, ParsedLogStream],
) -> None:
    """
    Add one log record from each parsed log stream to the heap, and will remove empty log stream from the dict after iterating.

    :param heap: heap to store log records
    :param parsed_log_streams: dict of parsed log streams
    """
    log_stream_to_remove: list[int] | None = None
    for idx, log_stream in parsed_log_streams.items():
        record: ParsedLog | None = next(log_stream, None)
        if record is None:
            if log_stream_to_remove is None:
                log_stream_to_remove = []
            log_stream_to_remove.append(idx)
            continue
        # add type hint to avoid mypy error
        record = cast("ParsedLog", record)
        timestamp, line_num, line = record
        # take int as sort key to avoid overhead of memory usage
        heapq.heappush(heap, (_sort_key(timestamp, line_num), line))
    # remove empty log stream from the dict
    if log_stream_to_remove is not None:
        for idx in log_stream_to_remove:
            del parsed_log_streams[idx]


def _interleave_logs(*log_streams: RawLogStream) -> StructuredLogStream:
    """
    Merge parsed log streams using K-way merge.

    By yielding HALF_CHUNK_SIZE records when heap size exceeds CHUNK_SIZE, we can reduce the chance of messing up the global order.
    Since there are multiple log streams, we can't guarantee that the records are in global order.

    e.g.

    log_stream1: ----------
    log_stream2:   ----
    log_stream3:     --------

    The first record of log_stream3 is later than the fourth record of log_stream1 !
    :param parsed_log_streams: parsed log streams
    :return: interleaved log stream
    """
    # don't need to push whole tuple into heap, which increases too much overhead
    # push only sort_key and line into heap
    heap: list[tuple[int, StructuredLogMessage]] = []
    # to allow removing empty streams while iterating, also turn the str stream into parsed log stream
    parsed_log_streams: dict[int, ParsedLogStream] = {
        idx: _log_stream_to_parsed_log_stream(log_stream) for idx, log_stream in enumerate(log_streams)
    }

    # keep adding records from logs until all logs are empty
    last = None
    while parsed_log_streams:
        _add_log_from_parsed_log_streams_to_heap(heap, parsed_log_streams)

        # yield HALF_HEAP_DUMP_SIZE records when heap size exceeds HEAP_DUMP_SIZE
        if len(heap) >= HEAP_DUMP_SIZE:
            for _ in range(HALF_HEAP_DUMP_SIZE):
                sort_key, line = heapq.heappop(heap)
                if line != last or _is_sort_key_with_default_timestamp(sort_key):  # dedupe
                    yield line
                last = line

    # yield remaining records
    for _ in range(len(heap)):
        sort_key, line = heapq.heappop(heap)
        if line != last or _is_sort_key_with_default_timestamp(sort_key):  # dedupe
            yield line
        last = line
    # free memory
    del heap
    del parsed_log_streams


def _is_logs_stream_like(log):
    """Check if the logs are stream-like."""
    return isinstance(log, chain) or isinstance(log, GeneratorType)


def _get_last_log_pos_from_metadata(
    metadata: LogMetadata | None,
    log_pos_name: str,
) -> int:
    """
    Get the last log position from metadata.

    :param metadata: Metadata dictionary to store log positions.
    :param log_pos_name: Name of the log position to retrieve.
    :return: The last log position, or 0 if not found.
    """
    log_pos: int = metadata.get(log_pos_name, 0) if metadata else 0
    if not isinstance(log_pos, int):
        raise TypeError(
            f"Invalid log position type for metadata[{log_pos_name}]. Expected int, got {type(log_pos).__name__}"
        )
    return log_pos


def _get_compatible_log_stream(
    log_messages: LogMessages,
    log_pos_identifier_prefix: str,
    metadata: LogMetadata | None = None,
) -> RawLogStream:
    """
    Convert legacy log message blobs into a generator that yields log lines.

    Instead of yielding each log message sequentially, this function interleaves log lines
    from multiple sources in parallel. This is important to maintain the correct chronological
    order when consumed by `_interleave_logs`.

    Example:
    ```
    log_messages[0]:     a1a2a3a4a5
    log_messages[1]:   b1b2
    log_messages[2]:    c1c2c3c4

    Parallel yielding: start from the beginning of each stream
    output_log_stream -> a1b1c1a2b2c2...

    Sequential yielding: exhaust one stream before moving to the next
    output_log_stream -> a1a2a3a4a5b1b2c1c2c3c4
    ```

    :param log_messages: List of legacy log message strings.
    :param log_pos_identifier_prefix: Prefix for log position identifiers.
    :param metadata: Metadata dictionary to store log positions.
    :return: A generator that yields interleaved log lines.
    """

    def _parallel_yield_log_streams(log_streams: list[RawLogStream]) -> RawLogStream:
        """
        Parallel yield log lines from multiple streams.

        :param log_streams: List of log line generators.
        :return: A generator that yields lines from each stream in round-robin fashion.
        """
        lines_to_yield = []
        while True:
            if not log_streams:
                break
            # Add line from each stream
            for stream in log_streams:
                line: str | None = next(stream, None)
                if line is None:
                    log_streams.remove(stream)
                    continue
                lines_to_yield.append(line)

            if len(lines_to_yield) >= PARALLEL_YIELD_SIZE:
                yield from lines_to_yield
                lines_to_yield.clear()

        # Yield any remaining lines
        if lines_to_yield:
            yield from lines_to_yield
        del lines_to_yield

    log_streams: list[RawLogStream] = []
    for idx, log_message in enumerate(log_messages):
        # Skip log messages that already read
        log_pos_name = LAST_LOG_POS_FORMAT.format(identifier=f"{log_pos_identifier_prefix}_{idx}")
        last_end_log_pos: int = _get_last_log_pos_from_metadata(metadata, log_pos_name)
        current_end_log_pos: int = len(log_message.encode("utf-8"))
        # Update the metadata with the last log position
        if metadata is not None:
            metadata[log_pos_name] = current_end_log_pos
        # Append the log stream to the list
        log_streams.append(
            _stream_lines_by_chunk(io.StringIO(log_message), last_end_log_pos, current_end_log_pos)
        )

    return _parallel_yield_log_streams(log_streams)


def _ensure_ti(ti: TaskInstanceKey | TaskInstance, session) -> TaskInstance:
    """
    Given TI | TIKey, return a TI object.

    Will raise exception if no TI is found in the database.
    """
    from airflow.models.taskinstance import TaskInstance

    if isinstance(ti, TaskInstance):
        return ti
    val = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.task_id == ti.task_id,
            TaskInstance.dag_id == ti.dag_id,
            TaskInstance.run_id == ti.run_id,
            TaskInstance.map_index == ti.map_index,
        )
        .one_or_none()
    )
    if not val:
        raise AirflowException(f"Could not find TaskInstance for {ti}")
    val.try_number = ti.try_number
    return val


def get_compatible_output_log_stream(
    input_logs: list[StructuredLogMessage],
) -> Generator[StructuredLogMessage, None, None]:
    """
    Convert a list of structured log messages into a generator.

    This helper ensures compatibility with `os_task_handler` and `es_task_handler`,
    and is intended to be removed after the log handler refactor in providers.

    :param input_logs: List of structured log messages.
    :return: A generator yielding structured log messages.
    """
    yield from input_logs


class FileTaskHandler(logging.Handler):
    """
    FileTaskHandler is a python log handler that handles and reads task instance logs.

    It creates and delegates log handling to `logging.FileHandler` after receiving task
    instance context.  It reads logs from task instance's host machine.

    :param base_log_folder: Base log folder to place logs.
    :param max_bytes: max bytes size for the log file
    :param backup_count: backup file count for the log file
    :param delay:  default False -> StreamHandler, True -> Handler
    """

    trigger_should_wrap = True
    inherits_from_empty_operator_log_message = (
        "Operator inherits from empty operator and thus does not have logs"
    )
    executor_instances: dict[str, BaseExecutor] = {}
    DEFAULT_EXECUTOR_KEY = "_default_executor"

    def __init__(
        self,
        base_log_folder: str,
        max_bytes: int = 0,
        backup_count: int = 0,
        delay: bool = False,
    ):
        super().__init__()
        self.handler: logging.Handler | None = None
        self.local_base = base_log_folder
        self.maintain_propagate: bool = False
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.delay = delay
        """
        If true, overrides default behavior of setting propagate=False

        :meta private:
        """

        self.ctx_task_deferred = False
        """
        If true, task exited with deferral to trigger.

        Some handlers emit "end of log" markers, and may not wish to do so when task defers.
        """

    def set_context(self, ti: TaskInstance, *, identifier: str | None = None) -> None | SetContextPropagate:
        """
        Provide task_instance context to airflow task handler.

        Generally speaking returns None.  But if attr `maintain_propagate` has
        been set to propagate, then returns sentinel MAINTAIN_PROPAGATE. This
        has the effect of overriding the default behavior to set `propagate`
        to False whenever set_context is called.  At time of writing, this
        functionality is only used in unit testing.

        :param ti: task instance object
        :param identifier: if set, adds suffix to log file. For use when relaying exceptional messages
            to task logs from a context other than task or trigger run
        """
        local_loc = self._init_file(ti, identifier=identifier)
        self.handler = NonCachingRotatingFileHandler(
            local_loc,
            encoding="utf-8",
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            delay=self.delay,
        )
        if self.formatter:
            self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)
        return SetContextPropagate.MAINTAIN_PROPAGATE if self.maintain_propagate else None

    @staticmethod
    def add_triggerer_suffix(full_path, job_id=None):
        """
        Derive trigger log filename from task log filename.

        E.g. given /path/to/file.log returns /path/to/file.log.trigger.123.log, where 123
        is the triggerer id.  We use the triggerer ID instead of trigger ID to distinguish
        the files because, rarely, the same trigger could get picked up by two different
        triggerer instances.
        """
        full_path = Path(full_path).as_posix()
        full_path += f".{LogType.TRIGGER.value}"
        if job_id:
            full_path += f".{job_id}.log"
        return full_path

    def emit(self, record):
        if self.handler:
            self.handler.emit(record)

    def flush(self):
        if self.handler:
            self.handler.flush()

    def close(self):
        if self.handler:
            self.handler.close()

    @provide_session
    def _render_filename(self, ti: TaskInstance, try_number: int, session=NEW_SESSION) -> str:
        """Return the worker log filename."""
        ti = _ensure_ti(ti, session)
        dag_run = ti.get_dagrun(session=session)

        date = dag_run.logical_date or dag_run.run_after
        date = date.isoformat()

        template = dag_run.get_log_template(session=session).filename
        str_tpl, jinja_tpl = parse_template_string(template)
        if jinja_tpl:
            return render_template(jinja_tpl, {"ti": ti, "ts": date, "try_number": try_number}, native=False)

        if str_tpl:
            data_interval = (dag_run.data_interval_start, dag_run.data_interval_end)
            if data_interval[0]:
                data_interval_start = data_interval[0].isoformat()
            else:
                data_interval_start = ""
            if data_interval[1]:
                data_interval_end = data_interval[1].isoformat()
            else:
                data_interval_end = ""
            return str_tpl.format(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                data_interval_start=data_interval_start,
                data_interval_end=data_interval_end,
                logical_date=date,
                try_number=try_number,
            )
        raise RuntimeError(f"Unable to render log filename for {ti}. This should never happen")

    def _get_executor_get_task_log(
        self, ti: TaskInstance
    ) -> Callable[[TaskInstance, int], tuple[list[str], list[str]]]:
        """
        Get the get_task_log method from executor of current task instance.

        Since there might be multiple executors, so we need to get the executor of current task instance instead of getting from default executor.

        :param ti: task instance object
        :return: get_task_log method of the executor
        """
        executor_name = ti.executor or self.DEFAULT_EXECUTOR_KEY
        executor = self.executor_instances.get(executor_name)
        if executor is not None:
            return executor.get_task_log

        if executor_name == self.DEFAULT_EXECUTOR_KEY:
            self.executor_instances[executor_name] = ExecutorLoader.get_default_executor()
        else:
            self.executor_instances[executor_name] = ExecutorLoader.load_executor(executor_name)
        return self.executor_instances[executor_name].get_task_log

    def _read(
        self,
        ti: TaskInstance,
        try_number: int,
        metadata: LogMetadata | None = None,
    ) -> tuple[LogHandlerOutputStream, LogMetadata]:
        """
        Template method that contains custom logic of reading logs given the try_number.

        :param ti: task instance record
        :param try_number: current try_number to read log from
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
                         Following attributes are used:
                         log_pos: (absolute) Char position to which the log
                                  which was retrieved in previous calls, this
                                  part will be skipped and only following test
                                  returned to be added to tail.
        :return: log message as a string and metadata.
                 Following attributes are used in metadata:
                 end_of_log: Boolean, True if end of log is reached or False
                             if further calls might get more log text.
                             This is determined by the status of the TaskInstance
                 log_pos: (absolute) Char position to which the log is retrieved
        """
        # Task instance here might be different from task instance when
        # initializing the handler. Thus explicitly getting log location
        # is needed to get correct log path.
        worker_log_rel_path = self._render_filename(ti, try_number)
        sources: LogSourceInfo = []
        source_list: list[str] = []
        remote_logs: list[RawLogStream] = []
        local_logs: list[RawLogStream] = []
        executor_logs: list[RawLogStream] = []
        served_logs: list[RawLogStream] = []
        with suppress(NotImplementedError):
            sources, logs = self._read_remote_logs(ti, try_number, metadata)
            if not len(logs):
                remote_logs = []
            elif isinstance(logs, list) and isinstance(logs[0], str):
                # If the logs are in legacy format, convert them to a generator of log lines
                remote_logs = [
                    # We don't need to use the log_pos here, as we are using the metadata to track the position
                    _get_compatible_log_stream(cast("list[str]", logs), "remote", metadata)
                ]
            elif isinstance(logs, list) and _is_logs_stream_like(logs[0]):
                # If the logs are already in a stream-like format, we can use them directly
                remote_logs = cast("list[RawLogStream]", logs)
            else:
                # If the logs are in a different format, raise an error
                raise TypeError("Logs should be either a list of strings or a generator of log lines.")
            # Extend LogSourceInfo
            source_list.extend(sources)
        has_k8s_exec_pod = False
        if ti.state == TaskInstanceState.RUNNING:
            executor_get_task_log = self._get_executor_get_task_log(ti)
            response = executor_get_task_log(ti, try_number)
            if response:
                sources, logs = response
                # make the logs stream-like compatible
                executor_logs = [_get_compatible_log_stream(logs, "executor", metadata)]
            if sources:
                source_list.extend(sources)
                has_k8s_exec_pod = True
        if not (remote_logs and ti.state not in State.unfinished):
            # when finished, if we have remote logs, no need to check local
            worker_log_full_path = Path(self.local_base, worker_log_rel_path)
            sources, local_logs = self._read_from_local(worker_log_full_path, metadata)
            source_list.extend(sources)
        if ti.state in (TaskInstanceState.RUNNING, TaskInstanceState.DEFERRED) and not has_k8s_exec_pod:
            sources, served_logs = self._read_from_logs_server(ti, worker_log_rel_path, metadata)
            source_list.extend(sources)
        elif ti.state not in State.unfinished and not (local_logs or remote_logs):
            # ordinarily we don't check served logs, with the assumption that users set up
            # remote logging or shared drive for logs for persistence, but that's not always true
            # so even if task is done, if no local logs or remote logs are found, we'll check the worker
            sources, served_logs = self._read_from_logs_server(ti, worker_log_rel_path, metadata)
            source_list.extend(sources)

        out_stream: LogHandlerOutputStream = _interleave_logs(
            *local_logs,
            *remote_logs,
            *executor_logs,
            *served_logs,
        )

        # Log message source details are grouped: they are not relevant for most users and can
        # distract them from finding the root cause of their errors
        header = [
            StructuredLogMessage(event="::group::Log message source details", sources=source_list),  # type: ignore[call-arg]
            StructuredLogMessage(event="::endgroup::"),
        ]
        end_of_log = ti.try_number != try_number or ti.state not in (
            TaskInstanceState.RUNNING,
            TaskInstanceState.DEFERRED,
        )

        # `log_pos` is not used in the new implementation, as we are using the metadata to track the position of each source
        # also `first_time_read` is used to track the first time the log is read.
        if not metadata:
            metadata = {}
        if metadata.get(FIRST_TIME_READ_KEY, True):
            metadata[FIRST_TIME_READ_KEY] = False
            out_stream = chain(header, out_stream)
        metadata["end_of_log"] = end_of_log
        return out_stream, metadata

    @staticmethod
    def _get_pod_namespace(ti: TaskInstance):
        pod_override = ti.executor_config.get("pod_override")
        namespace = None
        with suppress(Exception):
            namespace = pod_override.metadata.namespace
        return namespace or conf.get("kubernetes_executor", "namespace")

    def _get_log_retrieval_url(
        self, ti: TaskInstance, log_relative_path: str, log_type: LogType | None = None
    ) -> tuple[str, str]:
        """Given TI, generate URL with which to fetch logs from service log server."""
        if log_type == LogType.TRIGGER:
            if not ti.triggerer_job:
                raise RuntimeError("Could not build triggerer log URL; no triggerer job.")
            config_key = "triggerer_log_server_port"
            config_default = 8794
            hostname = ti.triggerer_job.hostname
            log_relative_path = self.add_triggerer_suffix(log_relative_path, job_id=ti.triggerer_job.id)
        else:
            hostname = ti.hostname
            config_key = "worker_log_server_port"
            config_default = 8793
        return (
            urljoin(
                f"http://{hostname}:{conf.get('logging', config_key, fallback=config_default)}/log/",
                log_relative_path,
            ),
            log_relative_path,
        )

    def read(
        self,
        task_instance: TaskInstance,
        try_number: int | None = None,
        metadata: LogMetadata | None = None,
    ) -> tuple[LogHandlerOutputStream, LogMetadata]:
        """
        Read logs of given task instance from local machine.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                            it returns the log of task_instance.try_number
        :param metadata: log metadata, can be used for steaming log reading and auto-tailing.
        :return: a list of listed tuples which order log string by host
        """
        if try_number is None:
            try_number = task_instance.try_number
        if try_number is None or try_number < 1:
            logs = [
                StructuredLogMessage(  # type: ignore[call-arg]
                    level="error", event=f"Error fetching the logs. Try number {try_number} is invalid."
                )
            ]
            return chain(logs), {"end_of_log": True, FIRST_TIME_READ_KEY: False}

        return self._read(task_instance, try_number, metadata)

    @staticmethod
    def _prepare_log_folder(directory: Path, new_folder_permissions: int):
        """
        Prepare the log folder and ensure its mode is as configured.

        To handle log writing when tasks are impersonated, the log files need to
        be writable by the user that runs the Airflow command and the user
        that is impersonated. This is mainly to handle corner cases with the
        SubDagOperator. When the SubDagOperator is run, all of the operators
        run under the impersonated user and create appropriate log files
        as the impersonated user. However, if the user manually runs tasks
        of the SubDagOperator through the UI, then the log files are created
        by the user that runs the Airflow command. For example, the Airflow
        run command may be run by the `airflow_sudoable` user, but the Airflow
        tasks may be run by the `airflow` user. If the log files are not
        writable by both users, then it's possible that re-running a task
        via the UI (or vice versa) results in a permission error as the task
        tries to write to a log file created by the other user.

        We leave it up to the user to manage their permissions by exposing configuration for both
        new folders and new log files. Default is to make new log folders and files group-writeable
        to handle most common impersonation use cases. The requirement in this case will be to make
        sure that the same group is set as default group for both - impersonated user and main airflow
        user.
        """
        for parent in reversed(directory.parents):
            parent.mkdir(mode=new_folder_permissions, exist_ok=True)
        directory.mkdir(mode=new_folder_permissions, exist_ok=True)

    def _init_file(self, ti, *, identifier: str | None = None):
        """
        Create log directory and give it permissions that are configured.

        See above _prepare_log_folder method for more detailed explanation.

        :param ti: task instance object
        :return: relative log path of the given task instance
        """
        new_file_permissions = int(
            conf.get("logging", "file_task_handler_new_file_permissions", fallback="0o664"), 8
        )
        local_relative_path = self._render_filename(ti, ti.try_number)
        full_path = os.path.join(self.local_base, local_relative_path)
        if identifier:
            full_path += f".{identifier}.log"
        elif ti.is_trigger_log_context is True:
            # if this is true, we're invoked via set_context in the context of
            # setting up individual trigger logging. return trigger log path.
            full_path = self.add_triggerer_suffix(full_path=full_path, job_id=ti.triggerer_job.id)
        new_folder_permissions = int(
            conf.get("logging", "file_task_handler_new_folder_permissions", fallback="0o775"), 8
        )
        self._prepare_log_folder(Path(full_path).parent, new_folder_permissions)

        if not os.path.exists(full_path):
            open(full_path, "a").close()
            try:
                os.chmod(full_path, new_file_permissions)
            except OSError as e:
                logger.warning("OSError while changing ownership of the log file. ", e)

        return full_path

    @staticmethod
    def _read_from_local(
        worker_log_path: Path,
        log_metadata: LogMetadata | None = None,
    ) -> LogResponse:
        sources: LogSourceInfo = []
        log_streams: list[RawLogStream] = []
        paths = sorted(worker_log_path.parent.glob(worker_log_path.name + "*"))
        if not paths:
            return sources, log_streams

        for path in paths:
            sources.append(os.fspath(path))
            # Move the file pointer to the last read position if there is `local_<identifier>_log_pos` in metadata
            log_pos_name = LAST_LOG_POS_FORMAT.format(identifier=path.name)
            last_end_log_pos: int = _get_last_log_pos_from_metadata(log_metadata, log_pos_name)
            current_end_log_pos: int = path.stat().st_size
            # Update the last read position in the metadata
            if log_metadata is not None:
                log_metadata[log_pos_name] = current_end_log_pos
            # Read the log file and yield lines
            log_streams.append(
                _stream_lines_by_chunk(open(path, encoding="utf-8"), last_end_log_pos, current_end_log_pos)
            )
        return sources, log_streams

    def _read_from_logs_server(
        self,
        ti: TaskInstance,
        worker_log_rel_path: str,
        log_metadata: LogMetadata | None = None,
    ) -> LogResponse:
        sources: LogSourceInfo = []
        parsed_log_streams: list[RawLogStream] = []
        try:
            log_type = LogType.TRIGGER if ti.triggerer_job else LogType.WORKER
            url, rel_path = self._get_log_retrieval_url(ti, worker_log_rel_path, log_type=log_type)
            response = _fetch_logs_from_service(url, rel_path)
            if response.status_code == 403:
                sources.append(
                    "!!!! Please make sure that all your Airflow components (e.g. "
                    "schedulers, webservers, workers and triggerer) have "
                    "the same 'secret_key' configured in 'webserver' section and "
                    "time is synchronized on all your machines (for example with ntpd)\n"
                    "See more at https://airflow.apache.org/docs/apache-airflow/"
                    "stable/configurations-ref.html#secret-key"
                )
            else:
                # Check if the resource was properly fetched
                response.raise_for_status()
                log_pos_name = LAST_LOG_POS_FORMAT.format(identifier=f"log_server_{rel_path}_{log_type}")
                last_end_log_pos: int = _get_last_log_pos_from_metadata(log_metadata, log_pos_name)
                current_end_log_pos: int = int(response.headers.get("Content-Length", 0))
                if current_end_log_pos > 0:
                    sources.append(url)
                    parsed_log_streams.append(
                        _stream_lines_by_chunk(
                            io.TextIOWrapper(response.raw), last_end_log_pos, current_end_log_pos
                        )
                    )
                    # update the last read position in the metadata
                    if log_metadata is not None:
                        log_metadata[log_pos_name] = current_end_log_pos
        except Exception as e:
            from requests.exceptions import InvalidURL

            if isinstance(e, InvalidURL) and ti.task.inherits_from_empty_operator is True:
                sources.append(self.inherits_from_empty_operator_log_message)
            else:
                sources.append(f"Could not read served logs: {e}")
                logger.exception("Could not read served logs")
        return sources, parsed_log_streams

    def _read_remote_logs(self, ti, try_number, metadata=None) -> LegacyLogResponse | LogResponse:
        """
        Implement in subclasses to read from the remote service.

        This method should return two lists, messages and logs.

        * Each element in the messages list should be a single message,
          such as, "reading from x file".
        * Each element in the logs list should be the content of one file.
        """
        remote_io = None
        try:
            from airflow.logging_config import REMOTE_TASK_LOG

            remote_io = REMOTE_TASK_LOG
        except Exception:
            pass

        if remote_io is None:
            # Import not found, or explicitly set to None
            raise NotImplementedError

        # This living here is not really a good plan, but it just about works for now.
        # Ideally we move all the read+combine logic in to TaskLogReader and out of the task handler.
        path = self._render_filename(ti, try_number)
        sources, logs = remote_io.read(path, ti)
        return sources, logs or []
