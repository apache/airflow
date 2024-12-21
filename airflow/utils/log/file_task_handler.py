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
import logging
import os
from collections.abc import Generator, Iterable
from contextlib import suppress
from enum import Enum
from functools import partial
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional
from urllib.parse import urljoin

import pendulum

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.utils.helpers import parse_template_string, render_template
from airflow.utils.log.logging_mixin import SetContextPropagate
from airflow.utils.log.non_caching_file_handler import NonCachingRotatingFileHandler
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from airflow.executors.base_executor import BaseExecutor
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024 * 5  # 5MB
DEFAULT_SORT_DATETIME = pendulum.datetime(2000, 1, 1)
HEAP_DUMP_SIZE = 500000
HALF_HEAP_DUMP_SIZE = HEAP_DUMP_SIZE // 2

_ParsedLogRecordType = tuple[Optional[pendulum.DateTime], int, str]
"""Tuple of timestamp, line number, and line."""
_ParsedLogStreamType = Generator[_ParsedLogRecordType, None, None]
"""Generator of parsed log streams, each yielding a tuple of timestamp, line number, and line."""
_LogSourceType = tuple[list[str], list[_ParsedLogStreamType], int]
"""Tuple of messages, parsed log streams, total size of logs."""


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


def _fetch_logs_from_service(url, log_relative_path):
    # Import occurs in function scope for perf. Ref: https://github.com/apache/airflow/pull/21438
    import requests

    from airflow.utils.jwt_signer import JWTSigner

    timeout = conf.getint("webserver", "log_fetch_timeout_sec", fallback=None)
    signer = JWTSigner(
        secret_key=conf.get("webserver", "secret_key"),
        expiration_time_in_seconds=conf.getint("webserver", "log_request_clock_grace", fallback=30),
        audience="task-instance-logs",
    )
    response = requests.get(
        url,
        timeout=timeout,
        headers={"Authorization": signer.generate_signed_token({"filename": log_relative_path})},
    )
    response.encoding = "utf-8"
    return response


_parse_timestamp = conf.getimport("logging", "interleave_timestamp_parser", fallback=None)

if not _parse_timestamp:

    def _parse_timestamp(line: str):
        timestamp_str, _ = line.split(" ", 1)
        return pendulum.parse(timestamp_str.strip("[]"))


def _get_parsed_log_stream(file_path: Path) -> _ParsedLogStreamType:
    with open(file_path) as f:
        for file_chunk in iter(partial(f.read, CHUNK_SIZE), b""):
            if not file_chunk:
                break
            # parse log lines
            lines = file_chunk.splitlines()
            timestamp = None
            next_timestamp = None
            for idx, line in enumerate(lines):
                if line:
                    with suppress(Exception):
                        # next_timestamp unchanged if line can't be parsed
                        next_timestamp = _parse_timestamp(line)
                    if next_timestamp:
                        timestamp = next_timestamp
                    yield timestamp, idx, line


def _sort_key(timestamp: pendulum.DateTime | None, line_num: int) -> int:
    """
    Generate a sort key for log record, to be used in K-way merge.

    :param timestamp: timestamp of the log line
    :param line_num: line number of the log line
    :return: a integer as sort key to avoid overhead of memory usage
    """
    return (timestamp or DEFAULT_SORT_DATETIME).int_timestamp * 10000000 + line_num


def _add_log_from_parsed_log_streams_to_heap(
    heap: list[tuple[int, str]],
    parsed_log_streams: list[_ParsedLogStreamType],
) -> None:
    """
    Add one log record from each parsed log stream to the heap.

    Remove any empty log stream from the list while iterating.

    :param heap: heap to store log records
    :param parsed_log_streams: list of parsed log streams
    """
    for log_stream in parsed_log_streams:
        if log_stream is None:
            parsed_log_streams.remove(log_stream)
            continue
        record: _ParsedLogRecordType | None = next(log_stream, None)
        if record is None:
            parsed_log_streams.remove(log_stream)
            continue
        timestamp, line_num, line = record
        # take int as sort key to avoid overhead of memory usage
        heapq.heappush(heap, (_sort_key(timestamp, line_num), line))


def _interleave_logs(*parsed_log_streams: _ParsedLogStreamType) -> Generator[str, None, None]:
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
    heap: list[tuple[int, str]] = []
    # to allow removing empty streams while iterating
    log_streams: list[_ParsedLogStreamType] = [log_stream for log_stream in parsed_log_streams]

    # add first record from each log stream to heap
    _add_log_from_parsed_log_streams_to_heap(heap, log_streams)

    # keep adding records from logs until all logs are empty
    last = None
    while heap:
        if not log_streams:
            break

        _add_log_from_parsed_log_streams_to_heap(heap, log_streams)

        # yield HALF_HEAP_DUMP_SIZE records when heap size exceeds HEAP_DUMP_SIZE
        if len(heap) >= HEAP_DUMP_SIZE:
            for _ in range(HALF_HEAP_DUMP_SIZE):
                _, line = heapq.heappop(heap)
                if line != last:  # dedupe
                    yield line
                last = line
            continue

    # yield remaining records
    for _, line in heap:
        if line != last:  # dedupe
            yield line
        last = line
    # free memory
    del heap
    del log_streams


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
        else:
            raise RuntimeError(f"Unable to render log filename for {ti}. This should never happen")

    def _read_grouped_logs(self):
        return False

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
        metadata: dict[str, Any] | None = None,
    ) -> tuple[Iterable[str], dict[str, Any]]:
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
        messages_list: list[str] = []
        remote_parsed_logs: list[_ParsedLogStreamType] = []
        remote_logs_size = 0
        local_parsed_logs: list[_ParsedLogStreamType] = []
        local_logs_size = 0
        executor_messages: list[str] = []
        executor_parsed_logs: list[_ParsedLogStreamType] = []
        executor_logs_size = 0
        served_parsed_logs: list[_ParsedLogStreamType] = []
        served_logs_size = 0
        with suppress(NotImplementedError):
            remote_messages, remote_logs = self._read_remote_logs(ti, try_number, metadata)
            messages_list.extend(remote_messages)
        has_k8s_exec_pod = False
        if ti.state == TaskInstanceState.RUNNING:
            executor_get_task_log = self._get_executor_get_task_log(ti)
            response = executor_get_task_log(ti, try_number)
            if response:
                executor_messages, executor_logs = response
            if executor_messages:
                messages_list.extend(executor_messages)
                has_k8s_exec_pod = True
        if not (remote_logs and ti.state not in State.unfinished):
            # when finished, if we have remote logs, no need to check local
            worker_log_full_path = Path(self.local_base, worker_log_rel_path)
            local_messages, local_parsed_logs, local_logs_size = self._read_from_local(worker_log_full_path)
            messages_list.extend(local_messages)
        if ti.state in (TaskInstanceState.RUNNING, TaskInstanceState.DEFERRED) and not has_k8s_exec_pod:
            served_messages, served_logs = self._read_from_logs_server(ti, worker_log_rel_path)
            messages_list.extend(served_messages)
        elif ti.state not in State.unfinished and not (local_parsed_logs or remote_logs):
            # ordinarily we don't check served logs, with the assumption that users set up
            # remote logging or shared drive for logs for persistence, but that's not always true
            # so even if task is done, if no local logs or remote logs are found, we'll check the worker
            served_messages, served_logs = self._read_from_logs_server(ti, worker_log_rel_path)
            messages_list.extend(served_messages)

        # Log message source details are grouped: they are not relevant for most users and can
        # distract them from finding the root cause of their errors
        messages = " INFO - ::group::Log message source details\n"
        messages += "".join([f"*** {x}\n" for x in messages_list])
        messages += " INFO - ::endgroup::\n"
        end_of_log = ti.try_number != try_number or ti.state not in (
            TaskInstanceState.RUNNING,
            TaskInstanceState.DEFERRED,
        )

        current_total_logs_size = local_logs_size + remote_logs_size + executor_logs_size + served_logs_size
        interleave_log_stream = _interleave_logs(
            *local_parsed_logs,
            *remote_parsed_logs,
            *(executor_parsed_logs or []),
            *served_parsed_logs,
        )

        # skip log stream until the last position
        if metadata and "log_pos" in metadata:
            offset = metadata["log_pos"]
            for _ in range(offset):
                next(interleave_log_stream, None)

        out_stream: Iterable[str]
        if "log_pos" in (metadata or {}):
            # don't need to add messages, since we're in the middle of the log
            out_stream = interleave_log_stream
        else:
            # first time reading log, add messages before interleaved log stream
            out_stream = chain((msg for msg in messages), interleave_log_stream)
        return out_stream, {"end_of_log": end_of_log, "log_pos": current_total_logs_size}

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
        self, task_instance, try_number=None, metadata=None
    ) -> tuple[list[str], list[Generator[str, None, None]], list[dict[str, Any]]]:
        """
        Read logs of given task instance from local machine.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from. If None
                           it returns all logs separated by try_number
        :param metadata: log metadata, can be used for steaming log reading and auto-tailing.
        :return: tuple of hosts, log streams, and metadata_array
        """
        # Task instance increments its try number when it starts to run.
        # So the log for a particular task try will only show up when
        # try number gets incremented in DB, i.e logs produced the time
        # after cli run and before try_number + 1 in DB will not be displayed.
        if try_number is None:
            next_try = task_instance.try_number + 1
            try_numbers = list(range(1, next_try))
        elif try_number < 1:
            error_logs = [(log for log in [f"Error fetching the logs. Try number {try_number} is invalid."])]
            return ["default_host"], error_logs, [{"end_of_log": True}]
        else:
            try_numbers = [try_number]

        hosts = [""] * len(try_numbers)
        logs: list = [] * len(try_numbers)
        metadata_array: list[dict] = [{}] * len(try_numbers)

        # subclasses implement _read and may not have log_type, which was added recently
        for i, try_number_element in enumerate(try_numbers):
            log_stream, out_metadata = self._read(task_instance, try_number_element, metadata)
            # es_task_handler return logs grouped by host. wrap other handler returning log string
            # with default/ empty host so that UI can render the response in the same way
            if not self._read_grouped_logs():
                hosts[i] = task_instance.hostname

            logs[i] = log_stream
            metadata_array[i] = out_metadata

        return hosts, logs, metadata_array

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
    def _read_from_local(worker_log_path: Path) -> _LogSourceType:
        """
        Read logs from local file.

        :param worker_log_path: Path to the worker log file
        :return: Tuple of messages, log streams, total size of logs
        """
        total_log_size: int = 0
        messages: list[str] = []
        parsed_log_streams: list[_ParsedLogStreamType] = []
        paths = sorted(worker_log_path.parent.glob(worker_log_path.name + "*"))
        if not paths:
            return messages, parsed_log_streams, total_log_size

        messages.append("Found local files:")
        for path in paths:
            total_log_size += path.stat().st_size
            messages.append(f"  * {path}")
            parsed_log_streams.append(_get_parsed_log_stream(path))

        return messages, parsed_log_streams, total_log_size

    def _read_from_logs_server(self, ti, worker_log_rel_path) -> tuple[list[str], list[str]]:
        messages = []
        logs = []
        try:
            log_type = LogType.TRIGGER if ti.triggerer_job else LogType.WORKER
            url, rel_path = self._get_log_retrieval_url(ti, worker_log_rel_path, log_type=log_type)
            response = _fetch_logs_from_service(url, rel_path)
            if response.status_code == 403:
                messages.append(
                    "!!!! Please make sure that all your Airflow components (e.g. "
                    "schedulers, webservers, workers and triggerer) have "
                    "the same 'secret_key' configured in 'webserver' section and "
                    "time is synchronized on all your machines (for example with ntpd)\n"
                    "See more at https://airflow.apache.org/docs/apache-airflow/"
                    "stable/configurations-ref.html#secret-key"
                )
            # Check if the resource was properly fetched
            response.raise_for_status()
            if response.text:
                messages.append(f"Found logs served from host {url}")
                logs.append(response.text)
        except Exception as e:
            from requests.exceptions import InvalidSchema

            if isinstance(e, InvalidSchema) and ti.task.inherits_from_empty_operator is True:
                messages.append(self.inherits_from_empty_operator_log_message)
            else:
                messages.append(f"Could not read served logs: {e}")
                logger.exception("Could not read served logs")
        return messages, logs

    def _read_remote_logs(self, ti, try_number, metadata=None) -> tuple[list[str], list[str]]:
        """
        Implement in subclasses to read from the remote service.

        This method should return two lists, messages and logs.

        * Each element in the messages list should be a single message,
          such as, "reading from x file".
        * Each element in the logs list should be the content of one file.
        """
        raise NotImplementedError
