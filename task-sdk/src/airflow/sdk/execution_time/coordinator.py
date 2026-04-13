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
Locale coordinator for non-Python DAG file processing and task execution.

Provides :class:`BaseLocaleCoordinator`, the base class for
language-specific coordinators that bridge subprocess I/O between the
Airflow supervisor and an external-language runtime (Java, Go, Rust, etc.).

The coordinator's :meth:`~BaseLocaleCoordinator.run_dag_parsing` method
handles the full lifecycle:

1. Creates TCP servers for comm and logs channels.
2. Calls :meth:`~BaseLocaleCoordinator.dag_parsing_locale_cmd` (provided
   by the subclass) to obtain the subprocess command.
3. Spawns the subprocess and accepts TCP connections from it.
4. Runs a selector-based bridge that transparently forwards bytes
   between fd 0 (supervisor) and the subprocess comm socket, and
   re-emits the subprocess's log output through structlog.

I/O multiplexing uses the same selector-based loop as
:class:`~airflow.sdk.execution_time.supervisor.WatchedSubprocess`,
driven by :func:`~airflow.sdk.execution_time.selector_loop.service_selector`.
"""

from __future__ import annotations

import contextlib
import os
import selectors
import socket
import subprocess
import time
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
    from airflow.sdk.execution_time.comms import StartupDetails


def _start_server() -> socket.socket:
    """Create a TCP server socket bound to a random port on localhost."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.setblocking(True)
    server.listen(1)
    return server


def _send_startup_details(locale_comm: socket.socket, startup_details: StartupDetails) -> None:
    """
    Re-encode and send the ``StartupDetails`` frame to the locale subprocess.

    In the task execution flow, ``task_runner.main()`` consumes the
    ``StartupDetails`` message from fd 0 (to determine routing) before
    delegating to the locale coordinator.  This function re-serializes
    the message and writes it to the locale subprocess's comm socket so
    the subprocess receives it as if it came directly from the supervisor.
    """
    from airflow.sdk.execution_time.comms import _ResponseFrame

    # Use mode="json" so that datetime, UUID, and other complex Python
    # types are serialized as plain strings/numbers in msgpack — avoiding
    # msgpack extension types (e.g. Timestamp) that non-Python decoders
    # may not support.
    frame = _ResponseFrame(id=0, body=startup_details.model_dump(mode="json"))
    locale_comm.sendall(frame.as_bytes())


def _bridge(
    supervisor_comm: socket.socket,
    locale_comm: socket.socket,
    locale_logs: socket.socket,
    locale_stderr: socket.socket,
    proc: subprocess.Popen,
    log: FilteringBoundLogger,
) -> None:
    """
    Multiplex I/O between the supervisor and a locale subprocess.

    Four channels are registered with the selector:

    - ``supervisor_comm`` -> ``locale_comm`` (raw byte forwarding)
    - ``locale_comm`` -> ``supervisor_comm`` (raw byte forwarding)
    - ``locale_logs`` -> structlog (line-buffered JSON logs)
    - ``locale_stderr`` -> structlog (line-buffered stderr output)

    Uses the same ``(handler, on_close)`` callback contract as
    :class:`~airflow.sdk.execution_time.supervisor.WatchedSubprocess`,
    driven by :func:`~airflow.sdk.execution_time.selector_loop.service_selector`.
    """
    from airflow.sdk.execution_time.selector_loop import (
        make_buffered_socket_reader,
        make_raw_forwarder,
        service_selector,
    )
    from airflow.sdk.execution_time.supervisor import (
        forward_to_log,
        process_log_messages_from_subprocess,
    )

    sel = selectors.DefaultSelector()

    def on_close(sock: socket.socket) -> None:
        with contextlib.suppress(KeyError):
            sel.unregister(sock)

    target_loggers = (log,)

    # Comm: bidirectional raw byte forwarding.
    sel.register(supervisor_comm, selectors.EVENT_READ, make_raw_forwarder(locale_comm, on_close))
    sel.register(locale_comm, selectors.EVENT_READ, make_raw_forwarder(supervisor_comm, on_close))

    # TCP logs channel: line-buffered JSON from the locale SDK's LogSender,
    # processed with the same handler as WatchedSubprocess (level mapping,
    # timestamp parsing, exception extraction).
    sel.register(
        locale_logs,
        selectors.EVENT_READ,
        make_buffered_socket_reader(process_log_messages_from_subprocess(target_loggers), on_close),
    )
    # stderr: plain-text output from the locale process's logging framework
    # (e.g. SLF4J simple logger).  Use forward_to_log which handles raw
    # text lines, not process_log_messages_from_subprocess which expects JSON.
    import logging

    sel.register(
        locale_stderr,
        selectors.EVENT_READ,
        make_buffered_socket_reader(
            forward_to_log(target_loggers, logger="task.stderr", level=logging.ERROR), on_close
        ),
    )

    # Event loop -- runs until the subprocess exits and all sockets are drained.
    while sel.get_map():
        service_selector(sel, timeout=1.0)
        if proc.poll() is not None:
            # Subprocess has exited -- drain remaining data with a short deadline.
            deadline = time.monotonic() + 5.0
            while sel.get_map() and time.monotonic() < deadline:
                service_selector(sel, timeout=0.5)
            break

    sel.close()
    for sock in (supervisor_comm, locale_comm, locale_logs, locale_stderr):
        with contextlib.suppress(OSError):
            sock.close()


class BaseLocaleCoordinator:
    """
    Base coordinator for locale-specific DAG file processing and task execution.

    Providers register subclasses in their ``provider.yaml`` under
    ``process-coordinators``.  Both :class:`ProvidersManager` (airflow-core)
    and :class:`ProvidersManagerTaskRuntime` (task-sdk) discover registered
    coordinators through this single extension point.

    Subclasses represent a specific language runtime (Java, Go, etc.) and
    only need to implement :meth:`can_handle_dag_file`,
    :meth:`dag_parsing_locale_cmd` and :meth:`task_execution_locale_cmd`.
    The base class owns the entire bridge lifecycle: TCP servers,
    subprocess management, selector-based I/O loop, and cleanup.
    """

    locale_name: str

    class DagParsingInfo(NamedTuple):
        """Information needed for locale Dag parsing."""

        dag_file_path: str
        bundle_name: str
        bundle_path: str
        mode: str = "dag-parsing"

    class TaskExecutionInfo(NamedTuple):
        """Information needed for locale task execution."""

        what: TaskInstance
        dag_rel_path: str | os.PathLike[str]
        bundle_info: BundleInfo
        startup_details: StartupDetails
        mode: str = "task-execution"

    @classmethod
    def can_handle_dag_file(cls, bundle_name: str, path: str | os.PathLike[str]) -> bool:
        """
        Return ``True`` if this coordinator should handle DAG-file parsing for *path*.

        Called by :meth:`DagFileProcessorProcess._resolve_processor_target` to
        decide whether to delegate parsing to this coordinator's
        :meth:`run_dag_parsing` instead of the default Python entrypoint.

        The default implementation returns ``False``; subclasses must override.
        """
        return False

    @classmethod
    def dag_parsing_locale_cmd(
        cls,
        *,
        dag_file_path: str,
        bundle_name: str,
        bundle_path: str,
        comm_addr: str,
        logs_addr: str,
    ) -> list[str]:
        """
        Return the subprocess command for DAG file parsing.

        :param dag_file_path: Absolute path to the DAG file to parse.
        :param bundle_name: Name of the DAG bundle.
        :param bundle_path: Root path of the DAG bundle.
        :param comm_addr: ``host:port`` the subprocess must connect to
            for the bidirectional msgpack comm channel.
        :param logs_addr: ``host:port`` the subprocess must connect to
            for the structured JSON log channel.
        :returns: Full command list (e.g. ``["java", "-cp", "...", ...]`` based on each locale).
        """
        raise NotImplementedError

    @classmethod
    def task_execution_locale_cmd(
        cls,
        *,
        what: TaskInstance,
        dag_rel_path: str | os.PathLike[str],
        bundle_info: BundleInfo,
        comm_addr: str,
        logs_addr: str,
    ) -> list[str]:
        """
        Return the subprocess command for task execution.

        :param what: The task instance to execute.
        :param dag_rel_path: Relative path to the DAG file within the bundle.
        :param bundle_info: Bundle metadata.
        :param comm_addr: ``host:port`` the subprocess must connect to
            for the bidirectional msgpack comm channel.
        :param logs_addr: ``host:port`` the subprocess must connect to
            for the structured JSON log channel.
        :returns: Full command list.
        """
        raise NotImplementedError

    @classmethod
    def run_dag_parsing(cls, *, path: str, bundle_name: str, bundle_path: str) -> None:
        """Entry point for running locale-specific Dag File Processing."""
        cls._locale_subprocess_entrypoint(
            cls.DagParsingInfo(
                dag_file_path=path,
                bundle_name=bundle_name,
                bundle_path=bundle_path,
            )
        )

    @classmethod
    def run_task_execution(
        cls,
        *,
        what: TaskInstance,
        dag_rel_path: str | os.PathLike[str],
        bundle_info: BundleInfo,
        startup_details: StartupDetails,
    ) -> None:
        cls._locale_subprocess_entrypoint(
            cls.TaskExecutionInfo(
                what=what,
                dag_rel_path=dag_rel_path,
                bundle_info=bundle_info,
                startup_details=startup_details,
            )
        )

    @classmethod
    def _locale_subprocess_entrypoint(cls, entrypoint_info: DagParsingInfo | TaskExecutionInfo) -> None:
        """
        Spawn the locale subprocess and bridge I/O with the supervisor.

        This is called inside the forked child process where fd 0 is the
        bidirectional comms socket to the supervisor.  The method:

        1. Creates TCP servers for comm and logs.
        2. Calls :meth:`dag_parsing_locale_cmd` or :meth:`task_execution_locale_cmd` to get the command.
        3. Spawns the subprocess with ``stdin=/dev/null`` and stderr
           captured via a socketpair.
        4. Runs the selector-based bridge until the subprocess exits.

        fd layout (set up by ``_reopen_std_io_handles`` before this runs):

        - fd 0 -- bidirectional comms socket to the supervisor
          (``DagFileParseRequest`` <-> ``DagFileParsingResult``,
          length-prefixed msgpack frames)
        - fd 1 -- stdout socket to the supervisor
        - fd 2 -- stderr socket to the supervisor
        - fd N -- structured JSON log channel (``log_fd``, configured by
          ``_configure_logs_over_json_channel`` -> structlog)
        """
        os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"

        import structlog

        log = structlog.get_logger(logger_name="task")
        log.info(
            "Starting locale subprocess",
            locale=cls.locale_name,
            mode=entrypoint_info.mode,
        )

        # TCP servers for the locale subprocess to connect to.
        comm_server = _start_server()
        logs_server = _start_server()
        comm_host, comm_port = comm_server.getsockname()
        logs_host, logs_port = logs_server.getsockname()

        comm_addr = f"{comm_host}:{comm_port}"
        logs_addr = f"{logs_host}:{logs_port}"

        # stderr uses a socketpair (instead of ``subprocess.PIPE``) so it
        # is a real socket compatible with ``make_buffered_socket_reader``.
        child_stderr, read_stderr = socket.socketpair()

        if isinstance(entrypoint_info, cls.DagParsingInfo):
            cmd = cls.dag_parsing_locale_cmd(
                dag_file_path=entrypoint_info.dag_file_path,
                bundle_name=entrypoint_info.bundle_name,
                bundle_path=entrypoint_info.bundle_path,
                comm_addr=comm_addr,
                logs_addr=logs_addr,
            )
        elif isinstance(entrypoint_info, cls.TaskExecutionInfo):
            cmd = cls.task_execution_locale_cmd(
                what=entrypoint_info.what,
                dag_rel_path=entrypoint_info.dag_rel_path,
                bundle_info=entrypoint_info.bundle_info,
                comm_addr=comm_addr,
                logs_addr=logs_addr,
            )
        else:
            raise ValueError(f"Unknown entrypoint_info type: {type(entrypoint_info)}")

        # stdin redirected to /dev/null so the subprocess does not inherit
        # fd 0 (the comms socket).
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stderr=child_stderr.fileno(),
        )
        child_stderr.close()

        # Wait for the subprocess to connect to both servers.
        locale_comm, _ = comm_server.accept()
        locale_logs, _ = logs_server.accept()
        comm_server.close()
        logs_server.close()

        # For task execution the supervisor already sent ``StartupDetails``
        # on fd 0 and ``task_runner.main()`` consumed it before delegating
        # here.  Re-encode and forward it to the locale subprocess so it
        # knows which task to execute.
        if isinstance(entrypoint_info, cls.TaskExecutionInfo):
            _send_startup_details(locale_comm, entrypoint_info.startup_details)

        # fd 0 is the bidirectional comms socket to the supervisor.
        supervisor_comm = socket.socket(fileno=os.dup(0))

        _bridge(supervisor_comm, locale_comm, locale_logs, read_stderr, proc, log)


__all__ = ["BaseLocaleCoordinator"]
