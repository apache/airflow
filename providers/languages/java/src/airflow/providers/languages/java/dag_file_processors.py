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
Java DAG file processor implementations.

The entrypoint runs inside a forked child process where fd 0 is a
bidirectional socket to the supervisor (set up by ``_fork_main``).
Instead of decoding messages with ``CommsDecoder``, we spawn a Java
subprocess, let it connect back over TCP, and bridge raw bytes between
fd 0 and the Java socket.  The supervisor's existing ``_handle_request``
handles the protocol on its side -- the bridge is transparent.

I/O multiplexing uses the same selector-based loop as
:class:`~airflow.sdk.execution_time.supervisor.WatchedSubprocess`:
sockets are registered with ``(handler, on_close)`` callback tuples
produced by :func:`~airflow.sdk.execution_time.selector_loop.make_buffered_socket_reader`
and :func:`~airflow.sdk.execution_time.selector_loop.make_raw_forwarder`,
then driven by :func:`~airflow.sdk.execution_time.selector_loop.service_selector`.
"""

from __future__ import annotations

import contextlib
import email
import json
import os
import pathlib
import selectors
import socket
import subprocess
import time
import zipfile
from typing import TYPE_CHECKING

from airflow.dag_processing.processor import BaseDagFileProcessor

if TYPE_CHECKING:
    from collections.abc import Generator

    from structlog.typing import FilteringBoundLogger


def _start_server() -> socket.socket:
    """Create a TCP server socket bound to a random port on localhost."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.setblocking(True)
    server.listen(1)
    return server


def _find_main_class(jar_path: pathlib.Path) -> str:
    """Read the Main-Class attribute from the JAR manifest."""
    with zipfile.ZipFile(jar_path) as zf:
        with zf.open("META-INF/MANIFEST.MF") as f:
            if main_class := email.message_from_binary_file(f).get("Main-Class"):
                return main_class
    raise FileNotFoundError(f"No Main-Class in manifest of {jar_path}")


class JavaDagFileProcessor(BaseDagFileProcessor):
    """
    DAG file processor for Java JAR bundle workloads.

    Registered via ``dag-file-processors`` in the Java provider's ``provider.yaml``.
    When the dag processor encounters a file that belongs to a Java bundle,
    this processor's :meth:`entrypoint` is used as the subprocess target instead
    of the default Python ``_parse_file_entrypoint``.
    """

    def can_handle(self, bundle_name: str, path: str | os.PathLike[str]) -> bool:
        # The parent class will only validate against the bundle name
        # If the configured bundle name doesn't match, we can skip the more expensive .jar content validation
        if not super().can_handle(bundle_name, path):
            return False

        # Then the dag_importer will validate based on the .jar content

        # TODO: If we decided to leverage AIP-85 `DagImporterRegistry`
        # We should reuse `dag_importer.can_handle`

        with contextlib.suppress(FileNotFoundError):
            return _find_main_class(pathlib.Path(path)) is not None
        return False

    @staticmethod
    def entrypoint(path: str, bundle_name: str, bundle_path: str) -> None:
        """Bridge fd 0 (supervisor comms) to a Java subprocess over TCP."""
        parse_jar_bundles_entrypoint(path, bundle_name, bundle_path)


def parse_jar_bundles_entrypoint(path: str, bundle_name: str, bundle_path: str) -> None:
    """
    Spawn a Java subprocess and bridge the supervisor-to-Java communication.

    After ``_fork_main``, fd 0 is a bidirectional socket to the supervisor.
    This function:

    1. Creates TCP servers for comm and logs channels.
    2. Spawns Java via ``subprocess.Popen``, passing both TCP addresses.
    3. Accepts connections from Java on both channels.
    4. Runs a selector-based bridge that transparently forwards bytes
       between fd 0 (supervisor) and the Java comm socket, and re-emits
       Java's log output through structlog (routed to ``log_fd``).

    No ``CommsDecoder`` is needed -- the supervisor and Java both speak
    the length-prefixed msgpack protocol; we just shuttle bytes.
    """
    os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"

    import structlog

    log = structlog.get_logger(logger_name="task")

    log.info("Starting Java DAG file processor", path=path, bundle_name=bundle_name, bundle_path=bundle_path)
    jar_path = pathlib.Path(path)

    # TCP servers for Java to connect to (comm + logs).
    comm_server = _start_server()
    logs_server = _start_server()
    comm_host, comm_port = comm_server.getsockname()
    logs_host, logs_port = logs_server.getsockname()

    # Build the classpath from all JARs in the bundle directory.
    # Java bundles are typically thin JARs: the main JAR (e.g. example.jar)
    # only contains the bundle's own classes while its dependencies (the
    # Airflow Java SDK, logging libraries, etc.) are separate JARs that live
    # alongside it.  Using the ``<dir>/*`` wildcard lets the JVM load every
    # JAR in the directory, matching the standard Gradle/Maven distribution
    # layout produced by ``copyDependencies``-style tasks.
    classpath = f"{bundle_path}/*"

    # Spawn the Java subprocess.
    #
    # fd layout in the forked child (set up by ``_reopen_std_io_handles``
    # before this entrypoint is called):
    #
    #   fd 0  -- bidirectional comms socket to the supervisor
    #            (``DagFileParseRequest`` <-> ``DagFileParsingResult``,
    #             length-prefixed msgpack frames)
    #   fd 1  -- stdout socket to the supervisor
    #   fd 2  -- stderr socket to the supervisor
    #   fd N  -- structured JSON log channel (``log_fd``, configured by
    #            ``_configure_logs_over_json_channel`` -> structlog)
    #
    # We redirect stdin to ``/dev/null`` so that the Java subprocess does
    # not inherit fd 0 (the comms socket).  Java communicates over the TCP
    # sockets passed as ``--comm`` / ``--logs``; the bridge shuttles bytes
    # between those TCP sockets and fd 0.
    #
    # stderr uses a socketpair (instead of ``subprocess.PIPE``) so it is a
    # real socket compatible with ``make_buffered_socket_reader``'s
    # ``recv_into``.
    child_stderr, read_stderr = socket.socketpair()

    proc = subprocess.Popen(
        [
            "java",
            "-classpath",
            classpath,
            _find_main_class(jar_path),
            f"--comm={comm_host}:{comm_port}",
            f"--logs={logs_host}:{logs_port}",
        ],
        stdin=subprocess.DEVNULL,
        stderr=child_stderr.fileno(),
    )
    child_stderr.close()  # Close the child's end in the parent.

    # Wait for Java to connect to both servers.
    java_comm, _ = comm_server.accept()
    java_logs, _ = logs_server.accept()
    comm_server.close()
    logs_server.close()

    # fd 0 is the bidirectional comms socket to the supervisor.
    supervisor_comm = socket.socket(fileno=os.dup(0))

    # Bridge: multiplex I/O between the supervisor and Java.
    _bridge(supervisor_comm, java_comm, java_logs, read_stderr, proc, log)


_JAVA_LEVEL_MAP = {"warn": "warning", "trace": "debug"}


def _java_log_forwarder(log: FilteringBoundLogger) -> Generator[None, bytes | bytearray, None]:
    """
    Receive line-buffered bytes from Java and re-emit via structlog.

    Follows the same generator protocol as
    :func:`~airflow.sdk.execution_time.supervisor.forward_to_log` and
    :func:`~airflow.sdk.execution_time.supervisor.process_log_messages_from_subprocess`:
    primed with ``next(gen)``, then fed lines via ``gen.send(line)``.
    """
    while True:
        raw_line = yield
        line = raw_line.decode("utf-8", errors="replace").rstrip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            level = msg.pop("level", "info")
            event = msg.pop("event", "")
            msg.pop("timestamp", None)
            level_name = _JAVA_LEVEL_MAP.get(level, level)
            log_fn = getattr(log, level_name, log.info)
            log_fn(event, **msg)
        except (json.JSONDecodeError, ValueError, TypeError):
            log.info(line)


def _bridge(
    supervisor_comm: socket.socket,
    java_comm: socket.socket,
    java_logs: socket.socket,
    java_stderr: socket.socket,
    proc: subprocess.Popen,
    log: FilteringBoundLogger,
) -> None:
    """
    Multiplex I/O between the supervisor and Java using a selector loop.

    Four channels are registered with the selector:

    - ``supervisor_comm`` -> ``java_comm`` (raw: ``DagFileParseRequest`` and
      intermediate responses)
    - ``java_comm`` -> ``supervisor_comm`` (raw: intermediate requests and
      ``DagFileParsingResult``)
    - ``java_logs`` -> structlog (line-buffered JSON from the Java SDK's
      ``LogSender``)
    - ``java_stderr`` -> structlog (line-buffered SLF4J output)

    The same ``(handler, on_close)`` callback contract used by
    :class:`~airflow.sdk.execution_time.supervisor.WatchedSubprocess`
    applies here, driven by :func:`service_selector`.
    """
    from airflow.sdk.execution_time.selector_loop import (
        make_buffered_socket_reader,
        make_raw_forwarder,
        service_selector,
    )

    sel = selectors.DefaultSelector()

    def on_close(sock: socket.socket) -> None:
        with contextlib.suppress(KeyError):
            sel.unregister(sock)

    # Comm: bidirectional raw byte forwarding.
    sel.register(supervisor_comm, selectors.EVENT_READ, make_raw_forwarder(java_comm, on_close))
    sel.register(java_comm, selectors.EVENT_READ, make_raw_forwarder(supervisor_comm, on_close))

    # Logs: line-buffered JSON -> structlog.
    sel.register(
        java_logs,
        selectors.EVENT_READ,
        make_buffered_socket_reader(_java_log_forwarder(log), on_close),
    )
    sel.register(
        java_stderr,
        selectors.EVENT_READ,
        make_buffered_socket_reader(_java_log_forwarder(log), on_close),
    )

    # Event loop -- runs until Java exits and all sockets are drained.
    while sel.get_map():
        service_selector(sel, timeout=1.0)
        if proc.poll() is not None:
            # Java has exited -- drain remaining data with a short deadline.
            deadline = time.monotonic() + 5.0
            while sel.get_map() and time.monotonic() < deadline:
                service_selector(sel, timeout=0.5)
            break

    sel.close()
    for sock in (supervisor_comm, java_comm, java_logs, java_stderr):
        with contextlib.suppress(OSError):
            sock.close()
