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
handles the protocol on its side — the bridge is transparent.
"""

from __future__ import annotations

import contextlib
import email
import os
import pathlib
import socket
import subprocess
import threading
import zipfile

from airflow.dag_processing.processor import BaseDagFileProcessor


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
        if not super().can_handle(bundle_name):
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
    4. Runs a threaded bridge that transparently forwards bytes between
       fd 0 (supervisor) and the Java comm socket, and forwards Java's
       structured log output to the child's stderr.

    No ``CommsDecoder`` is needed — the supervisor and Java both speak
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

    # Spawn the Java subprocess.
    proc = subprocess.Popen(
        [
            "java",
            "-classpath",
            jar_path.as_posix(),
            _find_main_class(jar_path),
            f"--comm={comm_host}:{comm_port}",
            f"--logs={logs_host}:{logs_port}",
        ],
        # Java stdout/stderr are inherited from the forked child
        # (fd 1 and fd 2 already go to the supervisor's log readers).
    )

    # Wait for Java to connect to both servers.
    java_comm, _ = comm_server.accept()
    java_logs, _ = logs_server.accept()
    comm_server.close()
    logs_server.close()

    # fd 0 is the bidirectional comms socket to the supervisor.
    supervisor_comm = socket.socket(fileno=os.dup(0))

    # Bridge: forward raw bytes between the supervisor and Java.
    _bridge(supervisor_comm, java_comm, java_logs, proc)


def _pipe(src: socket.socket, dest: socket.socket) -> None:
    """Forward all bytes from *src* to *dest* until EOF or error."""
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dest.sendall(data)
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass


def _forward_logs(src: socket.socket) -> None:
    """Forward Java's structured log lines to stderr for the supervisor to capture."""
    try:
        while True:
            data = src.recv(4096)
            if not data:
                break
            os.write(2, data)
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass


def _bridge(
    supervisor_comm: socket.socket,
    java_comm: socket.socket,
    java_logs: socket.socket,
    proc: subprocess.Popen,
) -> None:
    """
    Forward bytes between the supervisor and Java until the Java process exits.

    Three threads run concurrently:
    - supervisor → Java comm (forwards ``DagFileParseRequest`` and intermediate responses)
    - Java comm → supervisor (forwards intermediate requests and ``DagFileParsingResult``)
    - Java logs → stderr (structured log lines from the Java SDK)
    """
    sup_to_java = threading.Thread(target=_pipe, args=(supervisor_comm, java_comm), daemon=True)
    java_to_sup = threading.Thread(target=_pipe, args=(java_comm, supervisor_comm), daemon=True)
    logs_fwd = threading.Thread(target=_forward_logs, args=(java_logs,), daemon=True)

    sup_to_java.start()
    java_to_sup.start()
    logs_fwd.start()

    # Wait for the Java process to complete.
    proc.wait()

    # java_to_sup sees EOF when Java closes its comm socket; wait for it
    # to finish forwarding all remaining data (including DagFileParsingResult).
    java_to_sup.join(timeout=30.0)
    logs_fwd.join(timeout=5.0)

    # Unblock the sup_to_java thread — the supervisor won't send more data
    # now that Java has exited.
    for sock in (supervisor_comm, java_comm, java_logs):
        with contextlib.suppress(OSError):
            sock.shutdown(socket.SHUT_RDWR)

    sup_to_java.join(timeout=5.0)

    supervisor_comm.close()
    java_comm.close()
    java_logs.close()
