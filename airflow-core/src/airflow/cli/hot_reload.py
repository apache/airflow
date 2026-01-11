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
"""Hot reload utilities for development mode."""

from __future__ import annotations

import os
import signal
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    import subprocess

log = structlog.getLogger(__name__)


def run_with_reloader(
    callback: Callable,
    process_name: str = "process",
) -> None:
    """
    Run a callback function with automatic reloading on file changes.

    This function monitors specified paths for changes and restarts the process
    when changes are detected. Useful for development mode hot-reloading.

    :param callback: The function to run. This should be the main entry point
        of the command that needs hot-reload support.
    :param process_name: Name of the process being run (for logging purposes)
    """
    # Default watch paths - watch the airflow source directory
    import airflow

    airflow_root = Path(airflow.__file__).parent
    watch_paths = [airflow_root]

    log.info("Starting %s in development mode with hot-reload enabled", process_name)
    log.info("Watching paths: %s", watch_paths)

    # Check if we're the main process or a reloaded child
    reloader_pid = os.environ.get("AIRFLOW_DEV_RELOADER_PID")
    if reloader_pid is None:
        # We're the main process - set up the reloader
        os.environ["AIRFLOW_DEV_RELOADER_PID"] = str(os.getpid())
        _run_reloader(watch_paths)
    else:
        # We're a child process - just run the callback
        callback()


def _terminate_process_tree(
    process: subprocess.Popen[bytes],
    timeout: int = 5,
    force_kill_remaining: bool = True,
) -> None:
    """
    Terminate a process and all its children recursively.

    Uses psutil to ensure all child processes are properly terminated,
    which is important for cleaning up subprocesses like serve-log servers.

    :param process: The subprocess.Popen process to terminate
    :param timeout: Timeout in seconds to wait for graceful termination
    :param force_kill_remaining: If True, force kill processes that don't terminate gracefully
    """
    import subprocess

    import psutil

    try:
        parent = psutil.Process(process.pid)
        # Get all child processes recursively
        children = parent.children(recursive=True)

        # Terminate all children first
        for child in children:
            try:
                child.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        # Terminate the parent
        parent.terminate()

        # Wait for all processes to terminate
        gone, alive = psutil.wait_procs(children + [parent], timeout=timeout)

        # Force kill any remaining processes if requested
        if force_kill_remaining:
            for proc in alive:
                try:
                    log.warning("Force killing process %s", proc.pid)
                    proc.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

    except (psutil.NoSuchProcess, psutil.AccessDenied):
        # Process already terminated
        pass
    except Exception as e:
        log.warning("Error terminating process tree: %s", e)
        # Fallback to simple termination
        try:
            process.terminate()
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            if force_kill_remaining:
                log.warning("Process did not terminate gracefully, killing...")
                process.kill()
                process.wait()


def _run_reloader(watch_paths: Sequence[str | Path]) -> None:
    """
    Watch for changes and restart the process.

    Watches the provided paths and restarts the process by re-executing the
    Python interpreter with the same arguments.

    :param watch_paths: List of paths to watch for changes.
    """
    import subprocess

    from watchfiles import watch

    process = None
    should_exit = False

    def start_process():
        """Start or restart the subprocess."""
        nonlocal process
        if process is not None:
            log.info("Stopping process and all its children...")
            _terminate_process_tree(process, timeout=5, force_kill_remaining=True)

        log.info("Starting process...")
        # Restart the process by re-executing Python with the same arguments
        # Note: sys.argv is safe here as it comes from the original CLI invocation
        # and is only used in development mode for hot-reloading the same process
        process = subprocess.Popen([sys.executable] + sys.argv)
        return process

    def signal_handler(signum, frame):
        """Handle termination signals."""
        nonlocal should_exit, process
        should_exit = True
        log.info("Received signal %s, shutting down...", signum)
        if process:
            _terminate_process_tree(process, timeout=5, force_kill_remaining=False)
        sys.exit(0)

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start the initial process
    process = start_process()

    log.info("Hot-reload enabled. Watching for file changes...")
    log.info("Press Ctrl+C to stop")

    try:
        for changes in watch(*watch_paths):
            if should_exit:
                break

            log.info("Detected changes: %s", changes)
            log.info("Reloading...")

            # Restart the process
            process = start_process()

    except KeyboardInterrupt:
        log.info("Shutting down...")
        if process:
            process.terminate()
            process.wait()
