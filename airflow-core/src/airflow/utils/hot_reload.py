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
from collections.abc import Callable
from pathlib import Path

import structlog

log = structlog.getLogger(__name__)


def run_with_reloader(
    callback: Callable,
):
    """
    Run a callback function with automatic reloading on file changes.

    This function monitors specified paths for changes and restarts the process
    when changes are detected. Useful for development mode hot-reloading.

    :param callback: The function to run. This should be the main entry point
        of the command that needs hot-reload support.
    """
    # Default watch paths - watch the airflow source directory
    import airflow

    airflow_root = Path(airflow.__file__).parent
    watch_paths = [airflow_root]

    log.info("Starting in development mode with hot-reload enabled")
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


def _run_reloader(watch_paths: list[str]):
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
            log.info("Stopping process...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log.warning("Process did not terminate gracefully, killing...")
                process.kill()
                process.wait()

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
            process.terminate()
            process.wait()
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
