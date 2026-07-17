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
Gunicorn configuration hooks for the Airflow API server.

This module provides Gunicorn server hooks that are loaded via the -c config option.
These hooks handle:
- Setting process titles to indicate worker readiness (useful for debugging)
- Cleaning up ORM connections on worker exit

Usage:
    gunicorn -c python:airflow.api_fastapi.gunicorn_config airflow.api_fastapi.main:app
"""

from __future__ import annotations

import logging
import sys

from airflow import settings

log = logging.getLogger(__name__)

# On macOS, setproctitle has issues, so we skip it
if sys.platform == "darwin":

    def setproctitle(title):
        log.debug("macOS detected, skipping setproctitle")
else:
    from setproctitle import setproctitle


def post_worker_init(worker):
    """
    Execute after a worker has been initialized.

    This hook runs in each worker process after the ASGI app has been loaded.
    We set the process title with a ready prefix for debugging visibility.
    """
    ready_prefix = settings.GUNICORN_WORKER_READY_PREFIX
    proc_title = f"{ready_prefix}airflow api-server worker"
    setproctitle(proc_title)
    log.info("Worker initialized and ready, set process title: %s", proc_title)


def worker_exit(server, worker):
    """
    Execute when a worker is about to exit.

    This hook runs in the worker process itself (not the arbiter) just before exit.
    We clean up ORM connections to ensure proper resource cleanup.
    """
    log.debug("Worker %s exited, disposing ORM connections", worker.pid)
    settings.dispose_orm(do_log=False)
