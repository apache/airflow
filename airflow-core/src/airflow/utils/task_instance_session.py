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

from __future__ import annotations

import atexit
import contextlib
import logging
import traceback
from typing import TYPE_CHECKING

from airflow import settings

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__current_task_instance_session: Session | None = None
__cleanup_registered: bool = False
__fallback_sessions: set[Session] = set()

log = logging.getLogger(__name__)


def _cleanup_fallback_sessions() -> None:
    """Clean up all fallback sessions on process exit."""
    global __fallback_sessions

    if not __fallback_sessions:
        return

    sessions_to_cleanup = list(__fallback_sessions)
    for session in sessions_to_cleanup:
        try:
            if session.is_active:
                session.close()
                log.info("Cleaned up fallback session on process exit")
        except Exception as e:
            log.warning("Error closing fallback session during exit: %s", e)

    __fallback_sessions.clear()


def get_current_task_instance_session() -> Session:
    global __current_task_instance_session, __cleanup_registered, __fallback_sessions

    if not __current_task_instance_session:
        log.warning("No task session set for this task. Continuing but this likely causes a resource leak.")
        log.warning("Please report this and stacktrace below to https://github.com/apache/airflow/issues")
        for filename, line_number, name, line in traceback.extract_stack():
            log.warning('File: "%s", %s , in %s', filename, line_number, name)
            if line:
                log.warning("  %s", line.strip())

        # Create fallback session
        __current_task_instance_session = settings.Session()
        __fallback_sessions.add(__current_task_instance_session)

        # Register cleanup only once per process
        if not __cleanup_registered:
            atexit.register(_cleanup_fallback_sessions)
            __cleanup_registered = True

    return __current_task_instance_session


@contextlib.contextmanager
def set_current_task_instance_session(session: Session):
    global __current_task_instance_session
    if __current_task_instance_session:
        raise RuntimeError(
            "Session already set for this task. "
            "You can only have one 'set_current_task_session' context manager active at a time."
        )
    __current_task_instance_session = session
    try:
        yield
    finally:
        __current_task_instance_session = None
