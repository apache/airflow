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

import contextlib
import logging
import traceback
from typing import TYPE_CHECKING

from airflow.utils.session import create_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

__current_task_instance_session: Session | None = None

log = logging.getLogger(__name__)


def get_current_task_instance_session() -> Session:
    global __current_task_instance_session
    if not __current_task_instance_session:
        log.warning("No task session set for this task. Continuing but this likely causes a resource leak.")
        log.warning("Please report this and stacktrace below to https://github.com/apache/airflow/issues")
        for filename, line_number, name, line in traceback.extract_stack():
            log.warning('File: "%s", %s , in %s', filename, line_number, name)
            if line:
                log.warning("  %s", line.strip())
        __current_task_instance_session = create_session()
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
