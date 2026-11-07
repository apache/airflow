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
"""A structlog logger that writes to the task log on Airflow 2 and Airflow 3."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import structlog

from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from structlog.typing import EventDict, FilteringBoundLogger

_STDLIB_LOG_KWARGS = ("exc_info", "stack_info", "stacklevel")
# Frames between a ``log.warning(...)`` call and the stdlib logger inside structlog's
# BoundLogger, so the record names the caller's file and line rather than structlog's.
_CALLER_STACKLEVEL = 4


def _fold_fields_into_message(_logger: Any, _method_name: str, event_dict: EventDict) -> EventDict:
    """Append ``key=value`` fields to the message, since Airflow 2's task log format drops ``extra``."""
    fields = [key for key in event_dict if key != "event" and key not in _STDLIB_LOG_KWARGS]
    if fields:
        rendered = " ".join(f"{key}={event_dict.pop(key)!r}" for key in fields)
        event_dict["event"] = f"{event_dict['event']} {rendered}"
    event_dict.setdefault("stacklevel", _CALLER_STACKLEVEL)
    return event_dict


def get_task_logger() -> FilteringBoundLogger:
    """
    Return a structlog logger that writes to the task log.

    Airflow 3 configures structlog for task processes. Airflow 2 does not, so structlog there
    falls back to its defaults and prints every level, ``debug`` included, to stdout. On Airflow 2 the
    logger wraps the ``airflow.task`` stdlib logger instead, which applies the task log's level
    and handlers, without changing the process-wide structlog configuration.
    """
    if AIRFLOW_V_3_0_PLUS:
        return structlog.get_logger(logger_name="task")
    return structlog.wrap_logger(
        logging.getLogger("airflow.task"),
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            _fold_fields_into_message,
            structlog.stdlib.render_to_log_kwargs,
        ],
    )
