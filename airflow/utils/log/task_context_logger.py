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
from __future__ import annotations

import logging
from copy import copy
from functools import partial
from typing import TYPE_CHECKING

from airflow.config_templates.airflow_local_settings import TASK_CONTEXT_LOGGER_ENABLED

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


logger = logging.getLogger(__name__)

logging_levels = {
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
    "debug": logging.DEBUG,
}


class TaskContextLogger:
    """
    Class for sending messages to task instance logs from outside task execution context.

    This is intended to be used mainly in exceptional circumstances, to give visibility into
    events related to task execution when otherwise there would be none.

    :meta private:
    """

    def __init__(self, component_name: str):
        """
        Initialize the task context logger with the component name.

        :param component_name: the name of the component that will be used to identify the log messages
        """
        self.component_name = component_name
        self.task_handler = self._get_task_handler()
        self.should_log = self._should_log()

    def _should_log(self) -> bool:
        if not TASK_CONTEXT_LOGGER_ENABLED:
            return False
        if not getattr(self.task_handler, "supports_task_context_logging", False):
            logger.warning("Task handler does not support task context logging")
            return False
        return True

    @staticmethod
    def _get_task_handler():
        """Returns the task handler that supports task context logging."""
        handlers = [
            handler
            for handler in logging.getLogger("airflow.task").handlers
            if getattr(handler, "supports_task_context_logging", False)
        ]
        return handlers[0] if handlers else None

    def _log(
        self,
        level: str,
        msg: str,
        *args,
        ti: TaskInstance,
    ):
        """
        Emit a log message to the task instance logs.

        :param level: the log level
        :param message: the message to relay to task context log
        :param ti: the task instance
        """
        caller_logger = self.log
        if caller_logger.isEnabledFor(logging_levels[level.lower()]):
            caller_log_level_callable = getattr(caller_logger, level, None)
            if callable(caller_log_level_callable):
                # This logs the message using the calling method's configured logger
                caller_log_level_callable(msg % args)

        if not self.should_log:
            return

        task_handler = copy(self.task_handler)
        try:
            if hasattr(task_handler, "mark_end_on_close"):
                task_handler.mark_end_on_close = False
            task_handler.set_context(ti, identifier=self.component_name)
            filename, lineno, func, stackinfo = logger.findCaller()
            record = logging.LogRecord(
                self.component_name, logging_levels[level], filename, lineno, msg, args, None, func=func
            )
            task_handler.emit(record)
        finally:
            task_handler.close()

    def __getattr__(self, name: str):
        if name not in logging_levels:
            raise AttributeError(f"TaskContextLogger does not support attribute '{name}'")
        return partial(self._log, level=name)
