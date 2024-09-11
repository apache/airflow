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
from contextlib import suppress
from copy import copy
from logging import Logger
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.log.file_task_handler import _ensure_ti
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.log.file_task_handler import FileTaskHandler

logger = logging.getLogger(__name__)


class TaskContextLogger:
    """
    Class for sending messages to task instance logs from outside task execution context.

    This is intended to be used mainly in exceptional circumstances, to give visibility into
    events related to task execution when otherwise there would be none.

    :meta private:
    """

    def __init__(self, component_name: str, call_site_logger: Logger | None = None):
        """
        Initialize the task context logger with the component name.

        :param component_name: the name of the component that will be used to identify the log messages
        :param call_site_logger: if provided, message will also be emitted through this logger
        """
        self.component_name = component_name
        self.task_handler = self._get_task_handler()
        self.enabled = self._should_enable()
        self.call_site_logger = call_site_logger

    def _should_enable(self) -> bool:
        if not conf.getboolean("logging", "enable_task_context_logger"):
            return False
        if not self.task_handler:
            logger.warning("Task handler does not support task context logging")
            return False
        logger.info("Task context logging is enabled")
        return True

    @staticmethod
    def _get_task_handler() -> FileTaskHandler | None:
        """Return the task handler that supports task context logging."""
        handlers = [
            handler
            for handler in logging.getLogger("airflow.task").handlers
            if getattr(handler, "supports_task_context_logging", False)
        ]
        if not handlers:
            return None
        h = handlers[0]
        if TYPE_CHECKING:
            assert isinstance(h, FileTaskHandler)
        return h

    def _log(self, level: int, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message to the task instance logs.

        :param level: the log level
        :param msg: the message to relay to task context log
        :param ti: the task instance or the task instance key
        """
        if self.call_site_logger and self.call_site_logger.isEnabledFor(level=level):
            with suppress(Exception):
                self.call_site_logger.log(level, msg, *args)

        if not self.enabled:
            return

        if not self.task_handler:
            return

        task_handler = copy(self.task_handler)
        try:
            if isinstance(ti, TaskInstanceKey):
                with create_session() as session:
                    ti = _ensure_ti(ti, session)
            task_handler.set_context(ti, identifier=self.component_name)
            if hasattr(task_handler, "mark_end_on_close"):
                task_handler.mark_end_on_close = False
            filename, lineno, func, stackinfo = logger.findCaller(stacklevel=3)
            record = logging.LogRecord(
                self.component_name, level, filename, lineno, msg, args, None, func=func
            )
            task_handler.emit(record)
        finally:
            task_handler.close()

    def critical(self, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message with level CRITICAL to the task instance logs.

        :param msg: the message to relay to task context log
        :param ti: the task instance
        """
        self._log(logging.CRITICAL, msg, *args, ti=ti)

    def fatal(self, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message with level FATAL to the task instance logs.

        :param msg: the message to relay to task context log
        :param ti: the task instance
        """
        self._log(logging.FATAL, msg, *args, ti=ti)

    def error(self, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message with level ERROR to the task instance logs.

        :param msg: the message to relay to task context log
        :param ti: the task instance
        """
        self._log(logging.ERROR, msg, *args, ti=ti)

    def warn(self, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message with level WARN to the task instance logs.

        :param msg: the message to relay to task context log
        :param ti: the task instance
        """
        self._log(logging.WARNING, msg, *args, ti=ti)

    def warning(self, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message with level WARNING to the task instance logs.

        :param msg: the message to relay to task context log
        :param ti: the task instance
        """
        self._log(logging.WARNING, msg, *args, ti=ti)

    def info(self, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message with level INFO to the task instance logs.

        :param msg: the message to relay to task context log
        :param ti: the task instance
        """
        self._log(logging.INFO, msg, *args, ti=ti)

    def debug(self, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message with level DEBUG to the task instance logs.

        :param msg: the message to relay to task context log
        :param ti: the task instance
        """
        self._log(logging.DEBUG, msg, *args, ti=ti)

    def notset(self, msg: str, *args, ti: TaskInstance | TaskInstanceKey):
        """
        Emit a log message with level NOTSET to the task instance logs.

        :param msg: the message to relay to task context log
        :param ti: the task instance
        """
        self._log(logging.NOTSET, msg, *args, ti=ti)
