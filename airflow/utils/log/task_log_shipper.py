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
from typing import TYPE_CHECKING

from airflow.config_templates.airflow_local_settings import TASK_LOG_SHIPPER_ENABLED

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


logger = logging.getLogger(__name__)


class TaskLogShipper:
    """
    TaskLogShipper enables shipping, typically in exceptional circumstances, messages to the task instance
    logs from e.g. the executor or scheduler.

    :meta private:
    """

    def __init__(self, component_name: str):
        """
        Initialize the task log shipper with the component name.

        :param component_name: the name of the component that will be used to identify the log messages
        """
        self.component_name = component_name
        self.task_handler = self._get_task_handler()
        self.task_handler_can_ship_logs = self._can_ship_logs()

    def _can_ship_logs(self) -> bool:
        if not TASK_LOG_SHIPPER_ENABLED:
            return False
        if self.task_handler is None or not self.task_handler.supports_task_log_ship:
            logger.warning("Task handler does not support task log shipping")
            return False
        return True

    @staticmethod
    def _get_task_handler():
        """Returns the task handler that supports task log shipping."""
        handlers = [
            handler
            for handler in logging.getLogger("airflow.task").handlers
            if getattr(handler, "supports_task_log_ship", False)
        ]
        return handlers[0] if handlers else None

    def ship_task_message(self, ti: TaskInstance, message: str, level: int):
        """
        Ship task log message for the task instance to the task handler.

        :param ti: the task instance
        :param message: the message to ship
        :param level: the log level
        """
        if not self.task_handler_can_ship_logs:
            return

        task_handler = copy(self.task_handler)
        try:
            if hasattr(task_handler, "mark_end_on_close"):
                task_handler.mark_end_on_close = False
            task_handler.set_context(ti, identifier=self.component_name)
            filename, lineno, func, stackinfo = logger.findCaller()
            record = logging.LogRecord(
                self.component_name, level, filename, lineno, message, None, None, func=func
            )
            task_handler.emit(record)
        finally:
            task_handler.close()
