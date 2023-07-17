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
from typing import TYPE_CHECKING

from airflow.compat.functools import cached_property
from airflow.config_templates.airflow_local_settings import ARBITRARY_LOG_SHIPPING_ENABLED

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


class ArbitraryLogShipper:
    """
    Creates a new Arbitrary Log Shipper for the given component name.

    :meta private:
    """

    def __init__(self, component_name: str):
        """
        Initialize the arbitrary log shipper with the component name.

        :param component_name: the name of the component that will be used to identify the log messages
        """
        self.component_name = component_name

    @cached_property
    def task_handler(self):
        """Returns the task handler that supports arbitrary log shipping."""
        handlers = [h for h in logging.getLogger("airflow.task").handlers]
        if not handlers:
            return None
        handlers = [h for h in handlers if getattr(h, "_supports_arbitrary_ship", False)]
        if not handlers:
            return None
        return handlers[0]

    def ship_task_message(self, ti: TaskInstance, message: str, level: int):
        """
        Ship arbitrary log message for the task instance to the task handler.

        :param ti: the task instance
        :param message: the message to ship
        :param level: the log level
        """
        if not ARBITRARY_LOG_SHIPPING_ENABLED:
            logging.info("Arbitrary messages will not be shipped as arbitrary log shipping is disabled")
            return
        if not self.task_handler:
            logging.warning("Task handler does not support arbitrary log shipping")
            return
        identifier = self.component_name
        if hasattr(self.task_handler, "ship_arbitrary_message"):
            self.task_handler.ship_arbitrary_message(
                ti=ti, identifier=identifier, message=message, level=level
            )


def get_arbitrary_log_shipper(component_name: str) -> ArbitraryLogShipper:
    """
    Return the arbitrary log shipper for the given component name.

    :param component_name: the name of the component that will be used to identify the log messages
    """
    return ArbitraryLogShipper(component_name=component_name)
