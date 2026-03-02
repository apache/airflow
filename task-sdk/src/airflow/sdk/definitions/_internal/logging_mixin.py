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

from typing import TYPE_CHECKING, Any, TypeVar

import structlog

if TYPE_CHECKING:
    from airflow.sdk.types import Logger

_T = TypeVar("_T")


class LoggingMixin:
    """Convenience super-class to have a logger configured with the class name."""

    _log: Logger | None = None

    # Parent logger used by this class. It should match one of the loggers defined in the
    # `logging_config_class`. By default, this attribute is used to create the final name of the logger, and
    # will prefix the `_logger_name` with a separating dot.
    _log_config_logger_name: str | None = None

    _logger_name: str | None = None

    def __init__(self, context=None):
        self._set_context(context)
        super().__init__()

    @staticmethod
    def _create_logger_name(
        logged_class: type[_T],
        log_config_logger_name: str | None = None,
        class_logger_name: str | None = None,
    ) -> str:
        """
        Generate a logger name for the given `logged_class`.

        By default, this function returns the `class_logger_name` as logger name. If it is not provided,
        the {class.__module__}.{class.__name__} is returned instead. When a `parent_logger_name` is provided,
        it will prefix the logger name with a separating dot.
        """
        logger_name: str = (
            class_logger_name
            if class_logger_name is not None
            else f"{logged_class.__module__}.{logged_class.__name__}"
        )

        if log_config_logger_name:
            return f"{log_config_logger_name}.{logger_name}" if logger_name else log_config_logger_name
        return logger_name

    @classmethod
    def _get_log(cls, obj: Any, clazz: type[_T]) -> Logger:
        if obj._log is None:
            logger_name: str = cls._create_logger_name(
                logged_class=clazz,
                log_config_logger_name=obj._log_config_logger_name,
                class_logger_name=obj._logger_name,
            )
            obj._log = structlog.get_logger(logger_name)
        return obj._log

    @classmethod
    def logger(cls) -> Logger:
        """Return a logger."""
        return LoggingMixin._get_log(cls, cls)

    @property
    def log(self) -> Logger:
        """Return a logger."""
        return LoggingMixin._get_log(self, self.__class__)

    def _set_context(self, context): ...
