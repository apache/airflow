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
import traceback
from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, TypeVar

from fastapi import HTTPException, Request, status
from sqlalchemy.exc import IntegrityError

from airflow.configuration import conf
from airflow.exceptions import DeserializationError
from airflow.utils.strings import get_random_string

T = TypeVar("T", bound=Exception)

log = logging.getLogger(__name__)


class BaseErrorHandler(Generic[T], ABC):
    """Base class for error handlers."""

    def __init__(self, exception_cls: T) -> None:
        self.exception_cls = exception_cls

    @abstractmethod
    def exception_handler(self, request: Request, exc: T):
        """exception_handler method."""
        raise NotImplementedError


class _DatabaseDialect(Enum):
    SQLITE = "sqlite"
    MYSQL = "mysql"
    POSTGRES = "postgres"


class _UniqueConstraintErrorHandler(BaseErrorHandler[IntegrityError]):
    """Exception raised when trying to insert a duplicate value in a unique column."""

    unique_constraint_error_prefix_dict: dict[_DatabaseDialect, str] = {
        _DatabaseDialect.SQLITE: "UNIQUE constraint failed",
        _DatabaseDialect.MYSQL: "Duplicate entry",
        _DatabaseDialect.POSTGRES: "violates unique constraint",
    }

    def __init__(self):
        super().__init__(IntegrityError)
        self.dialect: _DatabaseDialect | None = None

    def exception_handler(self, request: Request, exc: IntegrityError):
        """Handle IntegrityError exception."""
        if self._is_dialect_matched(exc):
            exception_id = get_random_string()
            stacktrace = ""
            for tb in traceback.format_tb(exc.__traceback__):
                stacktrace += tb

            log_message = f"Error with id {exception_id}\n{stacktrace}"
            log.error(log_message)
            if conf.get("api", "expose_stacktrace") == "True":
                message = log_message
            else:
                message = (
                    "Serious error when handling your request. Check logs for more details - "
                    f"you will find it in api server when you look for ID {exception_id}"
                )

            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "Unique constraint violation",
                    "statement": str(exc.statement),
                    "orig_error": str(exc.orig),
                    "message": message,
                },
            )

    def _is_dialect_matched(self, exc: IntegrityError) -> bool:
        """Check if the exception matches the unique constraint error message for any dialect."""
        exc_orig_str = str(exc.orig)
        for dialect, error_msg in self.unique_constraint_error_prefix_dict.items():
            if error_msg in exc_orig_str:
                self.dialect = dialect
                return True
        return False


class DagErrorHandler(BaseErrorHandler[DeserializationError]):
    """Handler for Dag related errors."""

    def __init__(self):
        super().__init__(DeserializationError)

    def exception_handler(self, request: Request, exc: DeserializationError):
        """Handle Dag deserialization exceptions."""
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while trying to deserialize Dag: {exc}",
        )


ERROR_HANDLERS: list[BaseErrorHandler] = [_UniqueConstraintErrorHandler(), DagErrorHandler()]
