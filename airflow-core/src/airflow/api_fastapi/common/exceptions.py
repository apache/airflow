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

from fastapi import FastAPI, HTTPException, Request, status
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError

from airflow.configuration import conf
from airflow.exceptions import DeserializationError
from airflow.utils.strings import get_random_string

T = TypeVar("T", bound=Exception)
DBError = TypeVar("DBError", bound=SQLAlchemyError)

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


class _DatabaseErrorHandler(BaseErrorHandler[DBError]):
    """
    Base for handlers that turn a SQLAlchemy error into an actionable HTTP response.

    The failing statement is logged under a random lookup id and echoed back to the
    caller only when ``[api] expose_stacktrace`` is set; otherwise the response just
    points at that id in the api server logs. Subclasses set ``status_code`` and
    ``reason`` and may override ``_should_handle`` to skip exceptions they do not own.
    """

    status_code: int
    reason: str

    def _should_handle(self, exc: DBError) -> bool:
        return True

    def _raise_database_error_response(self, exc: DBError) -> None:
        statement = getattr(exc, "statement", "hidden")
        orig_error = getattr(exc, "orig", "hidden")
        exception_id = get_random_string()
        stacktrace = "".join(traceback.format_tb(exc.__traceback__))
        log_message = f"Error with id {exception_id}, statement: {statement}\n{stacktrace}"
        log.error(log_message)

        if conf.get("api", "expose_stacktrace") == "True":
            message = log_message
            statement_out = str(statement)
            orig_error_out = str(orig_error)
        else:
            message = (
                "Serious error when handling your request. Check logs for more details - "
                f"you will find it in api server when you look for ID {exception_id}"
            )
            statement_out = "hidden"
            orig_error_out = "hidden"

        raise HTTPException(
            status_code=self.status_code,
            detail={
                "reason": self.reason,
                "statement": statement_out,
                "orig_error": orig_error_out,
                "message": message,
            },
        )

    def exception_handler(self, request: Request, exc: DBError):
        if not self._should_handle(exc):
            return
        self._raise_database_error_response(exc)


class _UniqueConstraintErrorHandler(_DatabaseErrorHandler[IntegrityError]):
    """Translate a unique-constraint ``IntegrityError`` into a 409, matched per database dialect."""

    status_code = status.HTTP_409_CONFLICT
    reason = "Unique constraint violation"

    unique_constraint_error_prefix_dict: dict[_DatabaseDialect, str] = {
        _DatabaseDialect.SQLITE: "UNIQUE constraint failed",
        _DatabaseDialect.MYSQL: "Duplicate entry",
        _DatabaseDialect.POSTGRES: "violates unique constraint",
    }

    def __init__(self):
        super().__init__(IntegrityError)
        self.dialect: _DatabaseDialect | None = None

    def _should_handle(self, exc: IntegrityError) -> bool:
        return self._is_dialect_matched(exc)

    def _is_dialect_matched(self, exc: IntegrityError) -> bool:
        """Check if the exception matches the unique constraint error message for any dialect."""
        exc_orig_str = str(exc.orig)
        for dialect, error_msg in self.unique_constraint_error_prefix_dict.items():
            if error_msg in exc_orig_str:
                self.dialect = dialect
                return True
        return False


class DataErrorHandler(_DatabaseErrorHandler[DataError]):
    """
    Translate a ``sqlalchemy.exc.DataError`` into a 422.

    The database rejected a value that passed Pydantic validation (too long, out of
    range, or the wrong type for its column), so it is a client error, not a 500.
    """

    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    reason = "Value rejected by database"

    def __init__(self):
        super().__init__(DataError)


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


class SQLAlchemyErrorHandler(_DatabaseErrorHandler[SQLAlchemyError]):
    """Generic handler for SQLAlchemyError -> 500 responses."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    reason = "Database error"

    def __init__(self):
        super().__init__(SQLAlchemyError)


ERROR_HANDLERS: list[BaseErrorHandler] = [
    _UniqueConstraintErrorHandler(),
    DataErrorHandler(),
    SQLAlchemyErrorHandler(),
    DagErrorHandler(),
]


def init_error_handlers(app: FastAPI) -> None:
    for handler in ERROR_HANDLERS:
        app.add_exception_handler(handler.exception_cls, handler.exception_handler)
