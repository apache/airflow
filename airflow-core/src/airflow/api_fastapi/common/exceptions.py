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

from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, TypeVar

from fastapi import HTTPException, Request, status
from sqlalchemy.exc import IntegrityError, SQLAlchemyError, OperationalError, ProgrammingError

T = TypeVar("T", bound=Exception)


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
        self.dialect: _DatabaseDialect.value | None = None

    def exception_handler(self, request: Request, exc: IntegrityError):
        """Handle IntegrityError exception."""
        if self._is_dialect_matched(exc):
            error_message = self._get_user_friendly_message(exc)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"{error_message}\nType: unique_constraint_violation"
            )

    def _is_dialect_matched(self, exc: IntegrityError) -> bool:
        """Check if the exception matches the unique constraint error message for any dialect."""
        exc_orig_str = str(exc.orig)
        for dialect, error_msg in self.unique_constraint_error_prefix_dict.items():
            if error_msg in exc_orig_str:
                self.dialect = dialect
                return True
        return False

    def _get_user_friendly_message(self, exc: IntegrityError) -> str:
       """Convert database error to user-friendly message."""
       exc_orig_str = str(exc.orig)
      
       # Handle DAG run unique constraint
       if "dag_run.dag_id" in exc_orig_str and "dag_run.run_id" in exc_orig_str:
           return "A DAG run with this ID already exists. Please use a different run ID."
          
       # Handle task instance unique constrain
       if "task_instance.dag_id" in exc_orig_str and "task_instance.task_id" in exc_orig_str:
           return "A task instance with this ID already exists. Please use a different task ID."
          
       # Handle DAG run logical date unique constraint
       if "dag_run.dag_id" in exc_orig_str and "dag_run.logical_date" in exc_orig_str:
           return "A DAG run with this logical date already exists. Please use a different logical date."
          
       # Generic unique constraint message
       return "This operation would create a duplicate entry. Please ensure all unique fields have unique values."

class _DatabaseErrorHandler(BaseErrorHandler[SQLAlchemyError]):
   """Handler for general database errors."""

   def __init__(self):
       super().__init__(SQLAlchemyError)

   def exception_handler(self, request: Request, exc: SQLAlchemyError):
       """Handle SQLAlchemyError exception."""
       error_message = self._get_user_friendly_message(exc)
       raise HTTPException(
           status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
           detail=f"{error_message}\nType: database_error"
       )

   def _get_user_friendly_message(self, exc: SQLAlchemyError) -> str:
       """Convert database error to user-friendly message."""
       if isinstance(exc, OperationalError):
           return "A database operation failed. Please try again later."
       elif isinstance(exc, ProgrammingError):
           return "An error occurred while processing your request. Please check your input and try again."
       else:
           return "An unexpected database error occurred. Please try again later."

DatabaseErrorHandlers = [
    _UniqueConstraintErrorHandler(),
]
