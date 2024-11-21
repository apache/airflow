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
from typing import Generic, TypeVar

from fastapi import HTTPException, Request, status
from sqlalchemy.exc import IntegrityError

T = TypeVar("T")


class BaseErrorHandler(Generic[T], ABC):
    """Base class for error handlers."""

    def __init__(self, exception_cls: T) -> None:
        self.exception_cls = exception_cls

    @abstractmethod
    def exception_handler(self, request: Request, exc: T):
        """exception_handler method."""
        raise NotImplementedError


class _UniqueConstraintErrorHandler(BaseErrorHandler[IntegrityError]):
    """Exception raised when trying to insert a duplicate value in a unique column."""

    def __init__(self):
        super().__init__(IntegrityError)
        self.unique_constraint_error_messages = [
            "UNIQUE constraint failed",  # SQLite
            "Duplicate entry",  # MySQL
            "violates unique constraint",  # PostgreSQL
        ]

    def exception_handler(self, request: Request, exc: IntegrityError):
        """Handle IntegrityError exception."""
        exc_orig_str = str(exc.orig)
        if any(error_msg in exc_orig_str for error_msg in self.unique_constraint_error_messages):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Unique constraint violation",
            )


DatabaseErrorHandlers = [
    _UniqueConstraintErrorHandler(),
]
