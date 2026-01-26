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
"""FastAPI exceptions migrated from connexion."""
from __future__ import annotations

from http import HTTPStatus
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse

from airflow.providers.fab.www.api_fastapi.constants import EXCEPTIONS_LINK_MAP


def common_error_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Common error handler for FastAPI HTTPExceptions.
    
    Adds RFC 7807 Problem Details format with documentation links.
    """
    link = EXCEPTIONS_LINK_MAP.get(exc.status_code)
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "type": link or f"about:blank",
            "title": exc.detail or HTTPStatus(exc.status_code).phrase,
            "status": exc.status_code,
            "detail": exc.detail,
        },
        headers=exc.headers,
    )


class NotFound(HTTPException):
    """Raise when the object cannot be found."""

    def __init__(
        self,
        detail: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail or "Not Found",
            headers=headers,
        )


class BadRequest(HTTPException):
    """Raise when the server processes a bad request."""

    def __init__(
        self,
        detail: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail or "Bad Request",
            headers=headers,
        )


class Unauthenticated(HTTPException):
    """Raise when the user is not authenticated."""

    def __init__(
        self,
        detail: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail or "Unauthorized",
            headers=headers,
        )


class PermissionDenied(HTTPException):
    """Raise when the user does not have the required permissions."""

    def __init__(
        self,
        detail: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail or "Forbidden",
            headers=headers,
        )


class Conflict(HTTPException):
    """Raise when there is some conflict."""

    def __init__(
        self,
        detail: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail or "Conflict",
            headers=headers,
        )


class AlreadyExists(Conflict):
    """Raise when the object already exists."""


class Unknown(HTTPException):
    """Returns a response for HTTP 500 exception."""

    def __init__(
        self,
        detail: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail or "Internal Server Error",
            headers=headers,
        )
