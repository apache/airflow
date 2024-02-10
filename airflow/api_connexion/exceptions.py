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

from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import werkzeug
from connexion import FlaskApi, ProblemException, problem

from airflow.utils.docs import get_docs_url

if TYPE_CHECKING:
    import flask

doc_link = get_docs_url("stable-rest-api-ref.html")

EXCEPTIONS_LINK_MAP = {
    400: f"{doc_link}#section/Errors/BadRequest",
    404: f"{doc_link}#section/Errors/NotFound",
    405: f"{doc_link}#section/Errors/MethodNotAllowed",
    401: f"{doc_link}#section/Errors/Unauthenticated",
    409: f"{doc_link}#section/Errors/AlreadyExists",
    403: f"{doc_link}#section/Errors/PermissionDenied",
    500: f"{doc_link}#section/Errors/Unknown",
}


def common_error_handler(exception: BaseException) -> flask.Response:
    """Use to capture connexion exceptions and add link to the type field."""
    if isinstance(exception, ProblemException):
        link = EXCEPTIONS_LINK_MAP.get(exception.status)
        if link:
            response = problem(
                status=exception.status,
                title=exception.title,
                detail=exception.detail,
                type=link,
                instance=exception.instance,
                headers=exception.headers,
                ext=exception.ext,
            )
        else:
            response = problem(
                status=exception.status,
                title=exception.title,
                detail=exception.detail,
                type=exception.type,
                instance=exception.instance,
                headers=exception.headers,
                ext=exception.ext,
            )
    else:
        if not isinstance(exception, werkzeug.exceptions.HTTPException):
            exception = werkzeug.exceptions.InternalServerError()

        response = problem(title=exception.name, detail=exception.description, status=exception.code)

    return FlaskApi.get_response(response)


class NotFound(ProblemException):
    """Raise when the object cannot be found."""

    def __init__(
        self,
        title: str = "Not Found",
        detail: str | None = None,
        headers: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            status=HTTPStatus.NOT_FOUND,
            type=EXCEPTIONS_LINK_MAP[404],
            title=title,
            detail=detail,
            headers=headers,
            **kwargs,
        )


class BadRequest(ProblemException):
    """Raise when the server processes a bad request."""

    def __init__(
        self,
        title: str = "Bad Request",
        detail: str | None = None,
        headers: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            status=HTTPStatus.BAD_REQUEST,
            type=EXCEPTIONS_LINK_MAP[400],
            title=title,
            detail=detail,
            headers=headers,
            **kwargs,
        )


class Unauthenticated(ProblemException):
    """Raise when the user is not authenticated."""

    def __init__(
        self,
        title: str = "Unauthorized",
        detail: str | None = None,
        headers: dict | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            status=HTTPStatus.UNAUTHORIZED,
            type=EXCEPTIONS_LINK_MAP[401],
            title=title,
            detail=detail,
            headers=headers,
            **kwargs,
        )


class PermissionDenied(ProblemException):
    """Raise when the user does not have the required permissions."""

    def __init__(
        self,
        title: str = "Forbidden",
        detail: str | None = None,
        headers: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            status=HTTPStatus.FORBIDDEN,
            type=EXCEPTIONS_LINK_MAP[403],
            title=title,
            detail=detail,
            headers=headers,
            **kwargs,
        )


class AlreadyExists(ProblemException):
    """Raise when the object already exists."""

    def __init__(
        self,
        title="Conflict",
        detail: str | None = None,
        headers: dict | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            status=HTTPStatus.CONFLICT,
            type=EXCEPTIONS_LINK_MAP[409],
            title=title,
            detail=detail,
            headers=headers,
            **kwargs,
        )


class Unknown(ProblemException):
    """Returns a response body and status code for HTTP 500 exception."""

    def __init__(
        self,
        title: str = "Internal Server Error",
        detail: str | None = None,
        headers: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
            type=EXCEPTIONS_LINK_MAP[500],
            title=title,
            detail=detail,
            headers=headers,
            **kwargs,
        )
