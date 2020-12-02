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
from typing import Dict, Optional

import werkzeug
from connexion import FlaskApi, ProblemException, problem

from airflow import version

if any(suffix in version.version for suffix in ['dev', 'a', 'b']):
    doc_link = (
        "http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/latest"
        "/stable-rest-api-ref.html"
    )
else:
    doc_link = f'https://airflow.apache.org/docs/{version.version}/stable-rest-api-ref.html'

EXCEPTIONS_LINK_MAP = {
    400: f"{doc_link}#section/Errors/BadRequest",
    404: f"{doc_link}#section/Errors/NotFound",
    405: f"{doc_link}#section/Errors/MethodNotAllowed",
    401: f"{doc_link}#section/Errors/Unauthenticated",
    409: f"{doc_link}#section/Errors/AlreadyExists",
    403: f"{doc_link}#section/Errors/PermissionDenied",
    500: f"{doc_link}#section/Errors/Unknown",
}


def common_error_handler(exception):
    """
    Used to capture connexion exceptions and add link to the type field

    :type exception: Exception
    """
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
    """Raise when the object cannot be found"""

    def __init__(
        self, title: str = 'Not Found', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs
    ):
        super().__init__(
            status=404, type=EXCEPTIONS_LINK_MAP[404], title=title, detail=detail, headers=headers, **kwargs
        )


class BadRequest(ProblemException):
    """Raise when the server processes a bad request"""

    def __init__(
        self,
        title: str = 'Bad Request',
        detail: Optional[str] = None,
        headers: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(
            status=400, type=EXCEPTIONS_LINK_MAP[400], title=title, detail=detail, headers=headers, **kwargs
        )


class Unauthenticated(ProblemException):
    """Raise when the user is not authenticated"""

    def __init__(
        self,
        title: str = 'Unauthorized',
        detail: Optional[str] = None,
        headers: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(
            status=401, type=EXCEPTIONS_LINK_MAP[401], title=title, detail=detail, headers=headers, **kwargs
        )


class PermissionDenied(ProblemException):
    """Raise when the user does not have the required permissions"""

    def __init__(
        self, title: str = 'Forbidden', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs
    ):
        super().__init__(
            status=403, type=EXCEPTIONS_LINK_MAP[403], title=title, detail=detail, headers=headers, **kwargs
        )


class AlreadyExists(ProblemException):
    """Raise when the object already exists"""

    def __init__(
        self, title='Conflict', detail: Optional[str] = None, headers: Optional[Dict] = None, **kwargs
    ):
        super().__init__(
            status=409, type=EXCEPTIONS_LINK_MAP[409], title=title, detail=detail, headers=headers, **kwargs
        )


class Unknown(ProblemException):
    """Returns a response body and status code for HTTP 500 exception"""

    def __init__(
        self,
        title: str = 'Internal Server Error',
        detail: Optional[str] = None,
        headers: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(
            status=500, type=EXCEPTIONS_LINK_MAP[500], title=title, detail=detail, headers=headers, **kwargs
        )
