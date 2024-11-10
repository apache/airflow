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
"""Compatibility layer for API to provide both FastAPI as wenn as Connexion based endpoints."""

from __future__ import annotations

from packaging.version import Version

from airflow import __version__ as airflow_version

AIRFLOW_VERSION = Version(airflow_version)
AIRFLOW_V_3_0_PLUS = Version(AIRFLOW_VERSION.base_version) >= Version("3.0.0")

if AIRFLOW_V_3_0_PLUS:
    # Just re-import the types from FastAPI and Airflow Core
    from fastapi import Depends as Depends, Header as Header, HTTPException as HTTPException, status as status

    from airflow.api_fastapi.common.router import AirflowRouter as AirflowRouter
    from airflow.api_fastapi.core_api.openapi.exceptions import (
        create_openapi_http_exception_doc as create_openapi_http_exception_doc,
    )
else:
    # Mock the external dependnecies
    from typing import Callable

    from connexion import ProblemException

    class Depends:  # type: ignore[no-redef]
        def __init__(self, *_, **__):
            pass

    class Header:  # type: ignore[no-redef]
        pass

    def create_openapi_http_exception_doc(responses_status_code: list[int]) -> dict:
        return {}

    class status:  # type: ignore[no-redef]
        HTTP_400_BAD_REQUEST = 400
        HTTP_403_FORBIDDEN = 403
        HTTP_500_INTERNAL_SERVER_ERROR = 500

    class HTTPException(ProblemException):  # type: ignore[no-redef]
        """Raise when the user does not have the required permissions."""

        def __init__(
            self,
            status: int,
            detail: str,
        ) -> None:
            from airflow.utils.docs import get_docs_url

            doc_link = get_docs_url("stable-rest-api-ref.html")
            EXCEPTIONS_LINK_MAP = {
                400: f"{doc_link}#section/Errors/BadRequest",
                403: f"{doc_link}#section/Errors/PermissionDenied",
                500: f"{doc_link}#section/Errors/Unknown",
            }
            TITLE_MAP = {
                400: "BadRequest",
                403: "PermissionDenied",
                500: "InternalServerError",
            }
            super().__init__(
                status=status,
                type=EXCEPTIONS_LINK_MAP[status],
                title=TITLE_MAP[status],
                detail=detail,
            )

    class AirflowRouter:  # type: ignore[no-redef]
        def __init__(self, *_, **__):
            pass

        def get(self, *_, **__):
            def decorator(func: Callable) -> Callable:
                return func

            return decorator

        def post(self, *_, **__):
            def decorator(func: Callable) -> Callable:
                return func

            return decorator
