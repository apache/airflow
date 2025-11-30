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
"""Compatibility layer for API to provide both FastAPI as well as Connexion based endpoints."""

from __future__ import annotations

from airflow.providers.edge3.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    # Just re-import the types from FastAPI and Airflow Core
    from fastapi import Body, Depends, Header, HTTPException, Path, Request, status

    from airflow.api_fastapi.common.db.common import SessionDep
    from airflow.api_fastapi.common.router import AirflowRouter
    from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc

    # In Airflow 3 with AIP-72 we get workload addressed by ExecuteTask
    from airflow.executors.workloads import ExecuteTask

    def parse_command(command: str) -> ExecuteTask:
        return ExecuteTask.model_validate_json(command)
else:
    # Mock the external dependencies
    from collections.abc import Callable

    from connexion import ProblemException

    class Body:  # type: ignore[no-redef]
        def __init__(self, *_, **__):
            pass

    class Depends:  # type: ignore[no-redef]
        def __init__(self, *_, **__):
            pass

    class Header:  # type: ignore[no-redef]
        def __init__(self, *_, **__):
            pass

    class Path:  # type: ignore[no-redef]
        def __init__(self, *_, **__):
            pass

    class Request:  # type: ignore[no-redef]
        pass

    class SessionDep:  # type: ignore[no-redef]
        pass

    def create_openapi_http_exception_doc(responses_status_code: list[int]) -> dict:
        return {}

    class status:  # type: ignore[no-redef]
        HTTP_204_NO_CONTENT = 204
        HTTP_400_BAD_REQUEST = 400
        HTTP_403_FORBIDDEN = 403
        HTTP_404_NOT_FOUND = 404
        HTTP_409_CONFLICT = 409
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
                409: f"{doc_link}#section/Errors/Conflict",
                500: f"{doc_link}#section/Errors/Unknown",
            }
            TITLE_MAP = {
                400: "BadRequest",
                403: "PermissionDenied",
                409: "Conflict",
                500: "InternalServerError",
            }
            super().__init__(
                status=status,
                type=EXCEPTIONS_LINK_MAP[status],
                title=TITLE_MAP[status],
                detail=detail,
            )

        @property
        def status_code(self) -> int:
            """Alias for status to match FastAPI's HTTPException interface."""
            return self.status

        def to_response(self):
            from flask import Response

            return Response(response=self.detail, status=self.status)

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

        def patch(self, *_, **__):
            def decorator(func: Callable) -> Callable:
                return func

            return decorator

    # In Airflow 3 with AIP-72 we get workload addressed by ExecuteTask
    # But in Airflow 2.10 it is a command line array
    ExecuteTask = list[str]  # type: ignore[assignment,misc]

    def parse_command(command: str) -> ExecuteTask:
        from ast import literal_eval

        return literal_eval(command)
