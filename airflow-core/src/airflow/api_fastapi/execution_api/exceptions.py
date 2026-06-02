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

import structlog
from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from airflow.api_fastapi.common.exceptions import BaseErrorHandler

logger = structlog.get_logger(logger_name=__name__)


class _DatabaseErrorHandler(BaseErrorHandler[SQLAlchemyError]):
    """Handle SQLAlchemyError exceptions raised within the execution API."""

    def __init__(self):
        super().__init__(SQLAlchemyError)

    def exception_handler(self, request: Request, exc: SQLAlchemyError):
        logger.exception(
            "Database error handling request",
            path=request.url.path,
            method=request.method,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        content: dict[str, str] = {"detail": "Database error occurred"}
        if correlation_id := request.headers.get("correlation-id"):
            content["correlation-id"] = correlation_id
        return JSONResponse(status_code=500, content=content)


EXECUTION_API_ERROR_HANDLERS: list[BaseErrorHandler] = [_DatabaseErrorHandler()]
