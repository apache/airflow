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

from collections.abc import Sequence

from airflow.api_fastapi.core_api.base import BaseModel


class HTTPExceptionResponse(BaseModel):
    """HTTPException Model used for error response."""

    detail: str | dict


def create_openapi_http_exception_doc(
    responses_status_code: Sequence[int | tuple[int, str]],
) -> dict:
    """
    Will create additional response example for errors raised by the endpoint.

    There is no easy way to introspect the code and automatically see what HTTPException are actually
    raised by the endpoint implementation. This piece of documentation needs to be kept
    in sync with the endpoint code manually.

    Each item can be either a status code or a ``(status_code, description)`` tuple.
    Validation error i.e 422 are natively added to the openapi documentation by FastAPI.
    """
    openapi_responses: list[tuple[int, dict[str, str | type[HTTPExceptionResponse]]]] = []
    for response_status_code in responses_status_code:
        response: dict[str, str | type[HTTPExceptionResponse]] = {"model": HTTPExceptionResponse}

        if isinstance(response_status_code, tuple):
            status_code, description = response_status_code
            response["description"] = description
        else:
            status_code = response_status_code

        openapi_responses.append((status_code, response))

    return {
        status_code: response for status_code, response in sorted(openapi_responses, key=lambda item: item[0])
    }
