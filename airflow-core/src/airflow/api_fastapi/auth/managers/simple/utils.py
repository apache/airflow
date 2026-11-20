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

from json import JSONDecodeError

from fastapi import HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from starlette.requests import ClientDisconnect

from airflow.api_fastapi.auth.managers.simple.datamodels.login import LoginBody
from airflow.api_fastapi.common.headers import HeaderContentTypeJsonOrForm
from airflow.api_fastapi.common.types import Mimetype


async def parse_login_body(
    request: Request,
    content_type: HeaderContentTypeJsonOrForm,
) -> LoginBody:
    # ``/token`` takes its body through this dependency rather than declaring one, so FastAPI
    # parses no body for it and the reads below are the only parse. The route is unauthenticated,
    # so anything left unhandled here is a 500 that any caller can reach without credentials.
    if content_type == Mimetype.JSON:
        try:
            body = await request.json()
        except JSONDecodeError as e:
            # This arm and the next are the chain FastAPI runs around its own ``request.json()``,
            # so an unreadable body gets the same status here as on ``/token/cli``, where the
            # body is declared and FastAPI validates it natively.
            raise RequestValidationError(
                [{"type": "json_invalid", "loc": ["body", e.pos], "msg": "JSON decode error"}]
            ) from e
        except Exception as e:
            # ``json.loads`` also raises UnicodeDecodeError, a bare ValueError (a number over
            # ``int_max_str_digits``) and RecursionError. A server-side failure such as
            # MemoryError is reported as a client error too, which is the trade FastAPI makes
            # at the same point.
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="There was an error parsing the body",
            ) from e
        if not isinstance(body, dict):
            # Valid JSON, but a list, scalar or null cannot be splatted into the model below.
            raise RequestValidationError(
                [
                    {
                        "type": "model_attributes_type",
                        "loc": ["body"],
                        "msg": "Input should be a valid dictionary or object to extract fields from",
                    }
                ]
            )
    elif content_type == Mimetype.FORM:
        try:
            form = await request.form()
        except ClientDisconnect as e:
            # Only the transport failure is translated. Starlette reports its own form-parser
            # limits as a 400 carrying a specific detail ("Too many fields..."), so those are
            # left to propagate rather than be flattened into the generic message above.
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="There was an error parsing the body",
            ) from e
        body = {
            "username": form.get("username"),
            "password": form.get("password"),
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Unsupported Media Type",
        )

    try:
        return LoginBody(**body)
    except ValidationError as e:
        raise RequestValidationError(repr(e))
