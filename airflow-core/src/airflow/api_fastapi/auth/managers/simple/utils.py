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

from fastapi import HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from airflow.api_fastapi.auth.managers.simple.datamodels.login import LoginBody
from airflow.api_fastapi.common.headers import HeaderContentTypeJsonOrForm
from airflow.api_fastapi.common.types import Mimetype


async def parse_login_body(
    request: Request,
    content_type: HeaderContentTypeJsonOrForm,
) -> LoginBody:
    try:
        if content_type == Mimetype.JSON:
            body = await request.json()
        elif content_type == Mimetype.FORM:
            form = await request.form()
            body = {
                "username": form.get("username"),
                "password": form.get("password"),
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail="Unsupported Media Type",
            )
        return LoginBody(**body)
    except ValidationError as e:
        raise RequestValidationError(repr(e))
