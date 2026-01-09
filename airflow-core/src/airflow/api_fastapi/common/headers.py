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

from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from airflow.api_fastapi.common.types import Mimetype


def header_accept_json_or_text_depends(
    accept: Annotated[
        str,
        Header(
            json_schema_extra={
                "type": "string",
                "enum": [Mimetype.JSON, Mimetype.TEXT, Mimetype.ANY],
            }
        ),
    ] = Mimetype.ANY,
) -> Mimetype:
    if accept.startswith(Mimetype.ANY):
        return Mimetype.ANY
    if accept.startswith(Mimetype.JSON):
        return Mimetype.JSON
    if accept.startswith(Mimetype.TEXT):
        return Mimetype.TEXT
    raise HTTPException(
        status_code=status.HTTP_406_NOT_ACCEPTABLE,
        detail="Only application/json or text/plain is supported",
    )


HeaderAcceptJsonOrText = Annotated[Mimetype, Depends(header_accept_json_or_text_depends)]


def header_accept_json_or_ndjson_depends(
    accept: Annotated[
        str,
        Header(
            json_schema_extra={
                "type": "string",
                "enum": [Mimetype.JSON, Mimetype.NDJSON, Mimetype.ANY],
            }
        ),
    ] = Mimetype.ANY,
) -> Mimetype:
    if accept.startswith(Mimetype.ANY):
        return Mimetype.ANY
    if accept.startswith(Mimetype.JSON):
        return Mimetype.JSON
    if accept.startswith(Mimetype.NDJSON) or accept.startswith(Mimetype.ANY):
        return Mimetype.NDJSON

    raise HTTPException(
        status_code=status.HTTP_406_NOT_ACCEPTABLE,
        detail="Only application/json or application/x-ndjson is supported",
    )


HeaderAcceptJsonOrNdjson = Annotated[Mimetype, Depends(header_accept_json_or_ndjson_depends)]


def header_content_type_json_or_form_depends(
    content_type: Annotated[
        str,
        Header(
            alias="Content-Type",
            description="Content-Type of the request body",
            json_schema_extra={"enum": [Mimetype.JSON, Mimetype.FORM]},
        ),
    ] = Mimetype.JSON,
) -> Mimetype:
    if content_type.startswith(Mimetype.JSON):
        return Mimetype.JSON
    if content_type.startswith(Mimetype.FORM):
        return Mimetype.FORM
    raise HTTPException(
        status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        detail="Only application/json or application/x-www-form-urlencoded is supported",
    )


HeaderContentTypeJsonOrForm = Annotated[Mimetype, Depends(header_content_type_json_or_form_depends)]
