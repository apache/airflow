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

from fastapi import status

# HTTP_422_UNPROCESSABLE_CONTENT was added in Starlette 0.48.0, replacing HTTP_422_UNPROCESSABLE_ENTITY
# to align with RFC 9110 (HTTP Semantics).
#
# FastAPI 0.128.0 (our minimum version) requires starlette>=0.40.0. With "Low dep tests"
# (uv sync --resolution lowest-direct), starlette 0.40.0 is installed, which only has
# HTTP_422_UNPROCESSABLE_ENTITY. So we need this fallback for backward compatibility.
#
# Refs:
#   - https://www.starlette.io/release-notes/
#   - https://www.rfc-editor.org/rfc/rfc9110#status.422
try:
    HTTP_422_UNPROCESSABLE_CONTENT = status.HTTP_422_UNPROCESSABLE_CONTENT
except AttributeError:
    HTTP_422_UNPROCESSABLE_CONTENT = status.HTTP_422_UNPROCESSABLE_ENTITY  # type: ignore[attr-defined]

__all__ = ["HTTP_422_UNPROCESSABLE_CONTENT"]
