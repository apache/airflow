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

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware


# Custom Middleware Class
class FlaskExceptionsMiddleware(BaseHTTPMiddleware):
    """Middleware that converts exceptions thrown in the Flask application to Fastapi exceptions."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Check if the WSGI response contains an error
        if response.status_code >= 400 and response.media_type == "application/json":
            body = await response.json()
            if "error" in body:
                # Transform the WSGI app's exception into a FastAPI HTTPException
                raise HTTPException(
                    status_code=response.status_code,
                    detail=body["error"],
                )
        return response
