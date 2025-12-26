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

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.auth.managers.simple.services.login import SimpleAuthManagerLogin


class SimpleAllAdminMiddleware(BaseHTTPMiddleware):
    """Middleware that automatically generates and includes auth header for simple auth manager."""

    async def dispatch(self, request: Request, call_next):
        # Starlette Request is expected to be immutable, but we modify it to add the auth header
        # https://github.com/fastapi/fastapi/issues/2727#issuecomment-770202019
        token = SimpleAuthManagerLogin.create_token_all_admins()
        request.scope["headers"].append((b"authorization", f"Bearer {token}".encode()))
        return await call_next(request)
