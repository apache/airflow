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

from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.routing import APIRoute
from fastapi.types import DecoratedCallable
from starlette.responses import Response

from airflow.utils.session import create_session


class AirflowRouter(APIRouter):
    """Extends the FastAPI default router."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Ensure our custom route commits/rolls back DB session before sending the response
        super().__init__(*args, **kwargs)
        self.route_class = _AirflowRoute

    def api_route(
        self,
        path: str,
        operation_id: str | None = None,
        **kwargs: Any,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            self.add_api_route(
                path,
                func,
                operation_id=operation_id or func.__name__,
                **kwargs,
            )
            return func

        return decorator


def _route_uses_session(route: APIRoute) -> bool:
    """
    Detect if the route depends on the synchronous DB session dependency.

    We look for the internal dependency function `_get_session` from
    `airflow.api_fastapi.common.db.common` in the dependency tree.
    """
    stack = list(route.dependant.dependencies)
    while stack:
        dep = stack.pop()
        call = getattr(dep, "call", None)
        if call is not None:
            mod = getattr(call, "__module__", "")
            name = getattr(call, "__name__", "")
            if mod == "airflow.api_fastapi.common.db.common" and name == "_get_session":
                return True
        stack.extend(getattr(dep, "dependencies", []) or [])
    return False


class _AirflowRoute(APIRoute):
    """
    Custom route that finalizes DB transactions before sending the response.

    This restores pre-FastAPI 0.118 behavior for DB session lifecycle so that
    commits/rollbacks happen before the response body has started.
    Only applies to routes that declare the sync session dependency.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[None, None, Response]]:
        default_handler = super().get_route_handler()
        needs_db = _route_uses_session(self)

        async def handler(request: Request) -> Response:
            if not needs_db:
                return await default_handler(request)

            with create_session(scoped=False) as session:
                setattr(request.state, "__airflow_db_session", session)
                response = await default_handler(request)

            delattr(request.state, "__airflow_db_session")
            return response

        return handler
