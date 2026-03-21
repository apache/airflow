#
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
"""In-process Core API helper for trusted Airflow processes."""

from __future__ import annotations

from contextlib import AsyncExitStack
from functools import cached_property
from typing import TYPE_CHECKING, Any

import attrs
from fastapi import FastAPI, Request

from airflow.api_fastapi.auth.managers.models.system_user import SystemUser

if TYPE_CHECKING:
    import httpx

    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser


class _SystemAccessAuthManagerProxy:
    """
    Auth manager proxy that grants full access for all authorization checks.

    Used by :class:`InProcessCoreAPI` to bypass authorization for trusted system
    processes while still delegating non-auth methods (e.g. ``init``, ``get_url_login``)
    to the real auth manager.
    """

    def __init__(self, real_manager: Any) -> None:
        object.__setattr__(self, "_real", real_manager)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    # -- All is_authorized_* methods return True unconditionally --

    def is_authorized_configuration(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_connection(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_dag(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_asset(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_asset_alias(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_pool(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_team(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_variable(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_view(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_custom_view(self, **kwargs: Any) -> bool:
        return True

    def is_authorized_hitl_task(self, **kwargs: Any) -> bool:
        return True

    # -- Batch methods --

    def batch_is_authorized_connection(self, *args: Any, **kwargs: Any) -> bool:
        return True

    def batch_is_authorized_dag(self, *args: Any, **kwargs: Any) -> bool:
        return True

    def batch_is_authorized_pool(self, *args: Any, **kwargs: Any) -> bool:
        return True

    def batch_is_authorized_variable(self, *args: Any, **kwargs: Any) -> bool:
        return True

    # -- Filter / get_authorized methods must return full sets to avoid empty-set filtering --

    def filter_authorized_menu_items(self, menu_items: list, **kwargs: Any) -> list:
        return menu_items

    def filter_authorized_pools(self, *, pool_names: set[str], **kwargs: Any) -> set[str]:
        return pool_names

    def filter_authorized_connections(self, *, conn_ids: set[str], **kwargs: Any) -> set[str]:
        return conn_ids

    def filter_authorized_variables(self, *, variable_keys: set[str], **kwargs: Any) -> set[str]:
        return variable_keys

    def filter_authorized_dags(self, *, dag_ids: set[str], **kwargs: Any) -> set[str]:
        return dag_ids

    def get_authorized_pools(self, **kwargs: Any) -> set[str]:
        from sqlalchemy import select

        from airflow.models.pool import Pool

        session = kwargs.get("session")
        if session is None:
            from airflow.utils.session import create_session

            with create_session() as session:
                return {row[0] for row in session.execute(select(Pool.pool)).all()}
        return {row[0] for row in session.execute(select(Pool.pool)).all()}

    def get_authorized_dag_ids(self, **kwargs: Any) -> set[str]:
        from sqlalchemy import select

        from airflow.models.dag import DagModel

        session = kwargs.get("session")
        if session is None:
            from airflow.utils.session import create_session

            with create_session() as session:
                return {row[0] for row in session.execute(select(DagModel.dag_id)).all()}
        return {row[0] for row in session.execute(select(DagModel.dag_id)).all()}

    def get_authorized_connections(self, **kwargs: Any) -> set[str]:
        from sqlalchemy import select

        from airflow.models import Connection

        session = kwargs.get("session")
        if session is None:
            from airflow.utils.session import create_session

            with create_session() as session:
                return {row[0] for row in session.execute(select(Connection.conn_id)).all()}
        return {row[0] for row in session.execute(select(Connection.conn_id)).all()}

    def get_authorized_variables(self, **kwargs: Any) -> set[str]:
        from sqlalchemy import select

        from airflow.models.variable import Variable

        session = kwargs.get("session")
        if session is None:
            from airflow.utils.session import create_session

            with create_session() as session:
                return {row[0] for row in session.execute(select(Variable.key)).all()}
        return {row[0] for row in session.execute(select(Variable.key)).all()}


@attrs.define()
class InProcessCoreAPI:
    """
    Run the Core API in-process with auth bypassed for system calls.

    Follows the same pattern as ``InProcessExecutionAPI`` in the execution API,
    but targets the Core API (``/api/v2/``) endpoints.

    The sync version uses a2wsgi to run the async FastAPI app in a separate thread,
    allowing use with the sync ``httpx.Client``.
    """

    _app: FastAPI | None = None
    _cm: AsyncExitStack | None = None
    _process_type: str = "unknown"

    @cached_property
    def app(self) -> FastAPI:
        if not self._app:
            # Lazy imports to avoid circular dependencies and heavy imports at module level
            from airflow.api_fastapi.app import _AuthManagerState, init_auth_manager
            from airflow.api_fastapi.common.dagbag import create_dag_bag
            from airflow.api_fastapi.core_api.app import init_config, init_error_handlers, init_views
            from airflow.api_fastapi.core_api.security import auth_manager_from_app, get_user

            self._app = FastAPI(title="Airflow Core API (in-process)")
            self._app.state.dag_bag = create_dag_bag()
            real_am = init_auth_manager(self._app)
            init_views(self._app)
            init_error_handlers(self._app)
            init_config(self._app)

            # Wrap the real auth manager with a proxy that grants full access.
            # This is needed because requires_access_* dependencies call
            # get_auth_manager() (the global singleton) internally.
            proxy = _SystemAccessAuthManagerProxy(real_am)
            _AuthManagerState.instance = proxy
            self._app.state.auth_manager = proxy

            process_type = self._process_type

            async def system_user_override(
                request: Request,
            ) -> BaseUser:
                return SystemUser(process_type=process_type)

            self._app.dependency_overrides[get_user] = system_user_override
            self._app.dependency_overrides[auth_manager_from_app] = lambda: proxy

        return self._app

    @cached_property
    def transport(self) -> httpx.WSGITransport:
        import asyncio

        import httpx
        from a2wsgi import ASGIMiddleware

        middleware = ASGIMiddleware(self.app)

        async def start_lifespan(cm: AsyncExitStack, app: FastAPI) -> None:
            await cm.enter_async_context(app.router.lifespan_context(app))

        self._cm = AsyncExitStack()

        asyncio.run_coroutine_threadsafe(start_lifespan(self._cm, self.app), middleware.loop)
        return httpx.WSGITransport(app=middleware)  # type: ignore[arg-type]

    @cached_property
    def atransport(self) -> httpx.ASGITransport:
        import httpx

        return httpx.ASGITransport(app=self.app)
