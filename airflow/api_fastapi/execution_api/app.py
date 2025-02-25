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

from contextlib import asynccontextmanager
from functools import cached_property
from typing import TYPE_CHECKING

import attrs
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

if TYPE_CHECKING:
    import httpx


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Context manager for the lifespan of the FastAPI app. For now does nothing."""
    app.state.lifespan_called = True
    yield


def create_task_execution_api_app() -> FastAPI:
    """Create FastAPI app for task execution API."""
    from airflow.api_fastapi.execution_api.routes import execution_api_router

    # TODO: Add versioning to the API
    app = FastAPI(
        title="Airflow Task Execution API",
        description="The private Airflow Task Execution API.",
        lifespan=lifespan,
    )

    def custom_openapi() -> dict:
        """
        Customize the OpenAPI schema to include additional schemas not tied to specific endpoints.

        This is particularly useful for client SDKs that require models for types
        not directly exposed in any endpoint's request or response schema.

        References:
            - https://fastapi.tiangolo.com/how-to/extending-openapi/#modify-the-openapi-schema
        """
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi(
            title=app.title,
            description=app.description,
            version=app.version,
            routes=app.routes,
            servers=app.servers,
        )

        extra_schemas = get_extra_schemas()
        for schema_name, schema in extra_schemas.items():
            if schema_name not in openapi_schema["components"]["schemas"]:
                openapi_schema["components"]["schemas"][schema_name] = schema

        # The `JsonValue` component is missing any info. causes issues when generating models
        openapi_schema["components"]["schemas"]["JsonValue"] = {
            "title": "Any valid JSON value",
            "anyOf": [
                {"type": t} for t in ("string", "number", "integer", "object", "array", "boolean", "null")
            ],
        }

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore[method-assign]

    app.include_router(execution_api_router)
    return app


def get_extra_schemas() -> dict[str, dict]:
    """Get all the extra schemas that are not part of the main FastAPI app."""
    from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance
    from airflow.executors.workloads import BundleInfo
    from airflow.utils.state import TerminalTIState

    return {
        "TaskInstance": TaskInstance.model_json_schema(),
        "BundleInfo": BundleInfo.model_json_schema(),
        # Include the combined state enum too. In the datamodels we separate out SUCCESS from the other states
        # as that has different payload requirements
        "TerminalTIState": {"type": "string", "enum": list(TerminalTIState)},
    }


@attrs.define()
class InProcessExecuctionAPI:
    """
    A helper class to make it possible to run the ExecutionAPI "in-process".

    The sync version of this makes use of a2wsgi which runs the async loop in a separate thread. This is
    needed so that we can use the sync httpx client
    """

    _app: FastAPI | None = None

    @cached_property
    def app(self):
        if not self._app:
            from airflow.api_fastapi.execution_api.app import create_task_execution_api_app

            self._app = create_task_execution_api_app()

        return self._app

    @cached_property
    def transport(self) -> httpx.WSGITransport:
        import httpx
        from a2wsgi import ASGIMiddleware

        return httpx.WSGITransport(app=ASGIMiddleware(self.app))  # type: ignore[arg-type]

    @cached_property
    def atransport(self) -> httpx.ASGITransport:
        import httpx

        return httpx.ASGITransport(app=self.app)
