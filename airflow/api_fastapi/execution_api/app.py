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

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Context manager for the lifespan of the FastAPI app. For now does nothing."""
    app.state.lifespan_called = True
    yield


def create_task_execution_api_app(app: FastAPI) -> FastAPI:
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

        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore[method-assign]

    app.include_router(execution_api_router)
    return app


def get_extra_schemas() -> dict[str, dict]:
    """Get all the extra schemas that are not part of the main FastAPI app."""
    from airflow.api_fastapi.execution_api.datamodels import taskinstance

    return {
        "TaskInstance": taskinstance.TaskInstance.model_json_schema(),
    }
