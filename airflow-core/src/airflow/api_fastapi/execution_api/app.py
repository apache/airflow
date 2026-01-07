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

import json
import time
from contextlib import AsyncExitStack
from functools import cached_property
from typing import TYPE_CHECKING, Any

import attrs
import svcs
from cadwyn import (
    Cadwyn,
)
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.auth.tokens import (
    JWTGenerator,
    JWTValidator,
    get_sig_validation_args,
    get_signing_args,
)

if TYPE_CHECKING:
    import httpx
    from fastapi.routing import APIRoute

import structlog
from structlog.contextvars import bind_contextvars

logger = structlog.get_logger(logger_name=__name__)

__all__ = [
    "create_task_execution_api_app",
    "lifespan",
    "CorrelationIdMiddleware",
]


def _jwt_validator():
    from airflow.configuration import conf

    required_claims = frozenset(["aud", "exp", "iat"])

    if issuer := conf.get("api_auth", "jwt_issuer", fallback=None):
        required_claims = required_claims | {"iss"}
    validator = JWTValidator(
        required_claims=required_claims,
        issuer=issuer,
        audience=conf.get_mandatory_list_value("execution_api", "jwt_audience"),
        **get_sig_validation_args(make_secret_key_if_needed=False),
    )
    return validator


def _jwt_generator():
    from airflow.configuration import conf

    generator = JWTGenerator(
        valid_for=conf.getint("execution_api", "jwt_expiration_time"),
        audience=conf.get_mandatory_list_value("execution_api", "jwt_audience")[0],
        issuer=conf.get("api_auth", "jwt_issuer", fallback=None),
        # Since this one is used across components/server, there is no point trying to generate one, error
        # instead
        **get_signing_args(make_secret_key_if_needed=False),
    )
    return generator


@svcs.fastapi.lifespan
async def lifespan(app: FastAPI, registry: svcs.Registry):
    app.state.lifespan_called = True

    # According to svcs's docs this shouldn't be needed, but something about SubApps is odd, and we need to
    # record this here
    app.state.svcs_registry = registry

    registry.register_factory(JWTGenerator, _jwt_generator)
    # Create an app scoped validator, so that we don't have to fetch it every time
    registry.register_value(JWTValidator, _jwt_validator(), ping=JWTValidator.status)

    yield


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle correlation-id for request tracing.

    This middleware:
    1. Extracts correlation-id from request headers
    2. Binds it to structlog context for all logs within the request
    3. Echoes correlation-id back in response headers for tracing

    Note: Context variables are automatically isolated per async task in Python,
    so manual cleanup is not necessary. Each request gets its own context copy.
    """

    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get("correlation-id")

        if correlation_id:
            bind_contextvars(correlation_id=correlation_id)

        response: Response = await call_next(request)

        if correlation_id:
            response.headers["correlation-id"] = correlation_id

        return response


class JWTReissueMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        from airflow.configuration import conf

        response: Response = await call_next(request)

        refreshed_token: str | None = getattr(request.state, "refreshed_token", None)

        if not refreshed_token:
            auth_header = request.headers.get("authorization")
            if auth_header and auth_header.lower().startswith("bearer "):
                token = auth_header.split(" ", 1)[1]
                try:
                    async with svcs.Container(request.app.state.svcs_registry) as services:
                        validator: JWTValidator = await services.aget(JWTValidator)
                        claims = await validator.avalidated_claims(token, {})

                        now = int(time.time())
                        validity = conf.getint("execution_api", "jwt_expiration_time")
                        refresh_when_less_than = max(int(validity * 0.20), 30)
                        valid_left = int(claims.get("exp", 0)) - now
                        if valid_left <= refresh_when_less_than:
                            generator: JWTGenerator = await services.aget(JWTGenerator)
                            refreshed_token = generator.generate(claims)
                except Exception as err:
                    # Do not block the response if refreshing fails; log a warning for visibility
                    logger.warning(
                        "JWT reissue middleware failed to refresh token", error=str(err), exc_info=True
                    )

        if refreshed_token:
            response.headers["Refreshed-API-Token"] = refreshed_token
        return response


class CadwynWithOpenAPICustomization(Cadwyn):
    # Workaround lack of customzation https://github.com/zmievsa/cadwyn/issues/255
    async def openapi_jsons(self, req: Request) -> JSONResponse:
        resp = await super().openapi_jsons(req)
        open_apischema = json.loads(resp.body)
        open_apischema = self.customize_openapi(open_apischema)

        resp.body = resp.render(open_apischema)

        return resp

    def customize_openapi(self, openapi_schema: dict[str, Any]) -> dict[str, Any]:
        """
        Customize the OpenAPI schema to include additional schemas not tied to specific endpoints.

        This is particularly useful for client SDKs that require models for types
        not directly exposed in any endpoint's request or response schema.

        We also replace ``anyOf`` with ``oneOf`` in the API spec as this produces better results for the code
        generators. This is because anyOf can technically be more than of the given schemas, but 99.9% of the
        time (perhaps 100% in this API) the types are mutually exclusive, so oneOf is more correct

        References:
            - https://fastapi.tiangolo.com/how-to/extending-openapi/#modify-the-openapi-schema
        """
        extra_schemas = get_extra_schemas()
        for schema_name, schema in extra_schemas.items():
            if schema_name not in openapi_schema["components"]["schemas"]:
                openapi_schema["components"]["schemas"][schema_name] = schema

        # The `JsonValue` component is missing any info. causes issues when generating models
        openapi_schema["components"]["schemas"]["JsonValue"] = {
            "title": "Any valid JSON value",
            "oneOf": [
                {"type": t} for t in ("string", "number", "integer", "object", "array", "boolean", "null")
            ],
        }

        def replace_any_of_with_one_of(spec):
            if isinstance(spec, dict):
                return {
                    ("oneOf" if key == "anyOf" else key): replace_any_of_with_one_of(value)
                    for key, value in spec.items()
                }
            if isinstance(spec, list):
                return [replace_any_of_with_one_of(item) for item in spec]
            return spec

        openapi_schema = replace_any_of_with_one_of(openapi_schema)

        for comp in openapi_schema["components"]["schemas"].values():
            for prop in comp.get("properties", {}).values():
                # {"type": "string", "const": "deferred"}
                # to
                # {"type": "string", "enum": ["deferred"]}
                #
                # this produces better results in the code generator
                if prop.get("type") == "string" and (const := prop.pop("const", None)):
                    prop["enum"] = [const]

        return openapi_schema


def create_task_execution_api_app() -> FastAPI:
    """Create FastAPI app for task execution API."""
    from airflow.api_fastapi.execution_api.routes import execution_api_router
    from airflow.api_fastapi.execution_api.versions import bundle

    def custom_generate_unique_id(route: APIRoute):
        # This is called only if the route doesn't provide an explicit operation ID
        return route.name

    # See https://docs.cadwyn.dev/concepts/version_changes/ for info about API versions
    app = CadwynWithOpenAPICustomization(
        title="Airflow Task Execution API",
        description="The private Airflow Task Execution API.",
        lifespan=lifespan,
        generate_unique_id_function=custom_generate_unique_id,
        api_version_parameter_name="Airflow-API-Version",
        api_version_default_value=bundle.versions[0].value,
        versions=bundle,
    )

    # Add correlation-id middleware for request tracing
    app.add_middleware(CorrelationIdMiddleware)
    app.add_middleware(JWTReissueMiddleware)

    app.generate_and_include_versioned_routers(execution_api_router)

    # As we are mounted as a sub app, we don't get any logs for unhandled exceptions without this!
    @app.exception_handler(Exception)
    def handle_exceptions(request: Request, exc: Exception):
        logger.exception("Handle died with an error", exc_info=(type(exc), exc, exc.__traceback__))
        content = {"message": "Internal server error"}
        if correlation_id := request.headers.get("correlation-id"):
            content["correlation-id"] = correlation_id
        return JSONResponse(status_code=500, content=content)

    return app


def get_extra_schemas() -> dict[str, dict]:
    """Get all the extra schemas that are not part of the main FastAPI app."""
    from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance
    from airflow.executors.workloads import BundleInfo
    from airflow.task.trigger_rule import TriggerRule
    from airflow.task.weight_rule import WeightRule
    from airflow.utils.state import TaskInstanceState, TerminalTIState

    return {
        "TaskInstance": TaskInstance.model_json_schema(),
        "BundleInfo": BundleInfo.model_json_schema(),
        # Include the combined state enum too. In the datamodels we separate out SUCCESS from the other states
        # as that has different payload requirements
        "TerminalTIState": {"type": "string", "enum": list(TerminalTIState)},
        "TaskInstanceState": {"type": "string", "enum": list(TaskInstanceState)},
        "WeightRule": {"type": "string", "enum": list(WeightRule)},
        "TriggerRule": {"type": "string", "enum": list(TriggerRule)},
    }


@attrs.define()
class InProcessExecutionAPI:
    """
    A helper class to make it possible to run the ExecutionAPI "in-process".

    The sync version of this makes use of a2wsgi which runs the async loop in a separate thread. This is
    needed so that we can use the sync httpx client
    """

    _app: FastAPI | None = None
    _cm: AsyncExitStack | None = None

    @cached_property
    def app(self):
        if not self._app:
            from airflow.api_fastapi.common.dagbag import create_dag_bag
            from airflow.api_fastapi.execution_api.app import create_task_execution_api_app
            from airflow.api_fastapi.execution_api.deps import (
                JWTBearerDep,
                JWTBearerTIPathDep,
            )
            from airflow.api_fastapi.execution_api.routes.connections import has_connection_access
            from airflow.api_fastapi.execution_api.routes.variables import has_variable_access
            from airflow.api_fastapi.execution_api.routes.xcoms import has_xcom_access

            self._app = create_task_execution_api_app()

            # Set up dag_bag in app state for dependency injection
            self._app.state.dag_bag = create_dag_bag()

            async def always_allow(): ...

            self._app.dependency_overrides[JWTBearerDep.dependency] = always_allow
            self._app.dependency_overrides[JWTBearerTIPathDep.dependency] = always_allow
            self._app.dependency_overrides[has_connection_access] = always_allow
            self._app.dependency_overrides[has_variable_access] = always_allow
            self._app.dependency_overrides[has_xcom_access] = always_allow

        return self._app

    @cached_property
    def transport(self) -> httpx.WSGITransport:
        import asyncio

        import httpx
        from a2wsgi import ASGIMiddleware

        middleware = ASGIMiddleware(self.app)

        # https://github.com/abersheeran/a2wsgi/discussions/64
        async def start_lifespan(cm: AsyncExitStack, app: FastAPI):
            await cm.enter_async_context(app.router.lifespan_context(app))

        self._cm = AsyncExitStack()

        asyncio.run_coroutine_threadsafe(start_lifespan(self._cm, self.app), middleware.loop)
        return httpx.WSGITransport(app=middleware)  # type: ignore[arg-type]

    @cached_property
    def atransport(self) -> httpx.ASGITransport:
        import httpx

        return httpx.ASGITransport(app=self.app)
