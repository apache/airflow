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

import asyncio
import json
import threading
import time
import weakref
from contextlib import AsyncExitStack
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

import attrs
import svcs
from cadwyn import (
    Cadwyn,
    current_dependency_solver,
)
from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from opentelemetry import context as otel_context, propagate as otel_propagate
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.auth.tokens import (
    JWTGenerator,
    JWTValidator,
    get_sig_validation_args,
    get_signing_args,
)

if TYPE_CHECKING:
    import httpx

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

    # InProcessExecutionAPI stubs out JWTValidator: don't re-register in that case.
    if JWTValidator not in registry:
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
        response: Response = await call_next(request)

        refreshed_token: str | None = None
        auth_header = request.headers.get("authorization")
        if auth_header and auth_header.lower().startswith("bearer "):
            token = auth_header.split(" ", 1)[1]
            try:
                async with svcs.Container(request.app.state.svcs_registry) as services:
                    validator: JWTValidator = await services.aget(JWTValidator)
                    claims = await validator.avalidated_claims(token, {})

                    # Workload tokens are long-lived and meant to survive queue
                    # wait times so avoid refreshing them. If avalidated_claims
                    # raises for a workload token, the outer except handles it.
                    if claims.get("scope") == "workload":
                        return response

                    now = int(time.time())
                    token_lifetime = int(claims.get("exp", 0)) - int(claims.get("iat", 0))
                    refresh_when_less_than = max(int(token_lifetime * 0.20), 30)
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
        open_apischema = json.loads(cast("bytes", resp.body))
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

        # Remove internal x-airflow-* extension fields from OpenAPI spec
        # These are used for runtime validation but shouldn't be exposed in the public API
        for path_item in openapi_schema.get("paths", {}).values():
            for operation in path_item.values():
                if isinstance(operation, dict):
                    keys_to_remove = [key for key in operation.keys() if key.startswith("x-airflow-")]
                    for key in keys_to_remove:
                        del operation[key]

        return openapi_schema


async def _extract_w3c_trace_context(
    request: Request,
    dependency_solver=Depends(current_dependency_solver),
):
    # Cadwyn solves dependencies twice (the real request, then again to migrate the
    # request body). Only act in the real "fastapi" pass so we attach/detach exactly
    # once, in the context the endpoint runs in.
    if dependency_solver != "fastapi":
        yield
        return
    ctx = otel_propagate.extract(request.headers)
    attached_in = asyncio.current_task()
    token = otel_context.attach(ctx)
    try:
        yield
    finally:
        if asyncio.current_task() is attached_in:
            otel_context.detach(token)


def _inject_trace_context_dep(routes, mode: str) -> None:
    dep = Depends(_extract_w3c_trace_context)
    for route in routes:
        if not isinstance(route, APIRoute):
            continue
        # Idempotent: create_task_execution_api_app() runs more than once per process
        # (cached_app + InProcessExecutionAPI), and execution_api_router is shared
        # module state, so strip any prior injection first.
        route.dependencies[:] = [
            d for d in route.dependencies if getattr(d, "dependency", None) is not _extract_w3c_trace_context
        ]
        match mode:
            case "unsafe-always":
                route.dependencies.insert(0, dep)
            case "only-authenticated":
                from airflow.api_fastapi.execution_api.security import require_auth

                if any(getattr(d, "dependency", None) is require_auth for d in route.dependencies):
                    route.dependencies.append(dep)


def create_task_execution_api_app(lifespan: svcs.fastapi.lifespan = lifespan) -> FastAPI:
    """Create FastAPI app for task execution API."""
    from airflow.api_fastapi.execution_api.routes import execution_api_router
    from airflow.api_fastapi.execution_api.versions import bundle
    from airflow.configuration import conf

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

    mode = conf.get("execution_api", "otel_trace_propagation", fallback="only-authenticated")
    _inject_trace_context_dep(execution_api_router.routes, mode)

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
    from airflow.serialization.enums import DagAttributeTypes
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
        "DagAttributeTypes": {
            "type": "string",
            "enum": [DagAttributeTypes.OP.value, DagAttributeTypes.TASK_GROUP.value],
            "x-enum-varnames": [DagAttributeTypes.OP.name, DagAttributeTypes.TASK_GROUP.name],
        },
    }


# Note: _shutdown_loop is used as a finalizer for the WSGI transport returned by
# ``InProcessExecutionAPI.transport``. As such, its arguments must not directly or indirectly reference that
# transport, as this would prevent the transport from being garbage collected.
def _shutdown_loop(
    loop: asyncio.AbstractEventLoop,
    thread: threading.Thread,
    cm: AsyncExitStack,
) -> None:
    """Close the FastAPI lifespan and stop the background event loop + thread."""
    try:
        asyncio.run_coroutine_threadsafe(cm.aclose(), loop).result(timeout=5)
    except Exception:
        logger.exception("Error while closing in-process execution API lifespan")
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)


@attrs.define()
class InProcessExecutionAPI:
    """
    A helper class to make it possible to run the ExecutionAPI "in-process".

    The sync version of this makes use of a2wsgi which runs the async loop in a separate thread. This is
    needed so that we can use the sync httpx client
    """

    _app: FastAPI | None = None

    @cached_property
    def app(self):
        if not self._app:
            from airflow.api_fastapi.common.dagbag import create_dag_bag
            from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
            from airflow.api_fastapi.execution_api.routes.connections import has_connection_access
            from airflow.api_fastapi.execution_api.routes.variables import has_variable_access
            from airflow.api_fastapi.execution_api.routes.xcoms import has_xcom_access
            from airflow.api_fastapi.execution_api.security import _jwt_bearer

            # Give this app its own lifespan + services registry so that stubbing services
            # (e.g. JWTValidator) doesn't affect the module-level ``lifespan.registry``.
            registry = svcs.Registry()
            private_lifespan = attrs.evolve(lifespan, registry=registry)
            self._app = create_task_execution_api_app(lifespan=private_lifespan)

            # In-process callers don't need a real JWTValidator: auth is bypassed below via
            # ``dependency_overrides``.
            registry.register_value(JWTValidator, None)

            # Set up dag_bag in app state for dependency injection
            self._app.state.dag_bag = create_dag_bag()

            async def always_allow(request: Request):
                from uuid import UUID

                ti_id = UUID(
                    request.path_params.get("task_instance_id", "00000000-0000-0000-0000-000000000000")
                )
                claims = TIClaims(scope="execution")
                return TIToken(id=ti_id, claims=claims)

            self._app.dependency_overrides[_jwt_bearer] = always_allow
            self._app.dependency_overrides[has_connection_access] = always_allow
            self._app.dependency_overrides[has_variable_access] = always_allow
            self._app.dependency_overrides[has_xcom_access] = always_allow

        return self._app

    @cached_property
    def transport(self) -> httpx.WSGITransport:
        import httpx
        from a2wsgi import ASGIMiddleware

        # We choose to own the event loop + executor thread here so that we can have explicit control over
        # their lifecycle.
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, name="InProcessExecutionAPI-loop", daemon=True)
        thread.start()

        middleware = ASGIMiddleware(self.app, loop=loop)

        # https://github.com/abersheeran/a2wsgi/discussions/64
        async def start_lifespan(cm: AsyncExitStack, app: FastAPI):
            await cm.enter_async_context(app.router.lifespan_context(app))

        cm = AsyncExitStack()

        # Wait for lifespan startup to complete so callers see a ready app and so the finalizer can
        # safely aclose() a context whose __aenter__ has actually run.
        asyncio.run_coroutine_threadsafe(start_lifespan(cm, self.app), loop).result()

        transport = httpx.WSGITransport(app=middleware)  # type: ignore[arg-type]

        # Stop the loop + thread and unwind the lifespan when the *transport* is garbage collected, not
        # this InProcessExecutionAPI instance. Callers commonly build a Client from ``.transport`` and drop
        # the factory object (e.g. ``Client(transport=InProcessExecutionAPI().transport)``); finalizing on
        # ``self`` would stop the loop while the transport is still in use, so every later request would
        # hang on the now-dead loop.
        weakref.finalize(transport, _shutdown_loop, loop, thread, cm)

        return transport

    @cached_property
    def atransport(self) -> httpx.ASGITransport:
        import httpx

        return httpx.ASGITransport(app=self.app)
