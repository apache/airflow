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

from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Response
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import JSONResponse

from airflow.webhooks.base import WebhookRequest
from airflow.webhooks.connections import ConfiguredWebhook
from airflow.webhooks.pipeline import DeliveryPipeline, DeliveryRejected

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    from airflow.webhooks.registry import WebhookRegistry

router = APIRouter()


def _get_pipeline(request: Request) -> DeliveryPipeline:
    return request.app.state.webhook_pipeline


def get_webhook(name: str, request: Request) -> ConfiguredWebhook:
    return _get_pipeline(request).get_webhook(name)


async def read_body(request: Request, webhook: Annotated[ConfiguredWebhook, Depends(get_webhook)]) -> bytes:
    return await _get_pipeline(request).read_body(
        webhook, request.headers.get("content-length"), request.stream()
    )


@router.get("/_health")
async def get_health() -> dict[str, str]:
    """Answer the probes of load balancers without reading the database, since anyone can call it."""
    return {"status": "healthy"}


# Also with a trailing slash: the app would answer it with a redirect otherwise, which senders such as
# GitHub do not follow.
@router.post("/{name}/")
@router.post("/{name}")
def receive_delivery(
    request: Request,
    webhook: Annotated[ConfiguredWebhook, Depends(get_webhook)],
    body: Annotated[bytes, Depends(read_body)],
) -> JSONResponse:
    webhook_request = WebhookRequest(
        method=request.method, headers=request.headers, query=request.query_params, body=body
    )
    answer = _get_pipeline(request).process(webhook, webhook_request)
    return JSONResponse(answer.body, status_code=answer.status)


class AnswerUnhandledErrorsMiddleware:
    """
    Answer an error that no exception handler takes with a 500 without detail, and log it.

    Not an exception handler for ``Exception``: Starlette raises the error again after calling one, so the
    server would log every such request with its traceback, past the pipeline's throttle. Nor
    ``init_error_handlers``, which answers database errors with their SQL when
    ``[api] expose_stacktrace`` is on.
    """

    def __init__(self, app: ASGIApp, pipeline: DeliveryPipeline) -> None:
        self.app = app
        self.pipeline = pipeline

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        response_started = False

        async def send_and_track(message: Message) -> None:
            nonlocal response_started
            response_started = response_started or message["type"] == "http.response.start"
            await send(message)

        try:
            await self.app(scope, receive, send_and_track)
        except Exception as e:
            # The router sets the path parameters on the scope it is given, which is this one.
            self.pipeline.log_unhandled_error(scope.get("path_params", {}).get("name"), e)
            if not response_started:
                await JSONResponse({"detail": "Internal Server Error"}, status_code=500)(scope, receive, send)


def create_webhooks_app(registry: WebhookRegistry) -> FastAPI:
    """
    Create the inbound webhooks app, which receives deliveries at ``POST /<name>``.

    Anyone who can reach the app can call it, so it serves exactly ``GET /_health``, ``POST /{name}``
    and ``POST /{name}/``, has no API documentation, and answers errors without details.

    :param registry: The webhooks to serve.
    """
    app = FastAPI(title="Airflow inbound webhooks", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.webhook_pipeline = DeliveryPipeline(registry)
    app.include_router(router)
    app.add_middleware(AnswerUnhandledErrorsMiddleware, pipeline=app.state.webhook_pipeline)

    # Async, so that answering a refused request does not take a worker thread of its own.
    @app.exception_handler(DeliveryRejected)
    async def answer_rejection(request: Request, exc: DeliveryRejected) -> JSONResponse:
        _get_pipeline(request).count_rejection(request.path_params.get("name"), exc.status)
        headers = None if exc.retry_after is None else {"Retry-After": str(exc.retry_after)}
        return JSONResponse({"detail": exc.detail}, status_code=exc.status, headers=headers)

    # Requests that no route serves, such as POST /<name>/x or GET /<name>, which the router refuses.
    @app.exception_handler(HTTPStatus.NOT_FOUND)
    @app.exception_handler(HTTPStatus.METHOD_NOT_ALLOWED)
    async def answer_unrouted(request: Request, exc: HTTPException) -> Response:
        _get_pipeline(request).count_rejection(request.path_params.get("name"), exc.status_code)
        return await http_exception_handler(request, exc)

    return app
