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

"""Opt-in parsing API for the executor prototype, separate from the versioned task API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import jwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from fastapi import Depends, FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict
from starlette.responses import JSONResponse

from airflow.api_fastapi.auth.tokens import JWKS, JWTValidator, key_to_jwk_dict
from airflow.dag_processing.parsing_state import (
    ReceiptConflictError,
    ReceiptExpiredError,
    ReceiptInvalidResultError,
    ReceiptNotFoundError,
    ReceiptStore,
)
from airflow.executors.workloads.parsing import MAX_PARSING_REQUEST_BYTES, DagDefinitionResult

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Receive, Scope, Send

TOKEN_AUDIENCE = "dag-parsing-poc"
TOKEN_ISSUER = "dag-parsing-poc"
TOKEN_SCOPE = "dag-parsing-poc"
TOKEN_KEY_ID = "dag-parsing-poc"
ROUTE_PREFIX = "/execution/poc/parsing/workloads/{workload_id}/attempts/{attempt_id}"
MAX_REQUEST_BYTES = MAX_PARSING_REQUEST_BYTES


class _RequestBodyLimit:
    def __init__(self, app: ASGIApp, max_bytes: int):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http" or scope["method"] != "POST":
            return await self.app(scope, receive, send)
        chunks = []
        received = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            received += len(chunk)
            if received > self.max_bytes:
                response = JSONResponse(
                    status_code=413, content={"detail": "Prototype request size exceeded"}
                )
                return await response(scope, receive, send)
            chunks.append(chunk)
            if not message.get("more_body", False):
                break
        body = b"".join(chunks)
        chunks.clear()

        async def receive_body():
            nonlocal body
            if body is not None:
                message = {"type": "http.request", "body": body, "more_body": False}
                body = None
                return message
            return await receive()

        return await self.app(scope, receive_body, send)


class ClaimRequest(BaseModel):
    """Bind an attempt to one worker execution."""

    model_config = ConfigDict(extra="forbid")
    execution_id: UUID


class ResultRequest(ClaimRequest):
    """Publish a serialized result for a claimed attempt."""

    result: DagDefinitionResult


def create_app(
    store_path: str | Path,
    public_key_path: str | Path,
    *,
    max_request_bytes: int = MAX_REQUEST_BYTES,
    persist_metadata: bool = False,
    orchestrated: bool = False,
) -> FastAPI:
    """Construct the receipt API using only a public verification key and registered manifests."""
    public_key = load_pem_public_key(Path(public_key_path).read_bytes())
    if not isinstance(public_key, Ed25519PublicKey):
        raise ValueError("Prototype parsing requires an Ed25519 public key")
    jwks = JWKS(
        url="",
        jwks=jwt.PyJWKSet.from_dict({"keys": [key_to_jwk_dict(public_key, kid=TOKEN_KEY_ID)]}),
    )
    validator = JWTValidator(
        jwks=jwks,
        issuer=TOKEN_ISSUER,
        audience=TOKEN_AUDIENCE,
        algorithm=["EdDSA"],
        leeway=0,
        required_claims=frozenset({"sub", "scope", "attempt_ids", "exp", "iat", "nbf"}),
    )
    store: ReceiptStore
    if persist_metadata:
        from airflow.dag_processing.parsing_metadata import MetadataOrchestrationStore, MetadataReceiptStore

        store = MetadataOrchestrationStore(store_path) if orchestrated else MetadataReceiptStore(store_path)
    elif orchestrated:
        from airflow.dag_processing.orchestrator import OrchestrationStore

        store = OrchestrationStore(store_path)
    else:
        store = ReceiptStore(store_path)
    bearer = HTTPBearer(auto_error=False)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            await jwks.client.aclose()

    app = FastAPI(title="Dag parsing M0 receipt API (development only)", lifespan=lifespan)
    app.add_middleware(_RequestBodyLimit, max_bytes=max_request_bytes)

    @app.exception_handler(RequestValidationError)
    async def reject_invalid_request(request, error: RequestValidationError):
        # Invalid inputs can themselves contain nonfinite values that JSON responses cannot encode.
        return JSONResponse(
            status_code=422,
            content={
                "detail": [{key: item[key] for key in ("loc", "msg", "type")} for item in error.errors()]
            },
        )

    async def authorize_attempt(
        workload_id: UUID,
        attempt_id: UUID,
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    ) -> None:
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing workload token")
        try:
            claims = await validator.avalidated_claims(credentials.credentials)
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=403, detail="Invalid workload token") from None
        if (
            claims["sub"] != str(workload_id)
            or claims["scope"] != TOKEN_SCOPE
            or not isinstance(claims["attempt_ids"], list)
            or str(attempt_id) not in claims["attempt_ids"]
        ):
            raise HTTPException(status_code=403, detail="Token does not authorize this parsing attempt")

    def translate_error(error: ValueError) -> HTTPException:
        if isinstance(error, ReceiptNotFoundError):
            return HTTPException(status_code=404, detail=str(error))
        if isinstance(error, ReceiptExpiredError):
            return HTTPException(status_code=410, detail=str(error))
        if isinstance(error, ReceiptInvalidResultError):
            return HTTPException(status_code=422, detail=str(error))
        return HTTPException(status_code=409, detail=str(error))

    @app.post(f"{ROUTE_PREFIX}/claim", dependencies=[Depends(authorize_attempt)])
    def claim(workload_id: UUID, attempt_id: UUID, body: ClaimRequest):
        try:
            return store.claim(workload_id, attempt_id, body.execution_id)
        except (ReceiptNotFoundError, ReceiptExpiredError, ReceiptConflictError) as error:
            raise translate_error(error) from error

    @app.post(f"{ROUTE_PREFIX}/result", dependencies=[Depends(authorize_attempt)])
    def accept_result(workload_id: UUID, attempt_id: UUID, body: ResultRequest):
        try:
            return store.accept_result(workload_id, attempt_id, body.execution_id, body.result)
        except (
            ReceiptNotFoundError,
            ReceiptExpiredError,
            ReceiptConflictError,
            ReceiptInvalidResultError,
        ) as error:
            raise translate_error(error) from error

    @app.get("/health")
    def get_health():
        return {
            "status": "ok",
            "mode": "development-metadata" if persist_metadata else "development-receipts-only",
        }

    return app
