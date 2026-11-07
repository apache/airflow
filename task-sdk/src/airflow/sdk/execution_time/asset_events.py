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
import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING

import attrs

from airflow.sdk.definitions.asset import Asset, AssetEventReceipt, AssetNameRef, AssetUriRef
from airflow.sdk.exceptions import (
    AirflowRuntimeError,
    AssetEmissionError,
    AssetEmissionUnavailableError,
    ErrorType,
)
from airflow.sdk.execution_time.comms import AssetEventEmissionResult, EmitAssetEvent

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pydantic.types import JsonValue

    from airflow.sdk.definitions.asset import AssetRef


@attrs.frozen
class _EmissionScope:
    callback_index: int | None = None
    """The callback's position in the trusted parent's batch, for a Dag processor callback."""


# A process global, not a ContextVar: threads that the task starts get an empty context, but they
# must emit too. Each runtime process runs one task or one callback at a time.
_scope: _EmissionScope | None = None


@contextmanager
def set_asset_emission_scope(*, callback_index: int | None = None) -> Iterator[None]:
    """
    Let the process emit asset events while the code inside runs, from any of its threads.

    The runtime that starts user code sets this: the task runner around a task and its callbacks,
    the callback supervisor around an executor callback, and the Dag processor around a callback.
    A Dag file parse has supervisor comms too, but no scope, so it cannot emit.
    """
    global _scope
    previous, _scope = _scope, _EmissionScope(callback_index=callback_index)
    try:
        yield
    finally:
        _scope = previous


def _raise_emission_error(error: AirflowRuntimeError) -> None:
    detail = error.error.detail if isinstance(error.error.detail, dict) else {}
    body = detail.get("detail")
    server_detail = body.get("detail") if isinstance(body, dict) else None
    status_code = detail.get("status_code")
    if error.error.error == ErrorType.PERMISSION_DENIED:
        raise AssetEmissionUnavailableError(
            "This context cannot emit asset events", reason="forbidden"
        ) from error
    if not isinstance(server_detail, dict):
        # A server without the endpoint answers with FastAPI's plain "Not Found".
        if status_code == 404:
            raise AssetEmissionUnavailableError(
                "The API server cannot emit asset events", reason="unsupported_server", status_code=404
            ) from error
        # FastAPI request validation, for example of the partition key, gives a list of errors.
        if status_code == 422 and isinstance(server_detail, list):
            problems = "; ".join(
                f"{'.'.join(str(part) for part in item.get('loc', ())[1:])}: {item.get('msg')}"
                for item in server_detail
                if isinstance(item, dict)
            )
            raise AssetEmissionError(
                f"The API server rejected the request: {problems}", reason="invalid_request", status_code=422
            ) from error
        raise error
    raise AssetEmissionError(
        server_detail.get("message", str(error)),
        reason=server_detail.get("reason"),
        status_code=status_code,
    ) from error


def emit_asset_event(
    asset: Asset | AssetRef,
    *,
    extra: dict[str, JsonValue] | None = None,
    partition_key: str | None = None,
    idempotency_key: str | None = None,
) -> AssetEventReceipt:
    """
    Write an asset event now, from a running task or from a callback.

    Unlike an outlet, the event does not wait for the task to succeed, and a later failure does not
    remove it. The asset must already be in the Airflow database. An omitted partition key means no
    partition; the event does not inherit the partition of the current run.

    Give a stable ``idempotency_key`` when a retry must not write a second event. Without one, the
    SDK makes a key for this call, so only its own automatic HTTP retries share it.
    """
    if isinstance(asset, Asset):
        name, uri = asset.name, asset.uri
    elif isinstance(asset, AssetNameRef):
        name, uri = asset.name, None
    elif isinstance(asset, AssetUriRef):
        name, uri = None, asset.uri
    else:
        raise TypeError(f"Expected an Asset or an AssetRef, got {type(asset).__name__}")

    try:
        json.dumps(extra or {}, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise AssetEmissionError(
            f"extra must be strict JSON, without NaN or Infinity: {error}", reason="invalid_json"
        ) from error

    scope = _scope
    if scope is None:
        raise AssetEmissionUnavailableError(
            "Asset events can be emitted only from a running task or a callback", reason="no_runtime"
        )

    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    try:
        result = SUPERVISOR_COMMS.send(
            EmitAssetEvent(
                name=name,
                uri=uri,
                extra=extra or {},
                partition_key=partition_key,
                # An empty key goes to the server unchanged, which rejects it.
                idempotency_key=str(uuid.uuid4()) if idempotency_key is None else idempotency_key,
                callback_index=scope.callback_index,
            )
        )
    except AirflowRuntimeError as error:
        _raise_emission_error(error)
        raise
    if not isinstance(result, AssetEventEmissionResult):
        raise RuntimeError(f"Unexpected asset emission response: {type(result).__name__}")
    return AssetEventReceipt(id=result.id, timestamp=result.timestamp, replayed=result.replayed)
