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

from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING

from airflow.sdk.definitions.asset import AssetEventReceipt
from airflow.sdk.execution_time.comms import AssetEventEmissionResult, EmitAssetEvent

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from pydantic.types import JsonValue

    from airflow.sdk.definitions.asset import Asset

_callback_index: ContextVar[int | None] = ContextVar("asset_emission_callback_index", default=None)


@contextmanager
def set_asset_emission_callback(index: int) -> Iterator[None]:
    """Select a callback from the trusted parent's callback batch."""
    token = _callback_index.set(index)
    try:
        yield
    finally:
        _callback_index.reset(token)


def emit_asset_event(
    asset: Asset,
    *,
    extra: dict[str, JsonValue] | None,
    partition_key: str | None,
    partition_date: datetime | None,
) -> AssetEventReceipt:
    from airflow.sdk import get_current_context

    callback_index = _callback_index.get()
    if callback_index is None:
        # A parser also has supervisor comms; that alone is not a runtime context.
        context = get_current_context()
        if context.get("ti") is None:
            raise RuntimeError("Asset.emit() requires a running task or a Dag callback")

    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    result = SUPERVISOR_COMMS.send(
        EmitAssetEvent(
            name=asset.name,
            uri=asset.uri,
            extra=extra or {},
            partition_key=partition_key,
            partition_date=partition_date,
            callback_index=callback_index,
        )
    )
    if not isinstance(result, AssetEventEmissionResult):
        raise RuntimeError(f"Unexpected asset emission response: {type(result).__name__}")
    return AssetEventReceipt(id=result.id, timestamp=result.timestamp)
