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

from collections.abc import Callable, Iterable
from typing import Annotated

from pydantic import BeforeValidator, Field

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


def _call_function(function: Callable[[], int]) -> int:
    """
    Call the given function.

    Used for the BeforeValidator to get the actual values from the bound method.
    """
    return function()


PoolSlots = Annotated[
    int,
    Field(ge=-1, description="Number of slots. Use -1 for unlimited."),
]


class BasePool(BaseModel):
    """Base serializer for Pool."""

    pool: str = Field(serialization_alias="name")
    slots: PoolSlots
    description: str | None = Field(default=None)
    include_deferred: bool


def _sanitize_open_slots(value) -> int:
    if isinstance(value, float) and value == float("inf"):
        return -1
    return value


class PoolResponse(BasePool):
    """Pool serializer for responses."""

    occupied_slots: Annotated[int, BeforeValidator(_call_function)]
    running_slots: Annotated[int, BeforeValidator(_call_function)]
    queued_slots: Annotated[int, BeforeValidator(_call_function)]
    scheduled_slots: Annotated[int, BeforeValidator(_call_function)]
    open_slots: Annotated[int, BeforeValidator(lambda v: _sanitize_open_slots(_call_function(v)))]
    deferred_slots: Annotated[int, BeforeValidator(_call_function)]
    team_name: str | None


class PoolCollectionResponse(BaseModel):
    """Pool Collection serializer for responses."""

    pools: Iterable[PoolResponse]
    total_entries: int


class PoolPatchBody(StrictBaseModel):
    """Pool serializer for patch bodies."""

    name: str | None = Field(default=None, alias="pool")
    slots: PoolSlots | None = None
    description: str | None = None
    include_deferred: bool | None = None
    team_name: str | None = Field(max_length=50, default=None)


class PoolBody(BasePool, StrictBaseModel):
    """Pool serializer for post bodies."""

    pool: str = Field(alias="name", max_length=256)
    description: str | None = None
    include_deferred: bool = False
    team_name: str | None = Field(max_length=50, default=None)
