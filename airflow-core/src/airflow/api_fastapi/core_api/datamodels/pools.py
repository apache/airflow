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

from pydantic import BeforeValidator, Field, model_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.configuration import conf
from airflow.models.pool import Pool


def _call_function(function: Callable[[], int]) -> int:
    """
    Call the given function.

    Used for the BeforeValidator to get the actual values from the bound method.
    """
    return function()


def _apply_include_deferred_override(value: bool) -> bool:
    override = Pool.get_include_deferred_override()
    return value if override is None else override


def _reject_user_provided_include_deferred(provided_fields: set[str]) -> bool | None:
    """
    Get the cluster-wide ``include_deferred`` value, rejecting any value provided by the user.

    :param provided_fields: names of the fields explicitly present in the request body.
    :return: the cluster-wide value, or None when pools choose the flag themselves.
    :raises ValueError: if the flag is fixed cluster-wide but present in the request body.
    """
    override = Pool.get_include_deferred_override()
    if override is not None and "include_deferred" in provided_fields:
        raise ValueError(
            f"include_deferred cannot be set because it is fixed to {override} for all pools by the "
            "[core] pool_include_deferred configuration. Please contact your administrator."
        )
    return override


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

    # Report the effective value: the cluster-wide config value takes precedence over the stored column
    include_deferred: Annotated[bool, BeforeValidator(_apply_include_deferred_override)]

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

    @model_validator(mode="after")
    def validate_team_name(self) -> PoolPatchBody:
        if self.team_name is not None and not conf.getboolean("core", "multi_team"):
            raise ValueError(
                "team_name cannot be set when multi_team mode is disabled. Please contact your administrator."
            )
        return self

    @model_validator(mode="after")
    def enforce_include_deferred_override(self) -> PoolPatchBody:
        _reject_user_provided_include_deferred(self.model_fields_set)
        return self


class PoolBody(BasePool, StrictBaseModel):
    """Pool serializer for post bodies."""

    pool: str = Field(alias="name", max_length=256)
    description: str | None = None
    include_deferred: bool = False
    team_name: str | None = Field(max_length=50, default=None)

    @model_validator(mode="after")
    def validate_team_name(self) -> PoolBody:
        if self.team_name is not None and not conf.getboolean("core", "multi_team"):
            raise ValueError(
                "team_name cannot be set when multi_team mode is disabled. Please contact your administrator."
            )
        return self

    @model_validator(mode="after")
    def enforce_include_deferred_override(self) -> PoolBody:
        override = _reject_user_provided_include_deferred(self.model_fields_set)
        if override is not None:
            self.include_deferred = override
        return self
