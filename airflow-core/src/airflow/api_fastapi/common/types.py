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

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    AliasGenerator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
)

from airflow._shared.timezones import timezone

UtcDateTime = Annotated[AwareDatetime, AfterValidator(lambda d: d.astimezone(timezone.utc))]
"""UTCDateTime is a datetime with timezone information"""


def _validate_timedelta_field(td: timedelta | None) -> TimeDelta | None:
    """Validate the timedelta field and return it."""
    if td is None:
        return None
    return TimeDelta(
        days=td.days,
        seconds=td.seconds,
        microseconds=td.microseconds,
    )


class TimeDelta(BaseModel):
    """TimeDelta can be used to interact with datetime.timedelta objects."""

    object_type: str = "TimeDelta"
    days: int
    seconds: int
    microseconds: int

    model_config = ConfigDict(
        alias_generator=AliasGenerator(
            serialization_alias=lambda field_name: {
                "object_type": "__type",
            }.get(field_name, field_name),
        )
    )


TimeDeltaWithValidation = Annotated[TimeDelta, BeforeValidator(_validate_timedelta_field)]


class Mimetype(str, Enum):
    """Mimetype for the `Content-Type` header."""

    TEXT = "text/plain"
    JSON = "application/json"
    FORM = "application/x-www-form-urlencoded"
    NDJSON = "application/x-ndjson"
    ANY = "*/*"


@dataclass
class ExtraMenuItem:
    """Define a menu item that can be added to the menu by auth managers or plugins."""

    text: str
    href: str


class MenuItem(Enum):
    """Define all menu items defined in the menu."""

    REQUIRED_ACTIONS = "Required Actions"
    ASSETS = "Assets"
    AUDIT_LOG = "Audit Log"
    CONFIG = "Config"
    CONNECTIONS = "Connections"
    DAGS = "Dags"
    DOCS = "Docs"
    PLUGINS = "Plugins"
    POOLS = "Pools"
    PROVIDERS = "Providers"
    VARIABLES = "Variables"
    XCOMS = "XComs"


class UIAlert(BaseModel):
    """Optional alert to be shown at the top of the page."""

    text: str
    category: Literal["info", "warning", "error"]


class Theme(BaseModel):
    """JSON to modify Chakra's theme."""

    tokens: dict[
        Literal["colors"],
        dict[
            Literal["brand"],
            dict[
                Literal["50", "100", "200", "300", "400", "500", "600", "700", "800", "900", "950"],
                dict[Literal["value"], str],
            ],
        ],
    ]
