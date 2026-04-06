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

import re
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    AliasGenerator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    field_validator,
    model_serializer,
    model_validator,
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


# Common validator for theme icon fields (SVG-only, http(s) or app-relative path).
def _validate_theme_icon(value: str | None) -> str | None:
    if value is None:
        return value
    from urllib.parse import urlparse

    parsed = urlparse(value)
    if parsed.scheme in ("http", "https"):
        path = parsed.path or ""
    elif parsed.scheme == "" and value.startswith("/"):
        path = value
    else:
        raise ValueError("theme.icon must be http(s) URL or app-relative path starting with '/'")
    if not path.lower().endswith(".svg"):
        raise ValueError("theme.icon must point to an SVG file (*.svg)")
    return value


# Alias type for theme icon fields with shared validation
ThemeIconType = Annotated[str | None, BeforeValidator(_validate_theme_icon)]


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
    JOBS = "Jobs"
    PLUGINS = "Plugins"
    POOLS = "Pools"
    PROVIDERS = "Providers"
    VARIABLES = "Variables"
    XCOMS = "XComs"


class UIAlert(BaseModel):
    """Optional alert to be shown at the top of the page."""

    text: str
    category: Literal["info", "warning", "error"]


class OklchColor(BaseModel):
    """Validates OKLCH color format from string oklch(l c h)."""

    lightness: float
    chroma: float
    hue: float

    @model_validator(mode="before")
    @classmethod
    def parse_oklch_string(cls, data):
        if isinstance(data, str):
            oklch_regex_pattern = r"^oklch\((-?\d+(?:\.\d+)?) (-?\d+(?:\.\d+)?) (-?\d+(?:\.\d+)?)\)$"
            match = re.match(oklch_regex_pattern, data)

            if not match:
                raise ValueError(f"Invalid OKLCH format: {data} Expected format oklch(l c h)")

            ligthness_str, chroma_str, hue_str = match.groups()

            return {
                "lightness": float(ligthness_str),
                "chroma": float(chroma_str),
                "hue": float(hue_str),
            }
        return data

    @field_validator("lightness")
    @classmethod
    def validate_lightness(cls, value: float) -> float:
        if value < 0 or value > 1:
            raise ValueError(f"Invalid lightness: {value} Must be between 0 and 1")
        return value

    @field_validator("chroma")
    @classmethod
    def validate_chroma(cls, value: float) -> float:
        if value < 0 or value > 0.5:
            raise ValueError(f"Invalid chroma: {value} Must be between 0 and 0.5")
        return value

    @field_validator("hue")
    @classmethod
    def validate_hue(cls, value: float) -> float:
        if value < 0 or value > 360:
            raise ValueError(f"Invalid hue: {value} Must be between 0 and 360")
        return value

    @model_serializer(mode="plain")
    def serialize_model(self) -> str:
        return f"oklch({self.lightness} {self.chroma} {self.hue})"


# Private type aliases for theme token shapes
_ColorShade = dict[Literal["value"], OklchColor]
_SHADE_LITERAL = Literal["50", "100", "200", "300", "400", "500", "600", "700", "800", "900", "950"]
_ColorScale = dict[_SHADE_LITERAL, _ColorShade]


class ThemeColors(BaseModel):
    """Color tokens for the UI theme. All fields are optional; at least one must be provided."""

    brand: _ColorScale | None = None
    gray: _ColorScale | None = None
    black: _ColorShade | None = None
    white: _ColorShade | None = None

    @model_validator(mode="after")
    def check_at_least_one_color(self) -> ThemeColors:
        if not any([self.brand, self.gray, self.black, self.white]):
            raise ValueError("At least one color token must be provided: brand, gray, black, or white")
        return self

    @model_serializer(mode="wrap")
    def serialize_model(self, handler: Any) -> dict:
        return {k: v for k, v in handler(self).items() if v is not None}


class Theme(BaseModel):
    """JSON to modify Chakra's theme."""

    tokens: dict[Literal["colors"], ThemeColors]
    globalCss: dict[str, dict] | None = None
    icon: ThemeIconType = None
    icon_dark_mode: ThemeIconType = None
