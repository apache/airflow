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

from typing import Any

from pydantic import BaseModel, BeforeValidator, field_validator
from typing_extensions import Annotated

from airflow.plugins_manager import AirflowPluginSource


def coerce_to_string(data: Any) -> Any:
    return str(data)


class FastAPIAppResponse(BaseModel):
    """Serializer for Plugin FastAPI App responses."""

    app: str
    url_prefix: str
    name: str


class AppBuilderViewResponse(BaseModel):
    """Serializer for AppBuilder View responses."""

    name: str | None = None
    category: str | None = None
    view: str | None = None
    label: str | None = None


class AppBuilderMenuItemResponse(BaseModel):
    """Serializer for AppBuilder Menu Item responses."""

    name: str
    href: str | None = None
    category: str | None = None


class PluginResponse(BaseModel):
    """Plugin serializer."""

    name: str
    hooks: list[str]
    executors: list[str]
    macros: list[str]
    flask_blueprints: list[str]
    fastapi_apps: list[FastAPIAppResponse]
    appbuilder_views: list[AppBuilderViewResponse]
    appbuilder_menu_items: list[AppBuilderMenuItemResponse]
    global_operator_extra_links: list[str]
    operator_extra_links: list[str]
    source: Annotated[str, BeforeValidator(coerce_to_string)]
    ti_deps: list[Annotated[str, BeforeValidator(coerce_to_string)]]
    listeners: list[str]
    timetables: list[str]

    @field_validator("source", mode="before")
    @classmethod
    def convert_source(cls, data: Any) -> Any:
        if isinstance(data, AirflowPluginSource):
            return str(data)
        return data


class PluginCollectionResponse(BaseModel):
    """Plugin Collection serializer."""

    plugins: list[PluginResponse]
    total_entries: int
