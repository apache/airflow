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

from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, ConfigDict, Field, RootModel, field_validator, model_validator

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.plugins_manager import AirflowPluginSource, BaseDestinationLiteral


def coerce_to_string(data: Any) -> Any:
    return str(data)


class FastAPIAppResponse(BaseModel):
    """Serializer for Plugin FastAPI App responses."""

    model_config = ConfigDict(extra="allow")

    app: str
    url_prefix: str
    name: str


class FastAPIRootMiddlewareResponse(BaseModel):
    """Serializer for Plugin FastAPI root middleware responses."""

    model_config = ConfigDict(extra="allow")

    middleware: str
    name: str


class AppBuilderViewResponse(BaseModel):
    """Serializer for AppBuilder View responses."""

    model_config = ConfigDict(extra="allow")

    name: str | None = None
    category: str | None = None
    view: str | None = None
    label: str | None = None


class AppBuilderMenuItemResponse(BaseModel):
    """Serializer for AppBuilder Menu Item responses."""

    model_config = ConfigDict(extra="allow")

    name: str
    href: str
    category: str | None = None


class PluginAppliesToResponse(RootModel[dict[str, list[str]]]):
    """
    Serializer for the optional scoping criteria of a UI plugin.

    An open map of dotted field path to the values that path may take -- not a closed set of
    criteria. ``{"state": ["failed"], "dag.tags.name": ["ml"]}`` scopes to failed entities of
    ml-tagged Dags. An unqualified path is rooted at the entity the ``destination`` is about;
    a path may instead name a related record (``dag``, ``dag_run``, ``task``, ``task_instance``)
    as its first segment. Matching is equality against the listed values, OR within a path and
    AND across paths, and is evaluated client-side.
    """

    root: dict[str, list[str]] = Field(default_factory=dict)


class BaseUIResponse(BaseModel):
    """Base serializer for UI Plugin responses."""

    model_config = ConfigDict(extra="allow")

    name: str
    icon: str | None = None
    icon_dark_mode: str | None = None
    url_route: str | None = None
    category: str | None = None
    nav_top_level: bool | None = False
    # Optional visibility scoping, evaluated client-side. Values are OR-ed within a path and
    # AND-ed across paths, but only across paths whose root record the current destination
    # actually has (a `task_instance.*` path cannot be judged on a Dag-level page, so it is
    # skipped there rather than failing the match). Omitting `applies_to` shows the item
    # everywhere. Display gating only, not an authorization boundary.
    applies_to: PluginAppliesToResponse | None = None


class ExternalViewResponse(BaseUIResponse):
    """Serializer for External View Plugin responses."""

    model_config = ConfigDict(extra="allow")

    href: str
    destination: BaseDestinationLiteral = "nav"


class ReactAppResponse(BaseUIResponse):
    """Serializer for React App Plugin responses."""

    model_config = ConfigDict(extra="allow")

    bundle_url: str
    destination: Literal[BaseDestinationLiteral, "dashboard", "dag_overview", "task_overview"] = "nav"


class PluginResponse(BaseModel):
    """Plugin serializer."""

    name: str
    team_name: str | None = None
    macros: list[str]
    flask_blueprints: list[str]
    fastapi_apps: list[FastAPIAppResponse]
    fastapi_root_middlewares: list[FastAPIRootMiddlewareResponse]
    external_views: list[ExternalViewResponse] = Field(
        description="Aggregate all external views. Both 'external_views' and 'appbuilder_menu_items' are included here."
    )
    react_apps: list[ReactAppResponse]
    appbuilder_views: list[AppBuilderViewResponse]
    appbuilder_menu_items: list[AppBuilderMenuItemResponse] = Field(
        deprecated="Kept for backward compatibility, use `external_views` instead.",
    )
    global_operator_extra_links: list[str]
    operator_extra_links: list[str]
    source: Annotated[str, BeforeValidator(coerce_to_string)]
    listeners: list[str]
    timetables: list[str]

    @field_validator("source", mode="before")
    @classmethod
    def convert_source(cls, data: Any) -> Any:
        if isinstance(data, AirflowPluginSource):
            return str(data)
        return data

    @model_validator(mode="before")
    @classmethod
    def convert_external_views(cls, data: Any) -> Any:
        data["external_views"] = [*data["external_views"], *data["appbuilder_menu_items"]]
        return data


class PluginCollectionResponse(BaseModel):
    """Plugin Collection serializer."""

    plugins: list[PluginResponse]
    total_entries: int


class PluginImportErrorResponse(BaseModel):
    """Plugin Import Error serializer for responses."""

    source: str
    error: str


class PluginImportErrorCollectionResponse(BaseModel):
    """Plugin Import Error Collection serializer."""

    import_errors: list[PluginImportErrorResponse]
    total_entries: int
