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

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class FastAPIApp(BaseModel):
    """Class used to define a FastAPI app."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    app: Any
    url_prefix: str
    name: str


class FastAPIRootMiddleware(BaseModel):
    """Class used to define a FastAPI root middleware."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    middleware: Any
    name: str


class ExternalView(BaseModel):
    """Class used to define an external view."""

    model_config = ConfigDict(extra="allow")

    name: str
    href: str
    icon: str | None = None
    url_route: str | None = None
    category: str | None = None
    destination: Literal["nav", "dag", "dag_run", "task", "task_instance"] = "nav"


class ReactApp(BaseModel):
    """Class used to define a React app."""

    model_config = ConfigDict(extra="allow")

    name: str
    bundle_url: str
    icon: str | None = None
    url_route: str | None = None
    category: str | None = None
    destination: Literal["nav", "dag", "dag_run", "task", "task_instance"] = "nav"


class AppBuilderView(BaseModel):
    """Class used to define an AppBuilder view."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    view: Any
    name: str | None = None
    category: str | None = None
    label: str | None = None


class AppBuilderMenuItem(BaseModel):
    """Class used to define an AppBuilder menu item."""

    model_config = ConfigDict(extra="allow")

    name: str
    href: str
    category: str | None = None
    label: str | None = None
