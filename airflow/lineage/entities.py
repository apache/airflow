#
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
"""Defines base entities used for providing lineage information."""

from __future__ import annotations

from typing import Any, ClassVar

import attr


@attr.s(auto_attribs=True)
class File:
    """File entity. Refers to a file."""

    template_fields: ClassVar = ("url",)

    url: str = attr.ib()
    type_hint: str | None = None


@attr.s(auto_attribs=True, kw_only=True)
class User:
    """User entity. Identifies a user."""

    email: str = attr.ib()
    first_name: str | None = None
    last_name: str | None = None

    template_fields: ClassVar = ("email", "first_name", "last_name")


@attr.s(auto_attribs=True, kw_only=True)
class Tag:
    """Tag or classification entity."""

    tag_name: str = attr.ib()

    template_fields: ClassVar = ("tag_name",)


@attr.s(auto_attribs=True, kw_only=True)
class Column:
    """Column of a Table."""

    name: str = attr.ib()
    description: str | None = None
    data_type: str = attr.ib()
    tags: list[Tag] = []

    template_fields: ClassVar = ("name", "description", "data_type", "tags")


# this is a temporary hack to satisfy mypy. Once
# https://github.com/python/mypy/issues/6136 is resolved, use
# `attr.converters.default_if_none(default=False)`


def default_if_none(arg: bool | None) -> bool:
    """Get default value when None."""
    return arg or False


@attr.s(auto_attribs=True, kw_only=True)
class Table:
    """Table entity."""

    database: str = attr.ib()
    cluster: str = attr.ib()
    name: str = attr.ib()
    tags: list[Tag] = []
    description: str | None = None
    columns: list[Column] = []
    owners: list[User] = []
    extra: dict[str, Any] = {}
    type_hint: str | None = None

    template_fields: ClassVar = (
        "database",
        "cluster",
        "name",
        "tags",
        "description",
        "columns",
        "owners",
        "extra",
    )
