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

from typing import (
    Any,
)

import attrs

from airflow.sdk.definitions.asset import Asset, AssetAlias, _sanitize_uri

__all__ = ["Metadata"]


def extract_event_key(value: str | Asset | AssetAlias) -> str:
    """
    Extract the key of an inlet or an outlet event.

    If the input value is a string, it is treated as a URI and sanitized. If the
    input is a :class:`Asset`, the URI it contains is considered sanitized and
    returned directly. If the input is a :class:`AssetAlias`, the name it contains
    will be returned directly.

    :meta private:
    """
    if isinstance(value, AssetAlias):
        return value.name

    if isinstance(value, Asset):
        return value.uri
    return _sanitize_uri(str(value))


@attrs.define(init=False)
class Metadata:
    """Metadata to attach to an AssetEvent."""

    uri: str
    extra: dict[str, Any]
    alias_name: str | None = None

    def __init__(
        self,
        target: str | Asset,
        extra: dict[str, Any],
        alias: AssetAlias | str | None = None,
    ) -> None:
        self.uri = extract_event_key(target)
        self.extra = extra
        if isinstance(alias, AssetAlias):
            self.alias_name = alias.name
        else:
            self.alias_name = alias
