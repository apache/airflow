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

import attrs


def _validate_teams(instance, attribute, value):
    if value is None:
        return value
    for entry in value:
        if not isinstance(entry, str) or not entry or entry.isspace():
            raise ValueError(f"Each entry in {attribute.name} must be a non-empty string")
    return value


@attrs.define
class AssetAccessControl:
    """Access control configuration for an Asset."""

    producer_teams: list[str] = attrs.field(
        factory=list,
        validator=[_validate_teams],
    )
    consumer_teams: list[str] | None = attrs.field(
        default=None,
        validator=[_validate_teams],
    )
    allow_global: bool = attrs.field(default=True, validator=[attrs.validators.instance_of(bool)])
