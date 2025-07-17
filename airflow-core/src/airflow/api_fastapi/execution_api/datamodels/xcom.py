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

import sys
from typing import Any

from pydantic import JsonValue, RootModel

from airflow.api_fastapi.core_api.base import BaseModel

if sys.version_info < (3, 12):
    # zmievsa/cadwyn#262
    # Setting this to "Any" doesn't have any impact on the API as it has to be parsed as valid JSON regardless
    JsonValue = Any  # type: ignore [misc]


class XComResponse(BaseModel):
    """XCom schema for responses with fields that are needed for Runtime."""

    key: str
    value: JsonValue
    """The returned XCom value in a JSON-compatible format."""


class XComSequenceIndexResponse(RootModel):
    """XCom schema with minimal structure for index-based access."""

    root: JsonValue


class XComSequenceSliceResponse(RootModel):
    """XCom schema with minimal structure for slice-based access."""

    root: list[JsonValue]
