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
"""Helpers for reporting pydantic validation errors as readable CLI messages."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import BaseModel, ValidationError


def split_validation_errors(
    datamodel: type[BaseModel], error: ValidationError, display_name: Callable[[str], str]
) -> tuple[list[str], list[str]]:
    """
    Split a ``ValidationError`` into missing required fields and ``<name>: <msg>`` invalid values.

    Callers pass absent inputs as ``None``, so ``None`` on a required field means missing (or an explicit null).
    """
    missing: list[str] = []
    invalid: list[str] = []
    for err in error.errors():
        field = str(err["loc"][0]) if err["loc"] else ""
        field_info = datamodel.model_fields.get(field)
        name = display_name(field) if field else "input"
        if field_info is not None and field_info.is_required() and err.get("input") is None:
            if name not in missing:
                missing.append(name)
        else:
            invalid.append(f"{name}: {err['msg']}")
    return missing, invalid
