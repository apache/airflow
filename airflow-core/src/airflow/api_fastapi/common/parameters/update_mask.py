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

from collections.abc import Callable

from fastapi import HTTPException, Query, status
from pydantic import BaseModel


def validate_update_mask(patch_body_type: type[BaseModel], update_mask: list[str] | None) -> list[str] | None:
    """
    Reject ``update_mask`` entries that name no field of ``patch_body_type``.

    Every caller narrows the patch down with ``set(update_mask)``, which drops an entry matching
    nothing -- so a typo used to make the whole request a no-op that still answered ``200``.
    Aliases count as known names because that is what a caller sends in the body and reads back in
    the response; which of the two a given endpoint acts on is left untouched here. Surrounding
    whitespace is stripped so a stray space selects the field instead of silently selecting nothing.

    Routes take the mask from the query string through :func:`update_mask_param_factory`; this is
    for the bulk endpoints, which carry it in the request body instead.

    :param patch_body_type: the request body type the mask selects fields from.
    :param update_mask: the requested field names, or ``None``.
    :return: the mask with whitespace stripped, or ``None``.
    :raises HTTPException: 400 if any entry names no field.
    """
    if not update_mask:
        return update_mask

    fields = patch_body_type.model_fields
    known = set(fields) | {
        alias
        for field in fields.values()
        for alias in (field.alias, field.serialization_alias, field.validation_alias)
        if isinstance(alias, str)
    }

    stripped = [entry.strip() for entry in update_mask]
    if unknown := {entry for entry in stripped if entry not in known}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unknown field(s) in update_mask: {', '.join(sorted(map(repr, unknown)))}. "
                f"Valid fields are: {', '.join(sorted(known))}."
            ),
        )
    return stripped


def update_mask_param_factory(
    patch_body_type: type[BaseModel],
) -> Callable[[list[str] | None], list[str] | None]:
    """Build the validated ``update_mask`` query parameter for a PATCH endpoint on ``patch_body_type``."""

    def depends_update_mask(update_mask: list[str] | None = Query(None)) -> list[str] | None:
        return validate_update_mask(patch_body_type, update_mask)

    return depends_update_mask
