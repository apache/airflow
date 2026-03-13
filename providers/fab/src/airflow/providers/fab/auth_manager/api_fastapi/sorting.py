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

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException, status
from sqlalchemy import asc, desc

if TYPE_CHECKING:
    from sqlalchemy.orm import InstrumentedAttribute
    from sqlalchemy.sql.elements import ColumnElement


def build_ordering(
    order_by: str, *, allowed: Mapping[str, ColumnElement[Any]] | Mapping[str, InstrumentedAttribute[Any]]
) -> ColumnElement[Any]:
    """
    Build an SQLAlchemy ORDER BY expression from the `order_by` parameter.

    :param order_by: Public field name, optionally prefixed with "-" for descending.
    :param allowed: Map of public field to SQLAlchemy column/expression.
    """
    is_desc = order_by.startswith("-")
    key = order_by.lstrip("-")

    if key not in allowed:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Ordering with '{order_by}' is disallowed or the attribute does not exist on the model",
        )

    col = allowed[key]
    return desc(col) if is_desc else asc(col)
