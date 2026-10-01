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

from collections.abc import Callable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
)

from fastapi import HTTPException, Query
from sqlalchemy import Column
from sqlalchemy.inspection import inspect

from airflow.api_fastapi.common.parameters.base import BaseParam
from airflow.models import Base
from airflow.typing_compat import Self

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement, Select


class SortParam(BaseParam[list[str]]):
    """Order result by the attribute."""

    MAX_SORT_PARAMS = 10

    def __init__(
        self,
        allowed_attrs: list[str],
        model: Base,
        to_replace: dict[str, str | Column | list[Column]] | None = None,
    ) -> None:
        super().__init__()
        self.allowed_attrs = allowed_attrs
        self.model = model
        self.to_replace = to_replace
        self._cached_resolution: list[tuple[str, ColumnElement, bool]] | None = None

    def set_value(self, value: list[str] | None) -> Self:
        self._cached_resolution = None
        return super().set_value(value)

    def _resolve(self) -> list[tuple[str, ColumnElement, bool]]:
        """Resolve sort columns as (attr_name, column, is_descending) tuples. Cached after first call."""
        if self._cached_resolution is not None:
            return self._cached_resolution

        if self.value is None:
            self.value = [self.get_primary_key_string()]

        order_by_values = self.value
        if len(order_by_values) > self.MAX_SORT_PARAMS:
            raise HTTPException(
                400,
                f"Ordering with more than {self.MAX_SORT_PARAMS} parameters is not allowed. Provided: {order_by_values}",
            )

        resolved: list[tuple[str, ColumnElement, bool]] = []
        for order_by_value in order_by_values:
            lstriped_orderby = order_by_value.lstrip("-")
            # Store the user-facing name in the resolved tuple. ``row_value`` resolves
            # it back to the actual row accessor via ``to_replace`` when reading values
            # for cursor encoding.
            attr_name = lstriped_orderby
            column: Column | None = None
            if self.to_replace:
                replacement = self.to_replace.get(lstriped_orderby, lstriped_orderby)
                if isinstance(replacement, str):
                    lstriped_orderby = replacement
                elif isinstance(replacement, list):
                    # Compound sort: expand the list into multiple sort entries.
                    # Each column's ORM key becomes its attr_name so that
                    # row_value() can read the corresponding attribute via
                    # getattr(row, attr_name) without further to_replace lookups.
                    is_desc = order_by_value.startswith("-")
                    for col in replacement:
                        col_attr_name = col.key
                        resolved.append((col_attr_name, col, is_desc))
                    continue
                else:
                    column = replacement

            if (self.allowed_attrs and lstriped_orderby not in self.allowed_attrs) and column is None:
                raise HTTPException(
                    400,
                    f"Ordering with '{lstriped_orderby}' is disallowed or "
                    f"the attribute does not exist on the model",
                )
            if column is None:
                column = getattr(self.model, lstriped_orderby)

            resolved.append((attr_name, column, order_by_value.startswith("-")))

        primary_key_column = self.get_primary_key_column()
        pk_name = self.get_primary_key_string()
        resolved_column_keys = {getattr(col, "key", None) for _, col, _ in resolved}
        if pk_name not in resolved_column_keys:
            pk_desc = bool(order_by_values and order_by_values[0].startswith("-"))
            resolved.append((pk_name, primary_key_column, pk_desc))

        self._cached_resolution = resolved
        return self._cached_resolution

    def to_orm(self, select: Select, *, reversed: bool = False) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        resolved = self._resolve()
        if reversed:
            columns = [col.asc() if is_desc else col.desc() for _, col, is_desc in resolved]
        else:
            columns = [col.desc() if is_desc else col.asc() for _, col, is_desc in resolved]
        return select.order_by(None).order_by(*columns)

    def get_resolved_columns(self) -> list[tuple[str, ColumnElement, bool]]:
        """Return resolved sort columns as (attr_name, column_element, is_descending) tuples."""
        return self._resolve()

    def row_value(self, row: Any, name: str) -> Any:
        """
        Extract the sort-key value for ``name`` from a result row.

        Resolves the accessor through ``to_replace`` for string aliases
        (e.g. ``{"dag_run_id": "run_id"}``). For column-form mappings
        (e.g. ``{"run_after": DagRun.run_after}``), resolves through the
        primary model's attribute so association proxies can still be used
        for cursor values. Raises ``NotImplementedError`` when the model
        exposes no such attribute rather than emitting a ``None`` cursor token.
        """
        if self.to_replace:
            replacement = self.to_replace.get(name)
            if isinstance(replacement, str):
                return getattr(row, replacement, None)
            if replacement is not None and not isinstance(replacement, list):
                # Column-form mapping resolves through the primary model's attribute,
                # often an association proxy onto the joined entity
                # (``TaskInstance.run_after`` -> ``dag_run.run_after``). Fail loudly if the
                # model exposes no such attribute, rather than emitting a ``None`` cursor token.
                try:
                    return getattr(row, name)
                except AttributeError:
                    raise NotImplementedError(
                        f"Cursor pagination cannot resolve column-form ``to_replace`` for "
                        f"``{name}``: the primary model exposes no such attribute. Add an "
                        f"association proxy, use a string alias, or sort by a primary-model column."
                    )
            # List-form replacements are expanded in _resolve() into individual entries
            # each using the column's own ORM key as attr_name, so ``name`` at this point
            # is already a concrete model attribute (e.g. ``_rendered_map_index`` or
            # ``map_index``) — fall through to the getattr below.
        return getattr(row, name, None)

    def get_primary_key_column(self) -> Column:
        """Get the primary key column of the model of SortParam object."""
        return inspect(self.model).primary_key[0]

    def get_primary_key_string(self) -> str:
        """Get the primary key string of the model of SortParam object."""
        return self.get_primary_key_column().name

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use dynamic_depends, depends not implemented.")

    def dynamic_depends(self, default: str | Sequence[str] | None = None) -> Callable:
        # Include to_replace keys that are not already in allowed_attrs to avoid
        # duplicate entries in the spec description.
        allowed_set = set(self.allowed_attrs)
        to_replace_attrs = [k for k in self.to_replace if k not in allowed_set] if self.to_replace else []

        all_attrs = self.allowed_attrs + to_replace_attrs

        if default is None:
            default_list = [self.get_primary_key_string()]
        elif isinstance(default, str):
            default_list = [default]
        else:
            default_list = list(default)

        _order_by_query = Query(
            default=default_list,
            description=f"Attributes to order by, multi criteria sort is supported. Prefix with `-` for descending order. "
            f"Supported attributes: `{', '.join(all_attrs) if all_attrs else self.get_primary_key_string()}`",
        )

        def inner(order_by: list[str] = _order_by_query) -> SortParam:
            return SortParam(self.allowed_attrs, self.model, self.to_replace).set_value(order_by)

        return inner
