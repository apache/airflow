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

import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from typing import (
    TYPE_CHECKING,
    Any,
)

from fastapi import HTTPException, Query, status
from sqlalchemy import and_, or_, true as sql_true

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters.base import BaseParam
from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.typing_compat import Self
from airflow.utils.sqlalchemy import apply_regex_query_timeout

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement, Select


class _PrefixPatternParam(BaseParam[str], ABC):
    """
    Shared prefix pattern: pipe ``|`` for OR, ``~`` → empty (match all), Unicode prefix range.

    .. note::
        Trailing non-alphanumeric characters in a search term are stripped before the range
        is computed. A range scan with a punctuation-terminated upper bound is unsafe under
        PostgreSQL's default locale-aware collation (``en_US.utf8`` sorts punctuation in a
        way that breaks the range), and additionally stopping the range at an alphanumeric
        character keeps the upper bound alphanumeric too, so the predicate stays usable by
        default btree indexes. A user who asks for prefix ``"test_"`` gets matches starting
        with ``"test"`` — a small over-match trade-off made explicit in the public
        ``*_prefix_pattern`` query-param description.
    """

    pipe_as_or: bool = True

    @staticmethod
    def _prefix_range_upper(term: str) -> str | None:
        """
        Compute the exclusive upper bound for a prefix range scan.

        Returns ``None`` if the term has no alphanumeric characters. Trailing non-alphanumeric
        characters are dropped before bumping the last character so the resulting upper bound
        is itself alphanumeric and behaves predictably under locale-aware collations. If
        incrementing would land outside the alphanumeric range (e.g. ``'z' → '{'``), we drop
        that character and retry.
        """
        while term and not term[-1].isalnum():
            term = term[:-1]
        if not term:
            return None
        last = ord(term[-1])
        if last >= 0x10FFFF:
            return _PrefixPatternParam._prefix_range_upper(term[:-1])
        bumped = chr(last + 1)
        if not bumped.isalnum():
            return _PrefixPatternParam._prefix_range_upper(term[:-1])
        return term[:-1] + bumped

    @staticmethod
    def _prefix_lower_bound(term: str) -> str:
        """Return the matching lower bound: strip trailing non-alphanumeric chars to pair with the upper."""
        while term and not term[-1].isalnum():
            term = term[:-1]
        return term

    @abstractmethod
    def _prefix_clause(self, term: str):
        """Return the SQL boolean for one prefix term (including empty string after ``~`` alias)."""

    def to_orm(self, select: Select) -> Select:
        # ``skip_none`` only gates the "no value" behavior for the callers that must keep
        # the filter slot present (e.g. ``QueryDagIdPrefixPatternSearchWithNone``); applying
        # a ``None`` value as a filter produces nonsense predicates, so always skip it here.
        if self.value is None:
            return select

        val_str = str(self.value)
        if self.pipe_as_or and "|" in val_str:
            search_terms = [term.strip() for term in val_str.split("|") if term.strip()]
            if search_terms:
                return select.where(or_(*(self._prefix_clause(term) for term in search_terms)))

        return select.where(self._prefix_clause(val_str))

    def transform_aliases(self, value: str | None) -> str | None:
        if value == "~":
            value = ""
        return value


def _build_pipe_clause(pipe_as_or: bool) -> str:
    """Build the per-parameter pipe note. OR is the documented default (see the API description), so only the literal exception is spelled out."""
    return "" if pipe_as_or else "Here `|` is matched literally, not as OR. "


_LIKE_ESCAPE_CHAR = "\\"


def _escape_like_pattern(value: str) -> str:
    r"""
    Escape SQL ``LIKE`` / ``ILIKE`` metacharacters in a user-supplied value.

    Use together with ``column.ilike(f"%{_escape_like_pattern(value)}%", escape="\\")`` on filter
    parameters that intend literal substring matching (so a user-supplied ``%`` or ``_`` does not
    widen the match beyond what the filter semantics promise). Search parameters that explicitly
    expose wildcard semantics (see :class:`_SearchParam`) must not call this — they want the
    metacharacters to pass through.
    """
    return (
        value.replace(_LIKE_ESCAPE_CHAR, _LIKE_ESCAPE_CHAR * 2)
        .replace("%", _LIKE_ESCAPE_CHAR + "%")
        .replace("_", _LIKE_ESCAPE_CHAR + "_")
    )


class _SearchParam(BaseParam[str]):
    """
    Substring search on a column using ``ILIKE '%term%'`` (case-insensitive).

    .. note::
        This full-match substring search most of the time prevents the database
        from using B-tree indexes on ``attribute``, which can be very slow on
        large tables. Prefer :class:`_PrefixSearchParam` (the ``*_prefix_pattern``
        query-param counterpart) when matching from the beginning of the value
        is acceptable.
    """

    def __init__(self, attribute: ColumnElement, skip_none: bool = True, pipe_as_or: bool = True) -> None:
        super().__init__(skip_none=skip_none)
        self.attribute: ColumnElement = attribute
        self.pipe_as_or = pipe_as_or

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        val_str = str(self.value)
        if self.pipe_as_or and "|" in val_str:
            search_terms = [term.strip() for term in val_str.split("|") if term.strip()]
            if search_terms:
                return select.where(or_(*(self.attribute.ilike(f"%{term}%") for term in search_terms)))

        return select.where(self.attribute.ilike(f"%{val_str}%"))

    def transform_aliases(self, value: str | None) -> str | None:
        if value == "~":
            value = "%"
        return value

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use search_param_factory instead , depends is not implemented.")


class _PrefixSearchParam(_PrefixPatternParam):
    """
    Prefix search on a column using range comparison (case-sensitive, index-friendly).

    Unlike :class:`_SearchParam`, wildcard characters are treated as literals and the query
    plan can use the column's default B-tree index for the range scan. Trailing
    non-alphanumeric characters in ``term`` are stripped first (see
    :class:`_PrefixPatternParam` for why).
    """

    def __init__(self, attribute: ColumnElement, skip_none: bool = True, pipe_as_or: bool = True) -> None:
        super().__init__(skip_none=skip_none)
        self.attribute: ColumnElement = attribute
        self.pipe_as_or = pipe_as_or

    def _prefix_clause(self, term: str):
        lower = self._prefix_lower_bound(term)
        if not lower:
            return self.attribute.is_not(None)
        upper = self._prefix_range_upper(term)
        if upper is None:
            return self.attribute >= lower
        return and_(self.attribute >= lower, self.attribute < upper)

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use prefix_search_param_factory instead, depends is not implemented.")


class _TaskDisplayNamePrefixPatternParam(_PrefixPatternParam):
    """
    Prefix filter equivalent to :attr:`TaskInstance.task_display_name`, rewritten for composite-index use.

    The hybrid expression ``coalesce(_task_display_property_value, task_id)`` cannot use those indexes;
    this implementation applies an equivalent ``OR`` of simpler range predicates instead. Trailing
    non-alphanumeric characters in ``term`` are stripped first (see :class:`_PrefixPatternParam`).
    """

    def _prefix_clause(self, term: str):
        lower = self._prefix_lower_bound(term)
        if not lower:
            return sql_true()
        upper = self._prefix_range_upper(term)
        if upper is None:
            return or_(
                and_(
                    TaskInstance._task_display_property_value.is_(None),
                    TaskInstance.task_id >= lower,
                ),
                and_(
                    TaskInstance._task_display_property_value.is_not(None),
                    TaskInstance._task_display_property_value >= lower,
                ),
            )
        return or_(
            and_(
                TaskInstance._task_display_property_value.is_(None),
                TaskInstance.task_id >= lower,
                TaskInstance.task_id < upper,
            ),
            and_(
                TaskInstance._task_display_property_value.is_not(None),
                TaskInstance._task_display_property_value >= lower,
                TaskInstance._task_display_property_value < upper,
            ),
        )

    @classmethod
    def depends(
        cls,
        task_display_name_prefix_pattern: str | None = Query(
            default=None,
            description=(
                "Case-sensitive prefix match on task display name (`_task_display_property_value` else "
                "`task_id`). Index-friendly alternative to `task_display_name_pattern`; on large databases "
                "combine with `dag_id_prefix_pattern` (or a specific Dag in the path) so composite indexes "
                'apply. See "Filtering with pattern parameters".'
            ),
        ),
    ) -> Self:
        param = cls()
        return param.set_value(param.transform_aliases(task_display_name_prefix_pattern))


def search_param_factory(
    attribute: ColumnElement,
    pattern_name: str,
    skip_none: bool = True,
    pipe_as_or: bool = True,
) -> Callable[[str | None], _SearchParam]:
    prefix_pattern_name = pattern_name.replace("_pattern", "_prefix_pattern")
    DESCRIPTION = (
        "Case-insensitive substring match (SQL `ILIKE`). "
        f"{_build_pipe_clause(pipe_as_or)}"
        f'Slower than `{prefix_pattern_name}` on large tables — see "Filtering with pattern parameters".'
    )

    def depends_search(
        value: str | None = Query(alias=pattern_name, default=None, description=DESCRIPTION),
    ) -> _SearchParam:
        search_parm = _SearchParam(attribute, skip_none, pipe_as_or=pipe_as_or)
        value = search_parm.transform_aliases(value)
        return search_parm.set_value(value)

    return depends_search


class _RegexParam(BaseParam[str]):
    """
    Filter using database-level regex matching (regexp_match).

    The pattern is handed to the database's own regex engine (via SQLAlchemy's
    ``regexp_match``), so this filter is gated behind the ``[api] regexp_query_timeout``
    setting to contain the ReDoS attack surface: it cannot be instantiated with a value
    unless a positive timeout is configured (which both enables the feature and bounds it).

    Use :func:`regex_param_factory` to build the FastAPI dependency for this filter. That
    dependency also applies :func:`airflow.utils.sqlalchemy.apply_regex_query_timeout` to the
    request's session, so the query runtime is bounded automatically and callers never need to
    remember to do it in the view.
    """

    def __init__(self, attribute: ColumnElement, value: str | None = None, skip_none: bool = True) -> None:
        super().__init__(value=value, skip_none=skip_none)
        self.attribute: ColumnElement = attribute
        if value is not None and conf.getfloat("api", "regexp_query_timeout") <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Regexp query filters are disabled. "
                "Set [api] regexp_query_timeout to a positive number of seconds to enable them.",
            )

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select
        return select.where(self.attribute.regexp_match(self.value))

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use regex_param_factory instead, depends is not implemented.")


_DEFAULT_REGEX_DESCRIPTION = "Filter results by matching this regular expression against the field value."


def regex_param_factory(
    attribute: ColumnElement,
    pattern_name: str,
    skip_none: bool = True,
    description: str = _DEFAULT_REGEX_DESCRIPTION,
) -> Callable[..., Generator[_RegexParam, None, None]]:
    def depends_regex(
        session: SessionDep,
        value: str | None = Query(alias=pattern_name, default=None, description=description),
    ) -> Generator[_RegexParam, None, None]:
        if value is not None:
            try:
                re.compile(value)
            except re.error as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid regular expression: {e}",
                )
        # ``__init__`` rejects the request (400) when a pattern is supplied while the feature is off.
        param = _RegexParam(attribute, value, skip_none)
        if value is None:
            yield param
            return
        # Bound the query runtime for the whole request so a view can never forget to do it; the
        # previous timeout is restored on teardown (after the response) before the session closes.
        with apply_regex_query_timeout(session):
            yield param

    return depends_regex


def prefix_search_param_factory(
    attribute: ColumnElement,
    prefix_pattern_name: str,
    skip_none: bool = True,
    pipe_as_or: bool = True,
) -> Callable[[str | None], _PrefixSearchParam]:
    """
    Build a FastAPI ``Depends`` returning a :class:`_PrefixSearchParam` for prefix matching.

    Prefer this over :func:`search_param_factory` for performance: prefix matching uses a
    B-tree index range scan, while substring matching requires a full table scan.
    """
    DESCRIPTION = (
        "Case-sensitive, index-friendly prefix match. "
        f"{_build_pipe_clause(pipe_as_or)}"
        'See "Filtering with pattern parameters".'
    )

    def depends_prefix_search(
        value: str | None = Query(alias=prefix_pattern_name, default=None, description=DESCRIPTION),
    ) -> _PrefixSearchParam:
        search_parm = _PrefixSearchParam(attribute, skip_none, pipe_as_or=pipe_as_or)
        value = search_parm.transform_aliases(value)
        return search_parm.set_value(value)

    return depends_prefix_search
