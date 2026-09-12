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
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
)

from fastapi import Depends, Query
from pydantic import BaseModel
from sqlalchemy import and_, not_, or_, select as sql_select

from airflow.api_fastapi.common.parameters.base import BaseParam, T
from airflow.api_fastapi.common.parameters.filter import FilterParam, filter_param_factory
from airflow.api_fastapi.common.parameters.search import (
    _LIKE_ESCAPE_CHAR,
    _escape_like_pattern,
    _PrefixSearchParam,
    _SearchParam,
    prefix_search_param_factory,
    search_param_factory,
)
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.models.dag import DagModel, DagTag
from airflow.models.dag_favorite import DagFavorite
from airflow.models.dagbundle import DagBundleModel
from airflow.typing_compat import Self

if TYPE_CHECKING:
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql import ColumnElement, Select


class _FavoriteFilter(BaseParam[bool]):
    """Filter Dags by favorite status."""

    def __init__(self, user_id: str, value: T | None = None, skip_none: bool = True) -> None:
        super().__init__(skip_none=skip_none)
        self.user_id = user_id

    def to_orm(self, select_stmt: Select) -> Select:
        if self.value is None and self.skip_none:
            return select_stmt

        if self.value:
            select_stmt = select_stmt.join(DagFavorite, DagFavorite.dag_id == DagModel.dag_id).where(
                DagFavorite.user_id == self.user_id
            )
        else:
            select_stmt = select_stmt.where(
                not_(
                    sql_select(DagFavorite)
                    .where(and_(DagFavorite.dag_id == DagModel.dag_id, DagFavorite.user_id == self.user_id))
                    .exists()
                )
            )

        return select_stmt

    @classmethod
    def depends(cls, user: GetUserDep, is_favorite: bool | None = Query(None)) -> _FavoriteFilter:
        instance = cls(user_id=str(user.get_id())).set_value(is_favorite)
        return instance


class _ExcludeStaleFilter(BaseParam[bool]):
    """Filter on is_stale."""

    def to_orm(self, select: Select) -> Select:
        if self.value and self.skip_none:
            return select.where(DagModel.is_stale != self.value)
        return select

    @classmethod
    def depends(cls, exclude_stale: bool = True) -> _ExcludeStaleFilter:
        return cls().set_value(exclude_stale)


class _TagFilterModel(BaseModel):
    """Tag Filter Model with a match mode parameter."""

    tags: list[str]
    tags_match_mode: Literal["any", "all"] | None


class _TagsFilter(BaseParam[_TagFilterModel]):
    """Filter on tags."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value or not self.value.tags:
            return select

        conditions = [DagModel.tags.any(DagTag.name == tag) for tag in self.value.tags]
        operator = or_ if not self.value.tags_match_mode or self.value.tags_match_mode == "any" else and_
        return select.where(operator(*conditions))

    @classmethod
    def depends(
        cls,
        tags: list[str] = Query(default_factory=list),
        tags_match_mode: Literal["any", "all"] | None = None,
    ) -> _TagsFilter:
        return cls().set_value(_TagFilterModel(tags=tags, tags_match_mode=tags_match_mode))


class _OwnersFilter(BaseParam[list[str]]):
    """Filter on owners."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value:
            return select

        conditions = [
            DagModel.owners.ilike(f"%{_escape_like_pattern(owner)}%", escape=_LIKE_ESCAPE_CHAR)
            for owner in self.value
        ]
        return select.where(or_(*conditions))

    @classmethod
    def depends(cls, owners: list[str] = Query(default_factory=list)) -> _OwnersFilter:
        return cls().set_value(owners)


class _TeamsFilter(BaseParam[list[str]]):
    """Filter Dags by team name (via bundle association)."""

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value:
            return select

        from airflow.models.team import Team

        return select.where(
            DagModel.bundle_name.in_(
                sql_select(DagBundleModel.name).join(DagBundleModel.teams).where(Team.name.in_(self.value))
            )
        )

    @classmethod
    def depends(cls, teams: list[str] = Query(default_factory=list)) -> _TeamsFilter:
        return cls().set_value(teams)


class _DagIdTeamsFilter(BaseParam[list[str]]):
    """Filter rows by team name through their ``dag_id`` (via bundle association)."""

    def __init__(
        self,
        dag_id_attribute: ColumnElement | InstrumentedAttribute,
        value: list[str] | None = None,
        skip_none: bool = True,
    ) -> None:
        super().__init__(value, skip_none)
        self.dag_id_attribute = dag_id_attribute

    def to_orm(self, select: Select) -> Select:
        if self.skip_none is False:
            raise ValueError(f"Cannot set 'skip_none' to False on a {type(self)}")

        if not self.value:
            return select

        from airflow.models.team import Team

        return select.where(
            self.dag_id_attribute.in_(
                sql_select(DagModel.dag_id)
                .join(DagBundleModel, DagModel.bundle_name == DagBundleModel.name)
                .join(DagBundleModel.teams)
                .where(Team.name.in_(self.value))
            )
        )

    @classmethod
    def depends(cls, *args: Any, **kwargs: Any) -> Self:
        raise NotImplementedError("Use teams_filter_factory instead, depends is not implemented.")


def teams_filter_factory(
    dag_id_attribute: ColumnElement | InstrumentedAttribute,
) -> Callable[[list[str]], _DagIdTeamsFilter]:
    """Build a ``teams`` filter that scopes rows by team through the given ``dag_id`` column."""

    def depends_teams_filter(teams: list[str] = Query(default_factory=list)) -> _DagIdTeamsFilter:
        return _DagIdTeamsFilter(dag_id_attribute).set_value(teams)

    return depends_teams_filter


QueryPausedFilter = Annotated[
    FilterParam[bool | None],
    Depends(filter_param_factory(DagModel.is_paused, bool | None, filter_name="paused")),
]

QueryHasImportErrorsFilter = Annotated[
    FilterParam[bool | None],
    Depends(
        filter_param_factory(
            DagModel.has_import_errors,
            bool | None,
            filter_name="has_import_errors",
            description="Filter Dags by having import errors. Only Dags that have been successfully loaded before will be returned.",
        )
    ),
]

QueryFavoriteFilter = Annotated[_FavoriteFilter, Depends(_FavoriteFilter.depends)]

QueryExcludeStaleFilter = Annotated[_ExcludeStaleFilter, Depends(_ExcludeStaleFilter.depends)]

QueryDagIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_id, "dag_id_pattern"))
]

QueryDagIdPrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(DagModel.dag_id, "dag_id_prefix_pattern"))
]

QueryDagDisplayNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_display_name, "dag_display_name_pattern"))
]

QueryDagDisplayNamePrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(DagModel.dag_display_name, "dag_display_name_prefix_pattern")),
]

QueryTimetableTypePrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(DagModel.timetable_type, "timetable_type_prefix_pattern")),
]

QueryBundleNameFilter = Annotated[
    FilterParam[str | None],
    Depends(filter_param_factory(DagModel.bundle_name, str | None, filter_name="bundle_name")),
]

QueryBundleVersionFilter = Annotated[
    FilterParam[str | None],
    Depends(filter_param_factory(DagModel.bundle_version, str | None, filter_name="bundle_version")),
]

QueryDagIdPatternSearchWithNone = Annotated[
    _SearchParam, Depends(search_param_factory(DagModel.dag_id, "dag_id_pattern", False))
]

QueryDagIdPrefixPatternSearchWithNone = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(DagModel.dag_id, "dag_id_prefix_pattern", False)),
]

QueryTagsFilter = Annotated[_TagsFilter, Depends(_TagsFilter.depends)]

QueryOwnersFilter = Annotated[_OwnersFilter, Depends(_OwnersFilter.depends)]

QueryTeamsFilter = Annotated[_TeamsFilter, Depends(_TeamsFilter.depends)]


# DagTags
QueryDagTagPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagTag.name, "tag_name_pattern"))
]

QueryDagTagPrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(DagTag.name, "tag_name_prefix_pattern"))
]
