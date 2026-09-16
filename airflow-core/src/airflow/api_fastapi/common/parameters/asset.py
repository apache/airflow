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
    TYPE_CHECKING,
    Annotated,
)

from fastapi import Depends, Query
from sqlalchemy import or_, select as sql_select

from airflow.api_fastapi.common.parameters.base import BaseParam
from airflow.api_fastapi.common.parameters.filter import (
    FilterOptionEnum,
    FilterParam,
    _JsonKVFilter,
    filter_param_factory,
    json_kv_filter_factory,
)
from airflow.api_fastapi.common.parameters.search import (
    _LIKE_ESCAPE_CHAR,
    _escape_like_pattern,
    _PrefixSearchParam,
    _RegexParam,
    _SearchParam,
    prefix_search_param_factory,
    regex_param_factory,
    search_param_factory,
)
from airflow.models.asset import (
    AssetAliasModel,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    DagScheduleAssetReference,
    TaskInletAssetReference,
    TaskOutletAssetReference,
    association_table,
)
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun

if TYPE_CHECKING:
    from sqlalchemy.sql import Select


class _DagIdAssetReferenceFilter(BaseParam[list[str]]):
    """Search on dag_id."""

    def __init__(self, skip_none: bool = True) -> None:
        super().__init__(skip_none=skip_none)

    @classmethod
    def depends(cls, dag_ids: list[str] = Query(None)) -> _DagIdAssetReferenceFilter:
        # needed to handle cases where dag_ids=a1,b1
        if dag_ids and len(dag_ids) == 1 and "," in dag_ids[0]:
            dag_ids = dag_ids[0].split(",")
        return cls().set_value(dag_ids)

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        # At this point, self.value is either a list[str] or None -> coerce falsy None to an empty list
        dag_ids = self.value or []
        return select.where(
            (AssetModel.scheduled_dags.any(DagScheduleAssetReference.dag_id.in_(dag_ids)))
            | (AssetModel.producing_tasks.any(TaskOutletAssetReference.dag_id.in_(dag_ids)))
            | (AssetModel.consuming_tasks.any(TaskInletAssetReference.dag_id.in_(dag_ids)))
        )


class _HasAssetScheduleFilter(BaseParam[bool]):
    """Filter Dags that have asset-based scheduling."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        asset_ref_subquery = sql_select(DagScheduleAssetReference.dag_id).distinct()

        if self.value:
            # Filter Dags that have asset-based scheduling
            return select.where(DagModel.dag_id.in_(asset_ref_subquery))

        # Filter Dags that do NOT have asset-based scheduling
        return select.where(DagModel.dag_id.notin_(asset_ref_subquery))

    @classmethod
    def depends(
        cls,
        has_asset_schedule: bool | None = Query(None, description="Filter Dags with asset-based scheduling"),
    ) -> _HasAssetScheduleFilter:
        return cls().set_value(has_asset_schedule)


class _AssetDependencyFilter(BaseParam[str]):
    """Filter Dags by specific asset dependencies."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None:
            return select

        escaped = _escape_like_pattern(self.value)
        asset_dag_subquery = (
            sql_select(DagScheduleAssetReference.dag_id)
            .join(AssetModel, DagScheduleAssetReference.asset_id == AssetModel.id)
            .where(
                or_(
                    AssetModel.name.ilike(f"%{escaped}%", escape=_LIKE_ESCAPE_CHAR),
                    AssetModel.uri.ilike(f"%{escaped}%", escape=_LIKE_ESCAPE_CHAR),
                )
            )
            .distinct()
        )

        return select.where(DagModel.dag_id.in_(asset_dag_subquery))

    @classmethod
    def depends(
        cls,
        asset_dependency: str | None = Query(
            None, description="Filter Dags by asset dependency (name or URI)"
        ),
    ) -> _AssetDependencyFilter:
        return cls().set_value(asset_dependency)


QueryHasAssetScheduleFilter = Annotated[_HasAssetScheduleFilter, Depends(_HasAssetScheduleFilter.depends)]

QueryAssetDependencyFilter = Annotated[_AssetDependencyFilter, Depends(_AssetDependencyFilter.depends)]


class _ConsumingAssetFilter(BaseParam[str | None]):
    """Filter Dag runs by consuming asset (name or URI)."""

    def to_orm(self, select: Select) -> Select:
        if not self.value:
            return select

        escaped = _escape_like_pattern(self.value)
        event_subquery = (
            sql_select(AssetEvent.id)
            .join(AssetModel, AssetEvent.asset_id == AssetModel.id)
            .where(
                or_(
                    AssetModel.name.ilike(f"%{escaped}%", escape=_LIKE_ESCAPE_CHAR),
                    AssetModel.uri.ilike(f"%{escaped}%", escape=_LIKE_ESCAPE_CHAR),
                )
            )
            .distinct()
        )

        dagrun_subquery = (
            sql_select(association_table.c.dag_run_id)
            .where(association_table.c.event_id.in_(event_subquery))
            .distinct()
        )

        return select.where(DagRun.id.in_(dagrun_subquery))

    @classmethod
    def depends(
        cls,
        consuming_asset_pattern: str | None = Query(
            None,
            description=(
                "Case-insensitive substring match against the consuming asset name or URI. "
                "Unlike the wildcard `*_pattern` parameters, `%` and `_` are matched literally, "
                "`|` is not an OR separator, and `~` does not match everything."
            ),
        ),
    ) -> _ConsumingAssetFilter:
        return cls().set_value(consuming_asset_pattern)


QueryConsumingAssetPatternSearch = Annotated[_ConsumingAssetFilter, Depends(_ConsumingAssetFilter.depends)]


QueryAssetNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(AssetModel.name, "name_pattern"))
]

QueryAssetNamePrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(AssetModel.name, "name_prefix_pattern"))
]

QueryUriPatternSearch = Annotated[_SearchParam, Depends(search_param_factory(AssetModel.uri, "uri_pattern"))]

QueryUriPrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(AssetModel.uri, "uri_prefix_pattern"))
]

QueryUriExactMatch = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            AssetModel.uri,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            filter_name="uri",
            default_factory=list,
            description=(
                "Exact-match filter on the full asset URI. Compiles to an indexed equality "
                "comparison (``uri = ...``). Repeat the parameter (``?uri=a&uri=b``) to match "
                "multiple assets."
            ),
        )
    ),
]

QueryAssetGroupPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(AssetModel.group, "group_pattern"))
]

QueryAssetGroupPrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(AssetModel.group, "group_prefix_pattern"))
]

QueryAssetAliasNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(AssetAliasModel.name, "name_pattern"))
]

QueryAssetAliasNamePrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(AssetAliasModel.name, "name_prefix_pattern"))
]

QueryAssetEventPartitionKeyFilter = Annotated[
    FilterParam[str | None],
    Depends(filter_param_factory(AssetEvent.partition_key, str | None, filter_name="partition_key")),
]

QueryAssetEventPartitionKeyRegex = Annotated[
    _RegexParam,
    # ``function`` scope so the dependency can depend on the (function-scoped) session it bounds.
    Depends(regex_param_factory(AssetEvent.partition_key, "partition_key_regexp_pattern"), scope="function"),
]

QueryAssetDagIdPatternSearch = Annotated[
    _DagIdAssetReferenceFilter, Depends(_DagIdAssetReferenceFilter.depends)
]

QueryAssetEventExtraFilter = Annotated[_JsonKVFilter, Depends(json_kv_filter_factory(AssetEvent.extra))]

QueryPartitionedDagRunHasCreatedDagRunIdFilter = Annotated[
    FilterParam[bool | None],
    Depends(
        filter_param_factory(
            AssetPartitionDagRun.created_dag_run_id,
            bool | None,
            FilterOptionEnum.IS_NONE,
            filter_name="has_created_dag_run_id",
            transform_callable=lambda v: not v if v is not None else None,
        )
    ),
]

QueryPartitionedDagRunDagIdFilter = Annotated[
    FilterParam[str | None],
    Depends(
        filter_param_factory(
            AssetPartitionDagRun.target_dag_id,
            str | None,
            filter_name="dag_id",
        )
    ),
]
