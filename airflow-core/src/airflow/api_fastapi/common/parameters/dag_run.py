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

from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Annotated,
)

from fastapi import Depends, HTTPException, Query
from sqlalchemy import select as sql_select
from sqlalchemy.orm import aliased

from airflow.api_fastapi.common.parameters.base import BaseParam
from airflow.api_fastapi.common.parameters.filter import FilterOptionEnum, FilterParam, filter_param_factory
from airflow.api_fastapi.common.parameters.search import (
    _PrefixSearchParam,
    _SearchParam,
    prefix_search_param_factory,
    search_param_factory,
)
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from sqlalchemy.sql import Select


class _AnyDagRunStateFilter(BaseParam[DagRunState | None]):
    """Filter Dags that have any DagRun in the given state, not only the latest one."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        # Alias DagRun so this EXISTS subquery cannot auto-correlate to a DagRun the outer query
        # may already reference (e.g. the last_dag_run_state filter), which would strip the
        # subquery's FROM and raise. EXISTS resolves each Dag via the (dag_id, state) index.
        any_run = aliased(DagRun)
        has_run_in_state = (
            sql_select(any_run.dag_id)
            .where(any_run.dag_id == DagModel.dag_id, any_run.state == self.value)
            .exists()
        )
        return select.where(has_run_in_state)

    @classmethod
    def depends(
        cls,
        dag_run_state: DagRunState | None = Query(
            None,
            description="Filter Dags that have any DagRun in the given state.",
        ),
    ) -> _AnyDagRunStateFilter:
        return cls().set_value(dag_run_state)


QueryLastDagRunStateFilter = Annotated[
    FilterParam[DagRunState | None],
    Depends(filter_param_factory(DagRun.state, DagRunState | None, filter_name="last_dag_run_state")),
]

QueryAnyDagRunStateFilter = Annotated[_AnyDagRunStateFilter, Depends(_AnyDagRunStateFilter.depends)]


def _transform_dag_run_states(states: Iterable[str] | None) -> list[DagRunState | None] | None:
    try:
        if not states:
            return None
        return [None if s in ("none", None) else DagRunState(s) for s in states]
    except ValueError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Invalid value for state. Valid values are {', '.join(DagRunState)}",
        )


QueryDagRunStateFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            DagRun.state,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            transform_callable=_transform_dag_run_states,
        )
    ),
]


def _transform_dag_run_types(types: list[str] | None) -> list[DagRunType | None] | None:
    try:
        if not types:
            return None
        return [None if run_type in ("none", None) else DagRunType(run_type) for run_type in types]
    except ValueError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Invalid value for run type. Valid values are {', '.join(DagRunType)}",
        )


QueryDagRunRunTypesFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            attribute=DagRun.run_type,
            _type=list[str],
            filter_option=FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            transform_callable=_transform_dag_run_types,
        )
    ),
]


QueryDagRunTriggeringUserSearch = Annotated[
    _SearchParam, Depends(search_param_factory(DagRun.triggering_user_name, "triggering_user"))
]

QueryDagRunTriggeringUserPrefixSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(DagRun.triggering_user_name, "triggering_user_prefix")),
]

QueryDagRunPartitionKeySearch = Annotated[
    _SearchParam,
    Depends(search_param_factory(DagRun.partition_key, "partition_key_pattern", pipe_as_or=False)),
]

QueryDagRunPartitionKeyPrefixSearch = Annotated[
    _PrefixSearchParam,
    Depends(
        prefix_search_param_factory(DagRun.partition_key, "partition_key_prefix_pattern", pipe_as_or=False)
    ),
]

QueryDagRunVersionFilter = Annotated[
    FilterParam[list[int]],
    Depends(
        filter_param_factory(
            DagVersion.version_number,
            list[int],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            filter_name="dag_version",
        )
    ),
]
