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
    Any,
    cast,
)

from fastapi import Depends, HTTPException, Query, status

from airflow.api_fastapi.common.parameters.base import BaseParam, _MySQLCollate
from airflow.api_fastapi.common.parameters.filter import FilterOptionEnum, FilterParam, filter_param_factory
from airflow.api_fastapi.common.parameters.search import (
    _PrefixSearchParam,
    _SearchParam,
    _TaskDisplayNamePrefixPatternParam,
    prefix_search_param_factory,
    search_param_factory,
)
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.models.dag_version import DagVersion
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement, Select

    from airflow.serialization.definitions.dag import SerializedDAG


class QueryTaskInstanceTaskGroupFilter(BaseParam[str]):
    """Task group filter - returns all tasks in the specified group."""

    def __init__(self, dag=None, skip_none: bool = True):
        super().__init__(skip_none=skip_none)
        self._dag: None | SerializedDAG = dag

    @property
    def dag(self) -> None | SerializedDAG:
        return self._dag

    @dag.setter
    def dag(self, value: None | SerializedDAG) -> None:
        self._dag = value

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        if not self.dag:
            raise ValueError("Dag must be set before calling to_orm")

        if not hasattr(self.dag, "task_group"):
            return select

        # Exact matching on group_id
        task_groups = self.dag.task_group.get_task_group_dict()
        task_group = task_groups.get(self.value)
        if not task_group:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                detail={
                    "reason": "not_found",
                    "message": f"Task group {self.value} not found",
                },
            )

        return select.where(TaskInstance.task_id.in_(task.task_id for task in task_group.iter_tasks()))

    @classmethod
    def depends(
        cls,
        value: str | None = Query(
            alias="task_group_id",
            default=None,
            description="Filter by exact task group ID. Returns all tasks within the specified task group.",
        ),
    ) -> QueryTaskInstanceTaskGroupFilter:
        return cls(dag=None).set_value(value)


def _transform_ti_states(states: list[str] | None) -> list[TaskInstanceState | None] | None:
    """Transform a list of state strings into a list of TaskInstanceState enums handling special 'None' cases."""
    if not states:
        return None

    try:
        return [None if s in ("no_status", "none", None) else TaskInstanceState(s) for s in states]
    except ValueError:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Invalid value for state. Valid values are {', '.join(TaskInstanceState)}",
        )


QueryTIStateFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            TaskInstance.state,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            transform_callable=_transform_ti_states,
        )
    ),
]

QueryTIPoolFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(TaskInstance.pool, list[str], FilterOptionEnum.ANY_EQUAL, default_factory=list)
    ),
]

QueryTIQueueFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(TaskInstance.queue, list[str], FilterOptionEnum.ANY_EQUAL, default_factory=list)
    ),
]

QueryTIPoolNamePatternSearch = Annotated[
    _SearchParam,
    Depends(search_param_factory(TaskInstance.pool, "pool_name_pattern")),
]

QueryTIPoolNamePrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(TaskInstance.pool, "pool_name_prefix_pattern")),
]


QueryTIQueueNamePatternSearch = Annotated[
    _SearchParam,
    Depends(search_param_factory(TaskInstance.queue, "queue_name_pattern")),
]

QueryTIQueueNamePrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(TaskInstance.queue, "queue_name_prefix_pattern")),
]

QueryTIExecutorFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            TaskInstance.executor, list[str], FilterOptionEnum.ANY_EQUAL, default_factory=list
        )
    ),
]

QueryTITaskDisplayNamePatternSearch = Annotated[
    _SearchParam,
    Depends(search_param_factory(TaskInstance.task_display_name, "task_display_name_pattern")),
]

QueryTITaskDisplayNamePrefixPatternSearch = Annotated[
    _TaskDisplayNamePrefixPatternParam, Depends(_TaskDisplayNamePrefixPatternParam.depends)
]

QueryTITaskGroupFilter = Annotated[
    QueryTaskInstanceTaskGroupFilter, Depends(QueryTaskInstanceTaskGroupFilter.depends)
]

QueryTIDagVersionFilter = Annotated[
    FilterParam[list[int]],
    Depends(
        filter_param_factory(
            DagVersion.version_number,
            list[int],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
        )
    ),
]

QueryTITryNumberFilter = Annotated[
    FilterParam[list[int]],
    Depends(
        filter_param_factory(
            TaskInstance.try_number, list[int], FilterOptionEnum.ANY_EQUAL, default_factory=list
        )
    ),
]


QueryTIOperatorFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            TaskInstance.operator, list[str], FilterOptionEnum.ANY_EQUAL, default_factory=list
        )
    ),
]

QueryTIOperatorNamePatternSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            TaskInstance.custom_operator_name,
            "operator_name_pattern",
        )
    ),
]

QueryTIOperatorNamePrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(
        prefix_search_param_factory(
            TaskInstance.custom_operator_name,
            "operator_name_prefix_pattern",
        )
    ),
]


QueryTIMapIndexFilter = Annotated[
    FilterParam[list[int]],
    Depends(
        filter_param_factory(
            TaskInstance.map_index, list[int], FilterOptionEnum.ANY_EQUAL, default_factory=list
        )
    ),
]

# On MySQL the CASE expression that backs rendered_map_index mixes a stored
# VARCHAR column (utf8mb4_bin, IMPLICIT) with CAST(map_index AS CHAR)
# (utf8mb4_0900_ai_ci, IMPLICIT), which gives the whole expression NONE
# coercibility.  Comparing it against a bound parameter then fails with
# "Illegal mix of collations".  _MySQLCollate wraps the expression so that
# on MySQL an explicit COLLATE clause is emitted (giving EXPLICIT coercibility);
# on PostgreSQL and SQLite the wrapper is transparent.
_rendered_map_index_collated = _MySQLCollate(
    cast("ColumnElement[Any]", TaskInstance.rendered_map_index), "utf8mb4_0900_ai_ci"
)


QueryTIRenderedMapIndexPatternSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            _rendered_map_index_collated,
            "rendered_map_index_pattern",
        )
    ),
]

QueryTIRenderedMapIndexPrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(
        prefix_search_param_factory(
            _rendered_map_index_collated,
            "rendered_map_index_prefix_pattern",
        )
    ),
]
