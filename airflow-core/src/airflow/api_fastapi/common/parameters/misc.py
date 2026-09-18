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
from pydantic import AfterValidator
from sqlalchemy import func, select as sql_select

from airflow.api_fastapi.common.parameters.base import BaseParam
from airflow.api_fastapi.common.parameters.filter import FilterOptionEnum, FilterParam, filter_param_factory
from airflow.api_fastapi.common.parameters.search import (
    _PrefixSearchParam,
    _SearchParam,
    prefix_search_param_factory,
    search_param_factory,
)
from airflow.models.connection import Connection
from airflow.models.dag import DagModel
from airflow.models.errors import ParseImportError
from airflow.models.hitl import HITLDetail
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.sql import Select


class _PendingActionsFilter(BaseParam[bool]):
    """Filter Dags by having pending HITL actions (more than 1)."""

    def to_orm(self, select: Select) -> Select:
        if self.value is None and self.skip_none:
            return select

        from airflow.models.hitl import HITLDetail
        from airflow.models.taskinstance import TaskInstance

        # Join with HITLDetail and TaskInstance to find Dags
        pending_actions_count_subquery = (
            sql_select(func.count(HITLDetail.ti_id))
            .join(TaskInstance, HITLDetail.ti_id == TaskInstance.id)
            .where(
                HITLDetail.responded_at.is_(None),
                TaskInstance.state.in_((TaskInstanceState.DEFERRED, TaskInstanceState.AWAITING_INPUT)),
            )
            .where(TaskInstance.dag_id == DagModel.dag_id)
            .scalar_subquery()
        )

        if self.value is True:
            # Filter to show only Dags with pending actions
            where_clause = pending_actions_count_subquery >= 1
        else:
            # Filter to show only Dags without pending actions
            where_clause = pending_actions_count_subquery == 0

        return select.where(where_clause)

    @classmethod
    def depends(cls, has_pending_actions: bool | None = Query(None)) -> _PendingActionsFilter:
        return cls().set_value(has_pending_actions)


QueryPendingActionsFilter = Annotated[_PendingActionsFilter, Depends(_PendingActionsFilter.depends)]


# Variables
QueryVariableKeyPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(Variable.key, "variable_key_pattern"))
]

QueryVariableKeyPrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(Variable.key, "variable_key_prefix_pattern")),
]


# Pools
QueryPoolNamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(Pool.pool, "pool_name_pattern"))
]

QueryPoolNamePrefixPatternSearch = Annotated[
    _PrefixSearchParam, Depends(prefix_search_param_factory(Pool.pool, "pool_name_prefix_pattern"))
]


# UI Shared
def _optional_boolean(value: bool | None) -> bool | None:
    return value if value is not None else False


QueryIncludeUpstream = Annotated[bool, AfterValidator(_optional_boolean)]

QueryIncludeDownstream = Annotated[bool, AfterValidator(_optional_boolean)]


state_priority: list[None | TaskInstanceState] = [
    TaskInstanceState.FAILED,
    TaskInstanceState.UPSTREAM_FAILED,
    TaskInstanceState.UP_FOR_RETRY,
    TaskInstanceState.UP_FOR_RESCHEDULE,
    TaskInstanceState.RUNNING,
    TaskInstanceState.RESTARTING,
    TaskInstanceState.DEFERRED,
    TaskInstanceState.AWAITING_INPUT,
    TaskInstanceState.QUEUED,
    TaskInstanceState.SCHEDULED,
    None,
    TaskInstanceState.SUCCESS,
    TaskInstanceState.SKIPPED,
    TaskInstanceState.REMOVED,
]


# Connections
QueryConnectionIdPatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(Connection.conn_id, "connection_id_pattern"))
]

QueryConnectionIdPrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(Connection.conn_id, "connection_id_prefix_pattern")),
]


# Human in the loop
QueryHITLDetailDagIdPatternSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            TaskInstance.dag_id,
            "dag_id_pattern",
        )
    ),
]

QueryHITLDetailDagIdPrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(
        prefix_search_param_factory(
            TaskInstance.dag_id,
            "dag_id_prefix_pattern",
        )
    ),
]

QueryHITLDetailTaskIdPatternSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            TaskInstance.task_id,
            "task_id_pattern",
        )
    ),
]

QueryHITLDetailTaskIdPrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(
        prefix_search_param_factory(
            TaskInstance.task_id,
            "task_id_prefix_pattern",
        )
    ),
]

QueryHITLDetailTaskIdFilter = Annotated[
    FilterParam[str | None],
    Depends(
        filter_param_factory(
            TaskInstance.task_id,
            str | None,
            filter_name="task_id",
        )
    ),
]

QueryHITLDetailMapIndexFilter = Annotated[
    FilterParam[int | None],
    Depends(
        filter_param_factory(
            TaskInstance.map_index,
            int | None,
            filter_name="map_index",
        )
    ),
]

QueryHITLDetailSubjectSearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            HITLDetail.subject,
            "subject_search",
        )
    ),
]

QueryHITLDetailBodySearch = Annotated[
    _SearchParam,
    Depends(
        search_param_factory(
            HITLDetail.body,
            "body_search",
        )
    ),
]

QueryHITLDetailResponseReceivedFilter = Annotated[
    FilterParam[bool | None],
    Depends(
        filter_param_factory(
            HITLDetail.response_received,
            bool | None,
            filter_name="response_received",
        )
    ),
]

QueryHITLDetailRespondedUserIdFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            HITLDetail.responded_by_user_id,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            filter_name="responded_by_user_id",
        )
    ),
]

QueryHITLDetailRespondedUserNameFilter = Annotated[
    FilterParam[list[str]],
    Depends(
        filter_param_factory(
            HITLDetail.responded_by_user_name,
            list[str],
            FilterOptionEnum.ANY_EQUAL,
            default_factory=list,
            filter_name="responded_by_user_name",
        )
    ),
]


# Parse Import Errors
QueryParseImportErrorFilenamePatternSearch = Annotated[
    _SearchParam, Depends(search_param_factory(ParseImportError.filename, "filename_pattern"))
]

QueryParseImportErrorFilenamePrefixPatternSearch = Annotated[
    _PrefixSearchParam,
    Depends(prefix_search_param_factory(ParseImportError.filename, "filename_prefix_pattern")),
]

QueryParseImportErrorFilenameFilter = Annotated[
    FilterParam,
    Depends(
        filter_param_factory(
            ParseImportError.filename,
            str | None,
            filter_name="filename",
            description="Exact filename match. Returns only the import error for this specific file path.",
        )
    ),
]

QueryParseImportErrorBundleNameFilter = Annotated[
    FilterParam,
    Depends(
        filter_param_factory(
            ParseImportError.bundle_name,
            str | None,
            filter_name="bundle_name",
            description="Exact bundle name match. Returns only import errors from this specific bundle.",
        )
    ),
]
