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

from typing import Annotated

from fastapi import Depends
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import (
    SessionDep,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    QueryDagTagPatternSearch,
    QueryLimit,
    QueryOffset,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_tags import DAGTagCollectionResponse
from airflow.api_fastapi.core_api.security import ReadableTagsFilterDep, requires_access_dag
from airflow.models.dag import DagTag

dag_tags_router = AirflowRouter(tags=["DAG"], prefix="/dagTags")


@dag_tags_router.get("", dependencies=[Depends(requires_access_dag(method="GET"))])
def get_dag_tags(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["name"],
                DagTag,
            ).dynamic_depends()
        ),
    ],
    tag_name_pattern: QueryDagTagPatternSearch,
    readable_tags_filter: ReadableTagsFilterDep,
    session: SessionDep,
) -> DAGTagCollectionResponse:
    """Get all DAG tags."""
    query = select(DagTag.name).group_by(DagTag.name)
    dag_tags_select, total_entries = paginated_select(
        statement=query,
        filters=[tag_name_pattern, readable_tags_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    dag_tags = session.execute(dag_tags_select).scalars().all()
    return DAGTagCollectionResponse(tags=[x for x in dag_tags], total_entries=total_entries)
