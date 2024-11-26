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

from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, Request, status
from sqlalchemy.orm import Session

from airflow.api_fastapi.core_api.datamodels.ui.graph import GraphDataResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.utils.dag_edges import dag_edges
from airflow.utils.task_group import task_group_to_dict

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter

graph_data_router = AirflowRouter(tags=["Graph"])


@graph_data_router.get(
    "/graph_data",
    include_in_schema=False,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST]),
)
def graph_data(
    session: Annotated[Session, Depends(get_session)],
    dag_id: str,
    request: Request,
    root: str | None = None,
    filter_upstream: bool = False,
    filter_downstream: bool = False,
) -> GraphDataResponse:
    """Get Graph Data."""
    dag = request.app.state.dag_bag.get_dag(dag_id)
    if root:
        dag = dag.partial_subset(
            task_ids_or_regex=root, include_upstream=filter_upstream, include_downstream=filter_downstream
        )

    nodes = task_group_to_dict(dag.task_group)
    edges = dag_edges(dag)

    data = {
        "arrange": dag.orientation,
        "nodes": nodes,
        "edges": edges,
    }

    print(data)

    return GraphDataResponse.model_validate(data)
