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

from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import QueryIncludeDownstream, QueryIncludeUpstream
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.structure import StructureDataResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.structure import get_upstream_assets
from airflow.models.dag_version import DagVersion
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.dag_edges import dag_edges
from airflow.utils.task_group import task_group_to_dict

structure_router = AirflowRouter(tags=["Structure"], prefix="/structure")


@structure_router.get(
    "/structure_data",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_dag("GET")),
        Depends(requires_access_dag("GET", DagAccessEntity.DEPENDENCIES)),
        Depends(requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE)),
    ],
)
def structure_data(
    session: SessionDep,
    dag_id: str,
    include_upstream: QueryIncludeUpstream = False,
    include_downstream: QueryIncludeDownstream = False,
    root: str | None = None,
    external_dependencies: bool = False,
    version_number: int | None = None,
) -> StructureDataResponse:
    """Get Structure Data."""
    if version_number is None:
        dag_version_model = DagVersion.get_latest_version(dag_id)
        if dag_version_model is None:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"Dag with id {dag_id} was not found",
            )
        version_number = dag_version_model.version_number

    serialized_dag: SerializedDagModel = session.scalar(
        select(SerializedDagModel)
        .join(DagVersion)
        .where(SerializedDagModel.dag_id == dag_id, DagVersion.version_number == version_number)
    )
    if serialized_dag is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Dag with id {dag_id} and version number {version_number} was not found",
        )
    dag = serialized_dag.dag

    if root:
        dag = dag.partial_subset(
            task_ids=root, include_upstream=include_upstream, include_downstream=include_downstream
        )

    nodes = [task_group_to_dict(child) for child in dag.task_group.topological_sort()]
    edges = dag_edges(dag)

    data = {
        "nodes": nodes,
        "edges": edges,
    }

    if external_dependencies:
        entry_node_ref = nodes[0] if nodes else None
        exit_node_ref = nodes[-1] if nodes else None

        start_edges: list[dict] = []
        end_edges: list[dict] = []

        for dependency_dag_id, dependencies in sorted(SerializedDagModel.get_dag_dependencies().items()):
            for dependency in dependencies:
                # Dependencies not related to `dag_id` are ignored
                if dependency_dag_id != dag_id and dependency.target != dag_id:
                    continue

                # upstream assets are handled by the `get_upstream_assets` function.
                if dependency.target != dependency.dependency_type and dependency.dependency_type in [
                    "asset-alias",
                    "asset",
                ]:
                    continue

                # Add edges
                # start dependency
                if (
                    dependency.source == dependency.dependency_type or dependency.target == dag_id
                ) and entry_node_ref:
                    start_edges.append({"source_id": dependency.node_id, "target_id": entry_node_ref["id"]})

                # end dependency
                elif (
                    dependency.target == dependency.dependency_type or dependency.source == dag_id
                ) and exit_node_ref:
                    end_edges.append({"source_id": exit_node_ref["id"], "target_id": dependency.node_id})

                # Add nodes
                nodes.append(
                    {
                        "id": dependency.node_id,
                        "label": dependency.label,
                        "type": dependency.dependency_type,
                    }
                )

        if asset_expression := serialized_dag.dag_model.asset_expression:
            upstream_asset_nodes, upstream_asset_edges = get_upstream_assets(
                asset_expression, entry_node_ref["id"]
            )
            data["nodes"] += upstream_asset_nodes
            data["edges"] = upstream_asset_edges

        data["edges"] += start_edges + edges + end_edges

    return StructureDataResponse(**data)
