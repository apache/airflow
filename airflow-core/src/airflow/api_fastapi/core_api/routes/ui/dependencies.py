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

from fastapi import Depends, status
from fastapi.exceptions import HTTPException

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.common import BaseGraphResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.dependencies import extract_single_connected_component
from airflow.models.serialized_dag import SerializedDagModel

dependencies_router = AirflowRouter(tags=["Dependencies"])


@dependencies_router.get(
    "/dependencies",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.DEPENDENCIES))],
)
def get_dependencies(session: SessionDep, node_id: str | None = None) -> BaseGraphResponse:
    """Dependencies graph."""
    nodes_dict: dict[str, dict] = {}
    edge_tuples: set[tuple[str, str]] = set()

    for dag, dependencies in sorted(SerializedDagModel.get_dag_dependencies().items()):
        dag_node_id = f"dag:{dag}"
        if dag_node_id not in nodes_dict:
            for dep in dependencies:
                # Add nodes
                nodes_dict[dag_node_id] = {"id": dag_node_id, "label": dag, "type": "dag"}
                if dep.node_id not in nodes_dict:
                    nodes_dict[dep.node_id] = {
                        "id": dep.node_id,
                        "label": dep.label,
                        "type": dep.dependency_type,
                    }

                # Add edges
                # not start dep
                if dep.source != dep.dependency_type:
                    source = dep.source if ":" in dep.source else f"dag:{dep.source}"
                    target = dep.node_id
                    edge_tuples.add((source, target))

                # not end dep
                if dep.target != dep.dependency_type:
                    source = dep.node_id
                    target = dep.target if ":" in dep.target else f"dag:{dep.target}"
                    edge_tuples.add((source, target))

    nodes = list(nodes_dict.values())
    edges = [{"source_id": source, "target_id": target} for source, target in sorted(edge_tuples)]

    data = {
        "nodes": nodes,
        "edges": edges,
    }

    if node_id is not None:
        try:
            data = extract_single_connected_component(node_id, data["nodes"], data["edges"])
        except ValueError as e:
            raise HTTPException(404, str(e))

    return BaseGraphResponse(**data)
