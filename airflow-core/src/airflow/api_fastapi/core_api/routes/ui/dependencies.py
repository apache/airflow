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

from fastapi import Depends, Query, status
from fastapi.exceptions import HTTPException

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
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
def get_dependencies(
    node_id: str | None = None,
    node_ids: str | None = Query(None, description="Comma-separated list of node ids"),
) -> BaseGraphResponse:
    """Dependencies graph. Supports a single node_id or multiple node_ids separated by commas."""
    # Parse node_ids (priority to node_ids, fallback to node_id)
    ids_to_fetch: list[str] = []
    # If node_id contains commas, treat as multiple IDs (extra protection)
    if node_ids:
        ids_to_fetch = [nid.strip() for nid in node_ids.split(",") if nid.strip()]
    elif node_id:
        if "," in node_id:
            ids_to_fetch = [nid.strip() for nid in node_id.split(",") if nid.strip()]
        else:
            ids_to_fetch = [node_id]

    nodes_dict: dict[str, dict] = {}
    edge_tuples: set[tuple[str, str]] = set()

    dag_deps = SerializedDagModel.get_dag_dependencies()

    for dag, dependencies in sorted(dag_deps.items()):
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

    # If ids_to_fetch is filled, filter for each connected component
    if ids_to_fetch:
        try:
            # Join all connected components of the requested ids
            all_nodes = []
            all_edges = []
            seen_nodes = set()
            seen_edges = set()
            for nid in ids_to_fetch:
                comp = extract_single_connected_component(nid, data["nodes"], data["edges"])
                for n in comp["nodes"]:
                    if n["id"] not in seen_nodes:
                        all_nodes.append(n)
                        seen_nodes.add(n["id"])
                for e in comp["edges"]:
                    edge_tuple = (e["source_id"], e["target_id"])
                    if edge_tuple not in seen_edges:
                        all_edges.append(e)
                        seen_edges.add(edge_tuple)
            data = {"nodes": all_nodes, "edges": all_edges}
        except ValueError as e:
            raise HTTPException(404, str(e))

    return BaseGraphResponse(**data)
