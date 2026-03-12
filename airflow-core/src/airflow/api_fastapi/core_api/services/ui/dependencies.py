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

from collections import defaultdict, deque
from typing import TYPE_CHECKING

from airflow.models.asset import AssetModel

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _dfs_connected_components(
    temp: list[str], node_id: str, visited: dict[str, bool], adjacency_matrix: dict[str, list[str]]
) -> list[str]:
    visited[node_id] = True

    temp.append(node_id)

    for adj_node_id in adjacency_matrix[node_id]:
        if not visited[adj_node_id]:
            temp = _dfs_connected_components(temp, adj_node_id, visited, adjacency_matrix)

    return temp


def extract_connected_components(adjacency_matrix: dict[str, list[str]]) -> list[list[str]]:
    """Extract all connected components of a graph."""
    visited: dict[str, bool] = {node_id: False for node_id in adjacency_matrix}

    connected_components: list[list[str]] = []

    for node_id in adjacency_matrix:
        if visited[node_id] is False:
            temp: list[str] = []
            connected_components.append(_dfs_connected_components(temp, node_id, visited, adjacency_matrix))
    return connected_components


def extract_single_connected_component(
    node_id: str, nodes: list[dict], edges: list[dict]
) -> dict[str, list[dict]]:
    """Find the connected component that contains the node with the id ``node_id``."""
    adjacency_matrix: dict[str, list[str]] = defaultdict(list)

    for edge in edges:
        adjacency_matrix[edge["source_id"]].append(edge["target_id"])
        adjacency_matrix[edge["target_id"]].append(edge["source_id"])

    connected_components = extract_connected_components(adjacency_matrix)

    filtered_connected_components = [cc for cc in connected_components if node_id in cc]

    if len(filtered_connected_components) != 1:
        raise ValueError(
            f"Unique connected component not found, got {filtered_connected_components} for connected components of node {node_id}, expected only 1 connected component."
        )

    connected_component = filtered_connected_components[0]

    nodes = [node for node in nodes if node["id"] in connected_component]
    edges = [
        edge
        for edge in edges
        if (edge["source_id"] in connected_component and edge["target_id"] in connected_component)
    ]

    return {"nodes": nodes, "edges": edges}


def get_scheduling_dependencies(readable_dag_ids: set[str] | None = None) -> dict[str, list[dict]]:
    """Get scheduling dependencies between DAGs."""
    from airflow.models.serialized_dag import SerializedDagModel

    nodes_dict: dict[str, dict] = {}
    edge_tuples: set[tuple[str, str]] = set()

    dag_dependencies = SerializedDagModel.get_dag_dependencies()
    for dag, dependencies in sorted(dag_dependencies.items()):
        if readable_dag_ids is not None and dag not in readable_dag_ids:
            continue
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

    return {
        "nodes": list(nodes_dict.values()),
        "edges": [{"source_id": source, "target_id": target} for source, target in sorted(edge_tuples)],
    }


def get_data_dependencies(
    asset_id: int, session: Session, readable_dag_ids: set[str] | None = None
) -> dict[str, list[dict]]:
    """Get full task dependencies for an asset."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from airflow.models.asset import TaskInletAssetReference, TaskOutletAssetReference

    SEPARATOR = "__SEPARATOR__"

    nodes_dict: dict[str, dict] = {}
    edge_set: set[tuple[str, str]] = set()

    # BFS to trace full dependencies
    assets_to_process: deque[int] = deque([asset_id])
    processed_assets: set[int] = set()
    processed_tasks: set[tuple[str, str]] = set()  # (dag_id, task_id)

    while assets_to_process:
        current_asset_id = assets_to_process.popleft()
        if current_asset_id in processed_assets:
            continue
        processed_assets.add(current_asset_id)

        # Eagerload producing_tasks and consuming_tasks to avoid lazy queries
        asset = session.scalar(
            select(AssetModel)
            .where(AssetModel.id == current_asset_id)
            .options(
                selectinload(AssetModel.producing_tasks),
                selectinload(AssetModel.consuming_tasks),
            )
        )
        if not asset:
            continue

        asset_node_id = f"asset:{current_asset_id}"

        # Add asset node
        if asset_node_id not in nodes_dict:
            nodes_dict[asset_node_id] = {"id": asset_node_id, "label": asset.name, "type": "asset"}

        # Process producing tasks (tasks that output this asset)
        for ref in asset.producing_tasks:
            # Filter out tasks from Dags the user doesn't have access to
            if readable_dag_ids is not None and ref.dag_id not in readable_dag_ids:
                continue
            task_key = (ref.dag_id, ref.task_id)
            task_node_id = f"task:{ref.dag_id}{SEPARATOR}{ref.task_id}"

            # Add task node with dag_id.task_id label for disambiguation
            if task_node_id not in nodes_dict:
                nodes_dict[task_node_id] = {
                    "id": task_node_id,
                    "label": f"{ref.dag_id}.{ref.task_id}",
                    "type": "task",
                }

            # Add edge: task → asset
            edge_set.add((task_node_id, asset_node_id))

            # Find other assets this task consumes (inlets) to trace upstream
            if task_key not in processed_tasks:
                processed_tasks.add(task_key)
                inlet_refs = session.scalars(
                    select(TaskInletAssetReference).where(
                        TaskInletAssetReference.dag_id == ref.dag_id,
                        TaskInletAssetReference.task_id == ref.task_id,
                    )
                ).all()
                for inlet_ref in inlet_refs:
                    if inlet_ref.asset_id not in processed_assets:
                        assets_to_process.append(inlet_ref.asset_id)

        # Process consuming tasks (tasks that input this asset)
        for ref in asset.consuming_tasks:
            # Filter out tasks from Dags the user doesn't have access to
            if readable_dag_ids is not None and ref.dag_id not in readable_dag_ids:
                continue
            task_key = (ref.dag_id, ref.task_id)
            task_node_id = f"task:{ref.dag_id}{SEPARATOR}{ref.task_id}"

            # Add task node with dag_id.task_id label for disambiguation
            if task_node_id not in nodes_dict:
                nodes_dict[task_node_id] = {
                    "id": task_node_id,
                    "label": f"{ref.dag_id}.{ref.task_id}",
                    "type": "task",
                }

            # Add edge: asset → task
            edge_set.add((asset_node_id, task_node_id))

            # Find other assets this task produces (outlets) to trace downstream
            if task_key not in processed_tasks:
                processed_tasks.add(task_key)
                outlet_refs = session.scalars(
                    select(TaskOutletAssetReference).where(
                        TaskOutletAssetReference.dag_id == ref.dag_id,
                        TaskOutletAssetReference.task_id == ref.task_id,
                    )
                ).all()
                for outlet_ref in outlet_refs:
                    if outlet_ref.asset_id not in processed_assets:
                        assets_to_process.append(outlet_ref.asset_id)

    return {
        "nodes": list(nodes_dict.values()),
        "edges": [{"source_id": source, "target_id": target} for source, target in edge_set],
    }
