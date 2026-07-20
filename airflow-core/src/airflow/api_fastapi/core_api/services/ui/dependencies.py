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

from collections import defaultdict
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import select

from airflow.models.asset import AssetModel
from airflow.models.dag import DagModel

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = structlog.get_logger(logger_name=__name__)

ASSET_UPSTREAM_DEPENDENCY_TYPES = ("asset", "asset-alias", "asset-name-ref", "asset-uri-ref")


def _asset_node_id_and_label(asset: dict) -> tuple[str, str]:
    """Get the graph node id and label for a single asset/alias/ref entry in an asset_expression."""
    asset_type = asset["type"]

    if asset_type == "asset":
        return f"asset:{asset['id']}", asset["name"]
    if asset_type in ("asset-alias", "asset-name-ref"):
        return f"{asset_type}:{asset['name']}", asset["name"]
    if asset_type == "asset-uri-ref":
        return f"{asset_type}:{asset['uri']}", asset["uri"]
    raise TypeError(f"Unsupported type: {asset_type}")


def get_upstream_assets(
    asset_expression: dict, entry_node_ref: str, level: int = 0
) -> tuple[list[dict], list[dict]]:
    """Expand a Dag's asset trigger condition into asset-condition (AND/OR gate) nodes and edges."""
    edges: list[dict] = []
    nodes: list[dict] = []
    asset_expression_type: str | None = None

    # include assets, asset-alias, asset-name-refs, asset-uri-refs
    assets_info: list[dict] = []

    nested_expression: dict = {}

    expr_key = ""
    if asset_expression.keys() == {"any"}:
        asset_expression_type = "or-gate"
        expr_key = "any"
    elif asset_expression.keys() == {"all"}:
        asset_expression_type = "and-gate"
        expr_key = "all"

    if expr_key in asset_expression:
        asset_exprs: list[dict] = asset_expression[expr_key]
        for expr in asset_exprs:
            nested_expr_key = next(iter(expr.keys()))
            if nested_expr_key in ("any", "all"):
                nested_expression = expr
            elif nested_expr_key in ("asset", "alias", "asset-name-ref", "asset-uri-ref"):
                asset_info = expr[nested_expr_key]
                asset_info["type"] = nested_expr_key if nested_expr_key != "alias" else "asset-alias"

                assets_info.append(asset_info)
            elif nested_expr_key == "asset_ref":
                # Asset.ref(...) that hasn't been resolved to a concrete asset yet serializes as
                # {"asset_ref": {"name": ...}} or {"asset_ref": {"uri": ...}} -- disambiguate on
                # the inner field since, unlike the other branches, the key itself doesn't say which.
                ref_info = expr[nested_expr_key]
                ref_info["type"] = "asset-name-ref" if "name" in ref_info else "asset-uri-ref"

                assets_info.append(ref_info)
            else:
                raise TypeError(f"Unsupported type: {expr.keys()}")

    if not asset_expression_type:
        return nodes, edges

    # A condition combining exactly one branch isn't a real AND/OR -- connect it directly (or
    # recurse straight through a nested single-branch wrapper) instead of rendering a gate with
    # only one input.
    if len(assets_info) + (1 if nested_expression else 0) <= 1:
        if assets_info:
            source_id, label = _asset_node_id_and_label(assets_info[0])
            edges.append({"source_id": source_id, "target_id": entry_node_ref})
            nodes.append({"id": source_id, "label": label, "type": assets_info[0]["type"]})
        elif nested_expression:
            return get_upstream_assets(nested_expression, entry_node_ref, level=level)

        return nodes, edges

    # Scoped by entry_node_ref (unique per Dag) so gates from different Dags don't collide
    # when merged into a single multi-dag graph.
    asset_condition_id = f"{entry_node_ref}-{asset_expression_type}-{level}"
    edges.append(
        {
            "source_id": asset_condition_id,
            "target_id": entry_node_ref,
            "is_source_asset": level == 0,
        }
    )
    nodes.append(
        {
            "id": asset_condition_id,
            "label": asset_condition_id,
            "type": "asset-condition",
            "asset_condition_type": asset_expression_type,
        }
    )

    for asset in assets_info:
        source_id, label = _asset_node_id_and_label(asset)

        edges.append(
            {
                "source_id": source_id,
                "target_id": asset_condition_id,
            }
        )
        nodes.append(
            {
                "id": source_id,
                "label": label,
                "type": asset["type"],
            }
        )

    if nested_expression:
        n, e = get_upstream_assets(nested_expression, asset_condition_id, level=level + 1)

        nodes = nodes + n
        edges = edges + e

    return nodes, edges


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


def get_scheduling_dependencies(readable_dag_ids: set[str] | None, session: Session) -> dict[str, list[dict]]:
    """Get scheduling dependencies between Dags."""
    from airflow.models.serialized_dag import SerializedDagModel

    nodes_dict: dict[str, dict] = {}
    edge_tuples: set[tuple[str, str]] = set()

    dag_dependencies = SerializedDagModel.get_dag_dependencies()

    relevant_dag_ids = [
        dag for dag in dag_dependencies if readable_dag_ids is None or dag in readable_dag_ids
    ]

    # A Dag's asset trigger condition (AND/OR of assets) is expanded into asset-condition
    # gate nodes so it renders the same way here as it would per-Dag. A single-asset
    # schedule serializes as a bare `{"asset": {...}}`, not `{"any": [...]}`, so
    # `get_upstream_assets` returns nothing for it — only Dags with a genuine boolean
    # condition are gate-expanded; other Dags keep their flat asset edge untouched.
    expression_nodes: dict[str, dict] = {}
    expression_edges: set[tuple[str, str]] = set()
    gated_dag_ids: set[str] = set()
    if relevant_dag_ids:
        asset_expressions = session.execute(
            select(DagModel.dag_id, DagModel.asset_expression).where(DagModel.dag_id.in_(relevant_dag_ids))
        )
        for dag_id, asset_expression in asset_expressions:
            if not asset_expression:
                continue
            try:
                upstream_nodes, upstream_edges = get_upstream_assets(asset_expression, f"dag:{dag_id}")
            except TypeError:
                # A malformed/unrecognized asset_expression for one Dag must not break this
                # endpoint for every Dag -- fall back to that Dag's flat asset edge instead.
                log.warning("Could not expand asset_expression into gate nodes", dag_id=dag_id, exc_info=True)
                continue
            if upstream_nodes:
                gated_dag_ids.add(dag_id)
                for node in upstream_nodes:
                    expression_nodes[node["id"]] = node
                for edge in upstream_edges:
                    expression_edges.add((edge["source_id"], edge["target_id"]))

    for dag, dependencies in sorted(dag_dependencies.items()):
        if readable_dag_ids is not None and dag not in readable_dag_ids:
            continue
        dag_node_id = f"dag:{dag}"
        if dag_node_id not in nodes_dict:
            for dep in dependencies:
                # Skip dependency objects whose edge endpoints reference DAGs
                # outside the caller's readable set. ``dep.node_id`` /
                # ``dep.source`` / ``dep.target`` would otherwise embed those
                # DAG ids in the response even when the top-level filter
                # above hides the DAG itself.
                if readable_dag_ids is not None:
                    referenced_dag_ids: set[str] = set()
                    if dep.source != dep.dependency_type and ":" not in dep.source:
                        referenced_dag_ids.add(dep.source)
                    if dep.target != dep.dependency_type and ":" not in dep.target:
                        referenced_dag_ids.add(dep.target)
                    if not referenced_dag_ids.issubset(readable_dag_ids):
                        continue

                # This Dag's upstream asset condition is represented by the gate nodes
                # built above instead of a flat edge.
                if (
                    dag in gated_dag_ids
                    and dep.target == dag
                    and dep.dependency_type in ASSET_UPSTREAM_DEPENDENCY_TYPES
                ):
                    continue

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

    nodes_dict.update(expression_nodes)
    edge_tuples |= expression_edges

    # Create missing ``dag:`` nodes which may have been skipped by the loop above.
    # A DAG referenced only as a trigger target or a sensor source may have no
    # scheduling dependencies of its own. Without this loop, these DAGs will not be
    # materialised and will result in dangling edges.
    for source, target in edge_tuples:
        for endpoint in (source, target):
            if endpoint.startswith("dag:") and endpoint not in nodes_dict:
                nodes_dict[endpoint] = {
                    "id": endpoint,
                    "label": endpoint.removeprefix("dag:"),
                    "type": "dag",
                }

    dag_ids = [node["label"] for node in nodes_dict.values() if node["type"] == "dag"]
    if dag_ids:
        dag_id_to_team = DagModel.get_dag_id_to_team_name_mapping(dag_ids)
        for node in nodes_dict.values():
            if node["type"] == "dag":
                team_name = dag_id_to_team.get(node["label"])
                if team_name:
                    node["team"] = team_name

    return {
        "nodes": list(nodes_dict.values()),
        "edges": [{"source_id": source, "target_id": target} for source, target in sorted(edge_tuples)],
    }


def _get_dag_entry_points(dag_ids: set[str], session: Session) -> dict[str, tuple[str, dict | None]]:
    """
    Batch-resolve each Dag's topologically-first task/group id and its asset_expression.

    Fetches the latest SerializedDagModel row for every dag_id in one query (mirroring the
    latest-per-group window-function pattern in
    ``SerializedDagModel._prefetch_dag_write_metadata``) instead of one query per Dag --
    deserializing a Dag is expensive, and this can otherwise run once per scheduled Dag
    discovered while tracing an asset's dependencies.
    """
    from sqlalchemy import func, select
    from sqlalchemy.orm import joinedload

    from airflow.api_fastapi.core_api.services.ui.task_group import task_group_to_dict
    from airflow.models.dag_version import DagVersion
    from airflow.models.serialized_dag import SerializedDagModel

    if not dag_ids:
        return {}

    latest_per_dag = (
        select(
            SerializedDagModel.id,
            func.row_number()
            .over(partition_by=SerializedDagModel.dag_id, order_by=DagVersion.version_number.desc())
            .label("rn"),
        )
        .join(DagVersion, SerializedDagModel.dag_version_id == DagVersion.id)
        .where(SerializedDagModel.dag_id.in_(dag_ids))
        .subquery()
    )

    serialized_dags = session.scalars(
        select(SerializedDagModel)
        .join(latest_per_dag, SerializedDagModel.id == latest_per_dag.c.id)
        .where(latest_per_dag.c.rn == 1)
        .options(joinedload(SerializedDagModel.dag_model))
    ).all()

    entry_points: dict[str, tuple[str, dict | None]] = {}
    for serialized_dag in serialized_dags:
        entry_nodes = serialized_dag.dag.task_group.topological_sort()
        if not entry_nodes:
            continue
        entry_points[serialized_dag.dag_id] = (
            task_group_to_dict(entry_nodes[0])["id"],
            serialized_dag.dag_model.asset_expression,
        )

    return entry_points


def get_data_dependencies(
    asset_id: int, session: Session, readable_dag_ids: set[str] | None = None
) -> dict[str, list[dict]]:
    """Get full task dependencies for an asset."""
    from sqlalchemy import select, union_all
    from sqlalchemy.orm import selectinload

    from airflow.models.asset import (
        DagScheduleAssetReference,
        TaskInletAssetReference,
        TaskOutletAssetReference,
    )

    SEPARATOR = "__SEPARATOR__"

    # Hide the asset entirely if the user has no read access to any dag that produces,
    # consumes, or is scheduled by it. Without this check, visiting the asset graph page
    # for an unrelated asset would leak its existence and name (and of connected nodes
    # reachable through other readable dags) even though the user has no legitimate
    # lineage connection to it. A readable_dag_ids value of None means no filter is
    # applied (the user has unrestricted dag read access).
    if readable_dag_ids is not None:
        connected_dag_ids_query = union_all(
            select(TaskOutletAssetReference.dag_id).where(TaskOutletAssetReference.asset_id == asset_id),
            select(TaskInletAssetReference.dag_id).where(TaskInletAssetReference.asset_id == asset_id),
            select(DagScheduleAssetReference.dag_id).where(DagScheduleAssetReference.asset_id == asset_id),
        )
        connected_dag_ids = set(session.scalars(select(connected_dag_ids_query.subquery().c.dag_id)))
        if not connected_dag_ids & readable_dag_ids:
            return {"nodes": [], "edges": []}

    nodes_dict: dict[str, dict] = {}
    edge_set: set[tuple[str, str]] = set()

    processed_assets: set[int] = set()
    processed_tasks: set[tuple[str, str]] = set()  # (dag_id, task_id)
    processed_scheduled_dags: set[str] = set()

    # BFS by rounds (a whole frontier of assets at once) rather than one asset at a time, so
    # every Dag scheduled by assets in the same round has its entry point resolved through one
    # batched query (see _get_dag_entry_points) instead of one query -- and one full Dag
    # deserialization -- per Dag.
    frontier: set[int] = {asset_id}

    while frontier:
        round_asset_ids = frontier - processed_assets
        processed_assets |= round_asset_ids
        if not round_asset_ids:
            break

        # Eagerload producing_tasks, consuming_tasks, and scheduled_dags to avoid lazy queries
        assets = session.scalars(
            select(AssetModel)
            .where(AssetModel.id.in_(round_asset_ids))
            .options(
                selectinload(AssetModel.producing_tasks),
                selectinload(AssetModel.consuming_tasks),
                selectinload(AssetModel.scheduled_dags),
            )
        ).all()

        next_frontier: set[int] = set()
        pending_dag_ids: set[str] = set()
        triggering_asset_node_id_by_dag: dict[str, str] = {}

        for asset in assets:
            asset_node_id = f"asset:{asset.id}"

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
                            next_frontier.add(inlet_ref.asset_id)

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
                            next_frontier.add(outlet_ref.asset_id)

            # Process Dags scheduled by this asset at the Dag level (`schedule=...`). These have
            # no task-level inlet reference and would otherwise be a dead end in this graph --
            # collect them here and resolve every entry point for the round together, below.
            for ref in asset.scheduled_dags:
                if readable_dag_ids is not None and ref.dag_id not in readable_dag_ids:
                    continue
                if ref.dag_id in processed_scheduled_dags:
                    continue
                processed_scheduled_dags.add(ref.dag_id)
                pending_dag_ids.add(ref.dag_id)
                triggering_asset_node_id_by_dag[ref.dag_id] = asset_node_id

        # Route through each Dag's asset_expression (skipping straight to a flat edge for a
        # single-asset schedule) into the Dag's topologically-first task, same as the scheduling
        # graph does for its `dag:` nodes.
        entry_points = _get_dag_entry_points(pending_dag_ids, session)
        for dag_id in pending_dag_ids:
            entry_point = entry_points.get(dag_id)
            if entry_point is None:
                continue
            entry_task_id, asset_expression = entry_point
            task_key = (dag_id, entry_task_id)
            task_node_id = f"task:{dag_id}{SEPARATOR}{entry_task_id}"

            if task_node_id not in nodes_dict:
                nodes_dict[task_node_id] = {
                    "id": task_node_id,
                    "label": f"{dag_id}.{entry_task_id}",
                    "type": "task",
                }

            upstream_nodes: list[dict] = []
            upstream_edges: list[dict] = []
            if asset_expression:
                try:
                    upstream_nodes, upstream_edges = get_upstream_assets(asset_expression, task_node_id)
                except TypeError:
                    log.warning(
                        "Could not expand asset_expression into gate nodes",
                        dag_id=dag_id,
                        exc_info=True,
                    )

            if upstream_nodes:
                for node in upstream_nodes:
                    nodes_dict.setdefault(node["id"], node)
                    # Continue the BFS through any other concrete assets this gate combines
                    # with, so their own producers are traced too.
                    if node["id"].startswith("asset:"):
                        sibling_asset_id = int(node["id"].removeprefix("asset:"))
                        if sibling_asset_id not in processed_assets:
                            next_frontier.add(sibling_asset_id)
                for edge in upstream_edges:
                    edge_set.add((edge["source_id"], edge["target_id"]))
            else:
                # No gate (bare single-asset schedule) -- link directly.
                edge_set.add((triggering_asset_node_id_by_dag[dag_id], task_node_id))

            # Find other assets this entry task produces (outlets) to trace downstream.
            if task_key not in processed_tasks:
                processed_tasks.add(task_key)
                outlet_refs = session.scalars(
                    select(TaskOutletAssetReference).where(
                        TaskOutletAssetReference.dag_id == dag_id,
                        TaskOutletAssetReference.task_id == entry_task_id,
                    )
                ).all()
                for outlet_ref in outlet_refs:
                    if outlet_ref.asset_id not in processed_assets:
                        next_frontier.add(outlet_ref.asset_id)

        frontier = next_frontier

    all_dag_ids = list({dag_id for dag_id, _ in processed_tasks})
    if all_dag_ids:
        dag_id_to_team = DagModel.get_dag_id_to_team_name_mapping(all_dag_ids, session=session)
        for node in nodes_dict.values():
            if not node["id"].startswith("task:"):
                continue
            dag_id = node["id"].removeprefix("task:").split(SEPARATOR, 1)[0]
            team_name = dag_id_to_team.get(dag_id)
            if team_name:
                node["team"] = team_name

    return {
        "nodes": list(nodes_dict.values()),
        "edges": [{"source_id": source, "target_id": target} for source, target in edge_set],
    }
