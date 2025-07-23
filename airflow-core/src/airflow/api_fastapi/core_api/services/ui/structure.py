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

"""
Private service for dag structure.

:meta private:
"""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.models.asset import AssetAliasModel, AssetEvent
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel


def get_upstream_assets(
    asset_expression: dict, entry_node_ref: str, level: int = 0
) -> tuple[list[dict], list[dict]]:
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
            else:
                raise TypeError(f"Unsupported type: {expr.keys()}")

    if asset_expression_type and assets_info:
        asset_condition_id = f"{asset_expression_type}-{level}"
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
            asset_type = asset["type"]

            if asset_type == "asset":
                source_id = str(asset["id"])
                label = asset["name"]
            elif asset_type == "asset-alias" or asset_type == "asset-name-ref":
                source_id = asset["name"]
                label = asset["name"]
            elif asset_type == "asset-uri-ref":
                source_id = asset["uri"]
                label = asset["uri"]
            else:
                raise TypeError(f"Unsupported type: {asset_type}")

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
                    "type": asset_type,
                }
            )

        if nested_expression is not None:
            n, e = get_upstream_assets(nested_expression, asset_condition_id, level=level + 1)

            nodes = nodes + n
            edges = edges + e

    return nodes, edges


def bind_output_assets_to_tasks(
    edges: list[dict], serialized_dag: SerializedDagModel, version_number: int, session: Session
) -> None:
    """
    Try to bind the downstream assets to the relevant task that produces them.

    This function will mutate the `edges` in place.
    """
    # bind normal assets present in the `task_outlet_asset_references`
    outlet_asset_references = serialized_dag.dag_model.task_outlet_asset_references

    downstream_asset_edges = [
        edge
        for edge in edges
        if edge["target_id"].startswith("asset:") and not edge.get("resolved_from_alias")
    ]

    for edge in downstream_asset_edges:
        # Try to attach the outlet assets to the relevant tasks
        asset_id = int(edge["target_id"].replace("asset:", "", 1))
        outlet_asset_reference = next(
            outlet_asset_reference
            for outlet_asset_reference in outlet_asset_references
            if outlet_asset_reference.asset_id == asset_id
        )
        edge["source_id"] = outlet_asset_reference.task_id

    # bind assets resolved from aliases, they do not populate the `outlet_asset_references`
    downstream_alias_resolved_edges = [
        edge for edge in edges if edge["target_id"].startswith("asset:") and edge.get("resolved_from_alias")
    ]

    aliases_names = {edges["resolved_from_alias"] for edges in downstream_alias_resolved_edges}

    result = session.scalars(
        select(AssetEvent)
        .join(AssetEvent.source_aliases)
        .join(AssetEvent.source_dag_run)
        # That's a simplification, instead doing `version_number` in `DagRun.dag_versions`.
        .join(DagRun.created_dag_version)
        .where(AssetEvent.source_aliases.any(AssetAliasModel.name.in_(aliases_names)))
        .where(AssetEvent.source_dag_run.has(DagRun.dag_id == serialized_dag.dag_model.dag_id))
        .where(DagVersion.version_number == version_number)
    ).unique()

    asset_id_to_task_ids = defaultdict(set)
    for asset_event in result:
        asset_id_to_task_ids[asset_event.asset_id].add(asset_event.source_task_id)

    for edge in downstream_alias_resolved_edges:
        asset_id = int(edge["target_id"].replace("asset:", "", 1))
        task_ids = asset_id_to_task_ids.get(asset_id, set())

        for index, task_id in enumerate(task_ids):
            if index == 0:
                edge["source_id"] = task_id
                continue
            edge_copy = {**edge, "source_id": task_id}
            edges.append(edge_copy)
