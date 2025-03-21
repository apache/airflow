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
