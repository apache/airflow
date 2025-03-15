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

from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAll, AssetAny, BaseAsset


def get_upstream_assets(
    asset_condition: BaseAsset, entry_node_ref: str, level=0
) -> tuple[list[dict], list[dict]]:
    edges: list[dict] = []
    nodes: list[dict] = []
    asset_condition_type: str | None = None

    assets: list[Asset | AssetAlias] = []

    nested_expression: AssetAll | AssetAny | None = None

    if isinstance(asset_condition, AssetAny):
        asset_condition_type = "or-gate"

    elif isinstance(asset_condition, AssetAll):
        asset_condition_type = "and-gate"

    if hasattr(asset_condition, "objects"):
        for obj in asset_condition.objects:
            if isinstance(obj, (AssetAll, AssetAny)):
                nested_expression = obj
            elif isinstance(obj, (Asset, AssetAlias)):
                assets.append(obj)
            else:
                raise TypeError(f"Unsupported type: {type(obj)}")

    if asset_condition_type and assets:
        asset_condition_id = f"{asset_condition_type}-{level}"
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
                "asset_condition_type": asset_condition_type,
            }
        )

        for asset in assets:
            edges.append(
                {
                    "source_id": asset.name,
                    "target_id": asset_condition_id,
                }
            )
            nodes.append(
                {
                    "id": asset.name,
                    "label": asset.name,
                    "type": "asset-alias" if isinstance(asset, AssetAlias) else "asset",
                }
            )

        if nested_expression is not None:
            n, e = get_upstream_assets(nested_expression, asset_condition_id, level=level + 1)

            nodes = nodes + n
            edges = edges + e

    return nodes, edges
