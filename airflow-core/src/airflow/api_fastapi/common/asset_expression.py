#
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
"""Authorization-aware handling of the ``DagModel.asset_expression`` tree served by the API."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any


def redact_asset_expression(
    expression: dict[str, Any] | None, *, readable_asset_ids: Collection[int]
) -> dict[str, Any] | None:
    """
    Return a copy of an asset scheduling expression with the assets the caller may not read hidden.

    ``DagModel.asset_expression`` names every upstream asset of a Dag, so any endpoint that serves it
    must scope it to the caller the same way the asset list endpoints do. An ``asset`` leaf whose id is
    not in ``readable_asset_ids`` keeps its place in the boolean tree (so the shape of the schedule is
    still honest) but has its identifying fields blanked and ``hidden`` set. A leaf without an id, such
    as a row not yet re-enriched by the Dag processor, cannot be authorized and is hidden as well.

    ``alias`` and ``asset_ref`` leaves are returned unchanged: the auth manager exposes no batch
    authorization for aliases, and references are unresolved names by design.

    The input is never mutated: the value lives on an ORM instance whose session commits on exit.
    """
    if expression is None:
        return None
    return _redact_node(expression, readable_asset_ids)


def _redact_node(node: Any, readable_asset_ids: Collection[int]) -> Any:
    if not isinstance(node, dict):
        # Legacy pre-3.0 shapes hold bare strings; ``MaybeAssetExpression`` drops those later.
        return node
    if "asset" in node:
        asset = node["asset"]
        if not isinstance(asset, dict):
            return node
        if asset.get("id") in readable_asset_ids:
            return {"asset": dict(asset)}
        return {"asset": {"uri": None, "name": None, "group": asset.get("group"), "id": None, "hidden": True}}
    for key in ("all", "any"):
        if key in node and isinstance(node[key], list):
            return {key: [_redact_node(child, readable_asset_ids) for child in node[key]]}
    return node
