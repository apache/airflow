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
