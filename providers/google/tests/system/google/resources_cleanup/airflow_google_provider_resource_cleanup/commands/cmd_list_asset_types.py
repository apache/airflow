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
import argparse
import sys
from pathlib import Path
from typing import Any

from airflow_google_provider_resource_cleanup.helpers import get_resources_file, load_json


def __build_hierarchy(assets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """
    Builds a hierarchical tree structure from a flat list of assets.

    Args:
        assets: A list of dictionaries, where each dictionary represents an
                asset with "assetType" and "parentAssetType" keys.

    Returns:
        A dictionary representing the root(s) of the asset hierarchy.
    """
    # A dictionary to hold all asset types as nodes in our tree
    # The key is the assetType, and the value is the node object.
    nodes: dict[str, dict[str, Any]] = {}

    # A set to keep track of which asset types are children.
    # This helps us easily find the root node(s) later.
    child_asset_types: set[str] = set()

    # First pass: Create a node for every unique asset type found.
    for item in assets:
        asset_type = item["assetType"]
        parent_type = item.get("parentAssetType", "N/A")
        child_asset_types.add(asset_type)

        # Ensure a node exists for both the child and parent
        if asset_type not in nodes:
            nodes[asset_type] = {"children": {}}
        if parent_type not in nodes:
            nodes[parent_type] = {"children": {}}

    # Second pass: Link the children to their parents.
    for item in assets:
        asset_type = item["assetType"]
        parent_type = item.get("parentAssetType", "N/A")

        # Get the parent node and add the current asset as a child
        parent_node = nodes[parent_type]
        child_node = nodes[asset_type]
        parent_node["children"][asset_type] = child_node

    # Third pass: Identify the root(s) of the tree.
    # A root is any node that is never listed as a child.
    hierarchy: dict[str, dict[str, Any]] = {}
    for asset_type, node in nodes.items():
        if asset_type not in child_asset_types:
            hierarchy[asset_type] = node

    return hierarchy


def _get_asset_hierarchy(resource_file: Path) -> dict[str, dict[str, Any]]:
    resources = load_json(resource_file)
    if not isinstance(resources, list):
        raise TypeError(f'Expected resources file "{resource_file}" to contain a JSON list.')
    return __build_hierarchy(resources)


def _print_tree(nodes: dict[str, dict[str, Any]], prefix: str = ""):
    """
    Prints the hierarchical dictionary in a tree-like format.

    Args:
        nodes: The dictionary representing the hierarchy to print.
        prefix: The prefix string used for indentation and tree lines.
    """
    # Get a list of the keys to know which one is the last
    items = sorted(list(nodes.keys()))
    for i, key in enumerate(items):
        # Check if this is the last item in the list
        is_last = i == (len(items) - 1)

        # Print the current node with appropriate connectors
        print(prefix + ("└── " if is_last else "├── ") + key)

        # Prepare the prefix for the children
        child_prefix = prefix + ("    " if is_last else "│   ")

        # Recursively print the children
        children = nodes[key].get("children", {})
        if children:
            _print_tree(children, child_prefix)


def handle_list_asset_types(_: argparse.ArgumentParser, args: argparse.Namespace):
    project_id = args.project_id
    asset_type = args.asset_type

    resource_file = get_resources_file(project_id, asset_type, resource_file_path=args.resources_file_path)

    try:
        asset_types = _get_asset_hierarchy(resource_file)
        print("=" * 200)
        _print_tree(asset_types)
    except FileNotFoundError:
        print(f'The resource file "{resource_file}" cannot be found! Exiting...')
        sys.exit(1)
