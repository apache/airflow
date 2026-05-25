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
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, PackageLoader, Template

import airflow_google_provider_resource_cleanup.constants as c
from airflow_google_provider_resource_cleanup.helpers import (
    GCPProjectConfig,
    check_white_list,
    get_resources_file,
    init_directories,
    load_json,
    run_command,
)

_env = Environment(loader=PackageLoader("airflow_google_provider_resource_cleanup", "templates"))


def _format_node_for_template(node_details: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively formats node data for the template, including sorting children."""
    data = node_details["data"]
    formatted_node = {
        "display_name": data.get("name", "N/A"),
        "asset_type_short": data.get("type", "N/A").split("/")[-1],
        "resource_id": data.get("id", "N/A"),
        "location": data.get("location", "N/A"),
        "is_protected": data.get("is_protected", False),
        "children": [],
    }

    # Sort children by display name and recurse
    sorted_child_keys = sorted(
        node_details["children"].keys(),
        key=lambda k: node_details["children"][k]["data"].get("name", "N/A"),
    )

    for child_key in sorted_child_keys:
        formatted_node["children"].append(_format_node_for_template(node_details["children"][child_key]))

    return formatted_node


def _build_resource_tree(resources_data: Sequence[Dict[str, Any]], cfg: GCPProjectConfig) -> List[Dict[str, Any]]:
    """Builds a hierarchical tree of GCP resources sorted and formatted for templates."""
    all_nodes: dict[str, dict[str, Any]] = {}
    all_resource_names: set[str] = set()
    child_resource_names: set[str] = set()

    # Pass 1: Create nodes
    for resource in resources_data:
        resource_name = resource.get("name")
        if not resource_name:
            continue

        all_nodes[resource_name] = {
            "data": {
                "name": resource.get("displayName", resource_name.split("/")[-1]),
                "type": resource.get("assetType", "N/a"),
                "id": resource.get("project", resource_name.split("/")[-1]),
                "location": resource.get("location", "N/a"),
                "is_protected": not check_white_list(resource, cfg, silent=True),
            },
            "children": {},
        }
        all_resource_names.add(resource_name)

        if parent := resource.get("parentFullResourceName"):  # noqa: F841
            child_resource_names.add(resource_name)

    # Pass 2: Link parents
    for resource in resources_data:
        res_name = resource.get("name")
        parent_name = resource.get("parentFullResourceName")

        if not isinstance(res_name, str):
            continue

        if parent_name and res_name in all_nodes:
            if parent_name not in all_nodes:
                # Placeholder for missing parent
                parent_parts = parent_name.split("/")
                parent_type = "Unknown"
                if len(parent_parts) > 1:
                    parent_type = parent_parts[-2].capitalize()
                resource_type = f"cloudresourcemanager.googleapis.com/{parent_type}"

                all_nodes[parent_name] = {
                    "data": {
                        "name": parent_name.split("/")[-1],
                        "type": resource_type,
                        "id": parent_name.split("/")[-1],
                        "location": "unk",
                        "is_protected": True,
                    },
                    "children": {},
                }
                all_resource_names.add(parent_name)
            all_nodes[parent_name]["children"][res_name] = all_nodes[res_name]

    # Identify top-level and format
    top_level_names = sorted(
        all_resource_names - child_resource_names,
        key=lambda name: all_nodes[name]["data"].get("name", "N/A"),
    )

    return [_format_node_for_template(all_nodes[name]) for name in top_level_names]


def print_tree(nodes_list: List[Dict[str, Any]], indent: int = 0) -> None:
    if not nodes_list:
        return

    for node in nodes_list:
        indent_str = "\t" * indent
        display_name = node["display_name"]
        asset_type = node["asset_type_short"]
        location = node["location"]
        protected_str = " (Protected)" if node["is_protected"] else ""

        print(f"{indent_str}- {display_name} ({asset_type} @ {location}){protected_str}")

        if node["children"]:
            print_tree(node["children"], indent + 1)


def _get_html_template(resource_type: Optional[str] = None) -> Template:
    if resource_type is None:
        template_file_name = "resources.html"
    else:
        template_file_name = "resource_name.html"
    return _env.get_template(template_file_name)


def _get_html_file(system_tests_project: str, resource_type: Optional[str] = None) -> Path:
    if resource_type is None:
        html_file = c.OUTPUT_FOLDER / system_tests_project / "index.html"
    else:
        html_file = c.OUTPUT_FOLDER / system_tests_project / resource_type / "index.html"

    html_file.parent.mkdir(parents=True, exist_ok=True)
    return html_file


def handler_tree(_: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    init_directories()
    project_id = args.project_id
    asset_type = args.asset_type

    resource_file = get_resources_file(project_id, asset_type, resource_file_path=args.resources_file_path)
    if not resource_file.exists():
        print(f'The resource file "{resource_file}" cannot be found! Exiting...')
        exit(1)

    cfg = GCPProjectConfig.load_from_file(project_id, config_path=args.config_path)
    data = load_json(resource_file)
    if not isinstance(data, list):
        raise TypeError(f'Expected resources file "{resource_file}" to contain a JSON list.')
    resource_tree = _build_resource_tree(data, cfg)

    html_template = _get_html_template(asset_type)
    html_content = html_template.render(
        project_name=project_id,
        resource_type=asset_type if asset_type is not None else "Resource",
        resource_tree=resource_tree,
    )

    html_file = _get_html_file(project_id, asset_type)
    with html_file.open("w") as f:
        f.write(html_content)

    print(f"The output file created: {str(html_file)}! Opening...")
    run_command(f"open {html_file.absolute()}")
