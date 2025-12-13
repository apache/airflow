#!/usr/bin/env python3
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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import argparse
import json
from pathlib import Path

from rich import print
from rich.prompt import Confirm

# Debug port mappings for Airflow components
DEBUG_PORTS = {
    "scheduler": 50231,
    "dag-processor": 50232,
    "triggerer": 50233,
    "api-server": 50234,
    "celery-worker": 50235,
    "edge-worker": 50236,
}

# Component descriptions for better naming
COMPONENT_NAMES = {
    "scheduler": "Scheduler",
    "dag-processor": "DAG Processor",
    "triggerer": "Triggerer",
    "api-server": "API Server",
    "celery-worker": "Celery Worker",
    "edge-worker": "Edge Worker",
}

ROOT_AIRFLOW_FOLDER_PATH = Path(__file__).parent
VSCODE_FOLDER_PATH = ROOT_AIRFLOW_FOLDER_PATH / ".vscode"
LAUNCH_JSON_FILE = VSCODE_FOLDER_PATH / "launch.json"
MCP_JSON_FILE = VSCODE_FOLDER_PATH / "mcp.json"


def create_debug_configuration(component: str, port: int) -> dict:
    """Create a debug configuration for a specific Airflow component."""
    return {
        "name": f"Debug Airflow {COMPONENT_NAMES[component]}",
        "type": "debugpy",
        "request": "attach",
        "justMyCode": False,
        "connect": {"host": "localhost", "port": port},
        "pathMappings": [{"localRoot": "${workspaceFolder}", "remoteRoot": "/opt/airflow"}],
    }


def create_launch_json_content() -> dict:
    """Create the complete launch.json content with all debug configurations."""
    configurations = []

    for component, port in DEBUG_PORTS.items():
        config = create_debug_configuration(component, port)
        configurations.append(config)

    return {"version": "0.2.0", "configurations": configurations}


def create_mcp_json_content() -> dict:
    """Create the MCP configuration with Chakra UI server."""
    return {"servers": {"chakra-ui": {"command": "npx", "args": ["-y", "@chakra-ui/react-mcp"]}}}


def setup_mcp():
    """Set up MCP configuration for Chakra UI."""
    print("[green]Creating[/] MCP configuration for Chakra UI...")

    # Create the mcp.json content
    mcp_json_content = create_mcp_json_content()

    # Ensure .vscode directory exists
    VSCODE_FOLDER_PATH.mkdir(exist_ok=True)

    # Write the mcp.json file
    with open(MCP_JSON_FILE, "w") as f:
        json.dump(mcp_json_content, f, indent=4)

    print(f"[green]Successfully created[/] {MCP_JSON_FILE}")


def setup_vscode():
    """Set up VSCode debug configurations for Airflow components."""
    print("[green]Creating[/] VSCode debug configurations for Airflow components...")

    # Create configurations for each component
    for component, port in DEBUG_PORTS.items():
        print(f"[green]Adding[/] debug configuration: [blue]{COMPONENT_NAMES[component]}[/] (port {port})")

    # Create the launch.json content
    launch_json_content = create_launch_json_content()

    # Ensure .vscode directory exists
    VSCODE_FOLDER_PATH.mkdir(exist_ok=True)

    # Write the launch.json file
    with open(LAUNCH_JSON_FILE, "w") as f:
        json.dump(launch_json_content, f, indent=4)

    print(f"\n[green]Successfully created[/] {LAUNCH_JSON_FILE}")


def main():
    parser = argparse.ArgumentParser(
        description="Set up VSCode debug configurations for Airflow components. Also comes with optional VSCode configurations such as chakra MCP sercer"
        "such as Chakra UI MCP support.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--with-chakra-mcp",
        action="store_true",
        help="Also create a VSCode MCP configuration for enhanced Chakra UI development support",
    )
    args = parser.parse_args()

    print("\n[yellow]VSCode Airflow Debug Configuration Setup[/]\n")
    print("This script will create VSCode debug configurations for Airflow components:\n")

    for component, port in DEBUG_PORTS.items():
        print(f"* {COMPONENT_NAMES[component]}: port {port}")

    print(f"\nConfiguration will be written to: {LAUNCH_JSON_FILE}")
    if args.with_chakra_mcp:
        print(f"MCP configuration will be written to: {MCP_JSON_FILE}")

    # Check if files exist and prompt for overwrite
    files_exist = []
    if LAUNCH_JSON_FILE.exists():
        files_exist.append(str(LAUNCH_JSON_FILE))
    if args.with_chakra_mcp and MCP_JSON_FILE.exists():
        files_exist.append(str(MCP_JSON_FILE))

    if files_exist:
        print("\n[yellow]Warning:[/] The following files already exist:")
        for file_path in files_exist:
            print(f"  - {file_path}")
        should_overwrite = Confirm.ask("Overwrite the existing files?")
        if not should_overwrite:
            print("[yellow]Skipped[/] - No changes made")
            return
    else:
        should_continue = Confirm.ask("Create the configurations?")
        if not should_continue:
            print("[yellow]Skipped[/] - No changes made")
            return

    setup_vscode()

    if args.with_chakra_mcp:
        setup_mcp()

    print("\n[green]Setup complete![/]")
    print("\nFor more information, see: contributing-docs/20_debugging_airflow_components.rst")
    if args.with_chakra_mcp:
        print("MCP server for Chakra UI has been configured for enhanced development experience.")


if __name__ == "__main__":
    main()
