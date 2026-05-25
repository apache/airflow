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
import asyncio
import time
from collections.abc import Callable

from airflow_google_provider_resource_cleanup import constants as c
from airflow_google_provider_resource_cleanup.commands.cmd_delete import handle_delete
from airflow_google_provider_resource_cleanup.commands.cmd_list import handle_list
from airflow_google_provider_resource_cleanup.commands.cmd_list_asset_types import handle_list_asset_types
from airflow_google_provider_resource_cleanup.commands.cmd_tree import handler_tree

HANDLERS = {
    "list": handle_list,
    "list-asset-types": handle_list_asset_types,
    "tree": handler_tree,
    "delete": handle_delete,
}


def _init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airflow-google-system-test-cleanup",
        description="CLI to manage resources for a GCP project",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Global options
    parser.add_argument("--config-path", help="Direct path to a project config JSON file")
    parser.add_argument("--resources-file-path", help="Direct path to the resources.json file")

    # command: list
    parser_list = subparsers.add_parser(
        "list",
        help="Retrieve the GCP resources for the given GCP project",
    )
    parser_list.add_argument("--project-id", help="", required=True)
    parser_list.add_argument("--asset-type", help="", choices=c.ASSET_TYPE_OPTIONS)
    parser_list.add_argument(
        "--sync",
        action="store_true",
        default=False,
    )

    # command: list-asset-types
    parser_list_asset_types = subparsers.add_parser(
        "list-asset-types",
        help="List all the unique asset types hierarchically in the GCP project",
    )
    parser_list_asset_types.add_argument("--project-id", help="", required=True)
    parser_list_asset_types.add_argument("--asset-type", help="", choices=c.ASSET_TYPE_OPTIONS)

    # command: tree
    parser_tree = subparsers.add_parser(
        "tree",
        help="Show the resources hierarchically as an HTML file",
    )
    parser_tree.add_argument("--project-id", help="", required=True)
    parser_tree.add_argument("--asset-type", help="", choices=c.ASSET_TYPE_OPTIONS)

    # command: cleanup
    parser_delete = subparsers.add_parser(
        "delete",
        help="Delete the resources for the given GCP project",
    )
    parser_delete.add_argument("--project-id", help="", required=True)
    parser_delete.add_argument("--asset-type", help="", choices=c.ASSET_TYPE_OPTIONS)

    parser_delete.add_argument(
        "--min-age-days",
        type=int,
        help="Delete only resources created at least this many days ago",
    )
    parser_delete.add_argument(
        "--skip-asset-type",
        action="append",
        default=[],
        choices=c.ASSET_TYPE_OPTIONS,
        help="Asset type group to skip during deletion. Can be used multiple times.",
    )

    return parser


def main():
    _parser = _init_argparse()
    _args: argparse.Namespace = _parser.parse_args()
    handler: Callable[[argparse.ArgumentParser, argparse.Namespace], None] = HANDLERS[_args.command]
    start_time = time.monotonic()

    try:
        if asyncio.iscoroutinefunction(handler):
            asyncio.run(handler(_parser, _args))
        else:
            handler(_parser, _args)
    except Exception as e:
        print("-" * 10, "EXCEPTION DURING SCRIPT EXECUTION", "-" * 10)
        print(e)
        print("-" * 40)

    end_time = time.monotonic()
    duration = end_time - start_time
    hours, rem = divmod(duration, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if hours > 1:
        parts.append(f"{int(hours)} hours")
    if minutes > 1:
        parts.append(f"{int(minutes)} minutes")
    parts.append(f"{seconds:.2f} seconds")
    print("Script duration after parsing: ", " ".join(parts))


if __name__ == "__main__":
    main()
