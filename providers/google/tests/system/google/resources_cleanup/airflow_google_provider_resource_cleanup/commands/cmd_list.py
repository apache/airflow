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
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import airflow_google_provider_resource_cleanup.constants as c
from airflow_google_provider_resource_cleanup.helpers import (
    GCPProjectConfig,
    check_white_list,
    dump_json,
    ensure_path,
    get_resources_file,
    init_directories,
    load_json,
    run_command,
)

DATAFLOW_JOB_ASSET_TYPE = "dataflow.googleapis.com/Job"


def _get_archived_dataflow_job_names(project_id: str) -> set[str]:
    command = [
        "gcloud",
        "asset",
        "search-all-resources",
        f"--scope=projects/{project_id}",
        f"--asset-types={DATAFLOW_JOB_ASSET_TYPE}",
        "--read-mask=name,versionedResources",
        "--filter=versionedResources.resource.jobMetadata.userDisplayProperties.archived=true",
        "--format=value(name)",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return {name for name in result.stdout.splitlines() if name}


def _remove_archived_dataflow_jobs(project_id: str, resource_file: Path) -> None:
    resources = load_json(resource_file)
    if not isinstance(resources, list):
        raise TypeError(f'Expected resources file "{resource_file}" to contain a JSON list.')

    archived_job_names = _get_archived_dataflow_job_names(project_id)
    resources = [resource for resource in resources if resource.get("name") not in archived_job_names]
    dump_json(resource_file, resources)
    print(f"Excluded {len(archived_job_names)} already archived Dataflow job(s).")


def _sync_resources(system_tests_project, resource_file: Path, resource_type: str | None = None):
    if resource_type is None:
        cmd = (
            f"gcloud asset search-all-resources --scope projects/{system_tests_project} "
            f'--format json > "{str(resource_file)}"'
        )
    else:
        asset_type = c.ASSET_TYPES[resource_type]
        cmd = (
            f"gcloud asset search-all-resources --scope projects/{system_tests_project} "
            f'--asset-types="{asset_type}" --format json > "{str(resource_file)}"'
        )

    run_command(cmd)
    if resource_type in (None, "dataflow"):
        _remove_archived_dataflow_jobs(system_tests_project, resource_file)


def _print_resources(resources_file, cfg: GCPProjectConfig):
    resources = load_json(resources_file)
    if not isinstance(resources, list):
        raise TypeError(f'Expected resources file "{resources_file}" to contain a JSON list.')

    counter = 0
    ignored_counter = 0
    for resource in resources:
        if not check_white_list(resource, cfg):
            ignored_counter += 1
            continue

        counter += 1
        name = resource.get("name", "N/a")
        asset_type = resource.get("assetType", "N/a")
        location = resource.get("location", "N/a")
        print(f"({name})", f"@{location}", asset_type, sep=" >--< ")
        labels = resource.get("labels", {})
        if labels:
            print("\tLabels:")
            for label_key, label_value in labels.items():
                print(f'\t\t{label_key}="{label_value}"')

    print("-" * 120)
    print("Resource Count         :", counter)
    print("Ignored Resource Count :", ignored_counter)
    print("Total Count            :", counter + ignored_counter)


def handle_list(_: argparse.ArgumentParser, args: argparse.Namespace):
    init_directories()
    project_id = args.project_id
    asset_type = args.asset_type
    sync = args.sync

    resource_file = get_resources_file(project_id, asset_type, resource_file_path=args.resources_file_path)
    cfg = GCPProjectConfig.load_from_file(project_id, config_path=args.config_path)

    if sync:
        ensure_path(resource_file.parent)
        _sync_resources(project_id, resource_file, asset_type)
        _print_resources(resource_file, cfg)
    else:
        try:
            _print_resources(resource_file, cfg)
        except FileNotFoundError:
            print(f"Resource file not found: {resource_file}. Syncing resources...")
            ensure_path(resource_file.parent)
            _sync_resources(project_id, resource_file, asset_type)
            _print_resources(resource_file, cfg)
