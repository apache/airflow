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

import asyncio
import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import airflow_google_provider_resource_cleanup.constants as c


def run_command(cmd: str, log_prefix: str = ""):
    print(f'{log_prefix}Executing the command: "{cmd}"...')
    os.system(cmd)


async def run_command_async(cmd: str, log_prefix: str = ""):
    print(f'{log_prefix}Executing the command: "{cmd}"...')
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    print(f"{log_prefix}Command exited with code: {process.returncode}")
    if stdout:
        print(f"{log_prefix}Stdout:\n{stdout.decode().strip()}")
    if stderr:
        print(f"{log_prefix}Stderr:\n{stderr.decode().strip()}")


def get_resources_file(
    system_tests_project, resource_type: str | None = None, resource_file_path: Path | None = None
):
    if resource_file_path:
        return Path(resource_file_path)

    if resource_type is None:
        resources_file = c.RESOURCES_FOLDER / system_tests_project / "resources.json"
    else:
        resources_file = c.RESOURCES_FOLDER / system_tests_project / f"{resource_type}.json"

    return resources_file


def init_directories():
    directories = [c.RESOURCES_FOLDER, c.OUTPUT_FOLDER]
    for directory in directories:
        ensure_path(directory)


def ensure_path(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)


JsonData = dict[str, Any] | list[Any]


def load_json(file_path: Path) -> JsonData:
    with file_path.open() as file:
        return json.load(file)


def load_project_configuration(project_id) -> dict[str, Any]:
    config_file = c.CONFIG_FOLDER / f"{project_id}.json"
    data = load_json(config_file)
    if not isinstance(data, dict):
        raise TypeError(f'Expected project configuration "{config_file}" to be a JSON object.')
    return data


def check_white_list(resource: dict, config: GCPProjectConfig, silent: bool = False) -> bool:
    asset_type = resource.get("assetType", "N/a")

    if asset_type in c.DO_NOT_DELETE_ASSET_TYPES:
        if not silent:
            print(f'The asset type: "{asset_type}" is protected. Ignoring!')
        return False

    name = resource.get("name", "N/a")

    protected_resources = config.protected_resources.get(asset_type, [])
    if name in protected_resources:
        if not silent:
            print(f'The resource: "{name}" is protected. Ignoring!')
        return False

    labels = resource.get("labels", {})
    if labels:
        for label_key, label_value in labels.items():
            if label_key in c.DO_NOT_DELETE_LABELS or label_value in c.DO_NOT_DELETE_LABELS:
                if not silent:
                    print(
                        f'The label: "{label_key}={label_value}" is protected for resource "{name}". Ignoring!'
                    )
                return False

    return True


def get_aiplatform_client_options(resource: dict):
    location = resource.get("location")
    api_endpoint = f"{location}-aiplatform.googleapis.com"
    client_options = {"api_endpoint": api_endpoint}
    return client_options


def get_resource_path(resource: dict) -> str:
    raw_name = resource.get("name")
    if not isinstance(raw_name, str):
        raise TypeError("Resource name must be a string.")
    _, name = raw_name.split("googleapis.com/")
    return name


def get_resource_name_for_compute(resource: dict) -> str:
    resource_path = resource.get("name")
    if not isinstance(resource_path, str):
        raise TypeError("Resource name must be a string.")
    return resource_path.replace("//compute.googleapis.com/", "")


async def curl(url, method="DELETE", log_prefix=""):
    cmd = f"""
        curl -X {method} \
            -H "Authorization: Bearer $(gcloud auth print-access-token)" \
            "{url}"
    """
    try:
        await run_command_async(cmd)
    except Exception as e:
        print(f'{log_prefix}Error while running the curl command: "{cmd}"!', e)


def dump_json(file_path: Path, data: JsonData) -> None:
    with file_path.open("w") as file:
        json.dump(data, file)


@dataclass
class GCPProjectConfig:
    """
    JSON config representation for a GCP project.

    Example::

        {
            "project_id": "",
            "protected_resources": {
                "<asset_type>": ["<resource_name>", "<resource_name>"],
                "storage.googleapis.com/Bucket": ["//storage.googleapis.com/example-system-test-bucket"],
            },
        }

    :param project_id: The ID of the GCP project.
    :param protected_resources: Asset types mapped to resource names that should not be deleted.
    :param default_location: The default GCP location to use for resources.
    """

    project_id: str
    protected_resources: dict[str, list[str]] = field(default_factory=dict)
    default_location: str = "us-central1"

    @classmethod
    def load_from_file(cls, project_id, config_path: Path | None = None):
        file = Path(config_path) if config_path else c.CONFIG_FOLDER / f"{project_id}.json"
        data = load_json(file) if file.exists() else {}
        if not isinstance(data, dict):
            raise TypeError(f'Expected project configuration "{file}" to be a JSON object.')
        if "project_id" in data:
            del data["project_id"]  # override if the project_id defined in the json file
        cfg = GCPProjectConfig(project_id=project_id, **data)

        # Only dump if we are using the default path or if we want to ensure the custom one exists
        # To be safe, we always ensure parent exists and dump it.
        ensure_path(file.parent)
        dump_json(file, asdict(cfg))
        return cfg


def get_auxiliary_asset_data_item(
    project_id, asset_type="vertex_ai_raycluster", config_path: Path | None = None
):
    cfg = GCPProjectConfig.load_from_file(project_id, config_path=config_path)
    return {
        "assetType": asset_type,
        "project": project_id,
        "location": cfg.default_location,
    }


def provide_auxiliary_asset_resource_file(
    project_id,
    asset_type,
    resource_file_path: Path | None = None,
    config_path: Path | None = None,
):
    resources_file = get_resources_file(project_id, asset_type, resource_file_path=resource_file_path)
    ensure_path(resources_file.parent)
    dump_json(
        resources_file,
        [get_auxiliary_asset_data_item(project_id, asset_type, config_path=config_path)],
    )
