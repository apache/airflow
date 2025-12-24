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

import shlex
import subprocess
import sys
import textwrap
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import rich_click as click
from rich.console import Console

if TYPE_CHECKING:
    from fastapi import FastAPI


click.rich_click.COLOR_SYSTEM = "standard"
console = Console(width=400, color_system="standard")

AIRFLOW_ROOT_PATH = Path(__file__).resolve().parents[2]
AIRFLOW_CORE_SOURCES_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "src"
AIRFLOW_PROVIDERS_PATH = AIRFLOW_ROOT_PATH / "providers"
AIRFLOW_DOCS_PATH = AIRFLOW_ROOT_PATH / "docs"
AIRFLOW_DIST_PATH = Path("/dist")


@contextmanager
def ci_group(group_name: str, github_actions: bool):
    if github_actions:
        console.print(f"::group::{textwrap.shorten(group_name, width=200)}", markup=False)
    console.print(group_name, markup=False)
    try:
        yield
    finally:
        if github_actions:
            console.print("::endgroup::")


def run_command(cmd: list[str], github_actions: bool, **kwargs) -> subprocess.CompletedProcess:
    with ci_group(
        f"Running command: {' '.join([shlex.quote(arg) for arg in cmd])}", github_actions=github_actions
    ):
        result = subprocess.run(cmd, **kwargs)  # noqa: PLW1510 - check is handled below and added by callers
    if result.returncode != 0 and github_actions and kwargs.get("check", False):
        console.print(f"[red]Command failed: {' '.join([shlex.quote(entry) for entry in cmd])}[/]")
        console.print("[red]Please unfold the above group and to investigate the issue[/]")
    return result


def generate_openapi_file(app: FastAPI, file_path: Path, prefix: str = "", only_ui: bool = False):
    import yaml
    from fastapi.openapi.utils import get_openapi
    from fastapi.routing import APIRoute

    if only_ui:
        for route in app.routes:
            if not isinstance(route, APIRoute):
                continue
            route.include_in_schema = route.path.startswith("/ui/")

    with file_path.open("w+") as f:
        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            routes=app.routes,
        )
        if prefix:
            openapi_schema["paths"] = {
                prefix + path: path_dict for path, path_dict in openapi_schema["paths"].items()
            }
        yaml.dump(
            openapi_schema,
            f,
            default_flow_style=False,
            sort_keys=False,
        )


def validate_openapi_file(file_path: Path) -> bool:
    import yaml
    from openapi_spec_validator import validate_spec

    with file_path.open() as f:
        openapi_schema = yaml.safe_load(f)
    try:
        validate_spec(openapi_schema)
    except Exception as e:
        print(f"[ERROR] OpenAPI validation failed for {file_path}: {e}", file=sys.stderr)
        sys.exit(1)
    return True


def get_provider_id_from_path(file_path: Path) -> str | None:
    """
    Get the provider id from the path of the file it belongs to.
    """
    for parent in file_path.parents:
        # This works fine for both new and old providers structure - because we moved provider.yaml to
        # the top-level of the provider and this code finding "providers"  will find the "providers" package
        # in old structure and "providers" directory in new structure - in both cases we can determine
        # the provider id from the relative folders
        if (parent / "provider.yaml").exists():
            for providers_root_candidate in parent.parents:
                if providers_root_candidate.name == "providers":
                    return parent.relative_to(providers_root_candidate).as_posix().replace("/", ".")
            return None
    return None


def get_provider_base_dir_from_path(file_path: Path) -> Path | None:
    """
    Get the provider base dir (where provider.yaml is) from the path of the file it belongs to.
    """
    for parent in file_path.parents:
        if (parent / "provider.yaml").exists():
            return parent
    return None
