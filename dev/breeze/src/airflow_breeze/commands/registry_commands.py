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

import json
import sys
import uuid
from pathlib import Path

import click

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.common_options import option_dry_run, option_python, option_verbose
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.docker_command_utils import execute_command_in_shell, fix_ownership_using_docker
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command


@click.group(cls=BreezeGroup, name="registry", help="Tools for the Airflow Provider Registry")
def registry_group():
    pass


@registry_group.command(
    name="extract-data",
    help="Extract provider metadata, parameters, and connection types for the registry.",
)
@option_python
@click.option(
    "--provider",
    default=None,
    help="Extract only this provider ID (e.g. 'amazon'). Omit for full build.",
)
@option_verbose
@option_dry_run
def extract_data(python: str, provider: str | None):
    unique_project_name = f"breeze-registry-{uuid.uuid4().hex[:8]}"

    shell_params = ShellParams(
        python=python,
        project_name=unique_project_name,
        quiet=True,
        skip_environment_initialization=True,
        extra_args=(),
    )

    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)

    provider_flag = f" --provider '{provider}'" if provider else ""
    command = (
        f"python dev/registry/extract_metadata.py{provider_flag} && "
        "python dev/registry/extract_parameters.py && "
        "python dev/registry/extract_connections.py"
    )

    with ci_group("Extracting registry data"):
        result = execute_command_in_shell(
            shell_params=shell_params,
            project_name=unique_project_name,
            command=command,
            preserve_backend=True,
        )

    fix_ownership_using_docker()
    sys.exit(result.returncode)


@registry_group.command(
    name="publish-versions",
    help="Publish per-provider versions.json to S3 from deployed directories. "
    "Same pattern as 'breeze release-management publish-docs-to-s3'.",
)
@click.option(
    "--s3-bucket",
    required=True,
    envvar="S3_BUCKET",
    help="S3 bucket URL, e.g. s3://staging-docs-airflow-apache-org/registry/",
)
@click.option(
    "--providers-json",
    default=None,
    type=click.Path(exists=True),
    help="Path to providers.json. Auto-detected if not provided.",
)
def publish_versions(s3_bucket: str, providers_json: str | None):
    from airflow_breeze.utils.publish_registry_versions import publish_versions as _publish_versions

    providers_path = Path(providers_json) if providers_json else None
    _publish_versions(s3_bucket, providers_json_path=providers_path)


PROVIDERS_DIR = AIRFLOW_ROOT_PATH / "providers"
DEV_REGISTRY_DIR = AIRFLOW_ROOT_PATH / "dev" / "registry"

PROVIDERS_JSON_PATH = DEV_REGISTRY_DIR / "providers.json"

EXTRACT_SCRIPTS = [
    DEV_REGISTRY_DIR / "extract_parameters.py",
    DEV_REGISTRY_DIR / "extract_connections.py",
]


def _find_provider_yaml(provider_id: str) -> Path:
    """Find provider.yaml for a given provider ID (e.g. 'amazon', 'apache-beam', 'microsoft-azure')."""
    # Provider ID uses hyphens; directory structure uses slashes (e.g. microsoft-azure -> microsoft/azure)
    parts = provider_id.split("-")
    # Try nested first (e.g. 'microsoft/azure'), then single directory (e.g. 'amazon')
    candidates = [PROVIDERS_DIR / provider_id / "provider.yaml"]
    if len(parts) >= 2:
        candidates.insert(0, PROVIDERS_DIR / "/".join(parts) / "provider.yaml")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise click.ClickException(
        f"provider.yaml not found for '{provider_id}'. Tried: {', '.join(str(c) for c in candidates)}"
    )


def _read_provider_yaml_info(provider_id: str) -> tuple[str, list[str]]:
    """Read package name from provider.yaml and extras from pyproject.toml."""
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib

    import yaml

    provider_yaml_path = _find_provider_yaml(provider_id)
    with open(provider_yaml_path) as f:
        data = yaml.safe_load(f)
    package_name = data["package-name"]

    pyproject = provider_yaml_path.parent / "pyproject.toml"
    extras: list[str] = []
    if pyproject.exists():
        with open(pyproject, "rb") as f:
            toml_data = tomllib.load(f)
        optional_deps = toml_data.get("project", {}).get("optional-dependencies", {})
        extras = sorted(optional_deps.keys())

    return package_name, extras


def _build_pip_spec(package_name: str, extras: list[str], version: str) -> str:
    """Build pip install spec, e.g. 'apache-airflow-providers-amazon[pandas,s3fs]==9.21.0'."""
    if extras:
        extras_str = ",".join(extras)
        return f"{package_name}[{extras_str}]=={version}"
    return f"{package_name}=={version}"


def _ensure_providers_json(provider_id: str, package_name: str) -> Path:
    """Ensure dev/registry/providers.json exists with the target provider.

    The extraction scripts read this to determine which version to tag output with.
    If it exists (from a previous extract-data or S3 download), use it.
    If the provider is missing from an existing file, append it rather than replacing.

    NOTE: Does NOT touch registry/src/_data/providers.json, which is used by
    the Eleventy build and must contain all providers.
    """
    PROVIDERS_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    if PROVIDERS_JSON_PATH.exists():
        with open(PROVIDERS_JSON_PATH) as f:
            data = json.load(f)
        if any(p["id"] == provider_id for p in data.get("providers", [])):
            return PROVIDERS_JSON_PATH
        # Provider not in file — append it rather than replacing
        data["providers"].append({"id": provider_id, "package_name": package_name, "version": "0.0.0"})
        click.echo(f"Added {provider_id} to existing {PROVIDERS_JSON_PATH}")
    else:
        data = {"providers": [{"id": provider_id, "package_name": package_name, "version": "0.0.0"}]}
        click.echo(f"Created minimal {PROVIDERS_JSON_PATH}")

    with open(PROVIDERS_JSON_PATH, "w") as f:
        json.dump(data, f, indent=2)
    return PROVIDERS_JSON_PATH


def _patch_providers_json(providers_json_path: Path, provider_id: str, version: str) -> str:
    """Patch providers.json to set the target version. Returns the original version."""
    with open(providers_json_path) as f:
        data = json.load(f)
    for p in data["providers"]:
        if p["id"] == provider_id:
            original_version = p["version"]
            p["version"] = version
            with open(providers_json_path, "w") as f:
                json.dump(data, f, indent=2)
            return original_version
    raise click.ClickException(f"Provider '{provider_id}' not found in {providers_json_path}")


# TODO: The backfill command processes versions sequentially because extract_parameters.py
# and extract_connections.py write to shared files (modules.json, providers.json).
# To parallelize, each provider would need its own isolated output directory so that
# concurrent runs don't clobber each other. See also the registry-backfill.yml workflow
# which uses a GitHub Actions matrix to run providers in parallel CI jobs.


@registry_group.command(
    name="backfill",
    help="Extract runtime parameters and connections for older provider versions. "
    "Uses 'uv run --with' to install the specific version in a temporary environment "
    "and runs extract_parameters.py + extract_connections.py. No Docker needed.",
)
@click.option(
    "--provider",
    required=True,
    help="Provider ID (e.g. 'amazon', 'google', 'microsoft-azure').",
)
@click.option(
    "--version",
    "versions",
    required=True,
    multiple=True,
    help="Version(s) to extract. Can be specified multiple times: --version 9.21.0 --version 9.20.0",
)
@option_verbose
@option_dry_run
def backfill(provider: str, versions: tuple[str, ...]):
    package_name, extras = _read_provider_yaml_info(provider)
    providers_json_path = _ensure_providers_json(provider, package_name)

    click.echo(f"Provider: {provider} ({package_name})")
    click.echo(f"Versions: {', '.join(versions)}")
    if extras:
        click.echo(f"Extras: {', '.join(extras)}")
    click.echo()

    failed: list[str] = []

    for version in versions:
        click.echo(f"{'=' * 60}")
        click.echo(f"Extracting {provider} {version}")
        click.echo(f"{'=' * 60}")

        original_version = _patch_providers_json(providers_json_path, provider, version)

        try:
            pip_spec = _build_pip_spec(package_name, extras, version)
            base_spec = f"{package_name}=={version}"
            for script in EXTRACT_SCRIPTS:
                click.echo(f"\nRunning {script.name} with {pip_spec}...")
                result = run_command(
                    ["uv", "run", "--with", pip_spec, "python", str(script)],
                    check=False,
                    cwd=str(AIRFLOW_ROOT_PATH),
                )
                if result.returncode != 0 and pip_spec != base_spec:
                    click.echo(f"Retrying {script.name} without extras...")
                    result = run_command(
                        ["uv", "run", "--with", base_spec, "python", str(script)],
                        check=False,
                        cwd=str(AIRFLOW_ROOT_PATH),
                    )
                if result.returncode != 0:
                    click.echo(f"WARNING: {script.name} failed for {version} (exit {result.returncode})")
                    failed.append(f"{version}/{script.name}")
        finally:
            _patch_providers_json(providers_json_path, provider, original_version)

    click.echo(f"\n{'=' * 60}")
    if failed:
        click.echo(f"Completed with failures: {', '.join(failed)}")
        sys.exit(1)
    else:
        click.echo(f"Successfully extracted {len(versions)} version(s) for {provider}")
        click.echo(
            f"\nOutput written to:\n"
            f"  registry/src/_data/versions/{provider}/<version>/parameters.json\n"
            f"  registry/src/_data/versions/{provider}/<version>/connections.json"
        )
