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
import shutil
import sys
import tempfile
import uuid
from pathlib import Path

import click
import yaml

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.common_options import option_dry_run, option_python, option_verbose
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.docker_command_utils import execute_command_in_shell, fix_ownership_using_docker
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command

PROVIDERS_DIR = AIRFLOW_ROOT_PATH / "providers"


def _get_suspended_provider_packages() -> list[str]:
    """Return in-container pip-installable paths for providers with state: suspended."""
    packages = []
    for yaml_path in sorted(PROVIDERS_DIR.rglob("provider.yaml")):
        if "src" in yaml_path.relative_to(PROVIDERS_DIR).parts:
            continue
        with open(yaml_path) as f:
            data = yaml.safe_load(f)
        if data.get("state") == "suspended":
            # Use in-container path (providers/ is mounted at /opt/airflow/providers/)
            rel = yaml_path.parent.relative_to(PROVIDERS_DIR)
            packages.append(f"/opt/airflow/providers/{rel}")
    return packages


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
@click.option(
    "--allow-unreleased",
    is_flag=True,
    default=False,
    help=(
        "Include providers and versions that don't have a matching "
        "providers-<id>/<ver> git tag. Use for staging builds and local dev "
        "where maintainers want to preview unreleased provider pages before "
        "the tag lands. Forwarded to extract_metadata.py."
    ),
)
@option_verbose
@option_dry_run
def extract_data(python: str, provider: str | None, allow_unreleased: bool):
    unique_project_name = f"breeze-registry-{uuid.uuid4().hex[:8]}"

    shell_params = ShellParams(
        python=python,
        project_name=unique_project_name,
        quiet=True,
        skip_environment_initialization=True,
        extra_args=(),
    )

    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)

    # Install suspended providers that aren't in the CI image so runtime
    # discovery (issubclass) can find their classes.
    suspended_packages = _get_suspended_provider_packages()
    install_cmd = f"pip install --quiet {' '.join(suspended_packages)} && " if suspended_packages else ""

    provider_flag = f" --provider '{provider}'" if provider else ""
    # --allow-unreleased only applies to extract_metadata.py (which owns the
    # version filter). The other two scripts read from providers.json and
    # don't need it.
    metadata_extra = " --allow-unreleased" if allow_unreleased else ""
    command = (
        f"{install_cmd}"
        f"python dev/registry/extract_metadata.py{provider_flag}{metadata_extra} && "
        f"python dev/registry/extract_parameters.py{provider_flag} && "
        f"python dev/registry/extract_connections.py{provider_flag}"
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


DEV_REGISTRY_DIR = AIRFLOW_ROOT_PATH / "dev" / "registry"

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
        import tomli as tomllib  # type: ignore[no-redef]

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


def _create_isolated_providers_json(provider_id: str, package_name: str, version: str, tmp_dir: Path) -> Path:
    """Create a temp providers.json with only the target provider/version.

    This allows multiple providers to run in parallel without conflicting over
    the shared dev/registry/providers.json file.
    """
    tmp_providers = tmp_dir / f"providers-{provider_id}-{version}.json"
    data = {"providers": [{"id": provider_id, "package_name": package_name, "version": version}]}
    with open(tmp_providers, "w") as f:
        json.dump(data, f, indent=2)
    return tmp_providers


def _run_extract_script(
    script: Path,
    pip_spec: str,
    base_spec: str,
    provider_id: str,
    providers_json_path: Path,
) -> int:
    """Run an extraction script with --provider and --providers-json flags.

    Falls back to running without extras if the full spec fails.
    Returns the exit code.
    """
    base_cmd = [
        "uv",
        "run",
        "--with",
        pip_spec,
        "python",
        str(script),
        "--provider",
        provider_id,
        "--providers-json",
        str(providers_json_path),
    ]
    result = run_command(base_cmd, check=False, cwd=str(AIRFLOW_ROOT_PATH))
    if result.returncode != 0 and pip_spec != base_spec:
        click.echo(f"Retrying {script.name} without extras...")
        fallback_cmd = [
            "uv",
            "run",
            "--with",
            base_spec,
            "python",
            str(script),
            "--provider",
            provider_id,
            "--providers-json",
            str(providers_json_path),
        ]
        result = run_command(fallback_cmd, check=False, cwd=str(AIRFLOW_ROOT_PATH))
    return result.returncode


def _run_extract_versions(provider_id: str, version: str) -> int:
    """Run extract_versions.py on the host for a single provider/version.

    Resolves dependencies via ``--project dev/registry`` so uv reads
    ``dev/registry/pyproject.toml`` (pyyaml + pydantic) instead of syncing
    the airflow workspace. Without this flag uv pulls in providers like
    samba -> smbprotocol -> pyspnego[kerberos] -> gssapi, which fails on
    runners without libkrb5-dev. Returns the script exit code.

    No extras fallback (unlike :func:`_run_extract_script`): this script's
    deps live in ``dev/registry/pyproject.toml`` and aren't provider-version
    dependent, so a single invocation either works or doesn't.
    """
    cmd = [
        "uv",
        "run",
        "--project",
        str(DEV_REGISTRY_DIR),
        "python",
        str(DEV_REGISTRY_DIR / "extract_versions.py"),
        "--provider",
        provider_id,
        "--version",
        version,
    ]
    return run_command(cmd, check=False, cwd=str(AIRFLOW_ROOT_PATH)).returncode


def _backfill_docker(
    python: str,
    provider: str,
    versions: tuple[str, ...],
    package_name: str,
    extras: list[str],
) -> list[str]:
    """Run parameter/connection extraction inside the Breeze CI container."""
    failed: list[str] = []
    unique_project_name = f"breeze-backfill-{uuid.uuid4().hex[:8]}"

    shell_params = ShellParams(
        python=python,
        project_name=unique_project_name,
        quiet=True,
        skip_environment_initialization=True,
        extra_args=(),
    )

    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)

    # Place isolated providers.json under dev/registry/ so it's visible inside the container
    # at /opt/airflow/dev/registry/
    backfill_tmp_dir = DEV_REGISTRY_DIR / ".backfill_tmp"
    backfill_tmp_dir.mkdir(exist_ok=True)

    try:
        for version in versions:
            click.echo(f"{'=' * 60}")
            click.echo(f"Extracting {provider} {version} (Docker)")
            click.echo(f"{'=' * 60}")

            providers_json = _create_isolated_providers_json(
                provider, package_name, version, backfill_tmp_dir
            )
            container_providers_json = f"/opt/airflow/dev/registry/.backfill_tmp/{providers_json.name}"

            pip_spec = _build_pip_spec(package_name, extras, version)
            base_spec = f"{package_name}=={version}"

            command = (
                f"cd dev/registry && "
                f"uv run --with '{pip_spec}' bash -c '"
                f"python extract_parameters.py "
                f"--provider {provider} --providers-json {container_providers_json} && "
                f"python extract_connections.py "
                f"--provider {provider} --providers-json {container_providers_json}'"
            )

            result = execute_command_in_shell(
                shell_params=shell_params,
                project_name=unique_project_name,
                command=command,
                preserve_backend=True,
            )

            if result.returncode != 0 and pip_spec != base_spec:
                click.echo(f"Retrying without extras ({base_spec})...")
                command_fallback = (
                    f"cd dev/registry && "
                    f"uv run --with '{base_spec}' bash -c '"
                    f"python extract_parameters.py "
                    f"--provider {provider} --providers-json {container_providers_json} && "
                    f"python extract_connections.py "
                    f"--provider {provider} --providers-json {container_providers_json}'"
                )
                result = execute_command_in_shell(
                    shell_params=shell_params,
                    project_name=unique_project_name,
                    command=command_fallback,
                    preserve_backend=True,
                )

            if result.returncode != 0:
                click.echo(f"WARNING: extraction failed for {version} (exit {result.returncode})")
                failed.append(f"{version}/docker-extraction")
    finally:
        shutil.rmtree(backfill_tmp_dir, ignore_errors=True)
        fix_ownership_using_docker()

    return failed


def _backfill_uv(
    provider: str,
    versions: tuple[str, ...],
    package_name: str,
    extras: list[str],
) -> list[str]:
    """Run parameter/connection extraction via 'uv run --with' on the host."""
    failed: list[str] = []

    with tempfile.TemporaryDirectory(prefix=f"backfill-{provider}-") as tmp_dir:
        tmp_path = Path(tmp_dir)

        for version in versions:
            click.echo(f"{'=' * 60}")
            click.echo(f"Extracting {provider} {version} (uv)")
            click.echo(f"{'=' * 60}")

            providers_json = _create_isolated_providers_json(provider, package_name, version, tmp_path)

            pip_spec = _build_pip_spec(package_name, extras, version)
            base_spec = f"{package_name}=={version}"

            for script in EXTRACT_SCRIPTS:
                click.echo(f"\nRunning {script.name} with {pip_spec}...")
                returncode = _run_extract_script(script, pip_spec, base_spec, provider, providers_json)
                if returncode != 0:
                    click.echo(f"WARNING: {script.name} failed for {version} (exit {returncode})")
                    failed.append(f"{version}/{script.name}")

    return failed


@registry_group.command(
    name="backfill",
    help="Extract metadata, parameters, and connections for older provider versions. "
    "Runs extract_versions.py (host, git tags) for metadata.json, then "
    "extract_parameters.py + extract_connections.py inside the Breeze CI container "
    "(or via 'uv run --with' with --no-docker). "
    "Each version uses an isolated providers.json, so "
    "multiple providers can be backfilled in parallel.",
)
@option_python
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
@click.option(
    "--use-docker/--no-docker",
    default=True,
    help="Run extraction in CI Docker container (default) or via uv on host.",
)
@option_verbose
@option_dry_run
def backfill(python: str, provider: str, versions: tuple[str, ...], use_docker: bool):
    package_name, extras = _read_provider_yaml_info(provider)

    click.echo(f"Provider: {provider} ({package_name})")
    click.echo(f"Versions: {', '.join(versions)}")
    click.echo(f"Mode: {'Docker' if use_docker else 'uv (host)'}")
    if extras:
        click.echo(f"Extras: {', '.join(extras)}")
    click.echo()

    failed: list[str] = []

    # Step 1: extract_versions.py (host, reads git tags) -> metadata.json
    click.echo("Step 1: Extracting version metadata from git tags...")
    for version in versions:
        rc = _run_extract_versions(provider, version)
        if rc != 0:
            click.echo(f"WARNING: extract_versions.py failed for {version} (exit {rc})")
            failed.append(f"{version}/extract_versions.py")

    # Step 2: extract_parameters.py + extract_connections.py
    click.echo("\nStep 2: Extracting parameters and connections...")
    if use_docker:
        failed.extend(_backfill_docker(python, provider, versions, package_name, extras))
    else:
        failed.extend(_backfill_uv(provider, versions, package_name, extras))

    click.echo(f"\n{'=' * 60}")
    if failed:
        click.echo(f"Completed with failures: {', '.join(failed)}")
        sys.exit(1)
    else:
        click.echo(f"Successfully extracted {len(versions)} version(s) for {provider}")
        click.echo(
            f"\nOutput written to:\n"
            f"  registry/src/_data/versions/{provider}/<version>/metadata.json\n"
            f"  registry/src/_data/versions/{provider}/<version>/parameters.json\n"
            f"  registry/src/_data/versions/{provider}/<version>/connections.json"
        )
