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

import csv
import json
import os
import sys
from pathlib import Path
from typing import Any

import click

from airflow_breeze.commands.common_options import (
    option_airflow_version,
    option_answer,
    option_debug_resources,
    option_dry_run,
    option_github_token,
    option_historical_python_version,
    option_include_success_outputs,
    option_parallelism,
    option_python,
    option_run_in_parallel,
    option_skip_cleanup,
    option_verbose,
)
from airflow_breeze.global_constants import (
    AIRFLOW_PYTHON_COMPATIBILITY_MATRIX,
    ALL_HISTORICAL_PYTHON_VERSIONS,
    DEVEL_DEPS_PATH,
    PROVIDER_DEPENDENCIES,
)
from airflow_breeze.utils.cdxgen import (
    CHECK_DOCS,
    PROVIDER_REQUIREMENTS_DIR_PATH,
    SbomApplicationJob,
    SbomCoreJob,
    SbomProviderJob,
    build_all_airflow_versions_base_image,
    convert_sbom_entry_to_dict,
    get_cdxgen_port_mapping,
    get_field_names,
    get_requirements_for_provider,
    list_providers_from_providers_requirements,
    normalize_package_name,
)
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import perform_environment_checks
from airflow_breeze.utils.parallel import (
    DockerBuildxProgressMatcher,
    ShowLastLineProgressMatcher,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import FILES_SBOM_DIR, PROVIDER_METADATA_JSON_FILE_PATH
from airflow_breeze.utils.recording import generating_command_images
from airflow_breeze.utils.shared_options import get_dry_run


@click.group(
    cls=BreezeGroup,
    name="sbom",
    help="Tools that release managers can use to prepare sbom information",
)
def sbom():
    pass


SBOM_INDEX_TEMPLATE = """
{% set project_name = " " + provider_id + " " if provider_id else " " -%}
<html>
<head><title>CycloneDX SBOMs for Apache Airflow{{project_name}}{{ version }}</title></head>
<body>
    <h1>CycloneDX SBOMs for Apache Airflow{{project_name}}{{ version }}</h1>
    <ul>
    {% for sbom_file in sbom_files %}
        <li><a href="{{ sbom_file.name }}">{{ sbom_file.name }}</a></li>
    {% endfor %}
    </ul>
</body>
</html>
"""


@sbom.command(name="update-sbom-information", help="Update SBOM information in airflow-site project.")
@click.option(
    "--airflow-site-directory",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path, exists=True),
    required=True,
    envvar="AIRFLOW_SITE_DIRECTORY",
    help="Directory where airflow-site directory is located.",
)
@click.option(
    "--airflow-version",
    type=str,
    required=False,
    envvar="AIRFLOW_VERSION",
    help="Version of airflow to update sbom from. (defaulted to all active airflow versions)",
)
@option_historical_python_version
@click.option(
    "--include-provider-dependencies",
    is_flag=True,
    help="Whether to include provider dependencies in SBOM generation.",
)
@click.option(
    "--include-python/--no-include-python",
    is_flag=True,
    default=True,
    help="Whether to include python dependencies.",
)
@click.option(
    "--include-npm/--no-include-npm",
    is_flag=True,
    default=True,
    help="Whether to include npm dependencies.",
)
@option_run_in_parallel
@option_parallelism
@option_debug_resources
@option_include_success_outputs
@option_skip_cleanup
@click.option(
    "--force",
    is_flag=True,
    help="Force update of sbom even if it already exists.",
)
@click.option(
    "--all-combinations",
    is_flag=True,
    help="Produces all combinations of airflow sbom npm/python(airflow/full). Ignores --include flags",
)
@option_verbose
@option_dry_run
@option_answer
@click.option(
    "--package-filter",
    help="Filter(s) to use more than one can be specified. You can use glob pattern matching the "
    "full package name, for example `apache-airflow-providers-*`. Useful when you want to select"
    "several similarly named packages together.",
    type=BetterChoice(["apache-airflow-providers", "apache-airflow"]),
    required=False,
    default="apache-airflow",
)
def update_sbom_information(
    airflow_site_directory: Path,
    airflow_version: str | None,
    python: str | None,
    include_provider_dependencies: bool,
    include_python: bool,
    include_npm: bool,
    run_in_parallel: bool,
    parallelism: int,
    debug_resources: bool,
    include_success_outputs: bool,
    skip_cleanup: bool,
    force: bool,
    all_combinations: bool,
    package_filter: tuple[str, ...],
):
    import jinja2
    from jinja2 import StrictUndefined

    from airflow_breeze.utils.cdxgen import (
        produce_sbom_for_application_via_cdxgen_server,
        start_cdxgen_server,
    )
    from airflow_breeze.utils.github import get_active_airflow_versions

    if airflow_version is None:
        airflow_versions, _ = get_active_airflow_versions()
    else:
        airflow_versions = [airflow_version]
    if python is None:
        python_versions = ALL_HISTORICAL_PYTHON_VERSIONS
    else:
        python_versions = [python]
    application_root_path = FILES_SBOM_DIR
    start_cdxgen_server(application_root_path, run_in_parallel, parallelism)

    jobs_to_run: list[SbomApplicationJob] = []

    airflow_site_archive_directory = airflow_site_directory / "docs-archive"

    def _dir_exists_warn_and_should_skip(dir: Path, force: bool) -> bool:
        if dir.exists():
            if not force:
                get_console().print(f"[warning]The {dir} already exists. Skipping")
                return True
            else:
                get_console().print(f"[warning]The {dir} already exists. Forcing update")
                return False
        return False

    apache_airflow_documentation_directory = airflow_site_archive_directory / "apache-airflow"
    if package_filter == "apache-airflow":
        if all_combinations:
            for include_npm, include_python, include_provider_dependencies in [
                (True, False, False),
                (True, True, False),
                (True, True, True),
                (False, True, False),
                (False, True, True),
            ]:
                use_python_versions: list[str | None] = python_versions
                if not include_python:
                    use_python_versions = [None]
                core_jobs(
                    _dir_exists_warn_and_should_skip,
                    apache_airflow_documentation_directory,
                    airflow_versions,
                    application_root_path,
                    force,
                    include_npm,
                    include_provider_dependencies,
                    include_python,
                    jobs_to_run,
                    python_versions=use_python_versions,
                )
        else:
            use_python_versions = python_versions
            if not include_python:
                use_python_versions = [None]
            core_jobs(
                _dir_exists_warn_and_should_skip,
                apache_airflow_documentation_directory,
                airflow_versions,
                application_root_path,
                force,
                include_npm,
                include_provider_dependencies,
                include_python,
                jobs_to_run,
                python_versions=use_python_versions,
            )
    elif package_filter == "apache-airflow-providers":
        # Create providers jobs
        user_confirm(
            "You are about to update sbom information for providers, did you refresh the "
            "providers requirements with the command `breeze sbom generate-providers-requirements`?",
            quit_allowed=False,
            default_answer=Answer.YES,
        )
        for (
            node_name,
            provider_id,
            provider_version,
            provider_version_documentation_directory,
        ) in list_providers_from_providers_requirements(airflow_site_archive_directory):
            destination_dir = provider_version_documentation_directory / "sbom"

            destination_dir.mkdir(parents=True, exist_ok=True)

            get_console().print(
                f"[info]Attempting to update sbom for {provider_id} version {provider_version}."
            )

            python_versions = set(
                dir_name.replace("python", "")
                for dir_name in os.listdir(PROVIDER_REQUIREMENTS_DIR_PATH / node_name)
            )

            for python_version in python_versions:
                target_sbom_file_name = (
                    f"apache-airflow-sbom-{provider_id}-{provider_version}-python{python_version}.json"
                )
                target_sbom_path = destination_dir / target_sbom_file_name

                if _dir_exists_warn_and_should_skip(target_sbom_path, force):
                    continue

                jobs_to_run.append(
                    SbomProviderJob(
                        provider_id=provider_id,
                        provider_version=provider_version,
                        python_version=python_version,
                        target_path=target_sbom_path,
                        folder_name=node_name,
                    )
                )

    if len(jobs_to_run) == 0:
        get_console().print("[info]Nothing to do, there is no job to process")
        return

    if run_in_parallel:
        parallelism = min(parallelism, len(jobs_to_run))
        get_console().print(f"[info]Running {len(jobs_to_run)} jobs in parallel")
        with ci_group(f"Generating SBOMs for {jobs_to_run}"):
            all_params = [f"Generate SBOMs for {job.get_job_name()} " for job in jobs_to_run]
            with run_with_pool(
                parallelism=parallelism,
                all_params=all_params,
                debug_resources=debug_resources,
                progress_matcher=ShowLastLineProgressMatcher(),
            ) as (pool, outputs):
                port_map = get_cdxgen_port_mapping(parallelism, pool)
                results = [
                    pool.apply_async(
                        produce_sbom_for_application_via_cdxgen_server,
                        kwds={
                            "job": job,
                            "output": outputs[index],
                            "port_map": port_map,
                        },
                    )
                    for index, job in enumerate(jobs_to_run)
                ]
        check_async_run_results(
            results=results,
            success="All SBOMs were generated successfully",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        for job in jobs_to_run:
            produce_sbom_for_application_via_cdxgen_server(job, output=None)

    html_template = SBOM_INDEX_TEMPLATE

    def _generate_index(destination_dir: Path, provider_id: str | None, version: str) -> None:
        destination_index_path = destination_dir / "index.html"
        get_console().print(f"[info]Generating index for {destination_dir}")
        sbom_files = sorted(destination_dir.glob("apache-airflow-sbom-*"))
        if not get_dry_run():
            destination_index_path.write_text(
                jinja2.Template(html_template, autoescape=True, undefined=StrictUndefined).render(
                    provider_id=provider_id,
                    version=version,
                    sbom_files=sbom_files,
                )
            )

    if package_filter == "apache-airflow":
        for airflow_v in airflow_versions:
            airflow_version_dir = apache_airflow_documentation_directory / airflow_v
            destination_dir = airflow_version_dir / "sbom"
            _generate_index(destination_dir, None, airflow_v)
    elif package_filter == "apache-airflow-providers":
        for (
            _,
            provider_id,
            provider_version,
            provider_version_documentation_directory,
        ) in list_providers_from_providers_requirements(airflow_site_archive_directory):
            destination_dir = provider_version_documentation_directory / "sbom"
            _generate_index(destination_dir, provider_id, provider_version)


def core_jobs(
    _dir_exists_warn_and_should_skip,
    apache_airflow_documentation_directory: Path,
    airflow_versions: list[str],
    application_root_path: Path,
    force: bool,
    include_npm: bool,
    include_provider_dependencies: bool,
    include_python: bool,
    jobs_to_run: list[SbomApplicationJob],
    python_versions: list[str | None],
):
    # Create core jobs
    for airflow_v in airflow_versions:
        airflow_version_dir = apache_airflow_documentation_directory / airflow_v
        if not airflow_version_dir.exists():
            get_console().print(f"[warning]The {airflow_version_dir} does not exist. Skipping")
            continue
        destination_dir = airflow_version_dir / "sbom"

        if _dir_exists_warn_and_should_skip(destination_dir, force):
            continue

        destination_dir.mkdir(parents=True, exist_ok=True)
        get_console().print(f"[info]Attempting to update sbom for {airflow_v}.")
        for python_version in python_versions:
            if include_python and include_npm:
                suffix = f"-python{python_version}"
            elif include_python:
                suffix = f"-python{python_version}-python-only"
            elif include_npm:
                suffix = "-npm-only"
            else:
                get_console().print("[warning]Neither python nor npm provided. Skipping")
                continue
            if include_provider_dependencies:
                suffix += "-full"

            target_sbom_file_name = f"apache-airflow-sbom-{airflow_v}{suffix}.json"
            target_sbom_path = destination_dir / target_sbom_file_name

            if _dir_exists_warn_and_should_skip(target_sbom_path, force):
                continue

            jobs_to_run.append(
                SbomCoreJob(
                    airflow_version=airflow_v,
                    python_version=python_version,
                    application_root_path=application_root_path,
                    include_provider_dependencies=include_provider_dependencies,
                    target_path=target_sbom_path,
                    include_python=include_python,
                    include_npm=include_npm,
                )
            )


@sbom.command(name="build-all-airflow-images", help="Generate images with airflow versions pre-installed")
@option_historical_python_version
@option_verbose
@option_dry_run
@option_answer
@option_run_in_parallel
@option_parallelism
@option_debug_resources
@option_include_success_outputs
@option_skip_cleanup
def build_all_airflow_images(
    python: str,
    run_in_parallel: bool,
    parallelism: int,
    debug_resources: bool,
    include_success_outputs: bool,
    skip_cleanup: bool,
):
    if python is None:
        python_versions = ALL_HISTORICAL_PYTHON_VERSIONS
    else:
        python_versions = [python]

    if run_in_parallel:
        parallelism = min(parallelism, len(python_versions))
        get_console().print(f"[info]Running {len(python_versions)} jobs in parallel")
        with ci_group(f"Building all airflow base images for python: {python_versions}"):
            all_params = [
                f"Building all airflow base image for python: {python_version}"
                for python_version in python_versions
            ]
            with run_with_pool(
                parallelism=parallelism,
                all_params=all_params,
                debug_resources=debug_resources,
                progress_matcher=DockerBuildxProgressMatcher(),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        build_all_airflow_versions_base_image,
                        kwds={
                            "python_version": python_version,
                            "output": outputs[index],
                        },
                    )
                    for (index, python_version) in enumerate(python_versions)
                ]
        check_async_run_results(
            results=results,
            success="All airflow base images were built successfully",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        for python_version in python_versions:
            build_all_airflow_versions_base_image(
                python_version=python_version,
                output=None,
            )


@sbom.command(name="generate-providers-requirements", help="Generate requirements for selected provider.")
@option_historical_python_version
@click.option(
    "--provider-id",
    type=BetterChoice(list(PROVIDER_DEPENDENCIES.keys())),
    required=False,
    help="Provider id to generate the requirements for",
)
@click.option(
    "--provider-version",
    type=str,
    required=False,
    help="Provider version to generate the requirements for i.e `2.1.0`. `latest` is also a supported value "
    "to account for the most recent version of the provider",
)
@option_verbose
@option_dry_run
@option_answer
@option_run_in_parallel
@option_parallelism
@option_debug_resources
@option_include_success_outputs
@option_skip_cleanup
@click.option(
    "--force",
    is_flag=True,
    help="Force update providers requirements even if they already exist.",
)
def generate_providers_requirements(
    python: str,
    provider_id: str | None,
    provider_version: str | None,
    run_in_parallel: bool,
    parallelism: int,
    debug_resources: bool,
    include_success_outputs: bool,
    skip_cleanup: bool,
    force: bool,
):
    perform_environment_checks()

    if python is None:
        python_versions = ALL_HISTORICAL_PYTHON_VERSIONS
    else:
        python_versions = [python]

    with open(PROVIDER_METADATA_JSON_FILE_PATH) as f:
        provider_metadata = json.load(f)

    if provider_id is None:
        if provider_version is not None and provider_version != "latest":
            get_console().print(
                "[error] You cannot pin the version of the providers if you generate the requirements for "
                "all historical or latest versions. --provider-version needs to be unset when you pass None "
                "or latest to --provider-id"
            )
            sys.exit(1)
        provider_ids = provider_metadata.keys()
    else:
        provider_ids = [provider_id]

    if provider_version is None:
        user_confirm(
            f"You are about to generate providers requirements for all historical versions for "
            f"{len(provider_ids)} provider(s) based on `provider_metadata.json` file. "
            f"Do you want to proceed?",
            quit_allowed=False,
            default_answer=Answer.YES,
        )

    providers_info = []
    for provider_id in provider_ids:
        if provider_version is not None:
            if provider_version == "latest":
                # Only the latest version for each provider
                p_version, info = list(provider_metadata[provider_id].items())[-1]
            else:
                # Specified providers version
                info = provider_metadata[provider_id][provider_version]
                p_version = provider_version

            airflow_version = info["associated_airflow_version"]

            providers_info += [
                (provider_id, p_version, python_version, airflow_version)
                for python_version in AIRFLOW_PYTHON_COMPATIBILITY_MATRIX[airflow_version]
                if python_version in python_versions
            ]
        else:
            # All historical providers' versions
            providers_info += [
                (
                    provider_id,
                    p_version,
                    python_version,
                    info["associated_airflow_version"],
                )
                for (p_version, info) in provider_metadata[provider_id].items()
                for python_version in AIRFLOW_PYTHON_COMPATIBILITY_MATRIX[info["associated_airflow_version"]]
                if python_version in python_versions
            ]

    if run_in_parallel:
        parallelism = min(parallelism, len(providers_info))
        get_console().print(f"[info]Running {len(providers_info)} jobs in parallel")
        with ci_group(f"Generating provider requirements for {providers_info}"):
            all_params = [
                f"Generate provider requirements for {provider_id} version {provider_version} python "
                f"{python_version}"
                for (provider_id, provider_version, python_version, _) in providers_info
            ]
            with run_with_pool(
                parallelism=parallelism,
                all_params=all_params,
                debug_resources=debug_resources,
                progress_matcher=ShowLastLineProgressMatcher(),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        get_requirements_for_provider,
                        kwds={
                            "provider_id": provider_id,
                            "airflow_version": airflow_version,
                            "provider_version": provider_version,
                            "python_version": python_version,
                            "force": force,
                            "output": outputs[index],
                        },
                    )
                    for (
                        index,
                        (provider_id, provider_version, python_version, airflow_version),
                    ) in enumerate(providers_info)
                ]
        check_async_run_results(
            results=results,
            success="Providers requirements were generated successfully",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        for provider_id, provider_version, python_version, airflow_version in providers_info:
            get_requirements_for_provider(
                provider_id=provider_id,
                provider_version=provider_version,
                airflow_version=airflow_version,
                python_version=python_version,
                force=force,
                output=None,
            )


@sbom.command(name="export-dependency-information", help="Export dependency information from SBOM.")
@option_airflow_version
@option_python
@click.option(
    "-f",
    "--csv-file",
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path, writable=True),
    help="CSV file to produce. Mutually exclusive with Google Spreadsheet Id.",
    envvar="CSV_FILE",
    required=False,
)
@click.option(
    "-g",
    "--google-spreadsheet-id",
    type=str,
    help="Google Spreadsheet Id to produce. Mutually exclusive with CSV file.",
    envvar="GOOGLE_SPREADSHEET_ID",
    required=False,
)
@option_github_token
@click.option(
    "--json-credentials-file",
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path, writable=False),
    help="Gsheet JSON credentials file (defaults to ~/.config/gsheet/credentials.json",
    envvar="JSON_CREDENTIALS_FILE",
    default=Path.home() / ".config" / "gsheet" / "credentials.json"
    if not generating_command_images()
    else "credentials.json",
    required=False,
)
@click.option(
    "-s",
    "--include-open-psf-scorecard",
    help="Include statistics from the Open PSF Scorecard",
    is_flag=True,
    default=False,
)
@click.option(
    "-G",
    "--include-github-stats",
    help="Include statistics from GitHub",
    is_flag=True,
    default=False,
)
@click.option(
    "-l",
    "--limit-output",
    help="Limit the output to the first N dependencies. Default is to output all dependencies. "
    "If you want to output all dependencies, do not specify this option.",
    type=int,
    required=False,
)
@click.option(
    "--project-name",
    help="Only used for debugging purposes. The name of the project to generate the sbom for.",
    type=str,
    required=False,
)
@option_dry_run
@option_answer
def export_dependency_information(
    python: str,
    airflow_version: str,
    csv_file: Path | None,
    google_spreadsheet_id: str | None,
    github_token: str | None,
    json_credentials_file: Path,
    include_open_psf_scorecard: bool,
    include_github_stats: bool,
    limit_output: int | None,
    project_name: str | None,
):
    if not google_spreadsheet_id and not csv_file:
        get_console().print("[error]You need to specify either --csv-file or --google-spreadsheet-id")
        sys.exit(1)
    if google_spreadsheet_id and csv_file:
        get_console().print("[error]You cannot specify both --csv-file and --google-spreadsheet-id")
        sys.exit(1)
    if google_spreadsheet_id and not json_credentials_file.exists():
        get_console().print(
            f"[error]The JSON credentials file {json_credentials_file} does not exist. "
            "Please specify a valid path to the JSON credentials file."
        )
        sys.exit(1)
    import requests

    base_url = f"https://airflow.apache.org/docs/apache-airflow/{airflow_version}/sbom"
    sbom_file_base = f"apache-airflow-sbom-{airflow_version}-python{python}-python-only"

    sbom_core_url = f"{base_url}/{sbom_file_base}.json"
    sbom_full_url = f"{base_url}/{sbom_file_base}-full.json"
    core_sbom_r = requests.get(sbom_core_url)
    core_sbom_r.raise_for_status()
    full_sbom_r = requests.get(sbom_full_url)
    full_sbom_r.raise_for_status()

    core_sbom = core_sbom_r.json()
    full_sbom = full_sbom_r.json()

    all_dependency_value_dicts = convert_all_sbom_to_value_dictionaries(
        core_sbom=core_sbom,
        full_sbom=full_sbom,
        include_open_psf_scorecard=include_open_psf_scorecard,
        include_github_stats=include_github_stats,
        limit_output=limit_output,
        github_token=github_token,
        project_name=project_name,
    )
    all_dependency_value_dicts = sorted(all_dependency_value_dicts, key=sort_deps_key)

    fieldnames = get_field_names(include_open_psf_scorecard, include_github_stats)

    if csv_file:
        write_to_csv_file(
            csv_file=csv_file, all_dependencies=all_dependency_value_dicts, fieldnames=fieldnames
        )
    elif google_spreadsheet_id:
        write_to_google_spreadsheet(
            google_spreadsheet_id=google_spreadsheet_id,
            json_credentials_file=json_credentials_file,
            all_dependencies=all_dependency_value_dicts,
            fieldnames=fieldnames,
        )


def calculate_range(num_columns: int, row: int) -> str:
    import string

    # Generate column letters
    columns = list(string.ascii_uppercase)
    if num_columns > 26:
        columns += [f"{a}{b}" for a in string.ascii_uppercase for b in string.ascii_uppercase]

    # Calculate the range
    end_column = columns[num_columns - 1]
    return f"A{row}:{end_column}{row}"


def convert_sbom_dict_to_spreadsheet_data(headers: list[str], value_dict: dict[str, Any]):
    return [value_dict.get(header, "") for header in headers]


INTERESTING_OPSF_FIELDS = [
    "Score",
    "Code-Review",
    "Maintained",
    "Dangerous-Workflow",
    "Security-Policy",
    "Packaging",
    "Vulnerabilities",
]

INTERESTING_OPSF_SCORES = ["OPSF-" + field for field in INTERESTING_OPSF_FIELDS]
INTERESTING_OPSF_DETAILS = ["OPSF-Details-" + field for field in INTERESTING_OPSF_FIELDS]


def write_to_google_spreadsheet(
    google_spreadsheet_id: str,
    json_credentials_file: Path,
    all_dependencies: list[dict[str, Any]],
    fieldnames: list[str],
):
    token_path = Path.home() / ".config" / "gsheet" / "token.json"

    sheet = authorize_gsheet(json_credentials_file, token_path)

    # Use only interesting values from the scorecard
    cell_field_names = [
        fieldname
        for fieldname in fieldnames
        if fieldname in INTERESTING_OPSF_SCORES or not fieldname.startswith("OPSF-")
    ]

    num_rows = update_field_values(all_dependencies, cell_field_names, google_spreadsheet_id, sheet)
    update_opsf_detailed_comments(all_dependencies, fieldnames, num_rows, google_spreadsheet_id, sheet)


def update_opsf_detailed_comments(
    all_dependencies: list[dict[str, Any]],
    fieldnames: list[str],
    num_rows: int,
    google_spreadsheet_id: str,
    sheet,
):
    opsf_details_field_names = [
        fieldname for fieldname in fieldnames if fieldname in INTERESTING_OPSF_DETAILS
    ]
    start_opsf_column = fieldnames.index(opsf_details_field_names[0]) - 1
    opsf_details = []
    opsf_details.append(
        {
            "values": [
                {"note": CHECK_DOCS[check]}
                for check in INTERESTING_OPSF_FIELDS
                if check != INTERESTING_OPSF_FIELDS[0]
            ]
        }
    )
    for dependency in all_dependencies:
        note_row = convert_sbom_dict_to_spreadsheet_data(opsf_details_field_names, dependency)
        opsf_details.append({"values": [{"note": note} for note in note_row]})
    notes = {
        "updateCells": {
            "range": {
                "startRowIndex": 1,
                "endRowIndex": num_rows + 1,
                "startColumnIndex": start_opsf_column,
                "endColumnIndex": start_opsf_column + len(opsf_details_field_names) + 1,
            },
            "rows": opsf_details,
            "fields": "note",
        },
    }
    update_note_body = {"requests": [notes]}
    sheet.batchUpdate(spreadsheetId=google_spreadsheet_id, body=update_note_body).execute()


def simplify_field_names(fieldname: str):
    if fieldname.startswith("OPSF-"):
        return fieldname[5:]
    return fieldname


def update_field_values(
    all_dependencies: list[dict[str, Any]], cell_field_names: list[str], google_spreadsheet_id, sheet
) -> int:
    num_fields = len(cell_field_names)
    data = []
    top_header = []
    for field in cell_field_names:
        if field.startswith("OPSF-"):
            top_header.append("Relevant OPSF Scores and details")
            break
        else:
            top_header.append("")

    simplified_cell_field_names = [simplify_field_names(field) for field in cell_field_names]

    data.append({"range": calculate_range(num_fields, 1), "values": [top_header]})
    data.append({"range": calculate_range(num_fields, 2), "values": [simplified_cell_field_names]})
    row = 3
    for dependency in all_dependencies:
        spreadsheet_row = convert_sbom_dict_to_spreadsheet_data(cell_field_names, dependency)
        data.append({"range": calculate_range(num_fields, row), "values": [spreadsheet_row]})
        row += 1
    body = {"valueInputOption": "RAW", "data": data}
    result = sheet.values().batchUpdate(spreadsheetId=google_spreadsheet_id, body=body).execute()
    get_console().print(f"{result.get('totalUpdatedCells')} cells values set in the Google spreadsheet.")
    return row


def authorize_gsheet(json_credentials_file: Path, token_path: Path):
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(token_path.as_posix(), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(json_credentials_file.as_posix(), SCOPES)
            creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
        token_path.write_text(creds.to_json())
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    return sheet


def write_to_csv_file(csv_file: Path, all_dependencies: list[dict[str, Any]], fieldnames: list[str]):
    with csv_file.open("w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for dependency_value_dict in all_dependencies:
            writer.writerow(dependency_value_dict)
    get_console().print(f"[info]Exported {len(all_dependencies)} dependencies to {csv_file}")


def sort_deps_key(dependency: dict[str, Any]) -> str:
    if dependency.get("Vcs"):
        return "0:" + dependency["Name"]
    else:
        return "1:" + dependency["Name"]


def convert_all_sbom_to_value_dictionaries(
    core_sbom: dict[str, Any],
    full_sbom: dict[str, Any],
    include_open_psf_scorecard: bool,
    include_github_stats: bool,
    limit_output: int | None,
    github_token: str | None = None,
    project_name: str | None = None,
) -> list[dict[str, Any]]:
    core_dependencies = set()
    dev_deps = set(normalize_package_name(name) for name in DEVEL_DEPS_PATH.read_text().splitlines())
    num_deps = 0
    all_dependency_value_dicts = []
    for dependency in core_sbom["components"]:
        normalized_name = normalize_package_name(dependency["name"])
        if project_name and normalized_name != project_name:
            continue
        core_dependencies.add(normalized_name)
        is_devel = normalized_name in dev_deps
        value_dict = convert_sbom_entry_to_dict(
            dependency,
            is_core=True,
            is_devel=is_devel,
            include_open_psf_scorecard=include_open_psf_scorecard,
            include_github_stats=include_github_stats,
            github_token=github_token,
        )
        if value_dict:
            all_dependency_value_dicts.append(value_dict)
        num_deps += 1
        if limit_output and num_deps >= limit_output:
            return all_dependency_value_dicts
    for dependency in full_sbom["components"]:
        normalized_name = normalize_package_name(dependency["name"])
        if project_name and normalized_name != project_name:
            continue
        if normalized_name not in core_dependencies:
            is_devel = normalized_name in dev_deps
            value_dict = convert_sbom_entry_to_dict(
                dependency,
                is_core=False,
                is_devel=is_devel,
                include_open_psf_scorecard=include_open_psf_scorecard,
                include_github_stats=include_github_stats,
                github_token=github_token,
            )
            if value_dict:
                all_dependency_value_dicts.append(value_dict)
            num_deps += 1
        if limit_output and num_deps >= limit_output:
            return all_dependency_value_dicts
    get_console().print(f"[info]Processed {num_deps} dependencies")
    return all_dependency_value_dicts
