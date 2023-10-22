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
from pathlib import Path

import click

from airflow_breeze.global_constants import (
    ALL_HISTORICAL_PYTHON_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    PROVIDER_DEPENDENCIES,
)
from airflow_breeze.utils.cdxgen import (
    SbomApplicationJob,
    build_all_airflow_versions_base_image,
    get_cdxgen_port_mapping,
    get_requirements_for_provider,
)
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.common_options import (
    option_answer,
    option_debug_resources,
    option_dry_run,
    option_historical_python_version,
    option_include_success_outputs,
    option_parallelism,
    option_run_in_parallel,
    option_skip_cleanup,
    option_verbose,
)
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import perform_environment_checks
from airflow_breeze.utils.github import get_active_airflow_versions
from airflow_breeze.utils.parallel import ShowLastLineProgressMatcher, check_async_run_results, run_with_pool
from airflow_breeze.utils.path_utils import AIRFLOW_TMP_DIR_PATH, PROVIDER_METADATA_JSON_FILE_PATH
from airflow_breeze.utils.shared_options import get_dry_run


@click.group(
    cls=BreezeGroup,
    name="sbom",
    help="Tools that release managers can use to prepare sbom information",
)
def sbom():
    pass


SBOM_INDEX_TEMPLATE = """
<html>
<head><title>CycloneDX SBOMs for Apache Airflow {{ version }}</title></head>
<body>
    <h1>CycloneDX SBOMs for Apache Airflow {{ version }}</h1>
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
@option_verbose
@option_dry_run
@option_answer
def update_sbom_information(
    airflow_site_directory: Path,
    airflow_version: str | None,
    python: str | None,
    include_provider_dependencies: bool,
    run_in_parallel: bool,
    parallelism: int,
    debug_resources: bool,
    include_success_outputs: bool,
    skip_cleanup: bool,
    force: bool,
):
    import jinja2
    from jinja2 import StrictUndefined

    from airflow_breeze.utils.cdxgen import (
        produce_sbom_for_application_via_cdxgen_server,
        start_cdxgen_server,
    )
    from airflow_breeze.utils.github import get_active_airflow_versions

    if airflow_version is None:
        airflow_versions = get_active_airflow_versions()
    else:
        airflow_versions = [airflow_version]
    if python is None:
        python_versions = ALL_HISTORICAL_PYTHON_VERSIONS
    else:
        python_versions = [python]
    application_root_path = AIRFLOW_TMP_DIR_PATH
    start_cdxgen_server(application_root_path, run_in_parallel, parallelism)

    jobs_to_run: list[SbomApplicationJob] = []

    apache_airflow_directory = airflow_site_directory / "docs-archive" / "apache-airflow"

    for airflow_v in airflow_versions:
        airflow_version_dir = apache_airflow_directory / airflow_v
        if not airflow_version_dir.exists():
            get_console().print(f"[warning]The {airflow_version_dir} does not exist. Skipping")
            continue
        destination_dir = airflow_version_dir / "sbom"
        if destination_dir.exists():
            if not force:
                get_console().print(f"[warning]The {destination_dir} already exists. Skipping")
                continue
            else:
                get_console().print(f"[warning]The {destination_dir} already exists. Forcing update")

        destination_dir.mkdir(parents=True, exist_ok=True)

        get_console().print(f"[info]Attempting to update sbom for {airflow_v}.")
        get_console().print(f"[success]The {destination_dir} exists. Proceeding.")
        for python_version in python_versions:
            target_sbom_file_name = f"apache-airflow-sbom-{airflow_v}-python{python_version}.json"
            target_sbom_path = destination_dir / target_sbom_file_name
            if target_sbom_path.exists():
                if not force:
                    get_console().print(f"[warning]The {target_sbom_path} already exists. Skipping")
                    continue
                else:
                    get_console().print(f"[warning]The {target_sbom_path} already exists. Forcing update")
            jobs_to_run.append(
                SbomApplicationJob(
                    airflow_version=airflow_v,
                    python_version=python_version,
                    application_root_path=application_root_path,
                    include_provider_dependencies=include_provider_dependencies,
                    target_path=target_sbom_path,
                )
            )
    if run_in_parallel:
        parallelism = min(parallelism, len(jobs_to_run))
        get_console().print(f"[info]Running {len(jobs_to_run)} jobs in parallel")
        with ci_group(f"Generating SBoMs for {airflow_versions}:{python_versions}"):
            all_params = [
                f"Generate SBoMs for {job.airflow_version}:{job.python_version}" for job in jobs_to_run
            ]
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
            success="All SBoMs were generated successfully",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        for job in jobs_to_run:
            produce_sbom_for_application_via_cdxgen_server(job, output=None)

    for airflow_v in airflow_versions:
        airflow_version_dir = apache_airflow_directory / airflow_v
        destination_dir = airflow_version_dir / "sbom"
        destination_index_path = destination_dir / "index.html"
        get_console().print(f"[info]Generating index for {destination_dir}")
        sbom_files = sorted(destination_dir.glob("apache-airflow-sbom-*"))
        html_template = SBOM_INDEX_TEMPLATE
        if not get_dry_run():
            destination_index_path.write_text(
                jinja2.Template(html_template, autoescape=True, undefined=StrictUndefined).render(
                    version=airflow_v, sbom_files=sbom_files
                )
            )


@sbom.command(name="generate-providers-requirements", help="Generate requirements for selected provider.")
@click.option(
    "--airflow-version", type=str, required=False, help="Airflow version to use to generate the requirements"
)
@click.option(
    "--python",
    type=BetterChoice(ALL_HISTORICAL_PYTHON_VERSIONS),
    default=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    required=False,
    help="Python version to generate the requirements for",
)
@click.option(
    "--provider-id",
    type=BetterChoice(list(PROVIDER_DEPENDENCIES.keys())),
    required=False,
    help="Provider to generate the requirements for",
    multiple=True,
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
    airflow_version: str | None,
    python: str,
    provider_id: tuple[str, ...],
    run_in_parallel: bool,
    parallelism: int,
    debug_resources: bool,
    include_success_outputs: bool,
    skip_cleanup: bool,
    force: bool,
):
    perform_environment_checks()

    provider_ids = provider_id
    if len(provider_ids) == 0:
        user_confirm(
            "You are about to generate all providers requirements based on the "
            "`provider_metadata.json` file (for all releases). Do you want to proceed?",
            quit_allowed=False,
            default_answer=Answer.YES,
        )
        with open(PROVIDER_METADATA_JSON_FILE_PATH) as f:
            provider_metadata = json.load(f)
            providers_info = [
                (p_id, p_version, info["associated_airflow_version"])
                for (p_id, p_versions) in provider_metadata.items()
                for (p_version, info) in p_versions.items()
            ]
    else:
        if airflow_version is None:
            airflow_version = get_active_airflow_versions(confirm=False)[-1]
            get_console().print(f"[info]Using {airflow_version} as airflow version")
        providers_info = [(provider_id, None, airflow_version) for provider_id in provider_ids]

    build_all_airflow_versions_base_image(python_version=python)

    if run_in_parallel:
        parallelism = min(parallelism, len(providers_info))
        get_console().print(f"[info]Running {len(providers_info)} jobs in parallel")
        with ci_group(f"Generating provider requirements for {providers_info}"):
            all_params = [
                f"Generate provider requirements for {p_id} version {p_version}"
                for (p_id, p_version, _) in providers_info
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
                            "provider_id": p_id,
                            "provider_version": p_version,
                            "airflow_version": a_version,
                            "python_version": python,
                            "force": force,
                        },
                    )
                    for (p_id, p_version, a_version) in providers_info
                ]
        check_async_run_results(
            results=results,
            success="Providers requirements were generated successfully",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        for p_id, p_version, a_version in providers_info:
            get_requirements_for_provider(
                provider_id=p_id,
                provider_version=p_version,
                airflow_version=a_version,
                python_version=python,
                force=force,
            )
