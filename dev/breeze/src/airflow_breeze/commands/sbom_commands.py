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

from pathlib import Path

import click

from airflow_breeze.global_constants import (
    ALL_HISTORICAL_PYTHON_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    PROVIDER_DEPENDENCIES,
)
from airflow_breeze.utils.cdxgen import (
    SbomApplicationJob,
    build_providers_base_image,
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
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import perform_environment_checks
from airflow_breeze.utils.github import get_active_airflow_versions
from airflow_breeze.utils.parallel import ShowLastLineProgressMatcher, check_async_run_results, run_with_pool
from airflow_breeze.utils.path_utils import AIRFLOW_TMP_DIR_PATH
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
    "--airflow-site-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path, exists=True),
    required=True,
    envvar="AIRFLOW_SITE_DIR",
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
    airflow_site_dir: Path,
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

    apache_airflow_dir = airflow_site_dir / "docs-archive" / "apache-airflow"

    for airflow_v in airflow_versions:
        airflow_version_dir = apache_airflow_dir / airflow_v
        if not airflow_version_dir.exists():
            get_console().print(f"[warning]The {airflow_version_dir} does not exist. Skipping")
            continue
        destination_dir = airflow_version_dir / "sbom"
        destination_dir.mkdir(parents=True, exist_ok=True)
        if destination_dir.exists():
            if not force:
                get_console().print(f"[warning]The {destination_dir} already exists. Skipping")
                continue
            else:
                get_console().print(f"[warning]The {destination_dir} already exists. Forcing update")
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
            all_params = [f"CI {job.airflow_version}:{job.python_version}" for job in jobs_to_run]
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
        airflow_version_dir = apache_airflow_dir / airflow_v
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


@sbom.command(name="generate-provider-requirements", help="Generate requirements for selected provider.")
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
    required=True,
    help="Provider to generate the requirements for",
)
@click.option(
    "--provider-version", type=str, required=False, help="Provider version to generate the requirements for"
)
@option_verbose
@option_dry_run
@option_answer
def generate_provider_requirements(
    airflow_version: str | None,
    python: str,
    provider_id: str,
    provider_version: str | None,
):
    perform_environment_checks()
    if airflow_version is None:
        airflow_version = get_active_airflow_versions(confirm=False)[-1]
        get_console().print(f"[info]Using {airflow_version} as airflow version")
    build_providers_base_image(airflow_version=airflow_version, python_version=python)
    get_requirements_for_provider(
        provider_id=provider_id,
        provider_version=provider_version,
        airflow_version=airflow_version,
        python_version=python,
    )
