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
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from shlex import quote
from typing import List, Optional, Tuple

import click

from airflow_breeze.commands.production_image_commands import run_build_production_image
from airflow_breeze.global_constants import ALLOWED_EXECUTORS, ALLOWED_KUBERNETES_VERSIONS
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.common_options import (
    option_dry_run,
    option_include_success_outputs,
    option_parallelism,
    option_python,
    option_python_versions,
    option_run_in_parallel,
    option_verbose,
)
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import CacheableChoice, CacheableDefault
from airflow_breeze.utils.kubernetes_utils import (
    CHART_PATH,
    K8S_CLUSTERS_PATH,
    SCRIPTS_CI_KUBERNETES_PATH,
    create_virtualenv,
    get_config_folder,
    get_k8s_env,
    get_kind_cluster_config_path,
    get_kind_cluster_name,
    get_kubeconfig_file,
    get_kubectl_cluster_name,
    get_kubernetes_python_combos,
    make_sure_kubernetes_tools_are_installed,
    print_cluster_urls,
    run_command_with_k8s_env,
    set_random_cluster_ports,
)
from airflow_breeze.utils.parallel import (
    DockerBuildxProgressMatcher,
    GenericRegexpProgressMatcher,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.run_utils import RunCommandResult, check_if_image_exists, run_command


@click.group(cls=BreezeGroup, name='k8s', help='Tools that developers use to run Kubernetes tests')
def kubernetes_group():
    pass


option_executor = click.option(
    '--executor',
    help='Executor to use for a kubernetes cluster.',
    type=CacheableChoice(ALLOWED_EXECUTORS),
    show_default=True,
    default=CacheableDefault(ALLOWED_EXECUTORS[0]),
    envvar='EXECUTOR',
)

option_kubernetes_version = click.option(
    '--kubernetes-version',
    help='Kubernetes version used to create the KinD cluster of.',
    type=CacheableChoice(ALLOWED_KUBERNETES_VERSIONS),
    show_default=True,
    default=CacheableDefault(ALLOWED_KUBERNETES_VERSIONS[0]),
    envvar='KUBERNETES_VERSION',
)

option_image_tag = click.option(
    '-t',
    '--image-tag',
    help='Image tag used to build K8S image from',
    default='latest',
    show_default=True,
    envvar='IMAGE_TAG',
)

option_wait_time_in_seconds = click.option(
    '--wait-time-in-seconds',
    help="Wait for Airflow webserver for specified number of seconds.",
    type=click.IntRange(0),
    default=120,
    envvar='WAIT_TIME_IN_SECONDS',
)

option_wait_time_in_seconds_0_default = click.option(
    '--wait-time-in-seconds',
    help="Wait for Airflow webserver for specified number of seconds.",
    type=click.IntRange(0),
    default=0,
    envvar='WAIT_TIME_IN_SECONDS',
)


option_force_venv_setup = click.option(
    '--force-venv-setup',
    help="Force recreation of the virtualenv.",
    is_flag=True,
    envvar='FORCE_VENV_SETUP',
)

option_rebuild_base_image = click.option(
    '--rebuild-base-image',
    help="Rebuilds base Airflow image before building K8S image.",
    is_flag=True,
    envvar='REBUILD_BASE_IMAGE',
)

option_kubernetes_versions = click.option(
    '--kubernetes-versions',
    help='Kubernetes versions used to run in parallel (space separated).',
    type=str,
    show_default=True,
    default=" ".join(ALLOWED_KUBERNETES_VERSIONS),
    envvar='KUBERNETES_VERSIONS',
)

option_all = click.option('--all', help="Apply it to all created clusters", is_flag=True, envvar="ALL")

K8S_CLUSTER_CREATE_PROGRESS_REGEXP = r'.*airflow-python-[0-9.]+-v[0-9.].*|.*Connecting to localhost.*'
K8S_UPLOAD_PROGRESS_REGEXP = r'.*airflow-python-[0-9.]+-v[0-9.].*'
K8S_CONFIGURE_CLUSTER_PROGRESS_REGEXP = r'.*airflow-python-[0-9.]+-v[0-9.].*'
K8S_DEPLOY_PROGRESS_REGEXP = r'.*airflow-python-[0-9.]+-v[0-9.].*'
K8S_TEST_PROGRESS_REGEXP = r'.*airflow-python-[0-9.]+-v[0-9.].*|^kubernetes_tests/.*'
PERCENT_K8S_TEST_PROGRESS_REGEXP = r'^kubernetes_tests/.*\[[ \d*%]*\].*'
K8S_SKIP_TRUNCATION_REGEXP = r'^kubernetes_tests/.*'


@kubernetes_group.command(name="setup-env", help="Setup shared Kubernetes virtual environment and tools.")
@click.option(
    '--force',
    help="Force recreation of the virtualenv even if it is already created.",
    is_flag=True,
    envvar='FORCE',
)
@option_verbose
@option_dry_run
def setup_env(force: bool, verbose: bool, dry_run: bool):
    result = create_virtualenv(force, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(1)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    get_console().print("\n[warning]NEXT STEP:[/][info] You might now create your cluster by:\n")
    get_console().print("\nbreeze k8s create-cluster\n")


def _create_cluster(
    python: str,
    kubernetes_version: str,
    output: Optional[Output],
    num_tries: int,
    force: bool,
    verbose: bool,
    dry_run: bool,
) -> Tuple[int, str]:
    while True:
        if force:
            _delete_cluster(
                python=python,
                kubernetes_version=kubernetes_version,
                output=output,
                verbose=verbose,
                dry_run=dry_run,
            )
        kubeconfig_file = get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version)
        cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
        kubeconfig_file.parent.mkdir(parents=True, exist_ok=True)
        kubeconfig_file.touch(mode=0o700)
        get_console(output=output).print(f"[info]Creating KinD cluster {cluster_name}!")
        set_random_cluster_ports(python=python, kubernetes_version=kubernetes_version, output=output)
        result = run_command_with_k8s_env(
            [
                'kind',
                'create',
                'cluster',
                '--name',
                cluster_name,
                "--config",
                str(get_kind_cluster_config_path(python=python, kubernetes_version=kubernetes_version)),
                "--image",
                f"kindest/node:{kubernetes_version}",
            ],
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            text=True,
            dry_run=dry_run,
            verbose=verbose,
            check=False,
        )
        if result.returncode == 0:
            print_cluster_urls(python=python, kubernetes_version=kubernetes_version, output=output)
            get_console(output=output).print(f"[success]KinD cluster {cluster_name} created!\n")
            get_console().print("\n[warning]NEXT STEP:[/][info] You might now configure your cluster by:\n")
            get_console().print("\nbreeze k8s configure-cluster\n")
            return result.returncode, f"K8S cluster {cluster_name}."
        num_tries -= 1
        if num_tries == 0:
            return result.returncode, f"K8S cluster {cluster_name}."
        else:
            get_console(output=output).print(
                f"[warning]Failed to create KinD cluster {cluster_name}. "
                f"Retrying! There are {num_tries} tries left.\n"
            )
            _delete_cluster(
                python=python,
                kubernetes_version=kubernetes_version,
                output=output,
                verbose=verbose,
                dry_run=dry_run,
            )


@kubernetes_group.command(
    name="create-cluster",
    help="Create a KinD Cluster for Python and Kubernetes version specified "
    "(optionally create all clusters in parallel).",
)
@click.option(
    '--force',
    help="Force recreation of the cluster even if it is already created.",
    is_flag=True,
    envvar='FORCE',
)
@option_python
@option_kubernetes_version
@option_run_in_parallel
@option_parallelism
@option_kubernetes_versions
@option_python_versions
@option_include_success_outputs
@option_verbose
@option_dry_run
def create_cluster(
    force: bool,
    python: str,
    kubernetes_version: str,
    run_in_parallel: bool,
    parallelism: int,
    kubernetes_versions: str,
    python_versions: str,
    include_success_outputs: bool,
    verbose: bool,
    dry_run: bool,
):
    result = create_virtualenv(force=False, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    if run_in_parallel:
        python_version_array: List[str] = python_versions.split(" ")
        kubernetes_version_array: List[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Creating clusters {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                progress_matcher=GenericRegexpProgressMatcher(
                    K8S_CLUSTER_CREATE_PROGRESS_REGEXP, lines_to_search=15
                ),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _create_cluster,
                        kwds={
                            "python": combo.python_version,
                            "kubernetes_version": combo.kubernetes_version,
                            "force": False,
                            "num_tries": 3,  # when creating cluster in parallel, sometimes we need to retry
                            "verbose": verbose,
                            "dry_run": dry_run,
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success="All clusters created.",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _create_cluster(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
            force=force,
            num_tries=1,
            verbose=verbose,
            dry_run=dry_run,
        )
        if return_code != 0:
            sys.exit(return_code)


def _delete_cluster(
    python: str, kubernetes_version: str, output: Optional[Output], dry_run: bool, verbose: bool
):
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"[info]Deleting KinD cluster {cluster_name}!")
    folder = get_config_folder(python=python, kubernetes_version=kubernetes_version)
    run_command_with_k8s_env(
        [
            'kind',
            'delete',
            'cluster',
            '--name',
            cluster_name,
        ],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        text=True,
        dry_run=dry_run,
        verbose=verbose,
        check=False,
    )
    if not folder.exists():
        get_console(output=output).print(
            f"[warning]KinD cluster {cluster_name} was not created before but "
            f"running delete in case it was created manually !\n"
        )
    shutil.rmtree(folder, ignore_errors=True)
    get_console(output=output).print(f"[success]KinD cluster {cluster_name} deleted!\n")


def _delete_all_clusters(dry_run: bool, verbose: bool):
    clusters = list(K8S_CLUSTERS_PATH.iterdir())
    if len(clusters) == 0:
        get_console().print("\n[warning]No clusters.\n")
    else:
        get_console().print("\n[info]Deleting clusters")
        for cluster_name in clusters:
            resolved_path = cluster_name.resolve()
            python, kubernetes_version = _get_python_kubernetes_version_from_name(resolved_path.name)
            if python and kubernetes_version:
                _delete_cluster(
                    python=python,
                    kubernetes_version=kubernetes_version,
                    output=None,
                    verbose=verbose,
                    dry_run=dry_run,
                )
            else:
                get_console().print(
                    f"[warning]The cluster {resolved_path.name} does not match expected name. "
                    f"Just removing the {resolved_path}!\n"
                )
                if resolved_path.is_dir():
                    shutil.rmtree(cluster_name.resolve(), ignore_errors=True)
                else:
                    resolved_path.unlink()


@kubernetes_group.command(
    name="delete-cluster", help="Delete the current KinD Cluster (optionally all clusters)."
)
@option_python
@option_kubernetes_version
@option_all
@option_verbose
@option_dry_run
def delete_cluster(python: str, kubernetes_version: str, all: bool, verbose: bool, dry_run: bool):
    result = create_virtualenv(force=False, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    if all:
        _delete_all_clusters(verbose=verbose, dry_run=dry_run)
    else:
        _delete_cluster(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
            dry_run=dry_run,
            verbose=verbose,
        )


def _get_python_kubernetes_version_from_name(cluster_name: str) -> Tuple[Optional[str], Optional[str]]:
    matcher = re.compile(r'airflow-python-(\d+\.\d+)-(v\d+.\d+.\d+)')
    cluster_match = matcher.search(cluster_name)
    if cluster_match:
        python = cluster_match.group(1)
        kubernetes_version = cluster_match.group(2)
        return python, kubernetes_version
    else:
        return None, None


LIST_CONSOLE_WIDTH = 120


def _status(
    python: str, kubernetes_version: str, wait_time_in_seconds: int, dry_run: bool, verbose: bool
) -> bool:
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    kubectl_cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    if not get_kind_cluster_config_path(python=python, kubernetes_version=kubernetes_version).exists():
        get_console().print(f"\n[warning]Cluster: {cluster_name} has not been created yet\n")
        get_console().print(
            "[info]Run: "
            f"`breeze k8s create-cluster --python {python} --kubernetes-version {kubernetes_version}`"
            "to create it.\n"
        )
        return False
    get_console().print("[info]" + "=" * LIST_CONSOLE_WIDTH)
    get_console().print(f"[info]Cluster: {cluster_name}\n")
    kubeconfig_file = get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version)
    get_console().print(f"    * KUBECONFIG={kubeconfig_file}")
    kind_config_file = get_kind_cluster_config_path(python=python, kubernetes_version=kubernetes_version)
    get_console().print(f"    * KINDCONFIG={kind_config_file}")
    get_console().print(f"\n[info]Cluster info: {cluster_name}\n")
    result = run_command_with_k8s_env(
        ['kubectl', 'cluster-info', '--cluster', kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
        dry_run=dry_run,
        verbose=verbose,
        check=False,
    )
    if result.returncode != 0:
        return False
    get_console().print(f"\n[info]Storage class for {cluster_name}\n")
    result = run_command_with_k8s_env(
        ['kubectl', 'get', 'storageclass', '--cluster', kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
        dry_run=dry_run,
        verbose=verbose,
        check=False,
    )
    if result.returncode != 0:
        return False
    get_console().print(f"\n[info]Running pods for {cluster_name}\n")
    result = run_command_with_k8s_env(
        ['kubectl', 'get', '-n', "kube-system", 'pods', '--cluster', kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
        dry_run=dry_run,
        verbose=verbose,
        check=False,
    )
    if result.returncode != 0:
        return False
    print_cluster_urls(python, kubernetes_version, wait_time_in_seconds=wait_time_in_seconds, output=None)
    get_console().print(f"\n[success]Cluster healthy: {cluster_name}\n")
    return True


@kubernetes_group.command(
    name="status",
    help="Check status of the current cluster and airflow deployed to it (optionally all clusters).",
)
@option_python
@option_kubernetes_version
@option_wait_time_in_seconds_0_default
@option_all
@option_verbose
@option_dry_run
def status(
    kubernetes_version: str,
    python: str,
    wait_time_in_seconds: int,
    all: bool,
    verbose: bool,
    dry_run: bool,
):
    result = create_virtualenv(force=False, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    if all:
        clusters = list(K8S_CLUSTERS_PATH.iterdir())
        if len(clusters) == 0:
            get_console().print("\n[warning]No clusters.\n")
            sys.exit(1)
        else:
            failed = False
            get_console().print("[info]\nCluster status:\n")
            for cluster_name in clusters:
                name = cluster_name.name
                found_python, found_kubernetes_version = _get_python_kubernetes_version_from_name(name)
                if not found_python or not found_kubernetes_version:
                    get_console().print(f"[warning]\nCould not get cluster from {name}. Skipping.\n")
                    continue
                if not _status(
                    python=found_python,
                    kubernetes_version=found_kubernetes_version,
                    wait_time_in_seconds=wait_time_in_seconds,
                    dry_run=dry_run,
                    verbose=verbose,
                ):
                    failed = True
            if failed:
                get_console().print("\n[error]Some clusters are not healthy!\n")
                sys.exit(1)
    else:
        if not _status(
            python=python,
            kubernetes_version=kubernetes_version,
            wait_time_in_seconds=wait_time_in_seconds,
            dry_run=dry_run,
            verbose=verbose,
        ):
            get_console().print("\n[error]The cluster is not healthy!\n")
            sys.exit(1)


def check_if_base_image_exists(params: BuildProdParams, verbose: bool, dry_run: bool) -> bool:
    return check_if_image_exists(
        image=params.airflow_image_name_with_tag,
        verbose=verbose,
        dry_run=dry_run,
    )


def _rebuild_k8s_image(
    python: str,
    image_tag: str,
    rebuild_base_image: bool,
    dry_run: bool,
    verbose: bool,
    output: Optional[Output],
) -> Tuple[int, str]:
    params = BuildProdParams(python=python, image_tag=image_tag)
    if rebuild_base_image:
        run_build_production_image(prod_image_params=params, verbose=verbose, dry_run=dry_run, output=output)
    else:
        if not check_if_base_image_exists(params, verbose=verbose, dry_run=dry_run):
            get_console(output=output).print(
                f"[error]The base PROD image {params.airflow_image_name_with_tag} does not exist locally.\n"
            )
            if image_tag == 'latest':
                get_console(output=output).print(
                    "[warning]Please add `--rebuild-base-image` flag or rebuild it manually with:\n"
                )
                get_console(output=output).print(f"breeze prod-image build --python {python}\n")
            else:
                get_console(output=output).print("[warning]Please pull the image:\n")
                get_console(output=output).print(
                    f"breeze prod-image pull --python {python} --image-tag {image_tag}\n"
                )
            sys.exit(1)
    get_console(output=output).print(
        f"[info]Building the K8S image for Python {python} using "
        f"airflow base image: {params.airflow_image_name_with_tag}\n"
    )
    docker_image_for_kubernetes_tests = f"""
FROM {params.airflow_image_name_with_tag}

COPY airflow/example_dags/ /opt/airflow/dags/

COPY airflow/kubernetes_executor_templates/ /opt/airflow/pod_templates/

ENV GUNICORN_CMD_ARGS='--preload' AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL=0
"""
    image = f"{params.airflow_image_kubernetes}:latest"
    docker_build_result = run_command(
        ['docker', 'build', "--tag", image, ".", "-f", "-"],
        input=docker_image_for_kubernetes_tests,
        text=True,
        check=False,
        dry_run=dry_run,
        verbose=verbose,
        output=output,
    )
    if docker_build_result.returncode != 0:
        get_console(output=output).print("[error]Error when building the kubernetes image.")
    return docker_build_result.returncode, f"K8S image for Python {python}"


def _upload_k8s_image(
    python: str, kubernetes_version: str, output: Optional[Output], dry_run: bool, verbose: bool
) -> Tuple[int, str]:
    params = BuildProdParams(python=python)
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(
        f'[info]Uploading Airflow image {params.airflow_image_kubernetes} to cluster {cluster_name}'
    )
    kind_load_result = run_command_with_k8s_env(
        ['kind', 'load', "docker-image", "--name", cluster_name, params.airflow_image_kubernetes],
        python=python,
        output=output,
        kubernetes_version=kubernetes_version,
        dry_run=dry_run,
        verbose=verbose,
        check=False,
    )
    if kind_load_result.returncode != 0:
        get_console(output=output).print(
            f"[error]Error when uploading {params.airflow_image_kubernetes} image to "
            f"KinD cluster {cluster_name}."
        )
    return kind_load_result.returncode, f"Uploaded K8S image to {cluster_name}"


@kubernetes_group.command(
    name="build-k8s-image",
    help="Build k8s-ready airflow image (optionally all images in parallel).",
)
@option_python
@option_image_tag
@option_rebuild_base_image
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_include_success_outputs
@option_verbose
@option_dry_run
def build_k8s_image(
    python: str,
    image_tag: str,
    rebuild_base_image: bool,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    include_success_outputs: bool,
    verbose: bool,
    dry_run: bool,
):
    result = create_virtualenv(force=False, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    if run_in_parallel:
        python_version_array: List[str] = python_versions.split(" ")
        with ci_group(f"Building K8s images for {python_versions}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=[f"Image {python}" for python in python_version_array],
                progress_matcher=DockerBuildxProgressMatcher(),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _rebuild_k8s_image,
                        kwds={
                            "python": _python,
                            "image_tag": image_tag,
                            "rebuild_base_image": rebuild_base_image,
                            "verbose": verbose,
                            "dry_run": dry_run,
                            "output": outputs[index],
                        },
                    )
                    for index, _python in enumerate(python_version_array)
                ]
        check_async_run_results(
            results=results,
            success="All K8S images built correctly.",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _rebuild_k8s_image(
            python=python,
            image_tag=image_tag,
            rebuild_base_image=rebuild_base_image,
            verbose=verbose,
            dry_run=dry_run,
            output=None,
        )
        if return_code == 0:
            get_console().print("\n[warning]NEXT STEP:[/][info] You might now upload your k8s image by:\n")
            get_console().print("\nbreeze k8s upload-k8s-image\n")
        sys.exit(return_code)


@kubernetes_group.command(
    name="upload-k8s-image",
    help="Upload k8s-ready airflow image to the KinD cluster (optionally to all clusters in parallel)",
)
@option_python
@option_kubernetes_version
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_kubernetes_versions
@option_include_success_outputs
@option_verbose
@option_dry_run
def upload_k8s_image(
    python: str,
    kubernetes_version: str,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    kubernetes_versions: str,
    include_success_outputs: bool,
    verbose: bool,
    dry_run: bool,
):
    result = create_virtualenv(force=False, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    if run_in_parallel:
        python_version_array: List[str] = python_versions.split(" ")
        kubernetes_version_array: List[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Uploading K8s images for {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                progress_matcher=GenericRegexpProgressMatcher(
                    regexp=K8S_UPLOAD_PROGRESS_REGEXP, lines_to_search=2
                ),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _upload_k8s_image,
                        kwds={
                            "python": combo.python_version,
                            "kubernetes_version": combo.kubernetes_version,
                            "verbose": verbose,
                            "dry_run": dry_run,
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success="All K8S images uploaded correctly.",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _upload_k8s_image(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
            dry_run=dry_run,
            verbose=verbose,
        )
        if return_code == 0:
            get_console().print("\n[warning]NEXT STEP:[/][info] You might now deploy airflow by:\n")
            get_console().print("\nbreeze k8s deploy-airflow\n")
        sys.exit(return_code)


HELM_DEFAULT_NAMESPACE = "default"
HELM_AIRFLOW_NAMESPACE = "airflow"
TEST_NAMESPACE = "test-namespace"


def _recreate_namespaces(
    python: str,
    kubernetes_version: str,
    verbose: bool,
    dry_run: bool,
    output: Optional[Output],
) -> RunCommandResult:
    cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"[info]Deleting K8S namespaces for {cluster_name}")
    run_command_with_k8s_env(
        ["kubectl", "delete", "namespace", HELM_AIRFLOW_NAMESPACE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=False,
        verbose=verbose,
        dry_run=dry_run,
    )
    run_command_with_k8s_env(
        ["kubectl", "delete", "namespace", TEST_NAMESPACE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=False,
        verbose=verbose,
        dry_run=dry_run,
    )
    get_console(output=output).print("[info]Creating namespaces")
    result = run_command_with_k8s_env(
        ["kubectl", "create", "namespace", HELM_AIRFLOW_NAMESPACE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
    )
    if result.returncode != 0:
        return result
    result = run_command_with_k8s_env(
        ["kubectl", "create", "namespace", TEST_NAMESPACE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
    )
    if result.returncode == 0:
        get_console(output=output).print(f"[success]Created K8S namespaces for cluster {cluster_name}\n")
    return result


def _deploy_test_resources(
    python: str, kubernetes_version: str, output: Optional[Output], verbose: bool, dry_run: bool
) -> RunCommandResult:
    cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"[info]Deploying test resources for cluster {cluster_name}")
    result = run_command_with_k8s_env(
        [
            "kubectl",
            "apply",
            "-f",
            str(SCRIPTS_CI_KUBERNETES_PATH / "volumes.yaml"),
            "--namespace",
            HELM_DEFAULT_NAMESPACE,
        ],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
    )
    if result.returncode != 0:
        return result
    result = run_command_with_k8s_env(
        [
            "kubectl",
            "apply",
            "-f",
            str(SCRIPTS_CI_KUBERNETES_PATH / "nodeport.yaml"),
            "--namespace",
            HELM_AIRFLOW_NAMESPACE,
        ],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
    )
    if result.returncode == 0:
        get_console(output=output).print(f"[success]Deployed test resources for cluster {cluster_name}")
    return result


def _configure_k8s_cluster(
    python: str, kubernetes_version: str, output: Optional[Output], verbose: bool, dry_run: bool
) -> Tuple[int, str]:
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f'[info]Configuring {cluster_name} to be ready for Airflow deployment')
    result = _recreate_namespaces(
        python=python, kubernetes_version=kubernetes_version, output=output, dry_run=dry_run, verbose=verbose
    )
    if result.returncode == 0:
        result = _deploy_test_resources(
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            dry_run=dry_run,
            verbose=verbose,
        )
    return result.returncode, f"Configure {cluster_name}"


@kubernetes_group.command(
    name="configure-cluster",
    help="Configures cluster for airflow deployment - creates namespaces and test resources "
    "(optionally for all clusters in parallel).",
)
@option_python
@option_kubernetes_version
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_kubernetes_versions
@option_include_success_outputs
@option_verbose
@option_dry_run
def configure_cluster(
    python: str,
    kubernetes_version: str,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    kubernetes_versions: str,
    include_success_outputs: bool,
    verbose: bool,
    dry_run: bool,
):
    result = create_virtualenv(force=False, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    if run_in_parallel:
        python_version_array: List[str] = python_versions.split(" ")
        kubernetes_version_array: List[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Setting up clusters for {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                progress_matcher=GenericRegexpProgressMatcher(
                    regexp=K8S_CONFIGURE_CLUSTER_PROGRESS_REGEXP, lines_to_search=10
                ),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _configure_k8s_cluster,
                        kwds={
                            "python": combo.python_version,
                            "kubernetes_version": combo.kubernetes_version,
                            "verbose": verbose,
                            "dry_run": dry_run,
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success="All clusters configured correctly.",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _configure_k8s_cluster(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
            dry_run=dry_run,
            verbose=verbose,
        )
        if return_code == 0:
            get_console().print("\n[warning]NEXT STEP:[/][info] You might now build your k8s image by:\n")
            get_console().print("\nbreeze k8s build-k8s-image\n")
        sys.exit(return_code)


def _deploy_helm_chart(
    python: str,
    upgrade: bool,
    kubernetes_version: str,
    output: Optional[Output],
    executor: str,
    verbose: bool,
    dry_run: bool,
    extra_options: Optional[Tuple[str, ...]] = None,
) -> RunCommandResult:
    cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"[info]Deploying {cluster_name} with airflow Helm Chart.")
    with tempfile.TemporaryDirectory(prefix="chart_") as tmp_dir:
        tmp_chart_path = Path(tmp_dir).resolve() / "chart"
        shutil.copytree(CHART_PATH, os.fspath(tmp_chart_path), ignore_dangling_symlinks=True)
        get_console(output=output).print(f"[info]Copied chart sources to {tmp_chart_path}")
        kubectl_context = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
        params = BuildProdParams(python=python)
        airflow_kubernetes_image_name = params.airflow_image_kubernetes
        helm_command = [
            "helm",
            "upgrade" if upgrade else "install",
            "airflow",
            os.fspath(tmp_chart_path.resolve()),
            "--kube-context",
            kubectl_context,
            "--timeout",
            "10m0s",
            "--namespace",
            HELM_AIRFLOW_NAMESPACE,
            "--set",
            f"defaultAirflowRepository={airflow_kubernetes_image_name}",
            "--set",
            "defaultAirflowTag=latest",
            "-v",
            "1",
            "--set",
            f"images.airflow.repository={airflow_kubernetes_image_name}",
            "--set",
            "images.airflow.tag=latest",
            "-v",
            "1",
            "--set",
            "config.api.auth_backends=airflow.api.auth.backend.basic_auth",
            "--set",
            "config.logging.logging_level=DEBUG",
            "--set",
            f"executor={executor}",
        ]
        if upgrade:
            # force upgrade
            helm_command.append("--force")
        if extra_options:
            helm_command.extend(extra_options)
        get_console(output=output).print(f"[info]Deploying Airflow from {tmp_chart_path}")
        result = run_command_with_k8s_env(
            helm_command,
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            verbose=verbose,
            dry_run=dry_run,
            check=False,
        )
        if result.returncode == 0:
            get_console(output=output).print(f"[success]Deployed {cluster_name} with airflow Helm Chart.")
        return result


def _deploy_airflow(
    python: str,
    kubernetes_version: str,
    output: Optional[Output],
    executor: str,
    upgrade: bool,
    wait_time_in_seconds: int,
    verbose: bool,
    dry_run: bool,
    extra_options: Optional[Tuple[str, ...]] = None,
) -> Tuple[int, str]:
    action = "Deploying" if not upgrade else "Upgrading"
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f'[info]{action} Airflow for cluster {cluster_name}')
    result = _deploy_helm_chart(
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        upgrade=upgrade,
        executor=executor,
        verbose=verbose,
        dry_run=dry_run,
        extra_options=extra_options,
    )
    if result.returncode == 0:
        get_console(output=output).print(
            f"\n[success]Airflow for Python {python} and "
            f"K8S version {kubernetes_version} has been successfully deployed."
        )
        kubectl_cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
        get_console(output=output).print(
            f"\nThe KinD cluster name: {cluster_name}\nThe kubectl cluster name: {kubectl_cluster_name}.\n"
        )
        print_cluster_urls(
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            wait_time_in_seconds=wait_time_in_seconds,
        )
    return result.returncode, f"{action} Airflow to {cluster_name}"


@kubernetes_group.command(
    name="deploy-airflow",
    help="Deploy airflow image to the current KinD cluster (or all clusters).",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@option_python
@option_kubernetes_version
@option_executor
@click.option(
    '--upgrade',
    help="Upgrade Helm Chart rather than installing it.",
    is_flag=True,
    envvar='UPGRADE',
)
@option_wait_time_in_seconds
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_kubernetes_versions
@option_include_success_outputs
@option_verbose
@option_dry_run
@click.argument('extra_options', nargs=-1, type=click.UNPROCESSED)
def deploy_airflow(
    python: str,
    kubernetes_version: str,
    executor: str,
    upgrade: bool,
    wait_time_in_seconds: int,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    kubernetes_versions: str,
    include_success_outputs: bool,
    verbose: bool,
    dry_run: bool,
    extra_options: Optional[Tuple[str, ...]] = None,
):
    if run_in_parallel:
        python_version_array: List[str] = python_versions.split(" ")
        kubernetes_version_array: List[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Deploying airflow for: {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                progress_matcher=GenericRegexpProgressMatcher(K8S_DEPLOY_PROGRESS_REGEXP, lines_to_search=15),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _deploy_airflow,
                        kwds={
                            "python": combo.python_version,
                            "kubernetes_version": combo.kubernetes_version,
                            "executor": executor,
                            "upgrade": upgrade,
                            "wait_time_in_seconds": wait_time_in_seconds,
                            "dry_run": dry_run,
                            "verbose": verbose,
                            "extra_options": extra_options,
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success="All Airflow charts successfully deployed.",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _deploy_airflow(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
            executor=executor,
            upgrade=upgrade,
            wait_time_in_seconds=wait_time_in_seconds,
            verbose=verbose,
            dry_run=dry_run,
            extra_options=extra_options,
        )
        if return_code == 0:
            get_console().print(
                "\n[warning]NEXT STEP:[/][info] You might now run tests or interact "
                "with airflow via shell (kubectl, pytest etc.) or k9s commands:\n"
            )
            get_console().print("\nbreeze k8s tests")
            get_console().print("\nbreeze k8s shell")
            get_console().print("\nbreeze k8s k9s\n")
        sys.exit(return_code)


@kubernetes_group.command(
    name="k9s",
    help="Run k9s tool. You can pass any k9s args as extra args.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_kubernetes_version
@option_verbose
@option_dry_run
@click.argument('k9s_args', nargs=-1, type=click.UNPROCESSED)
def k9s(python: str, kubernetes_version: str, verbose: bool, dry_run: bool, k9s_args: Tuple[str, ...]):
    result = create_virtualenv(force=False, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    env = get_k8s_env(python=python, kubernetes_version=kubernetes_version)
    env['TERM'] = 'xterm-256color'
    editor = env.get('EDITOR')
    if not editor:
        env['EDITOR'] = 'vim'
    k9s_editor = env.get('K9S_EDITOR')
    if not k9s_editor:
        env['K9S_EDITOR'] = env['EDITOR']
    kubeconfig_file = get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version)
    result = run_command(
        [
            'docker',
            'run',
            "--rm",
            '-it',
            '--network',
            'host',
            '-e',
            'EDITOR',
            '-e',
            'K9S_EDITOR',
            '-v',
            f"{kubeconfig_file}:/root/.kube/config",
            'quay.io/derailed/k9s',
            '--namespace',
            HELM_AIRFLOW_NAMESPACE,
            *k9s_args,
        ],
        verbose=verbose,
        dry_run=dry_run,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        sys.exit(result.returncode)


def _logs(python: str, kubernetes_version: str, verbose: bool, dry_run: bool):
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    tmpdir = Path(tempfile.gettempdir()) / f"kind_logs_{cluster_name}"
    get_console().print(f"[info]\nDumping logs for {cluster_name} to {tmpdir}:\n")
    run_command_with_k8s_env(
        ['kind', '--name', cluster_name, 'export', 'logs', str(tmpdir)],
        python=python,
        kubernetes_version=kubernetes_version,
        verbose=verbose,
        dry_run=dry_run,
        check=False,
    )


@kubernetes_group.command(
    name="logs",
    help=f"Dump k8s logs to ${{TMP_DIR}}{os.sep}kind_logs_<cluster_name> directory "
    f"(optionally all clusters). ",
)
@option_python
@option_kubernetes_version
@option_all
@option_verbose
@option_dry_run
def logs(python: str, kubernetes_version: str, all: bool, verbose: bool, dry_run: bool):
    if all:
        clusters = list(K8S_CLUSTERS_PATH.iterdir())
        if len(clusters) == 0:
            get_console().print("\n[warning]No clusters.\n")
            sys.exit(1)
        else:
            get_console().print("[info]\nDumping cluster logs:\n")
            for cluster_name in clusters:
                name = cluster_name.name
                found_python, found_kubernetes_version = _get_python_kubernetes_version_from_name(name)
                if not found_python or not found_kubernetes_version:
                    get_console().print(f"[warning]\nCould not get cluster from {name}. Skipping.\n")
                    continue
                _logs(
                    python=found_python,
                    kubernetes_version=found_kubernetes_version,
                    verbose=verbose,
                    dry_run=dry_run,
                )
    else:
        _logs(python=python, kubernetes_version=kubernetes_version, verbose=verbose, dry_run=dry_run)


@kubernetes_group.command(
    name="shell",
    help="Run shell environment for the current KinD cluster.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_python
@option_kubernetes_version
@option_executor
@option_force_venv_setup
@option_verbose
@option_dry_run
@click.argument('shell_args', nargs=-1, type=click.UNPROCESSED)
def shell(
    python: str,
    kubernetes_version: str,
    executor: str,
    force_venv_setup: bool,
    verbose: bool,
    dry_run: bool,
    shell_args: Tuple[str, ...],
):
    result = create_virtualenv(force=force_venv_setup, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    env = get_k8s_env(python=python, kubernetes_version=kubernetes_version, executor=executor)
    get_console().print("\n[info]Entering interactive k8s shell.\n")
    shell_binary = env['SHELL']
    extra_args: List[str] = []
    if shell_binary.endswith("zsh"):
        extra_args.append('--no-rcs')
    elif shell_binary.endswith("bash"):
        extra_args.extend(['--norc', '--noprofile'])
    result = run_command(
        [shell_binary, *extra_args, *shell_args], verbose=verbose, dry_run=dry_run, env=env, check=False
    )
    if result.returncode != 0:
        sys.exit(result.returncode)


def _run_tests(
    python: str,
    kubernetes_version: str,
    output: Optional[Output],
    executor: str,
    test_args: Tuple[str, ...],
    verbose: bool,
    dry_run: bool,
) -> Tuple[int, str]:
    env = get_k8s_env(python=python, kubernetes_version=kubernetes_version, executor=executor)
    kubectl_cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"\n[info]Running tests with {kubectl_cluster_name} cluster.")
    shell_binary = env.get('SHELL', shutil.which('bash'))
    extra_shell_args: List[str] = []
    if shell_binary.endswith("zsh"):
        extra_shell_args.append('--no-rcs')
    elif shell_binary.endswith("bash"):
        extra_shell_args.extend(['--norc', '--noprofile'])
    the_tests = []
    if not any(arg.startswith("kubernetes_tests") for arg in test_args):
        # if no tests specified - use args
        the_tests.append('kubernetes_tests')
    command_to_run = ' '.join([quote(arg) for arg in ['pytest', *the_tests, *test_args]])
    get_console(output).print(f"[info] Command to run:[/] {command_to_run}")
    result = run_command(
        [shell_binary, *extra_shell_args, '-c', command_to_run],
        verbose=verbose,
        dry_run=dry_run,
        output=output,
        env=env,
        check=False,
    )
    return result.returncode, f"Tests {kubectl_cluster_name}"


@kubernetes_group.command(
    name="tests",
    help="Run tests against the current KinD cluster (optionally for all clusters in parallel).",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@option_python
@option_kubernetes_version
@option_executor
@option_force_venv_setup
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_kubernetes_versions
@option_include_success_outputs
@option_verbose
@option_dry_run
@click.argument('test_args', nargs=-1, type=click.Path())
def tests(
    python: str,
    kubernetes_version: str,
    executor: str,
    force_venv_setup: bool,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    kubernetes_versions: str,
    include_success_outputs: bool,
    verbose: bool,
    dry_run: bool,
    test_args: Tuple[str, ...],
):
    result = create_virtualenv(force=force_venv_setup, verbose=verbose, dry_run=dry_run)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed(verbose=verbose, dry_run=dry_run)
    if run_in_parallel:
        pytest_args = [
            "--verbosity=0",
            "--strict-markers",
            "--durations=100",
            "--maxfail=50",
            "--color=yes",
            # timeouts in seconds for individual tests
            "--timeouts-order",
            "moi",
            "--setup-timeout=60",
            "--execution-timeout=60",
            "--teardown-timeout=60",
            # Only display summary for non-expected case
            # f - failed
            # E - error
            # X - xpassed (passed even if expected to fail)
            # The following cases are not displayed:
            # s - skipped
            # x - xfailed (expected to fail and failed)
            # p - passed
            # P - passed with output
            "-rfEX",
            *test_args,
        ]
        python_version_array: List[str] = python_versions.split(" ")
        kubernetes_version_array: List[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Running tests for: {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                progress_matcher=GenericRegexpProgressMatcher(
                    K8S_TEST_PROGRESS_REGEXP,
                    lines_to_search=15,
                    regexp_for_joined_line=PERCENT_K8S_TEST_PROGRESS_REGEXP,
                    regexp_to_skip_truncation=K8S_SKIP_TRUNCATION_REGEXP,
                ),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _run_tests,
                        kwds={
                            "python": combo.python_version,
                            "kubernetes_version": combo.kubernetes_version,
                            "executor": executor,
                            "test_args": pytest_args,
                            "dry_run": dry_run,
                            "verbose": verbose,
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success="All K8S tests successfully completed.",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
        )
    else:
        result, _ = _run_tests(
            python=python,
            kubernetes_version=kubernetes_version,
            executor=executor,
            output=None,
            test_args=test_args,
            verbose=verbose,
            dry_run=dry_run,
        )
        sys.exit(result)
