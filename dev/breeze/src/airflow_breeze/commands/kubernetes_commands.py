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

import multiprocessing as mp
import os
import re
import shutil
import sys
import tempfile
from copy import deepcopy
from itertools import chain
from pathlib import Path
from shlex import quote

import click

from airflow_breeze.commands.common_options import (
    option_answer,
    option_debug_resources,
    option_dry_run,
    option_include_success_outputs,
    option_parallelism,
    option_python,
    option_python_versions,
    option_run_in_parallel,
    option_skip_cleanup,
    option_use_uv,
    option_verbose,
)
from airflow_breeze.commands.production_image_commands import run_build_production_image
from airflow_breeze.global_constants import (
    ALLOWED_EXECUTORS,
    ALLOWED_KUBERNETES_VERSIONS,
    CELERY_EXECUTOR,
    KUBERNETES_EXECUTOR,
)
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import CacheableChoice, CacheableDefault
from airflow_breeze.utils.kubernetes_utils import (
    CHART_PATH,
    K8S_CLUSTERS_PATH,
    KUBERNETES_TEST_PATH,
    SCRIPTS_CI_KUBERNETES_PATH,
    KubernetesPythonVersion,
    get_config_folder,
    get_k8s_env,
    get_kind_cluster_config_path,
    get_kind_cluster_name,
    get_kubeconfig_file,
    get_kubectl_cluster_name,
    get_kubernetes_port_numbers,
    get_kubernetes_python_combos,
    make_sure_kubernetes_tools_are_installed,
    print_cluster_urls,
    run_command_with_k8s_env,
    set_random_cluster_ports,
    sync_virtualenv,
)
from airflow_breeze.utils.parallel import (
    DockerBuildxProgressMatcher,
    GenericRegexpProgressMatcher,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.recording import generating_command_images
from airflow_breeze.utils.run_utils import RunCommandResult, check_if_image_exists, run_command

KUBERNETES_PYTEST_ARGS = [
    "--strict-markers",
    "--durations=100",
    "--maxfail=50",
    "--color=yes",
    # timeouts in seconds for individual tests
    "--timeouts-order=moi",
    "--setup-timeout=300",
    "--execution-timeout=300",
    "--teardown-timeout=300",
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
]

PARALLEL_KUBERNETES_PYTEST_ARGS = [
    *KUBERNETES_PYTEST_ARGS,
    "--verbosity=0",
]


@click.group(cls=BreezeGroup, name="k8s", help="Tools that developers use to run Kubernetes tests")
def kubernetes_group():
    pass


option_copy_local_sources = click.option(
    "--copy-local-sources/--no-copy-local-sources",
    help="Copy local sources to the image.",
    default=True,
    show_default=True,
    envvar="COPY_LOCAL_SOURCES",
)
option_executor = click.option(
    "--executor",
    help="Executor to use for a kubernetes cluster.",
    type=CacheableChoice(ALLOWED_EXECUTORS),
    show_default=True,
    default=CacheableDefault(ALLOWED_EXECUTORS[0]),
    envvar="EXECUTOR",
)
option_force_recreate_cluster = click.option(
    "--force-recreate-cluster",
    help="Force recreation of the cluster even if it is already created.",
    is_flag=True,
    envvar="FORCE_RECREATE_CLUSTER",
)
option_force_venv_setup = click.option(
    "--force-venv-setup",
    help="Force recreation of the virtualenv.",
    is_flag=True,
    envvar="FORCE_VENV_SETUP",
)
option_kubernetes_version = click.option(
    "--kubernetes-version",
    help="Kubernetes version used to create the KinD cluster of.",
    type=CacheableChoice(ALLOWED_KUBERNETES_VERSIONS),
    show_default=True,
    default=CacheableDefault(ALLOWED_KUBERNETES_VERSIONS[0]),
    envvar="KUBERNETES_VERSION",
)
option_kubernetes_versions = click.option(
    "--kubernetes-versions",
    help="Kubernetes versions used to run in parallel (space separated).",
    type=str,
    show_default=True,
    default=" ".join(ALLOWED_KUBERNETES_VERSIONS),
    envvar="KUBERNETES_VERSIONS",
)
option_multi_namespace_mode = click.option(
    "--multi-namespace-mode",
    help="Use multi namespace mode.",
    is_flag=True,
    envvar="MULTI_NAMESPACE_MODE",
)
option_rebuild_base_image = click.option(
    "--rebuild-base-image",
    help="Rebuilds base Airflow image before building K8S image.",
    is_flag=True,
    envvar="REBUILD_BASE_IMAGE",
)
option_upgrade = click.option(
    "--upgrade",
    help="Upgrade Helm Chart rather than installing it.",
    is_flag=True,
    envvar="UPGRADE",
)
option_use_docker = click.option(
    "--use-docker",
    help="Use Docker to start k8s executor (otherwise k9s from PATH is used and only"
    " run with docker if not found on PATH).",
    is_flag=True,
    envvar="USE_DOCKER",
)
option_use_standard_naming = click.option(
    "--use-standard-naming",
    help="Use standard naming.",
    is_flag=True,
    envvar="USE_STANDARD_NAMING",
)
option_wait_time_in_seconds = click.option(
    "--wait-time-in-seconds",
    help="Wait for Airflow api-server for specified number of seconds.",
    type=click.IntRange(0),
    default=120,
    envvar="WAIT_TIME_IN_SECONDS",
)
option_wait_time_in_seconds_0_default = click.option(
    "--wait-time-in-seconds",
    help="Wait for Airflow api-server for specified number of seconds.",
    type=click.IntRange(0),
    default=0,
    envvar="WAIT_TIME_IN_SECONDS",
)
option_parallelism_cluster = click.option(
    "--parallelism",
    help="Maximum number of processes to use while running the operation in parallel for cluster operations.",
    type=click.IntRange(1, max(1, (mp.cpu_count() + 1) // 3) if not generating_command_images() else 4),
    default=max(1, (mp.cpu_count() + 1) // 3) if not generating_command_images() else 2,
    envvar="PARALLELISM",
    show_default=True,
)
option_all = click.option("--all", help="Apply it to all created clusters", is_flag=True, envvar="ALL")

K8S_CLUSTER_CREATE_PROGRESS_REGEXP = r".*airflow-python-[0-9.]+-v[0-9.].*|.*Connecting to localhost.*"
K8S_UPLOAD_PROGRESS_REGEXP = r".*airflow-python-[0-9.]+-v[0-9.].*"
K8S_CONFIGURE_CLUSTER_PROGRESS_REGEXP = r".*airflow-python-[0-9.]+-v[0-9.].*"
K8S_DEPLOY_PROGRESS_REGEXP = r".*airflow-python-[0-9.]+-v[0-9.].*"
K8S_TEST_PROGRESS_REGEXP = r".*airflow-python-[0-9.]+-v[0-9.].*|^kubernetes-tests/.*"
PREVIOUS_LINE_K8S_TEST_REGEXP = r"^kubernetes-tests/.*"

COMPLETE_TEST_REGEXP = (
    r"\s*#(\d*) |"
    r".*airflow-python-[0-9.]+-v[0-9.].*|"
    r".*Connecting to localhost.*|"
    r"^kubernetes-tests/.*|"
    r".*Error during running tests.*|"
    r".*Successfully run tests.*"
)


@kubernetes_group.command(name="setup-env", help="Setup shared Kubernetes virtual environment and tools.")
@option_force_venv_setup
@option_verbose
@option_dry_run
def setup_env(force_venv_setup: bool):
    result = sync_virtualenv(force_venv_setup=force_venv_setup)
    if result.returncode != 0:
        sys.exit(1)
    make_sure_kubernetes_tools_are_installed()
    get_console().print("\n[warning]NEXT STEP:[/][info] You might now create your cluster by:\n")
    get_console().print("\nbreeze k8s create-cluster\n")


def _create_cluster(
    python: str,
    kubernetes_version: str,
    output: Output | None,
    num_tries: int,
    force_recreate_cluster: bool,
) -> tuple[int, str]:
    while True:
        if force_recreate_cluster:
            _delete_cluster(python=python, kubernetes_version=kubernetes_version, output=output)
        kubeconfig_file = get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version)
        cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
        kubeconfig_file.parent.mkdir(parents=True, exist_ok=True)
        kubeconfig_file.touch(mode=0o700)
        get_console(output=output).print(f"[info]Creating KinD cluster {cluster_name}!")
        set_random_cluster_ports(python=python, kubernetes_version=kubernetes_version, output=output)
        result = run_command_with_k8s_env(
            [
                "kind",
                "create",
                "cluster",
                "--name",
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
            check=False,
        )
        if result.returncode == 0:
            print_cluster_urls(python=python, kubernetes_version=kubernetes_version, output=output)
            kubeconfig_file = get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version)
            (KUBERNETES_TEST_PATH / ".env").write_text(f"KUBECONFIG={quote(kubeconfig_file.as_posix())}\n")
            get_console(output=output).print(f"[success]KinD cluster {cluster_name} created!\n")
            get_console(output=output).print(
                "\n[warning]NEXT STEP:[/][info] You might now configure your cluster by:\n"
            )
            get_console(output=output).print("\nbreeze k8s configure-cluster\n")
            return result.returncode, f"K8S cluster {cluster_name}."
        num_tries -= 1
        if num_tries == 0:
            return result.returncode, f"K8S cluster {cluster_name}."
        get_console(output=output).print(
            f"[warning]Failed to create KinD cluster {cluster_name}. "
            f"Retrying! There are {num_tries} tries left.\n"
        )
        _delete_cluster(python=python, kubernetes_version=kubernetes_version, output=output)


@kubernetes_group.command(
    name="create-cluster",
    help="Create a KinD Cluster for Python and Kubernetes version specified "
    "(optionally create all clusters in parallel).",
)
@option_force_recreate_cluster
@option_python
@option_kubernetes_version
@option_run_in_parallel
@option_parallelism_cluster
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_kubernetes_versions
@option_python_versions
@option_verbose
@option_dry_run
def create_cluster(
    force_recreate_cluster: bool,
    python: str,
    kubernetes_version: str,
    run_in_parallel: bool,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs: bool,
    parallelism: int,
    kubernetes_versions: str,
    python_versions: str,
):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    if run_in_parallel:
        python_version_array: list[str] = python_versions.split(" ")
        kubernetes_version_array: list[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Creating clusters {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                debug_resources=debug_resources,
                progress_matcher=GenericRegexpProgressMatcher(
                    regexp=K8S_CLUSTER_CREATE_PROGRESS_REGEXP, lines_to_search=15
                ),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _create_cluster,
                        kwds={
                            "python": combo.python_version,
                            "kubernetes_version": combo.kubernetes_version,
                            "force_recreate_cluster": False,
                            "num_tries": 3,  # when creating cluster in parallel, sometimes we need to retry
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success_message="All clusters created.",
            outputs=outputs,
            skip_cleanup=skip_cleanup,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _create_cluster(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
            force_recreate_cluster=force_recreate_cluster,
            num_tries=1,
        )
        if return_code != 0:
            sys.exit(return_code)


def _delete_cluster(python: str, kubernetes_version: str, output: Output | None):
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"[info]Deleting KinD cluster {cluster_name}!")
    folder = get_config_folder(python=python, kubernetes_version=kubernetes_version)
    run_command_with_k8s_env(
        [
            "kind",
            "delete",
            "cluster",
            "--name",
            cluster_name,
        ],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        text=True,
        check=False,
    )
    if not folder.exists():
        get_console(output=output).print(
            f"[warning]KinD cluster {cluster_name} was not created before but "
            f"running delete in case it was created manually !\n"
        )
    shutil.rmtree(folder, ignore_errors=True)
    get_console(output=output).print(f"[success]KinD cluster {cluster_name} deleted!\n")


def _delete_all_clusters():
    clusters = list(K8S_CLUSTERS_PATH.iterdir())
    if clusters:
        get_console().print("\n[info]Deleting clusters")
        for cluster_name in clusters:
            resolved_path = cluster_name.resolve()
            python, kubernetes_version = _get_python_kubernetes_version_from_name(resolved_path.name)
            if python and kubernetes_version:
                _delete_cluster(
                    python=python,
                    kubernetes_version=kubernetes_version,
                    output=None,
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
    else:
        get_console().print("\n[warning]No clusters.\n")


@kubernetes_group.command(
    name="delete-cluster", help="Delete the current KinD Cluster (optionally all clusters)."
)
@option_python
@option_kubernetes_version
@option_all
@option_verbose
@option_dry_run
def delete_cluster(python: str, kubernetes_version: str, all: bool):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    if all:
        _delete_all_clusters()
    else:
        _delete_cluster(python=python, kubernetes_version=kubernetes_version, output=None)


def _get_python_kubernetes_version_from_name(cluster_name: str) -> tuple[str | None, str | None]:
    matcher = re.compile(r"airflow-python-(\d+\.\d+)-(v\d+.\d+.\d+)")
    cluster_match = matcher.search(cluster_name)
    if cluster_match:
        python = cluster_match.group(1)
        kubernetes_version = cluster_match.group(2)
        return python, kubernetes_version
    return None, None


LIST_CONSOLE_WIDTH = 120


def _status(python: str, kubernetes_version: str, wait_time_in_seconds: int) -> bool:
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
        ["kubectl", "cluster-info", "--cluster", kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
        check=False,
    )
    if result.returncode != 0:
        return False
    get_console().print(f"\n[info]Storage class for {cluster_name}\n")
    result = run_command_with_k8s_env(
        ["kubectl", "get", "storageclass", "--cluster", kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
        check=False,
    )
    if result.returncode != 0:
        return False
    get_console().print(f"\n[info]Running pods for {cluster_name}\n")
    result = run_command_with_k8s_env(
        ["kubectl", "get", "-n", "kube-system", "pods", "--cluster", kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
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
def status(kubernetes_version: str, python: str, wait_time_in_seconds: int, all: bool):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    if all:
        clusters = list(K8S_CLUSTERS_PATH.iterdir())
        if clusters:
            failed = False
            get_console().print("[info]\nCluster status:\n")
            for cluster_name in clusters:
                name = cluster_name.name
                found_python, found_kubernetes_version = _get_python_kubernetes_version_from_name(name)
                if not found_python or not found_kubernetes_version:
                    get_console().print(f"[warning]\nCould not get cluster from {name}. Skipping.\n")
                elif not _status(
                    python=found_python,
                    kubernetes_version=found_kubernetes_version,
                    wait_time_in_seconds=wait_time_in_seconds,
                ):
                    failed = True
            if failed:
                get_console().print("\n[error]Some clusters are not healthy!\n")
                sys.exit(1)
        else:
            get_console().print("\n[warning]No clusters.\n")
            sys.exit(1)
    else:
        if not _status(
            python=python,
            kubernetes_version=kubernetes_version,
            wait_time_in_seconds=wait_time_in_seconds,
        ):
            get_console().print("\n[error]The cluster is not healthy!\n")
            sys.exit(1)


def check_if_base_image_exists(params: BuildProdParams) -> bool:
    return check_if_image_exists(image=params.airflow_image_name)


def _rebuild_k8s_image(
    python: str,
    rebuild_base_image: bool,
    copy_local_sources: bool,
    use_uv: bool,
    output: Output | None,
) -> tuple[int, str]:
    params = BuildProdParams(python=python, use_uv=use_uv)
    if rebuild_base_image:
        run_build_production_image(
            prod_image_params=params,
            param_description=f"Python: {params.python}, Platform: {params.platform}",
            output=output,
        )
    else:
        if not check_if_base_image_exists(params):
            get_console(output=output).print(
                f"[error]The base PROD image {params.airflow_image_name} does not exist locally.\n"
            )
            get_console(output=output).print(
                "[warning]Please add `--rebuild-base-image` flag or rebuild it manually with:\n"
            )
            get_console(output=output).print(f"breeze prod-image build --python {python}\n")
            sys.exit(1)
    get_console(output=output).print(
        f"[info]Building the K8S image for Python {python} using "
        f"airflow base image: {params.airflow_image_name}\n"
    )
    if copy_local_sources:
        extra_copy_command = "COPY --chown=airflow:0 . /opt/airflow/"
    else:
        extra_copy_command = ""
    docker_image_for_kubernetes_tests = f"""
FROM {params.airflow_image_name}

USER airflow

{extra_copy_command}

COPY --chown=airflow:0 airflow-core/src/airflow/example_dags/ /opt/airflow/dags/

COPY --chown=airflow:0 providers/cncf/kubernetes/src/airflow/providers/cncf/kubernetes/kubernetes_executor_templates/ /opt/airflow/pod_templates/

ENV GUNICORN_CMD_ARGS='--preload'
"""
    image = f"{params.airflow_image_kubernetes}:latest"
    docker_build_result = run_command(
        ["docker", "build", "--tag", image, ".", "-f", "-"],
        input=docker_image_for_kubernetes_tests,
        text=True,
        check=False,
        output=output,
    )
    if docker_build_result.returncode != 0:
        get_console(output=output).print("[error]Error when building the kubernetes image.")
    return docker_build_result.returncode, f"K8S image for Python {python}"


def _upload_k8s_image(python: str, kubernetes_version: str, output: Output | None) -> tuple[int, str]:
    params = BuildProdParams(python=python)
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(
        f"[info]Uploading Airflow image {params.airflow_image_kubernetes} to cluster {cluster_name}"
    )
    kind_load_result = run_command_with_k8s_env(
        ["kind", "load", "docker-image", "--name", cluster_name, params.airflow_image_kubernetes],
        python=python,
        output=output,
        kubernetes_version=kubernetes_version,
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
@option_answer
@option_copy_local_sources
@option_debug_resources
@option_dry_run
@option_include_success_outputs
@option_parallelism
@option_python
@option_python_versions
@option_rebuild_base_image
@option_run_in_parallel
@option_skip_cleanup
@option_use_uv
@option_verbose
def build_k8s_image(
    copy_local_sources: bool,
    debug_resources: bool,
    include_success_outputs: bool,
    parallelism: int,
    python: str,
    python_versions: str,
    rebuild_base_image: bool,
    run_in_parallel: bool,
    skip_cleanup: bool,
    use_uv: bool,
):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    if run_in_parallel:
        python_version_array: list[str] = python_versions.split(" ")
        with ci_group(f"Building K8s images for {python_versions}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=[f"Image {python}" for python in python_version_array],
                debug_resources=debug_resources,
                progress_matcher=DockerBuildxProgressMatcher(),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _rebuild_k8s_image,
                        kwds={
                            "python": _python,
                            "rebuild_base_image": rebuild_base_image,
                            "copy local sources": copy_local_sources,
                            "use_uv": use_uv,
                            "output": outputs[index],
                        },
                    )
                    for index, _python in enumerate(python_version_array)
                ]
        check_async_run_results(
            results=results,
            success_message="All K8S images built correctly.",
            outputs=outputs,
            skip_cleanup=skip_cleanup,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _rebuild_k8s_image(
            python=python,
            rebuild_base_image=rebuild_base_image,
            copy_local_sources=copy_local_sources,
            use_uv=use_uv,
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
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_python_versions
@option_kubernetes_versions
@option_verbose
@option_dry_run
def upload_k8s_image(
    python: str,
    kubernetes_version: str,
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs: bool,
    python_versions: str,
    kubernetes_versions: str,
):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    if run_in_parallel:
        python_version_array: list[str] = python_versions.split(" ")
        kubernetes_version_array: list[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Uploading K8s images for {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                debug_resources=debug_resources,
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
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success_message="All K8S images uploaded correctly.",
            outputs=outputs,
            skip_cleanup=skip_cleanup,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _upload_k8s_image(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
        )
        if return_code == 0:
            get_console().print("\n[warning]NEXT STEP:[/][info] You might now deploy airflow by:\n")
            get_console().print("\nbreeze k8s deploy-airflow\n")
            get_console().print(
                "\n[warning]Note:[/]\nIf you want to run tests with [info]--executor KubernetesExecutor[/], you should deploy airflow with [info]--multi-namespace-mode --executor KubernetesExecutor[/] flag.\n"
            )
            get_console().print(
                "\nbreeze k8s deploy-airflow --multi-namespace-mode --executor KubernetesExecutor\n"
            )
        sys.exit(return_code)


HELM_DEFAULT_NAMESPACE = "default"
HELM_AIRFLOW_NAMESPACE = "airflow"
TEST_NAMESPACE = "test-namespace"


def _recreate_namespaces(
    python: str,
    kubernetes_version: str,
    output: Output | None,
) -> RunCommandResult:
    cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"[info]Deleting K8S namespaces for {cluster_name}")
    run_command_with_k8s_env(
        ["kubectl", "delete", "namespace", HELM_AIRFLOW_NAMESPACE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=False,
    )
    run_command_with_k8s_env(
        ["kubectl", "delete", "namespace", TEST_NAMESPACE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=False,
    )
    get_console(output=output).print("[info]Creating namespaces")
    result = run_command_with_k8s_env(
        ["kubectl", "create", "namespace", HELM_AIRFLOW_NAMESPACE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=False,
    )
    if result.returncode != 0:
        return result
    result = run_command_with_k8s_env(
        ["kubectl", "create", "namespace", TEST_NAMESPACE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=False,
    )
    if result.returncode == 0:
        get_console(output=output).print(f"[success]Created K8S namespaces for cluster {cluster_name}\n")
    return result


def _deploy_test_resources(python: str, kubernetes_version: str, output: Output | None) -> RunCommandResult:
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
        check=False,
    )
    if result.returncode == 0:
        get_console(output=output).print(f"[success]Deployed test resources for cluster {cluster_name}")
    return result


def _configure_k8s_cluster(python: str, kubernetes_version: str, output: Output | None) -> tuple[int, str]:
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"[info]Configuring {cluster_name} to be ready for Airflow deployment")
    result = _recreate_namespaces(
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
    )
    if result.returncode == 0:
        result = _deploy_test_resources(
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
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
@option_parallelism_cluster
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_python_versions
@option_kubernetes_versions
@option_verbose
@option_dry_run
def configure_cluster(
    python: str,
    kubernetes_version: str,
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs: bool,
    python_versions: str,
    kubernetes_versions: str,
):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    if run_in_parallel:
        python_version_array: list[str] = python_versions.split(" ")
        kubernetes_version_array: list[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Setting up clusters for {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                debug_resources=debug_resources,
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
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success_message="All clusters configured correctly.",
            outputs=outputs,
            skip_cleanup=skip_cleanup,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _configure_k8s_cluster(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
        )
        if return_code == 0:
            get_console().print(
                "\n[warning]NEXT STEP:[/][info] You might now build your k8s image "
                "with all latest dependencies:\n"
            )
            get_console().print("\n breeze k8s build-k8s-image --rebuild-base-image\n")
            get_console().print(
                "\n[info]Later you can build image without --rebuild-base-image until "
                "airflow dependencies change (to speed up rebuilds).\n"
            )
        sys.exit(return_code)


def _deploy_helm_chart(
    python: str,
    upgrade: bool,
    kubernetes_version: str,
    output: Output | None,
    executor: str,
    use_standard_naming: bool,
    extra_options: tuple[str, ...] | None = None,
    multi_namespace_mode: bool = False,
) -> RunCommandResult:
    from packaging.version import Version

    cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    _, api_server_port = get_kubernetes_port_numbers(python=python, kubernetes_version=kubernetes_version)
    action = "Deploying" if not upgrade else "Upgrading"
    get_console(output=output).print(f"[info]{action} {cluster_name} with airflow Helm Chart.")
    with tempfile.TemporaryDirectory(prefix="chart_") as tmp_dir:
        tmp_chart_path = Path(tmp_dir).resolve() / "chart"
        shutil.copytree(CHART_PATH, os.fspath(tmp_chart_path), ignore_dangling_symlinks=True)
        get_console(output=output).print(f"[info]Copied chart sources to {tmp_chart_path}")
        kubectl_context = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
        params = BuildProdParams(python=python)
        # TODO (potiuk): we can also run on matrix of auth managers if we make SimpleAuthManager prod-ready ?
        use_flask_appbuilder = Version(python) < Version("3.13")
        if use_flask_appbuilder:
            auth_manager = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
        else:
            auth_manager = "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
        helm_command = [
            "helm",
            "upgrade" if upgrade else "install",
            "airflow",
            os.fspath(tmp_chart_path.resolve()),
            "--kube-context",
            kubectl_context,
            "--timeout",
            "20m0s",
            "--namespace",
            HELM_AIRFLOW_NAMESPACE,
            "--set",
            f"defaultAirflowRepository={params.airflow_image_kubernetes}",
            "--set",
            "defaultAirflowTag=latest",
            "-v",
            "1",
            "--set",
            f"images.airflow.repository={params.airflow_image_kubernetes}",
            "--set",
            "images.airflow.tag=latest",
            "-v",
            "1",
            "--set",
            "config.logging.logging_level=DEBUG",
            "--set",
            f"executor={executor}",
            "--set",
            f"airflowVersion={params.airflow_semver_version}",
            "--set",
            "config.api_auth.jwt_secret=foo",
            "--set",
            f"config.core.auth_manager={auth_manager}",
            "--set",
            f"config.api.base_url=http://localhost:{api_server_port}",
        ]
        if multi_namespace_mode:
            helm_command.extend(["--set", "multiNamespaceMode=true"])
        if not use_flask_appbuilder:
            helm_command.extend(["--set", "webserver.defaultUser.enabled=false"])
        if upgrade:
            # force upgrade
            helm_command.append("--force")
        if use_standard_naming:
            helm_command.extend(["--set", "useStandardNaming=true"])
        if extra_options:
            helm_command.extend(extra_options)
        get_console(output=output).print(f"[info]Deploying Airflow from {tmp_chart_path}")
        result = run_command_with_k8s_env(
            helm_command,
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            check=False,
            capture_output=True,
            text=True,
        )
        # Print captured output to the console/output file
        if result.stdout:
            get_console(output=output).print(result.stdout)
        if result.stderr:
            get_console(output=output).print(result.stderr)
        if result.returncode == 0:
            get_console(output=output).print(f"[success]Deployed {cluster_name} with airflow Helm Chart.")
        return result


def _is_helm_timeout_error(result: RunCommandResult) -> bool:
    """Check if the Helm command failed due to a timeout."""
    # Check stderr and stdout for timeout-related messages
    error_output = ""
    if hasattr(result, "stderr") and result.stderr:
        error_output += result.stderr if isinstance(result.stderr, str) else result.stderr.decode()
    if hasattr(result, "stdout") and result.stdout:
        error_output += result.stdout if isinstance(result.stdout, str) else result.stdout.decode()
    return "timed out waiting for the condition" in error_output


def _deploy_airflow(
    python: str,
    kubernetes_version: str,
    output: Output | None,
    executor: str,
    upgrade: bool,
    wait_time_in_seconds: int,
    use_standard_naming: bool,
    extra_options: tuple[str, ...] | None = None,
    multi_namespace_mode: bool = False,
    num_tries: int = 1,
) -> tuple[int, str]:
    action = "Deploying" if not upgrade else "Upgrading"
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    kubectl_context = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    while True:
        get_console(output=output).print(f"[info]{action} Airflow for cluster {cluster_name}")
        result = _deploy_helm_chart(
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            upgrade=upgrade,
            executor=executor,
            use_standard_naming=use_standard_naming,
            extra_options=extra_options,
            multi_namespace_mode=multi_namespace_mode,
        )
        if result.returncode == 0:
            break
        # Only retry on timeout errors, fail immediately for other errors
        if not _is_helm_timeout_error(result):
            return result.returncode, f"{action} Airflow to {cluster_name}"
        num_tries -= 1
        if num_tries == 0:
            return result.returncode, f"{action} Airflow to {cluster_name}"
        get_console(output=output).print(
            f"[warning]Helm deployment timed out for {cluster_name}. "
            f"Retrying! There are {num_tries} tries left.\n"
        )
        # Uninstall the failed release before retrying
        run_command_with_k8s_env(
            [
                "helm",
                "uninstall",
                "airflow",
                "--kube-context",
                kubectl_context,
                "--namespace",
                HELM_AIRFLOW_NAMESPACE,
                "--ignore-not-found",
            ],
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            check=False,
        )
    if result.returncode == 0:
        if multi_namespace_mode:
            # duplicate Airflow configmaps, secrets and service accounts to test namespace
            run_command_with_k8s_env(
                f"kubectl get secret -n {HELM_AIRFLOW_NAMESPACE} "
                "--field-selector type!=helm.sh/release.v1 -o yaml "
                f"| sed 's/namespace: {HELM_AIRFLOW_NAMESPACE}/namespace: {TEST_NAMESPACE}/' "
                f"| kubectl apply -n {TEST_NAMESPACE} -f -",
                python=python,
                kubernetes_version=kubernetes_version,
                output=output,
                check=False,
                shell=True,
            )

            run_command_with_k8s_env(
                f"kubectl get configmap -n {HELM_AIRFLOW_NAMESPACE} "
                "--field-selector  metadata.name!=kube-root-ca.crt -o yaml "
                f"| sed 's/namespace: {HELM_AIRFLOW_NAMESPACE}/namespace: {TEST_NAMESPACE}/' "
                f"| kubectl apply -n {TEST_NAMESPACE} -f -",
                python=python,
                kubernetes_version=kubernetes_version,
                output=output,
                check=False,
                shell=True,
            )

            run_command_with_k8s_env(
                f"kubectl get serviceaccount -n {HELM_AIRFLOW_NAMESPACE} "
                "--field-selector  metadata.name!=default -o yaml "
                f"| sed 's/namespace: {HELM_AIRFLOW_NAMESPACE}/namespace: {TEST_NAMESPACE}/' "
                f"| kubectl apply -n {TEST_NAMESPACE} -f -",
                python=python,
                kubernetes_version=kubernetes_version,
                output=output,
                check=False,
                shell=True,
            )

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
@option_upgrade
@option_wait_time_in_seconds
@option_run_in_parallel
@option_parallelism_cluster
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_use_standard_naming
@option_multi_namespace_mode
@option_python_versions
@option_kubernetes_versions
@option_verbose
@option_dry_run
@click.argument("extra_options", nargs=-1, type=click.UNPROCESSED)
def deploy_airflow(
    python: str,
    kubernetes_version: str,
    executor: str,
    upgrade: bool,
    wait_time_in_seconds: int,
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs: bool,
    use_standard_naming: bool,
    python_versions: str,
    kubernetes_versions: str,
    extra_options: tuple[str, ...],
    multi_namespace_mode: bool = False,
):
    if run_in_parallel:
        python_version_array: list[str] = python_versions.split(" ")
        kubernetes_version_array: list[str] = kubernetes_versions.split(" ")
        combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
            kubernetes_version_array, python_version_array
        )
        with ci_group(f"Deploying airflow for: {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                debug_resources=debug_resources,
                progress_matcher=GenericRegexpProgressMatcher(
                    regexp=K8S_DEPLOY_PROGRESS_REGEXP, lines_to_search=15
                ),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _deploy_airflow,
                        kwds={
                            "python": combo.python_version,
                            "kubernetes_version": combo.kubernetes_version,
                            "executor": executor,
                            "upgrade": upgrade,
                            "use_standard_naming": use_standard_naming,
                            "wait_time_in_seconds": wait_time_in_seconds,
                            "extra_options": extra_options,
                            "output": outputs[index],
                            "multi_namespace_mode": multi_namespace_mode,
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success_message="All Airflow charts successfully deployed.",
            outputs=outputs,
            skip_cleanup=skip_cleanup,
            include_success_outputs=include_success_outputs,
        )
    else:
        return_code, _ = _deploy_airflow(
            python=python,
            kubernetes_version=kubernetes_version,
            output=None,
            executor=executor,
            upgrade=upgrade,
            use_standard_naming=use_standard_naming,
            wait_time_in_seconds=wait_time_in_seconds,
            extra_options=extra_options,
            multi_namespace_mode=multi_namespace_mode,
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
@option_use_docker
@option_verbose
@option_dry_run
@click.argument("k9s_args", nargs=-1, type=click.UNPROCESSED)
def k9s(python: str, kubernetes_version: str, use_docker: bool, k9s_args: tuple[str, ...]):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    env = get_k8s_env(python=python, kubernetes_version=kubernetes_version)
    env["TERM"] = "xterm-256color"
    editor = env.get("EDITOR")
    if not editor:
        env["EDITOR"] = "vim"
    k9s_editor = env.get("K9S_EDITOR")
    if not k9s_editor:
        env["K9S_EDITOR"] = env["EDITOR"]
    kubeconfig_file = get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version)
    found_k9s = shutil.which("k9s")
    if not use_docker and found_k9s:
        get_console().print(
            "[info]Running k9s tool found in PATH at $(found_k9s). Use --use-docker to run using docker."
        )
        result = run_command(
            [
                "k9s",
                "--namespace",
                HELM_AIRFLOW_NAMESPACE,
                *k9s_args,
            ],
            env=env,
            check=False,
        )
        sys.exit(result.returncode)
    else:
        get_console().print("[info]Running k9s tool using docker.")
        result = run_command(
            [
                "docker",
                "run",
                "--rm",
                "-it",
                "--network",
                "host",
                "-e",
                "EDITOR",
                "-e",
                "K9S_EDITOR",
                "-v",
                f"{kubeconfig_file}:/root/.kube/config",
                "derailed/k9s",
                "--namespace",
                HELM_AIRFLOW_NAMESPACE,
                *k9s_args,
            ],
            env=env,
            check=False,
        )
        if result.returncode != 0:
            get_console().print(
                "\n[warning]If you see `exec /bin/k9s: exec format error` it might be because"
                " of known kind bug (https://github.com/kubernetes-sigs/kind/issues/3510).\n"
            )
            get_console().print(
                "\n[info]In such case you might want to pull latest `kindest` images. "
                "For example if you run kubernetes version v1.26.14 you might need to run:\n"
                "[special]* run `breeze k8s delete-cluster` (note k8s version printed after "
                "Python version)\n"
                "* run `docker pull kindest/node:v1.26.14`\n"
                "* restart docker engine\n\n"
            )
        sys.exit(result.returncode)


def _logs(python: str, kubernetes_version: str):
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    tmpdir = Path(tempfile.gettempdir()) / f"kind_logs_{cluster_name}"
    get_console().print(f"[info]\nDumping logs for {cluster_name} to {tmpdir}:\n")
    run_command_with_k8s_env(
        ["kind", "--name", cluster_name, "export", "logs", str(tmpdir)],
        python=python,
        kubernetes_version=kubernetes_version,
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
def logs(python: str, kubernetes_version: str, all: bool):
    if all:
        clusters = list(K8S_CLUSTERS_PATH.iterdir())
        if clusters:
            get_console().print("[info]\nDumping cluster logs:\n")
            for cluster_name in clusters:
                name = cluster_name.name
                found_python, found_kubernetes_version = _get_python_kubernetes_version_from_name(name)
                if not found_python or not found_kubernetes_version:
                    get_console().print(f"[warning]\nCould not get cluster from {name}. Skipping.\n")
                    continue
                _logs(python=found_python, kubernetes_version=found_kubernetes_version)
        else:
            get_console().print("\n[warning]No clusters.\n")
            sys.exit(1)
    else:
        _logs(python=python, kubernetes_version=kubernetes_version)


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
@click.argument("shell_args", nargs=-1, type=click.UNPROCESSED)
def shell(
    python: str,
    kubernetes_version: str,
    executor: str,
    force_venv_setup: bool,
    shell_args: tuple[str, ...],
):
    result = sync_virtualenv(force_venv_setup=force_venv_setup)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    env = get_k8s_env(python=python, kubernetes_version=kubernetes_version, executor=executor)
    get_console().print("\n[info]Entering interactive k8s shell.\n")
    shell_binary = env.get("SHELL", "bash")
    extra_args: list[str] = []
    if shell_binary.endswith("zsh"):
        extra_args.append("--no-rcs")
    elif shell_binary.endswith("bash"):
        extra_args.extend(["--norc", "--noprofile"])
    result = run_command(
        [shell_binary, *extra_args, *shell_args], env=env, check=False, cwd="kubernetes-tests"
    )
    if result.returncode != 0:
        sys.exit(result.returncode)


def _get_parallel_test_args(
    kubernetes_versions: str, python_versions: str, test_args: list[str]
) -> tuple[list[str], list[KubernetesPythonVersion], list[str], list[str]]:
    pytest_args = deepcopy(PARALLEL_KUBERNETES_PYTEST_ARGS)
    pytest_args.extend(test_args)
    python_version_array: list[str] = python_versions.split(" ")
    kubernetes_version_array: list[str] = kubernetes_versions.split(" ")
    combo_titles, short_combo_titles, combos = get_kubernetes_python_combos(
        kubernetes_version_array=kubernetes_version_array, python_version_array=python_version_array
    )
    return combo_titles, combos, pytest_args, short_combo_titles


def _is_deployed_with_same_executor(python: str, kubernetes_version: str, executor: str) -> bool:
    """Check if the current cluster is deployed with the same executor that the current tests are using.

    This is especially useful when running tests with executors like KubernetesExecutor, CeleryExecutor, etc.
    It verifies by checking the label of the airflow-scheduler deployment.
    """
    result = run_command_with_k8s_env(
        [
            "kubectl",
            "get",
            "deployment",
            "-n",
            "airflow",
            "airflow-scheduler",
            "-o",
            "jsonpath='{.metadata.labels.executor}'",
        ],
        python=python,
        kubernetes_version=kubernetes_version,
        capture_output=True,
        check=False,
    )
    return executor == result.stdout.decode().strip().replace("'", "")


def _run_tests(
    python: str,
    kubernetes_version: str,
    output: Output | None,
    executor: str,
    test_args: list[str],
) -> tuple[int, str]:
    env = get_k8s_env(python=python, kubernetes_version=kubernetes_version, executor=executor)
    kubectl_cluster_name = get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"\n[info]Running tests with {kubectl_cluster_name} cluster.")
    shell_binary = env.get("SHELL", shutil.which("bash"))
    extra_shell_args: list[str] = []
    if shell_binary.endswith("zsh"):
        extra_shell_args.append("--no-rcs")
    elif shell_binary.endswith("bash"):
        extra_shell_args.extend(["--norc", "--noprofile"])
    if (
        executor == KUBERNETES_EXECUTOR or executor == CELERY_EXECUTOR
    ) and not _is_deployed_with_same_executor(python, kubernetes_version, executor):
        get_console(output=output).print(
            f"[warning]{executor} not deployed. Please deploy airflow with {executor} first."
        )
        get_console(output=output).print(
            f"[info]You can deploy airflow with {executor} by running:[/]\nbreeze k8s configure-cluster\nbreeze k8s deploy-airflow --multi-namespace-mode --executor {executor}"
        )
        return 1, f"Tests {kubectl_cluster_name}"
    pytest_cmd = ["uv", "run", "pytest"]
    the_tests: list[str] = ["tests"]
    ordered_unique_args = dict.fromkeys(chain(pytest_cmd, the_tests, test_args))
    command_to_run = " ".join(quote(arg) for arg in ordered_unique_args)
    get_console(output).print(f"[info] Command to run:[/] {command_to_run}")
    result = run_command(
        [shell_binary, *extra_shell_args, "-c", command_to_run],
        output=output,
        env=env,
        check=False,
        cwd=KUBERNETES_TEST_PATH.as_posix(),
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
@option_parallelism_cluster
@option_skip_cleanup
@option_debug_resources
@option_include_success_outputs
@option_python_versions
@option_kubernetes_versions
@option_verbose
@option_dry_run
@click.argument("test_args", nargs=-1, type=click.Path())
def kubernetes_tests_command(
    python: str,
    kubernetes_version: str,
    executor: str,
    force_venv_setup: bool,
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    include_success_outputs: bool,
    python_versions: str,
    kubernetes_versions: str,
    test_args: tuple[str, ...],
):
    result = sync_virtualenv(force_venv_setup=force_venv_setup)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    if run_in_parallel:
        combo_titles, combos, pytest_args, short_combo_titles = _get_parallel_test_args(
            kubernetes_versions, python_versions, list(test_args)
        )
        with ci_group(f"Running tests for: {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                debug_resources=debug_resources,
                progress_matcher=GenericRegexpProgressMatcher(
                    regexp=K8S_TEST_PROGRESS_REGEXP,
                    regexp_for_joined_line=PREVIOUS_LINE_K8S_TEST_REGEXP,
                    lines_to_search=100,
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
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success_message="All K8S tests successfully completed.",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        result, _ = _run_tests(
            python=python,
            kubernetes_version=kubernetes_version,
            executor=executor,
            output=None,
            test_args=list(test_args),
        )
        sys.exit(result)


def _run_complete_tests(
    python: str,
    kubernetes_version: str,
    include_success_outputs: bool,
    executor: str,
    rebuild_base_image: bool,
    copy_local_sources: bool,
    use_uv: bool,
    upgrade: bool,
    wait_time_in_seconds: int,
    force_recreate_cluster: bool,
    use_standard_naming: bool,
    num_tries: int,
    extra_options: tuple[str, ...] | None,
    test_args: list[str],
    output: Output | None,
) -> tuple[int, str]:
    get_console(output=output).print(f"\n[info]Rebuilding k8s image for Python {python}\n")
    returncode, message = _rebuild_k8s_image(
        python=python,
        output=output,
        use_uv=use_uv,
        rebuild_base_image=rebuild_base_image,
        copy_local_sources=copy_local_sources,
    )
    if returncode != 0:
        return returncode, message
    get_console(output=output).print(
        f"\n[info]Creating k8s cluster for Python {python}, Kubernetes {kubernetes_version}\n"
    )
    returncode, message = _create_cluster(
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        num_tries=num_tries,
        force_recreate_cluster=force_recreate_cluster,
    )
    if returncode != 0:
        _logs(python=python, kubernetes_version=kubernetes_version)
        return returncode, message
    try:
        get_console(output=output).print(
            f"\n[info]Configuring k8s cluster for Python {python}, Kubernetes {kubernetes_version}\n"
        )
        returncode, message = _configure_k8s_cluster(
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
        )
        if returncode != 0:
            _logs(python=python, kubernetes_version=kubernetes_version)
            return returncode, message
        get_console(output=output).print(
            f"\n[info]Uploading k8s images for Python {python}, Kubernetes {kubernetes_version}\n"
        )
        returncode, message = _upload_k8s_image(
            python=python, kubernetes_version=kubernetes_version, output=output
        )
        if returncode != 0:
            _logs(python=python, kubernetes_version=kubernetes_version)
            return returncode, message
        get_console(output=output).print(
            f"\n[info]Deploying Airflow for Python {python}, Kubernetes {kubernetes_version}\n"
        )
        returncode, message = _deploy_airflow(
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            executor=executor,
            upgrade=False,
            use_standard_naming=use_standard_naming,
            wait_time_in_seconds=wait_time_in_seconds,
            extra_options=extra_options,
            multi_namespace_mode=True,
            num_tries=3,
        )
        if returncode != 0:
            _logs(python=python, kubernetes_version=kubernetes_version)
            return returncode, message
        get_console(output=output).print(
            f"\n[info]Running tests Python {python}, Kubernetes {kubernetes_version}\n"
        )
        pytest_args = deepcopy(KUBERNETES_PYTEST_ARGS)
        pytest_args.extend(test_args)
        returncode, message = _run_tests(
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
            executor=executor,
            test_args=pytest_args,
        )
        if returncode != 0:
            _logs(python=python, kubernetes_version=kubernetes_version)
            return returncode, message
        if upgrade:
            get_console(output=output).print(
                f"\n[info]Running upgrade for Python {python}, Kubernetes {kubernetes_version}\n"
            )
            returncode, message = _deploy_airflow(
                python=python,
                kubernetes_version=kubernetes_version,
                output=output,
                executor=executor,
                upgrade=True,
                use_standard_naming=use_standard_naming,
                wait_time_in_seconds=wait_time_in_seconds,
                extra_options=extra_options,
                multi_namespace_mode=True,
                num_tries=3,
            )
            if returncode != 0 or include_success_outputs:
                _logs(python=python, kubernetes_version=kubernetes_version)
        return returncode, message
    finally:
        get_console(output=output).print(
            f"\n[info]Deleting cluster for Python {python}, Kubernetes {kubernetes_version}\n"
        )
        _delete_cluster(
            python=python,
            kubernetes_version=kubernetes_version,
            output=output,
        )
        if returncode != 0:
            get_console(output=output).print(
                f"\n[error]Error during running tests for Python {python}, Kubernetes {kubernetes_version}\n"
            )
        else:
            get_console(output=output).print(
                f"\n[success]Successfully run tests for Python {python}, Kubernetes {kubernetes_version}\n"
            )


@kubernetes_group.command(
    name="run-complete-tests",
    help="Run complete k8s tests consisting of: creating cluster, building and uploading image, "
    "deploying airflow, running tests and deleting clusters (optionally for all clusters in parallel).",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@option_debug_resources
@option_dry_run
@option_copy_local_sources
@option_executor
@option_force_recreate_cluster
@option_force_venv_setup
@option_include_success_outputs
@option_kubernetes_version
@option_kubernetes_versions
@option_parallelism_cluster
@option_python
@option_python_versions
@option_rebuild_base_image
@option_run_in_parallel
@option_skip_cleanup
@option_upgrade
@option_use_standard_naming
@option_use_uv
@option_verbose
@option_wait_time_in_seconds
@click.argument("test_args", nargs=-1, type=click.Path())
def run_complete_tests(
    copy_local_sources: bool,
    debug_resources: bool,
    executor: str,
    force_recreate_cluster: bool,
    force_venv_setup: bool,
    include_success_outputs: bool,
    kubernetes_version: str,
    kubernetes_versions: str,
    parallelism: int,
    python: str,
    python_versions: str,
    rebuild_base_image: bool,
    run_in_parallel: bool,
    skip_cleanup: bool,
    test_args: tuple[str, ...],
    upgrade: bool,
    use_standard_naming: bool,
    use_uv: bool,
    wait_time_in_seconds: int,
):
    result = sync_virtualenv(force_venv_setup=force_venv_setup)
    if result.returncode != 0:
        sys.exit(1)
    make_sure_kubernetes_tools_are_installed()
    if run_in_parallel:
        combo_titles, combos, pytest_args, short_combo_titles = _get_parallel_test_args(
            kubernetes_versions, python_versions, list(test_args)
        )
        get_console().print(f"[info]Running complete tests for: {short_combo_titles}")
        get_console().print(f"[info]Parallelism: {parallelism}")
        get_console().print(f"[info]Extra test args: {executor}")
        get_console().print(f"[info]Executor: {executor}")
        get_console().print(f"[info]Use standard naming: {use_standard_naming}")
        get_console().print(f"[info]Upgrade: {upgrade}")
        get_console().print(f"[info]Use uv: {use_uv}")
        get_console().print(f"[info]Rebuild base image: {rebuild_base_image}")
        get_console().print(f"[info]Force recreate cluster: {force_recreate_cluster}")
        get_console().print(f"[info]Include success outputs: {include_success_outputs}")
        get_console().print(f"[info]Debug resources: {debug_resources}")
        get_console().print(f"[info]Skip cleanup: {skip_cleanup}")
        get_console().print(f"[info]Wait time in seconds: {wait_time_in_seconds}")
        with ci_group(f"Running complete tests for: {short_combo_titles}"):
            with run_with_pool(
                parallelism=parallelism,
                all_params=combo_titles,
                debug_resources=debug_resources,
                progress_matcher=GenericRegexpProgressMatcher(
                    regexp=COMPLETE_TEST_REGEXP,
                    regexp_for_joined_line=PREVIOUS_LINE_K8S_TEST_REGEXP,
                    lines_to_search=100,
                ),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _run_complete_tests,
                        kwds={
                            "python": combo.python_version,
                            "kubernetes_version": combo.kubernetes_version,
                            "executor": executor,
                            "rebuild_base_image": rebuild_base_image,
                            "copy_local_sources": copy_local_sources,
                            "use_uv": use_uv,
                            "upgrade": upgrade,
                            "wait_time_in_seconds": wait_time_in_seconds,
                            "force_recreate_cluster": force_recreate_cluster,
                            "use_standard_naming": use_standard_naming,
                            "num_tries": 3,  # when creating cluster in parallel, sometimes we need to retry
                            "extra_options": None,
                            "test_args": pytest_args,
                            "output": outputs[index],
                        },
                    )
                    for index, combo in enumerate(combos)
                ]
        check_async_run_results(
            results=results,
            success_message="All K8S tests successfully completed.",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        pytest_args = deepcopy(KUBERNETES_PYTEST_ARGS)
        pytest_args.extend(test_args)
        result, _ = _run_complete_tests(
            python=python,
            kubernetes_version=kubernetes_version,
            include_success_outputs=include_success_outputs,
            executor=executor,
            rebuild_base_image=rebuild_base_image,
            copy_local_sources=copy_local_sources,
            use_uv=use_uv,
            upgrade=upgrade,
            wait_time_in_seconds=wait_time_in_seconds,
            force_recreate_cluster=force_recreate_cluster,
            use_standard_naming=use_standard_naming,
            num_tries=1,
            extra_options=None,
            test_args=pytest_args,
            output=None,
        )
        if result != 0:
            sys.exit(result)
