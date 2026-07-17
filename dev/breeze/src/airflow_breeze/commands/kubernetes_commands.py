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
from collections.abc import Callable
from copy import deepcopy
from itertools import chain
from pathlib import Path
from shlex import quote
from typing import Any

import click
import yaml

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
from airflow_breeze.commands.ui_commands import run_compile_ui_assets
from airflow_breeze.global_constants import (
    AIRFLOW_SOURCES_TO,
    ALLOWED_EXECUTORS,
    ALLOWED_KUBERNETES_VERSIONS,
    ALLOWED_LOG_LEVELS,
    CELERY_EXECUTOR,
    DEFAULT_ALLOWED_EXECUTOR,
    DEFAULT_LOG_LEVEL,
    KUBERNETES_EXECUTOR,
)
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import MessageType, Output, console_print, get_console
from airflow_breeze.utils.custom_param_types import CacheableChoice, CacheableDefault
from airflow_breeze.utils.docker_command_utils import perform_environment_checks
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
    make_sure_skaffold_installed,
    print_cluster_urls,
    run_command_with_k8s_env,
    set_random_cluster_ports,
    sync_virtualenv,
)
from airflow_breeze.utils.parallel import (
    DockerBuildxProgressMatcher,
    GenericRegexpProgressMatcher,
    check_async_run_results,
    get_output_files,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.recording import generating_command_images
from airflow_breeze.utils.run_utils import (
    RunCommandResult,
    assert_prek_installed,
    check_if_image_exists,
    run_command,
)
from airflow_breeze.utils.shared_options import get_dry_run

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
    default=CacheableDefault(DEFAULT_ALLOWED_EXECUTOR),
    envvar="EXECUTOR",
)
option_log_level = click.option(
    "--log-level",
    help="Log level for Airflow components when using k8s dev.",
    type=CacheableChoice(ALLOWED_LOG_LEVELS),
    show_default=True,
    default=CacheableDefault(DEFAULT_LOG_LEVEL),
    envvar="LOG_LEVEL",
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
option_dags_path = click.option(
    "--dags-path",
    help="Local dags directory to sync.",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    default="files/dags",
    show_default=True,
)
option_dags_dest = click.option(
    "--dags-dest",
    help="Destination path inside the Airflow container for dags.",
    default="/opt/airflow/dags",
    show_default=True,
)
option_skaffold_deploy = click.option(
    "--deploy/--no-deploy",
    help="Let skaffold deploy/upgrade Airflow via Helm.",
    default=False,
    show_default=True,
    envvar="SKAFFOLD_DEPLOY",
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
option_skip_image_build = click.option(
    "--skip-image-build",
    help="Skips execution of breeze k8s build-k8s-image in deploy-cluster command.",
    is_flag=True,
    envvar="SKIP_IMAGE_BUILD",
)
option_skip_compile_ui_assets = click.option(
    "--skip-compile-ui-assets",
    help="Skips execution of breeze ui compile-assets in deploy-cluster command.",
    is_flag=True,
    envvar="SKIP_IMAGE_BUILD",
)
option_all = click.option("--all", help="Apply it to all created clusters", is_flag=True, envvar="ALL")
option_lang_sdk_test = click.option(
    "--lang-sdk-test/--no-lang-sdk-test",
    help="Provision the lang-SDK (Go + Java) coordinator env and run its system test as part of the "
    "run (KubernetesExecutor only). Off by default so the regular k8s suites skip the test.",
    default=False,
    show_default=True,
    envvar="RUN_LANG_SDK_K8S_TESTS",
)

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
    console_print("\n[warning]NEXT STEP:[/][info] You might now create your cluster by:\n")
    console_print("\nbreeze k8s create-cluster\n")


def _create_cluster(
    python: str,
    kubernetes_version: str,
    output: Output | None,
    num_tries: int,
    force_recreate_cluster: bool,
    *,
    show_hints: bool = True,
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

            if show_hints:
                get_console(output=output).print(
                    "\n[warning]NEXT STEP:[/][info] You might now configure your cluster by:\n"
                )
                get_console(output=output).print("\nbreeze k8s configure-cluster\n")
                # or breeze k8s dev to both configure and deploy
                get_console(output=output).print(
                    f"\n[warning]Alternatively, jump straight into development on Kubernetes with:[/]\n\n"
                    f"breeze k8s dev --python {python} --kubernetes-version {kubernetes_version}\n"
                )
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
        console_print("\n[info]Deleting clusters")
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
                console_print(
                    f"[warning]The cluster {resolved_path.name} does not match expected name. "
                    f"Just removing the {resolved_path}!\n"
                )
                if resolved_path.is_dir():
                    shutil.rmtree(cluster_name.resolve(), ignore_errors=True)
                else:
                    resolved_path.unlink()
    else:
        console_print("\n[warning]No clusters.\n")


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
        console_print(f"\n[warning]Cluster: {cluster_name} has not been created yet\n")
        console_print(
            "[info]Run: "
            f"`breeze k8s create-cluster --python {python} --kubernetes-version {kubernetes_version}`"
            "to create it.\n"
        )
        return False
    console_print("[info]" + "=" * LIST_CONSOLE_WIDTH)
    console_print(f"[info]Cluster: {cluster_name}\n")
    kubeconfig_file = get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version)
    console_print(f"    * KUBECONFIG={kubeconfig_file}")
    kind_config_file = get_kind_cluster_config_path(python=python, kubernetes_version=kubernetes_version)
    console_print(f"    * KINDCONFIG={kind_config_file}")
    console_print(f"\n[info]Cluster info: {cluster_name}\n")
    result = run_command_with_k8s_env(
        ["kubectl", "cluster-info", "--cluster", kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
        check=False,
    )
    if result.returncode != 0:
        return False
    console_print(f"\n[info]Storage class for {cluster_name}\n")
    result = run_command_with_k8s_env(
        ["kubectl", "get", "storageclass", "--cluster", kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
        check=False,
    )
    if result.returncode != 0:
        return False
    console_print(f"\n[info]Running pods for {cluster_name}\n")
    result = run_command_with_k8s_env(
        ["kubectl", "get", "-n", "kube-system", "pods", "--cluster", kubectl_cluster_name],
        python=python,
        kubernetes_version=kubernetes_version,
        check=False,
    )
    if result.returncode != 0:
        return False
    print_cluster_urls(python, kubernetes_version, wait_time_in_seconds=wait_time_in_seconds, output=None)
    console_print(f"\n[success]Cluster healthy: {cluster_name}\n")
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
            console_print("[info]\nCluster status:\n")
            for cluster_name in clusters:
                name = cluster_name.name
                found_python, found_kubernetes_version = _get_python_kubernetes_version_from_name(name)
                if not found_python or not found_kubernetes_version:
                    console_print(f"[warning]\nCould not get cluster from {name}. Skipping.\n")
                elif not _status(
                    python=found_python,
                    kubernetes_version=found_kubernetes_version,
                    wait_time_in_seconds=wait_time_in_seconds,
                ):
                    failed = True
            if failed:
                console_print("\n[error]Some clusters are not healthy!\n")
                sys.exit(1)
        else:
            console_print("\n[warning]No clusters.\n")
            sys.exit(1)
    else:
        if not _status(
            python=python,
            kubernetes_version=kubernetes_version,
            wait_time_in_seconds=wait_time_in_seconds,
        ):
            console_print("\n[error]The cluster is not healthy!\n")
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


# Test-suite container images that Airflow's K8s system tests pull from Docker
# Hub. Tagged (not `:latest`) so kubelet's default imagePullPolicy is
# IfNotPresent — combined with `kind load` below, this means kubelet uses the
# already-loaded image and never reaches out to Docker Hub.  The pin protects
# CI runs from Docker Hub anonymous-pull rate limits, which intermittently
# turn the scheduled K8s test job red. Auto-bumped by
# scripts/ci/prek/upgrade_important_versions.py.
#
# Scope: ONLY images referenced by the regular K8S system tests under
# kubernetes-tests/tests/kubernetes_tests/ (the suite `breeze k8s tests`
# runs against the deployed chart). Images that appear in a kustomize
# overlay under chart/kustomize-overlays/<name>/ must NOT be added here:
# `breeze k8s smoke-test-overlay` auto-discovers them from the rendered
# manifest; add to this list only if the image is also useful to the non-overlay
# K8S tests.
K8S_TEST_IMAGES_TO_PRELOAD: tuple[str, ...] = (
    "alpine:3.24.1",  # xcom_sidecar default in providers/cncf/kubernetes
    "bitnamilegacy/postgresql:16.1.0-debian-11-r15",  # chart/values.yaml postgresql subchart
    "busybox:1.38.0",  # busybox-based system tests in kubernetes-tests/
    "ubuntu:24.04",  # ubuntu-based system tests in kubernetes-tests/
)


def _docker_pull_with_429_retry(image: str, output: Output | None, max_attempts: int = 5) -> int:
    """Run `docker pull <image>` retrying with exponential backoff on Docker Hub 429s.

    Returns the final docker exit code (0 on success). Non-429 failures fail
    fast — only the rate-limit pattern is retried, since for everything else
    retrying would just amplify a real error.
    """
    import time

    delay = 5
    for attempt in range(1, max_attempts + 1):
        result = run_command(
            ["docker", "pull", image],
            check=False,
            output=output,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return 0
        stderr = (result.stderr or "") + (result.stdout or "")
        rate_limited = "429" in stderr or "Too Many Requests" in stderr or "toomanyrequests" in stderr
        if not rate_limited:
            get_console(output=output).print(
                f"[error]docker pull {image} failed (non-rate-limit): {stderr.strip()[:500]}"
            )
            return result.returncode
        if attempt == max_attempts:
            get_console(output=output).print(
                f"[error]docker pull {image} hit Docker Hub 429 on every {max_attempts} attempts; giving up."
            )
            return result.returncode
        get_console(output=output).print(
            f"[warning]docker pull {image} hit Docker Hub 429 "
            f"(attempt {attempt}/{max_attempts}); sleeping {delay}s before retry."
        )
        time.sleep(delay)
        delay *= 2
    return 1


def _preload_test_images_to_kind(
    python: str,
    kubernetes_version: str,
    output: Output | None,
) -> tuple[int, str]:
    """Pre-pull and `kind load` the pinned test-suite images.

    See K8S_TEST_IMAGES_TO_PRELOAD for the list and rationale. Each image is
    pulled once on the host (with retry-on-429), then loaded into every kind
    node. Pods that reference these images then start without kubelet ever
    reaching out to Docker Hub.
    """
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    for image in K8S_TEST_IMAGES_TO_PRELOAD:
        get_console(output=output).print(
            f"[info]Pre-pulling test image {image} for kind cluster {cluster_name}"
        )
        pull_rc = _docker_pull_with_429_retry(image, output=output)
        if pull_rc != 0:
            return pull_rc, f"docker pull {image} failed"
        get_console(output=output).print(f"[info]Loading {image} into kind cluster {cluster_name}")
        kind_load_result = run_command_with_k8s_env(
            ["kind", "load", "docker-image", "--name", cluster_name, image],
            python=python,
            output=output,
            kubernetes_version=kubernetes_version,
            check=False,
        )
        if kind_load_result.returncode != 0:
            get_console(output=output).print(
                f"[error]kind load docker-image {image} into {cluster_name} failed."
            )
            return kind_load_result.returncode, f"kind load {image} failed"
    return 0, f"Pre-loaded {len(K8S_TEST_IMAGES_TO_PRELOAD)} test images into {cluster_name}"


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
            console_print("\n[warning]NEXT STEP:[/][info] You might now upload your k8s image by:\n")
            console_print("\nbreeze k8s upload-k8s-image\n")
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
            console_print("\n[warning]NEXT STEP:[/][info] You might now deploy airflow by:\n")
            console_print("\nbreeze k8s deploy-airflow\n")
            console_print(
                "\n[warning]Note:[/]\nIf you want to run tests with [info]--executor KubernetesExecutor[/], you should deploy airflow with [info]--multi-namespace-mode --executor KubernetesExecutor[/] flag.\n"
            )
            console_print(
                "\nbreeze k8s deploy-airflow --multi-namespace-mode --executor KubernetesExecutor\n"
            )
        sys.exit(return_code)


HELM_DEFAULT_NAMESPACE = "default"
HELM_AIRFLOW_NAMESPACE = "airflow"
TEST_NAMESPACE = "test-namespace"


def _build_skaffold_config(
    python: str,
    kubernetes_version: str,
    executor: str,
    use_standard_naming: bool,
    multi_namespace_mode: bool,
    dags_relative_path: str,
    dags_dest: str,
    log_level: str,
) -> dict[str, Any]:
    from packaging.version import Version

    params = BuildProdParams(python=python)
    use_flask_appbuilder = Version(python) < Version("3.13")
    auth_manager = (
        "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
        if use_flask_appbuilder
        else "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
    )

    _, api_server_port = get_kubernetes_port_numbers(python=python, kubernetes_version=kubernetes_version)

    # --------------------
    # Helm values (NON-image)
    # --------------------
    set_values: dict[str, object] = {
        "config.logging.logging_level": log_level,
        "executor": executor,
        "airflowVersion": params.airflow_semver_version,
        "config.api_auth.jwt_secret": "foo",
        "config.core.auth_manager": auth_manager,
        "config.api.base_url": f"http://localhost:{api_server_port}",
        "apiServer.args": ["bash", "-c", "exec airflow api-server --dev"],
    }

    if multi_namespace_mode:
        set_values["multiNamespaceMode"] = True
    if not use_flask_appbuilder:
        set_values["webserver.defaultUser.enabled"] = False
    if use_standard_naming:
        set_values["useStandardNaming"] = True

    # --------------------
    # Sync configuration
    # --------------------
    sync_entries: list[dict[str, str]] = []
    dependencies_paths: list[str]

    dags_sync_entry = {
        "src": f"{dags_relative_path}/**",
        "dest": dags_dest,
    }

    if dags_relative_path != ".":
        dags_sync_entry["strip"] = f"{dags_relative_path}/"
        dependencies_paths = [f"{dags_relative_path}/**"]
    else:
        dependencies_paths = ["**"]

    sync_entries.append(dags_sync_entry)

    core_relative_path = "airflow-core/src/airflow"
    core_dest = f"{AIRFLOW_SOURCES_TO}/airflow-core/src/airflow"

    sync_entries.append(
        {
            "src": f"{core_relative_path}/**",
            "dest": core_dest,
            "strip": f"{core_relative_path}/",
        }
    )

    if dependencies_paths != ["**"]:
        dependencies_paths.append(f"{core_relative_path}/**")

    providers_relative_path = "providers"
    providers_dest = f"{AIRFLOW_SOURCES_TO}/providers"

    sync_entries.append(
        {
            "src": f"{providers_relative_path}/**",
            "dest": providers_dest,
            "strip": f"{providers_relative_path}/",
        }
    )

    if dependencies_paths != ["**"]:
        dependencies_paths.append(f"{providers_relative_path}/**")

    # --------------------
    # Skaffold config
    # --------------------
    image_var_suffix = params.airflow_image_kubernetes.replace("/", "_").replace(".", "_").replace("-", "_")

    return {
        "apiVersion": "skaffold/v4beta13",
        "kind": "Config",
        "metadata": {"name": "airflow-dags-dev"},
        "build": {
            "tagPolicy": {"envTemplate": {"template": "latest"}},
            "artifacts": [
                {
                    "image": params.airflow_image_kubernetes,
                    "context": AIRFLOW_ROOT_PATH.as_posix(),
                    "custom": {
                        "buildCommand": "true",
                        "dependencies": {"paths": dependencies_paths},
                    },
                    "sync": {"manual": sync_entries},
                }
            ],
            "local": {"push": False},
        },
        "deploy": {
            "helm": {
                "releases": [
                    {
                        "name": "airflow",
                        "chartPath": CHART_PATH.as_posix(),
                        "namespace": HELM_AIRFLOW_NAMESPACE,
                        "createNamespace": True,
                        "skipBuildDependencies": True,
                        # Let Skaffold inject the resolved image instead of hardcoding as `latest` here
                        "setValueTemplates": {
                            "defaultAirflowRepository": f"{{{{.IMAGE_REPO_{image_var_suffix}}}}}",
                            "defaultAirflowTag": f"{{{{.IMAGE_TAG_{image_var_suffix}}}}}",
                            "images.airflow.repository": f"{{{{.IMAGE_REPO_{image_var_suffix}}}}}",
                            "images.airflow.tag": f"{{{{.IMAGE_TAG_{image_var_suffix}}}}}",
                        },
                        "setValues": set_values,
                    }
                ],
                # include test sources (the `breeze k8s configure-cluster` command ) like nodeport for apiServer, volume for Dag, etc
                # https://skaffold.dev/docs/references/yaml/?version=v4beta13#deploy-kubectl
                "hooks": {
                    "after": [
                        {
                            "host": {
                                "command": [
                                    "kubectl",
                                    "apply",
                                    "-f",
                                    "volumes.yaml",
                                    "--namespace",
                                    HELM_DEFAULT_NAMESPACE,
                                ],
                                "dir": (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "kubernetes").as_posix(),
                            }
                        },
                        {
                            "host": {
                                "command": [
                                    "kubectl",
                                    "apply",
                                    "-f",
                                    "nodeport.yaml",
                                    "--namespace",
                                    HELM_AIRFLOW_NAMESPACE,
                                ],
                                "dir": (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "kubernetes").as_posix(),
                            }
                        },
                    ]
                },
            },
        },
    }


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
            console_print(
                "\n[warning]NEXT STEP:[/][info] You might now build your k8s image "
                "with all latest dependencies:\n"
            )
            console_print("\n breeze k8s build-k8s-image --rebuild-base-image\n")
            console_print(
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
            helm_command.extend(["--set", "createUserJob.enabled=false"])
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
            console_print(
                "\n[warning]NEXT STEP:[/][info] You might now run tests or interact "
                "with airflow via shell (kubectl, pytest etc.) or k9s commands:\n"
            )
            console_print("\nbreeze k8s tests")
            console_print("\nbreeze k8s shell")
            console_print("\nbreeze k8s k9s\n")
        sys.exit(return_code)


@kubernetes_group.command(
    name="dev",
    help=(
        "Run skaffold dev loop to sync dags, airflow-core, and providers sources to running pods "
        "(scheduler/triggerer/dag-processor/API Server hot-reload; UI auto-refresh not supported yet). "
    ),
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@option_python
@option_kubernetes_version
@option_executor
@option_log_level
@option_use_standard_naming
@option_multi_namespace_mode
@option_dags_path
@option_dags_dest
@option_skaffold_deploy
@option_verbose
@option_dry_run
@click.argument("skaffold_args", nargs=-1, type=click.UNPROCESSED)
def dev(
    python: str,
    kubernetes_version: str,
    executor: str,
    log_level: str,
    use_standard_naming: bool,
    multi_namespace_mode: bool,
    dags_path: Path,
    dags_dest: str,
    deploy: bool,
    skaffold_args: tuple[str, ...],
):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    make_sure_skaffold_installed()
    dags_path_abs = dags_path
    if not dags_path_abs.is_absolute():
        dags_path_abs = AIRFLOW_ROOT_PATH / dags_path_abs
    dags_path_abs = dags_path_abs.resolve()
    if not dags_path_abs.is_dir():
        console_print(f"[error]DAGs path does not exist or is not a directory: {dags_path_abs}")
        sys.exit(1)
    try:
        dags_relative_path = dags_path_abs.relative_to(AIRFLOW_ROOT_PATH).as_posix()
    except ValueError:
        console_print(f"[error]DAGs path must be under the Airflow sources: {AIRFLOW_ROOT_PATH}")
        sys.exit(1)
    if not get_kind_cluster_config_path(python=python, kubernetes_version=kubernetes_version).exists():
        console_print(
            f"\n[warning]Cluster for Python {python} and Kubernetes {kubernetes_version} "
            "has not been created yet.\n"
        )

        if confirm_action(
            f"Do you want to create cluster for Python {python} and Kubernetes {kubernetes_version} now?"
        ):
            return_code, _ = _create_cluster(
                python=python,
                kubernetes_version=kubernetes_version,
                output=None,
                force_recreate_cluster=False,
                num_tries=1,
                # Since we are using skaffold dev, so we don't need to show the hints for configuring the cluster
                show_hints=False,
            )
            if return_code != 0:
                sys.exit(return_code)
        else:
            console_print(
                "\n[info]To create the cluster, please run: [/]\n\n"
                f"breeze k8s create-cluster --python {python} --kubernetes-version {kubernetes_version}\n"
            )
            sys.exit(0)
    skaffold_config = _build_skaffold_config(
        python=python,
        kubernetes_version=kubernetes_version,
        executor=executor,
        use_standard_naming=use_standard_naming,
        multi_namespace_mode=multi_namespace_mode,
        dags_relative_path=dags_relative_path,
        dags_dest=dags_dest,
        log_level=log_level,
    )
    if not deploy:
        console_print(
            "[info]Running skaffold without deploying Helm resources. "
            "If sync cannot find pods, rerun with --deploy."
        )
    with tempfile.TemporaryDirectory(prefix="skaffold_") as tmp_dir:
        dev_env_values = {
            "scheduler": {"env": [{"name": "DEV_MODE", "value": "true"}]},
            "triggerer": {"env": [{"name": "DEV_MODE", "value": "true"}]},
            "dagProcessor": {"env": [{"name": "DEV_MODE", "value": "true"}]},
        }
        dev_env_values_path = Path(tmp_dir) / "dev-env-values.yaml"
        dev_env_values_path.write_text(yaml.safe_dump(dev_env_values, sort_keys=False))
        skaffold_config["deploy"]["helm"]["releases"][0]["valuesFiles"] = [dev_env_values_path.as_posix()]
        skaffold_config_path = Path(tmp_dir) / "skaffold.yaml"
        skaffold_config_path.write_text(yaml.safe_dump(skaffold_config, sort_keys=False))

        console_print(f"[info]Generated skaffold config at {skaffold_config_path}")

        skaffold_command = [
            "skaffold",
            "dev",
            "-f",
            skaffold_config_path.as_posix(),
            "--auto-build=false",
        ]
        if not deploy:
            skaffold_command.append("--auto-deploy=false")
            skaffold_command.append("--cleanup=false")
        if skaffold_args:
            skaffold_command.extend(skaffold_args)
        result = run_command_with_k8s_env(
            skaffold_command,
            python=python,
            kubernetes_version=kubernetes_version,
            executor=executor,
            check=False,
            cwd=AIRFLOW_ROOT_PATH.as_posix(),
        )
        sys.exit(result.returncode)


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
        console_print(
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
        console_print("[info]Running k9s tool using docker.")
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
            console_print(
                "\n[warning]If you see `exec /bin/k9s: exec format error` it might be because"
                " of known kind bug (https://github.com/kubernetes-sigs/kind/issues/3510).\n"
            )
            console_print(
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
    console_print(f"[info]\nDumping logs for {cluster_name} to {tmpdir}:\n")
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
            console_print("[info]\nDumping cluster logs:\n")
            for cluster_name in clusters:
                name = cluster_name.name
                found_python, found_kubernetes_version = _get_python_kubernetes_version_from_name(name)
                if not found_python or not found_kubernetes_version:
                    console_print(f"[warning]\nCould not get cluster from {name}. Skipping.\n")
                    continue
                _logs(python=found_python, kubernetes_version=found_kubernetes_version)
        else:
            console_print("\n[warning]No clusters.\n")
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
    console_print("\n[info]Entering interactive k8s shell.\n")
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
    lang_sdk_test: bool,
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
            f"\n[info]Pre-loading pinned test images into kind cluster for "
            f"Python {python}, Kubernetes {kubernetes_version}\n"
        )
        returncode, message = _preload_test_images_to_kind(
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
        if lang_sdk_test and executor == KUBERNETES_EXECUTOR:
            get_console(output=output).print(
                f"\n[info]Provisioning lang-SDK test env for Python {python}, "
                f"Kubernetes {kubernetes_version}\n"
            )
            _setup_lang_sdk_test(python=python, kubernetes_version=kubernetes_version, output=output)
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
@option_lang_sdk_test
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
    lang_sdk_test: bool,
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
        console_print(f"[info]Running complete tests for: {short_combo_titles}")
        console_print(f"[info]Parallelism: {parallelism}")
        console_print(f"[info]Extra test args: {executor}")
        console_print(f"[info]Executor: {executor}")
        console_print(f"[info]Use standard naming: {use_standard_naming}")
        console_print(f"[info]Upgrade: {upgrade}")
        console_print(f"[info]Use uv: {use_uv}")
        console_print(f"[info]Rebuild base image: {rebuild_base_image}")
        console_print(f"[info]Force recreate cluster: {force_recreate_cluster}")
        console_print(f"[info]Include success outputs: {include_success_outputs}")
        console_print(f"[info]Debug resources: {debug_resources}")
        console_print(f"[info]Skip cleanup: {skip_cleanup}")
        console_print(f"[info]Wait time in seconds: {wait_time_in_seconds}")
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
                            "lang_sdk_test": lang_sdk_test,
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
            lang_sdk_test=lang_sdk_test,
            num_tries=1,
            extra_options=None,
            test_args=pytest_args,
            output=None,
        )
        if result != 0:
            sys.exit(result)


@kubernetes_group.command(
    name="deploy-cluster",
    help="Create, configure kind cluster and build Airflow image for Airflow Chart deployment.",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@option_force_venv_setup
@option_force_recreate_cluster
@option_python
@option_kubernetes_version
@option_rebuild_base_image
@option_use_uv
@option_skip_image_build
@option_skip_compile_ui_assets
def deploy_cluster(
    force_venv_setup: bool,
    force_recreate_cluster: bool,
    python: str,
    kubernetes_version: str,
    rebuild_base_image: bool,
    use_uv: bool,
    skip_image_build: bool,
    skip_compile_ui_assets: bool,
):
    console_print("[info]Syncing Virtual Environment[/]")
    result = sync_virtualenv(force_venv_setup=force_venv_setup)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()

    return_code, _ = _create_cluster(
        python=python,
        kubernetes_version=kubernetes_version,
        output=None,
        force_recreate_cluster=force_recreate_cluster,
        num_tries=1,
        show_hints=False,
    )
    if return_code != 0:
        sys.exit(return_code)

    return_code, _ = _configure_k8s_cluster(
        python=python,
        kubernetes_version=kubernetes_version,
        output=None,
    )
    if return_code != 0:
        sys.exit(return_code)

    if skip_compile_ui_assets:
        console_print("[info]Skipping compilation of Airflow UI assets[/]")
    else:
        console_print("[info]Compiling Airflow UI assets[/]")
        perform_environment_checks()
        assert_prek_installed()
        result = run_compile_ui_assets(
            dev=False, run_in_background=False, force_clean=False, additional_ui_hooks=[]
        )
        if result.returncode != 0:
            sys.exit(result.returncode)

    if skip_image_build:
        console_print("[info]Skipping Airflow Image Build[/]")
    else:
        return_code, _ = _rebuild_k8s_image(
            python=python,
            rebuild_base_image=rebuild_base_image,
            copy_local_sources=True,
            use_uv=use_uv,
            output=None,
        )
        if return_code != 0:
            sys.exit(return_code)

    return_code, _ = _upload_k8s_image(
        python=python,
        kubernetes_version=kubernetes_version,
        output=None,
    )
    if return_code != 0:
        sys.exit(return_code)


# ---------------------------------------------------------------------------
# lang-SDK (Go + Java) coordinator system test on KubernetesExecutor.
# Assets live under kubernetes-tests/lang_sdk/. See that directory's README.md.
# ---------------------------------------------------------------------------
LANG_SDK_PATH = AIRFLOW_ROOT_PATH / "kubernetes-tests" / "lang_sdk"
# The Go/Java example sources live under the test dir (not in go-sdk/java-sdk). go_example is its
# own Go module (replace-directive onto the in-repo go-sdk); java_example is a standalone Gradle
# build that resolves the SDK from mavenLocal.
LANG_SDK_GO_EXAMPLE_PATH = LANG_SDK_PATH / "go_example"
LANG_SDK_GO_BUNDLE_NAME = "lang_sdk_combined"
LANG_SDK_JAVA_EXAMPLE_PATH = LANG_SDK_PATH / "java_example"
# Build the artifacts inside ephemeral toolchain containers so the host needs
# neither Go nor a JDK installed (mirrors the airflow-e2e-tests conftest).
LANG_SDK_GO_BUILDER_IMAGE = os.environ.get("GO_BUILDER_IMAGE", "golang:1.25-alpine")
LANG_SDK_JAVA_BUILDER_IMAGE = "eclipse-temurin:17-jdk"
LANG_SDK_MAVEN_CACHE_PATH = AIRFLOW_ROOT_PATH / "files" / "m2"
LANG_SDK_GRADLE_CACHE_PATH = AIRFLOW_ROOT_PATH / "files" / "gradle"
# The Java queue needs a JRE the JavaCoordinator can exec; the Go queue runs on
# the plain prod image. Building the Java worker image as a separate tag (prod +
# JRE, see Dockerfile.java) lets each coordinator route its queue to a distinct
# pod_template_file base image.
LANG_SDK_JAVA_WORKER_IMAGE = "lang-sdk-java-worker:latest"
LANG_SDK_JAVA_DOCKERFILE = LANG_SDK_PATH / "Dockerfile.java"
LANG_SDK_AWS_CONN_URI = (
    "aws://test:test@/?region_name=us-east-1&"
    "endpoint_url=http%3A%2F%2Flocalstack.airflow.svc.cluster.local%3A4566"
)
# The Go/Java SDKs are always built from upstream main so branches with stale or missing
# go-sdk/java-sdk copies still test current SDK sources. See kubernetes-tests/lang_sdk/README.md.
LANG_SDK_UPSTREAM_GIT_URL = "https://github.com/apache/airflow.git"
LANG_SDK_UPSTREAM_REF = "main"


def _lang_sdk_fetch_upstream_sdk_sources(staging: Path, output: Output | None) -> tuple[Path, Path]:
    """Extract go-sdk/ and java-sdk/ from upstream main into a throwaway staging dir.

    Prefers the ``upstream`` remote when configured, falling back to the canonical GitHub URL
    (CI has no ``upstream`` and ``origin`` may be a fork). Shallow fetch + ``git archive`` never
    touch the working tree or index.

    The real, local task-sdk is symlinked alongside the extraction because java-sdk's
    ``sdk/build.gradle.kts`` reads a sibling ``../task-sdk/.../schema.json``. The gradle wrapper
    scripts and jar are ``export-ignore`` (ASF LEGAL-570) so ``git archive`` drops them;
    ``git show`` restores them, re-marking ``gradlew`` executable.
    """
    remotes = run_command(
        ["git", "remote"], cwd=AIRFLOW_ROOT_PATH, output=output, capture_output=True, text=True, check=True
    ).stdout.split()
    fetch_source = "upstream" if "upstream" in remotes else LANG_SDK_UPSTREAM_GIT_URL
    get_console(output=output).print(
        f"[info]Fetching {LANG_SDK_UPSTREAM_REF} from {fetch_source} for the lang-SDK Go/Java sources"
    )
    run_command(
        ["git", "fetch", "--depth=1", fetch_source, LANG_SDK_UPSTREAM_REF],
        cwd=AIRFLOW_ROOT_PATH,
        output=output,
        check=True,
    )
    sha = run_command(
        ["git", "rev-parse", "FETCH_HEAD"],
        cwd=AIRFLOW_ROOT_PATH,
        output=output,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    get_console(output=output).print(f"[info]lang-SDK Go/Java sources pinned to upstream main @ {sha}")
    extracted = staging / "upstream_lang_sdk_sources"
    extracted.mkdir(parents=True, exist_ok=True)
    archive_path = staging / "upstream_lang_sdk_sources.tar"
    run_command(
        ["git", "archive", "--format=tar", f"--output={archive_path}", sha, "--", "go-sdk", "java-sdk"],
        cwd=AIRFLOW_ROOT_PATH,
        output=output,
        check=True,
    )
    run_command(["tar", "-xf", str(archive_path), "-C", str(extracted)], output=output, check=True)
    for rel_path, mode in (
        ("java-sdk/gradlew", 0o755),
        ("java-sdk/gradlew.bat", 0o644),
        ("java-sdk/gradle/wrapper/gradle-wrapper.jar", 0o644),
    ):
        restored = extracted / rel_path
        restored.parent.mkdir(parents=True, exist_ok=True)
        restored.write_bytes(
            run_command(
                ["git", "show", f"{sha}:{rel_path}"],
                cwd=AIRFLOW_ROOT_PATH,
                output=output,
                capture_output=True,
                check=True,
            ).stdout
            or b""
        )
        restored.chmod(mode)
    (extracted / "task-sdk").symlink_to(AIRFLOW_ROOT_PATH / "task-sdk")
    return extracted / "go-sdk", extracted / "java-sdk"


def _lang_sdk_build_go_bundle(
    staging: Path, upstream_go_sdk: Path, output: Output | None, *, native: bool = False
) -> None:
    """Build the Go bundle into ``staging/go-artifacts`` and copy the result into the staging dir.

    By default the build runs in an ephemeral Go toolchain container so the host needs no Go install,
    writing its caches into a gitignored dir under the repo. In ``native`` mode (used in CI, where the
    host already has a cached Go toolchain via ``actions/setup-go``) it invokes the host ``go`` directly,
    skipping the container image pull and reusing the runner's module/build cache.

    go_example's go.mod ``replace``s go-sdk by relative path, so the build runs in a scratch
    workspace mirroring the repo layout with ``upstream_go_sdk`` at ``<workspace>/go-sdk``,
    letting the unmodified directive resolve against the upstream copy.
    """
    go_dir = staging / "go-artifacts"
    go_dir.mkdir(parents=True, exist_ok=True)
    example_rel = LANG_SDK_GO_EXAMPLE_PATH.relative_to(AIRFLOW_ROOT_PATH)
    workspace = staging / "go_workspace"
    example_path = workspace / example_rel
    example_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(LANG_SDK_GO_EXAMPLE_PATH, example_path, ignore=shutil.ignore_patterns(".home"))
    # In dry-run the fetch/extract commands are skipped, so the upstream copy and build outputs
    # never materialize -- skip the filesystem work that depends on them.
    if not get_dry_run():
        shutil.copytree(upstream_go_sdk, workspace / "go-sdk")
    output_bin = example_path / "bin" / LANG_SDK_GO_BUNDLE_NAME
    output_bin.parent.mkdir(parents=True, exist_ok=True)

    # CGO_ENABLED=0 yields a fully static binary that runs on the stock worker. The package built is
    # the current dir (".") because go_example is its own module.
    if native:
        get_console(output=output).print("[info]Building Go bundle with the host Go toolchain")
        run_command(
            ["go", "tool", "airflow-go-pack", "--output", str(output_bin), "."],
            cwd=example_path,
            env={**os.environ, "CGO_ENABLED": "0"},
            output=output,
            check=True,
        )
    else:
        uid_gid = f"{os.getuid()}:{os.getgid()}"
        go_example_ctr = f"/repo/{example_rel.as_posix()}"
        # USER/HOME must be set because the SDK calls user.Current() at init; with cgo disabled Go's
        # pure-Go resolver reads those env vars and panics if either is empty. HOME is mounted from
        # the real go_example's gitignored cache dir so the caches persist across scratch workspaces.
        (LANG_SDK_GO_EXAMPLE_PATH / ".home").mkdir(parents=True, exist_ok=True)
        get_console(output=output).print(f"[info]Building Go bundle in {LANG_SDK_GO_BUILDER_IMAGE}")
        run_command(
            [
                "docker",
                "run",
                "--rm",
                "--user",
                uid_gid,
                "-e",
                f"HOME={go_example_ctr}/.home",
                "-e",
                "USER=airflow",
                "-e",
                "CGO_ENABLED=0",
                "-v",
                f"{workspace}:/repo",
                "-v",
                f"{LANG_SDK_GO_EXAMPLE_PATH / '.home'}:{go_example_ctr}/.home",
                "-w",
                go_example_ctr,
                LANG_SDK_GO_BUILDER_IMAGE,
                "go",
                "tool",
                "airflow-go-pack",
                "--output",
                f"{go_example_ctr}/bin/{LANG_SDK_GO_BUNDLE_NAME}",
                ".",
            ],
            output=output,
            check=True,
        )
    if not get_dry_run():
        shutil.copy(output_bin, go_dir / LANG_SDK_GO_BUNDLE_NAME)


def _lang_sdk_build_java_jar(
    staging: Path, upstream_java_sdk: Path, output: Output | None, *, native: bool = False
) -> None:
    """Publish the Java SDK to mavenLocal then build the java_example jar into ``staging/java-artifacts``.

    By default the build runs in an ephemeral JDK container so the host needs no JDK, persisting the
    Gradle distribution/dependency and Maven caches via mounted dirs. In ``native`` mode (used in CI,
    where the host already has a cached JDK + Gradle cache via ``actions/setup-java``) it invokes the
    host ``./gradlew`` directly, skipping the container image pull and reusing the runner's ``~/.gradle``
    cache. ``java_example`` resolves the SDK from ``mavenLocal()``, so the SDK is published first, then
    the bundle is built with java-sdk's gradle wrapper pointed at the example project (``-p``).

    Both gradle invocations run against ``upstream_java_sdk`` rather than the local ``java-sdk/``;
    only ``-p`` stays pointed at the local ``java_example``, which is test-harness code that keeps
    tracking the checked-out branch.
    """
    java_dir = staging / "java-artifacts"
    java_dir.mkdir(parents=True, exist_ok=True)

    if native:
        get_console(output=output).print(
            "[info]Publishing Java SDK artifacts to local Maven repository with the host Gradle toolchain"
        )
        run_command(
            ["./gradlew", "publishToMavenLocal", "-PskipSigning=true", "--no-daemon", "--console=plain"],
            cwd=upstream_java_sdk,
            output=output,
            check=True,
        )
        get_console(output=output).print("[info]Building Java jar with the host Gradle toolchain")
        run_command(
            ["./gradlew", "-p", str(LANG_SDK_JAVA_EXAMPLE_PATH), "bundle", "--no-daemon", "--console=plain"],
            cwd=upstream_java_sdk,
            output=output,
            check=True,
        )
    else:
        uid_gid = f"{os.getuid()}:{os.getgid()}"
        java_example_ctr = f"/repo/{LANG_SDK_JAVA_EXAMPLE_PATH.relative_to(AIRFLOW_ROOT_PATH).as_posix()}"
        # --user keeps build outputs owned by the host user; HOME is set explicitly because that UID has
        # no /etc/passwd entry. GRADLE_USER_HOME and the mounted ~/.m2 persist the Gradle distribution
        # and dependency caches between runs -- outside /repo/java-sdk, which is remounted to the
        # (per-run) upstream copy below.
        LANG_SDK_MAVEN_CACHE_PATH.mkdir(parents=True, exist_ok=True)
        LANG_SDK_GRADLE_CACHE_PATH.mkdir(parents=True, exist_ok=True)
        java_docker_prefix = [
            "docker",
            "run",
            "--rm",
            "--user",
            uid_gid,
            "-e",
            "GRADLE_USER_HOME=/workspace-home/.gradle",
            "-e",
            "HOME=/workspace-home",
            "-v",
            f"{LANG_SDK_MAVEN_CACHE_PATH}:/workspace-home/.m2",
            "-v",
            f"{LANG_SDK_GRADLE_CACHE_PATH}:/workspace-home/.gradle",
            "-v",
            f"{AIRFLOW_ROOT_PATH}:/repo",
            "-v",
            f"{upstream_java_sdk}:/repo/java-sdk",
        ]
        get_console(output=output).print("[info]Publishing Java SDK artifacts to local Maven repository")
        run_command(
            [
                *java_docker_prefix,
                "-w",
                "/repo/java-sdk",
                LANG_SDK_JAVA_BUILDER_IMAGE,
                "./gradlew",
                "publishToMavenLocal",
                "-PskipSigning=true",
                "--no-daemon",
                "--console=plain",
            ],
            output=output,
            check=True,
        )
        get_console(output=output).print(f"[info]Building Java jar in {LANG_SDK_JAVA_BUILDER_IMAGE}")
        run_command(
            [
                *java_docker_prefix,
                "-w",
                "/repo/java-sdk",
                LANG_SDK_JAVA_BUILDER_IMAGE,
                "./gradlew",
                "-p",
                java_example_ctr,
                "bundle",
                "--no-daemon",
                "--console=plain",
            ],
            output=output,
            check=True,
        )
    if get_dry_run():
        return
    jars = list((LANG_SDK_JAVA_EXAMPLE_PATH / "build" / "bundle").glob("*.jar"))
    if not jars:
        get_console(output=output).print("[error]No jar produced by the Java bundle build")
        sys.exit(1)
    shutil.copy(jars[0], java_dir / jars[0].name)


def _lang_sdk_kubectl(
    args: list[str], python: str, kubernetes_version: str, output: Output | None, check=True
):
    return run_command_with_k8s_env(
        ["kubectl", *args],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=check,
    )


def _lang_sdk_deploy_localstack(python: str, kubernetes_version: str, output: Output | None) -> None:
    get_console(output=output).print("[info]Deploying localstack (S3) into the airflow namespace")
    _lang_sdk_kubectl(
        ["apply", "-f", str(LANG_SDK_PATH / "manifests" / "localstack.yaml")],
        python,
        kubernetes_version,
        output,
    )
    _lang_sdk_kubectl(
        ["rollout", "status", "deployment/localstack", "-n", HELM_AIRFLOW_NAMESPACE, "--timeout=180s"],
        python,
        kubernetes_version,
        output,
    )


def _lang_sdk_upload_artifacts(
    staging: Path, python: str, kubernetes_version: str, output: Output | None
) -> None:
    """Copy artifacts + stub Dag into the localstack pod and create/fill S3 buckets via awslocal."""
    pod = run_command_with_k8s_env(
        [
            "kubectl",
            "get",
            "pod",
            "-n",
            HELM_AIRFLOW_NAMESPACE,
            "-l",
            "app=localstack",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    go_bundle = staging / "go-artifacts" / "lang_sdk_combined"
    if get_dry_run():
        # The dry-run build steps produce no jar; use the placeholder name so the commands still print.
        java_jar = staging / "java-artifacts" / "app.jar"
    else:
        java_jar = next((staging / "java-artifacts").glob("*.jar"))
    stub_dag = LANG_SDK_PATH / "dags" / "lang_sdk_combined.py"

    for src, dest in (
        (go_bundle, "/tmp/go_bundle"),
        (java_jar, "/tmp/app.jar"),
        (stub_dag, "/tmp/lang_sdk_combined.py"),
    ):
        _lang_sdk_kubectl(
            ["cp", str(src), f"{HELM_AIRFLOW_NAMESPACE}/{pod}:{dest}"], python, kubernetes_version, output
        )

    for bucket in ("go-artifacts", "java-artifacts", "dags"):
        _lang_sdk_kubectl(
            ["exec", "-n", HELM_AIRFLOW_NAMESPACE, pod, "--", "awslocal", "s3", "mb", f"s3://{bucket}"],
            python,
            kubernetes_version,
            output,
            check=False,
        )
    uploads = (
        ("/tmp/go_bundle", "s3://go-artifacts/lang_sdk_combined"),
        ("/tmp/app.jar", "s3://java-artifacts/app.jar"),
        ("/tmp/lang_sdk_combined.py", "s3://dags/lang_sdk_combined.py"),
    )
    for src, dest in uploads:
        _lang_sdk_kubectl(
            ["exec", "-n", HELM_AIRFLOW_NAMESPACE, pod, "--", "awslocal", "s3", "cp", src, dest],
            python,
            kubernetes_version,
            output,
        )


def _lang_sdk_apply_configmaps_and_secret(
    python: str, kubernetes_version: str, go_image: str, java_image: str, output: Output | None
) -> None:
    with tempfile.TemporaryDirectory(prefix="lang_sdk_pt_") as tmp:
        rendered = Path(tmp)
        for name in ("lang_sdk_golang.yaml", "lang_sdk_java.yaml"):
            text = (LANG_SDK_PATH / "pod_templates" / name).read_text()
            text = text.replace("__LANG_SDK_GO_IMAGE__", go_image).replace(
                "__LANG_SDK_JAVA_IMAGE__", java_image
            )
            (rendered / name).write_text(text)
        # Idempotent configmap/secret application via `--dry-run | apply`.
        for cm_args in (
            ["create", "configmap", "lang-sdk-pod-templates", f"--from-file={rendered}"],
            [
                "create",
                "configmap",
                "lang-sdk-scripts",
                f"--from-file={LANG_SDK_PATH / 'stage_artifacts.py'}",
            ],
            [
                "create",
                "secret",
                "generic",
                "lang-sdk-aws-conn",
                f"--from-literal=uri={LANG_SDK_AWS_CONN_URI}",
            ],
        ):
            manifest = run_command_with_k8s_env(
                ["kubectl", *cm_args, "-n", HELM_AIRFLOW_NAMESPACE, "--dry-run=client", "-o", "yaml"],
                python=python,
                kubernetes_version=kubernetes_version,
                output=output,
                capture_output=True,
                check=True,
            ).stdout
            run_command_with_k8s_env(
                ["kubectl", "apply", "-n", HELM_AIRFLOW_NAMESPACE, "-f", "-"],
                python=python,
                kubernetes_version=kubernetes_version,
                output=output,
                input=manifest,
                check=True,
            )


def _lang_sdk_build_java_worker_image(
    base_image: str, python: str, kubernetes_version: str, output: Output | None
) -> str:
    """Build the prod+JRE Java worker image and load it into the kind cluster.

    Returns the local image tag the Java pod template should reference.
    """
    get_console(output=output).print(
        f"[info]Building Java worker image {LANG_SDK_JAVA_WORKER_IMAGE} (JRE on top of {base_image})"
    )
    run_command(
        [
            "docker",
            "build",
            "--build-arg",
            f"BASE_IMAGE={base_image}",
            "-t",
            LANG_SDK_JAVA_WORKER_IMAGE,
            "-f",
            str(LANG_SDK_JAVA_DOCKERFILE),
            str(LANG_SDK_PATH),
        ],
        output=output,
        check=True,
    )
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    get_console(output=output).print(f"[info]Loading {LANG_SDK_JAVA_WORKER_IMAGE} into {cluster_name}")
    run_command_with_k8s_env(
        ["kind", "load", "docker-image", "--name", cluster_name, LANG_SDK_JAVA_WORKER_IMAGE],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=True,
    )
    return LANG_SDK_JAVA_WORKER_IMAGE


def _lang_sdk_deploy_airflow(python: str, kubernetes_version: str, output: Output | None) -> None:
    params = BuildProdParams(python=python)
    image = params.airflow_image_kubernetes
    get_console(output=output).print("[info]Upgrading airflow Helm release with lang-SDK values")
    run_command_with_k8s_env(
        [
            "helm",
            "upgrade",
            "--install",
            "airflow",
            os.fspath(CHART_PATH),
            "--kube-context",
            get_kubectl_cluster_name(python=python, kubernetes_version=kubernetes_version),
            "--namespace",
            HELM_AIRFLOW_NAMESPACE,
            # Layer the lang-SDK values on top of the already-deployed release rather than
            # re-rendering from chart defaults. Without this, helm discards the base deploy's
            # --set overrides (notably config.core.auth_manager=SimpleAuthManager on Python 3.13),
            # reverting the api-server to the chart-default FabAuthManager so it never writes
            # simple_auth_manager_passwords.json.generated and the API-login tests error out.
            "--reuse-values",
            "--set",
            f"defaultAirflowRepository={image}",
            "--set",
            "defaultAirflowTag=latest",
            "-f",
            str(LANG_SDK_PATH / "config" / "values.yaml"),
            "--timeout",
            "20m0s",
            "--wait",
        ],
        python=python,
        kubernetes_version=kubernetes_version,
        output=output,
        check=True,
    )


def _run_lang_sdk_parallel(
    steps: list[tuple[str, Callable[[Output | None], Any]]],
    output: Output | None,
) -> dict[str, Any]:
    """Run mutually-independent lang-SDK build/deploy steps concurrently.

    The Go build, Java jar build, Java worker image build and localstack deploy share no inputs, so
    running them together turns provisioning time into roughly the slowest single step. Each step
    captures its own output; the captured logs are streamed in a stable order once all steps finish
    (so parallel docker/kubectl output does not interleave), and the first failure is re-raised.
    Returns each step's return value keyed by its title.
    """
    from concurrent.futures import ThreadPoolExecutor

    titles = [title for title, _ in steps]
    step_outputs = get_output_files(titles)
    get_console(output=output).print(
        f"[info]Running lang-SDK provisioning steps in parallel: {', '.join(titles)}"
    )
    results: dict[str, Any] = {}
    errors: list[tuple[str, BaseException]] = []
    with ThreadPoolExecutor(max_workers=len(steps)) as pool:
        futures = [pool.submit(fn, step_output) for (_, fn), step_output in zip(steps, step_outputs)]
        for (title, _), future in zip(steps, futures):
            try:
                results[title] = future.result()
            except BaseException as error:
                errors.append((title, error))
    failed_titles = {title for title, _ in errors}
    for (title, _), step_output in zip(steps, step_outputs):
        message_type = MessageType.ERROR if title in failed_titles else MessageType.SUCCESS
        with ci_group(step_output.escaped_title, message_type):
            os.write(1, Path(step_output.file_name).read_bytes())
    if errors:
        get_console(output=output).print(
            f"[error]lang-SDK provisioning failed in: {', '.join(sorted(failed_titles))}"
        )
        raise errors[0][1]
    return results


def _setup_lang_sdk_test(
    python: str,
    kubernetes_version: str,
    go_image: str | None = None,
    java_image: str | None = None,
    output: Output | None = None,
) -> None:
    """Provision the lang-SDK coordinator env on an already-deployed KubernetesExecutor cluster.

    Fetches go-sdk/java-sdk from upstream main, then builds the Go/Java artifacts, the Java worker
    image and deploys localstack in parallel, then serially uploads the artifacts, applies the
    config + secret, and helm-upgrades Airflow with the lang-SDK values.
    """
    go_image = go_image or f"{BuildProdParams(python=python).airflow_image_kubernetes}:latest"
    build_java_image = java_image is None
    if java_image is None:
        # The worker-image build below produces this fixed tag; resolve it up-front so the config
        # rendering (which needs the tag, not the build result) does not depend on the parallel run.
        java_image = LANG_SDK_JAVA_WORKER_IMAGE
    # In CI the Go/Java toolchains are provisioned + cached on the host (actions/setup-go, setup-java),
    # so building the artifacts natively skips the toolchain-image pulls and reuses the runner caches.
    native = os.environ.get("LANG_SDK_NATIVE_TOOLCHAIN", "").lower() == "true"
    with tempfile.TemporaryDirectory(prefix="lang_sdk_artifacts_") as tmp:
        staging = Path(tmp)
        upstream_go_sdk, upstream_java_sdk = _lang_sdk_fetch_upstream_sdk_sources(staging, output)
        steps: list[tuple[str, Callable[[Output | None], Any]]] = [
            (
                "Build Go bundle",
                lambda o: _lang_sdk_build_go_bundle(staging, upstream_go_sdk, o, native=native),
            ),
            (
                "Build Java jar",
                lambda o: _lang_sdk_build_java_jar(staging, upstream_java_sdk, o, native=native),
            ),
            ("Deploy localstack", lambda o: _lang_sdk_deploy_localstack(python, kubernetes_version, o)),
        ]
        if build_java_image:
            steps.append(
                (
                    "Build Java worker image",
                    lambda o: _lang_sdk_build_java_worker_image(go_image, python, kubernetes_version, o),
                )
            )
        _run_lang_sdk_parallel(steps, output=output)
        _lang_sdk_upload_artifacts(staging, python, kubernetes_version, output)
    _lang_sdk_apply_configmaps_and_secret(python, kubernetes_version, go_image, java_image, output)
    _lang_sdk_deploy_airflow(python, kubernetes_version, output)


@kubernetes_group.command(
    name="setup-lang-sdk-test",
    help="Provision the lang-SDK (Go + Java) coordinator system test on an already-deployed "
    "KubernetesExecutor cluster: build artifacts, build + load the Java worker image, deploy "
    "localstack S3, upload artifacts + stub Dag, create config, and upgrade the Helm release. "
    "Run the test afterwards with `RUN_LANG_SDK_K8S_TESTS=true breeze k8s tests "
    "--executor KubernetesExecutor -- -k test_lang_sdk_combined_dag_succeeds`.",
)
@option_python
@option_kubernetes_version
@click.option(
    "--go-image",
    help="Image for the Go (ExecutableCoordinator) worker pod. Defaults to the k8s image.",
)
@click.option(
    "--java-image",
    help="Image for the Java (JavaCoordinator) worker pod. Must include a JRE. Defaults to building "
    "the prod image plus a headless JRE (Dockerfile.java) and loading it into the kind cluster.",
)
@option_verbose
@option_dry_run
def setup_lang_sdk_test(python: str, kubernetes_version: str, go_image: str | None, java_image: str | None):
    result = sync_virtualenv(force_venv_setup=False)
    if result.returncode != 0:
        sys.exit(result.returncode)
    make_sure_kubernetes_tools_are_installed()
    _setup_lang_sdk_test(
        python=python,
        kubernetes_version=kubernetes_version,
        go_image=go_image,
        java_image=java_image,
        output=None,
    )
    console_print(
        "\n[success]lang-SDK test environment is ready.[/]\n"
        "[info]Run the test with (the test is gated on RUN_LANG_SDK_K8S_TESTS):\n"
        "  RUN_LANG_SDK_K8S_TESTS=true breeze k8s tests --executor KubernetesExecutor "
        "-- -k test_lang_sdk_combined_dag_succeeds\n"
    )
