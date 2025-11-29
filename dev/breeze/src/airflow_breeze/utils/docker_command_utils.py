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
"""Various utils to prepare docker and docker compose commands."""

from __future__ import annotations

import copy
import json
import os
import re
import sys
from functools import lru_cache
from subprocess import DEVNULL, CompletedProcess
from typing import TYPE_CHECKING

from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.cache import read_from_cache_file
from airflow_breeze.utils.host_info_utils import get_host_group_id, get_host_os, get_host_user_id
from airflow_breeze.utils.path_utils import (
    AIRFLOW_ROOT_PATH,
    SCRIPTS_DOCKER_PATH,
    cleanup_python_generated_files,
    create_mypy_volume_if_needed,
)
from airflow_breeze.utils.shared_options import get_verbose
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE, CHEATSHEET, CHEATSHEET_STYLE

try:
    from packaging import version
except ImportError:
    # We handle the ImportError so that autocomplete works with just click installed
    version = None  # type: ignore[assignment]

from airflow_breeze.global_constants import (
    ALLOWED_CELERY_BROKERS,
    ALLOWED_DEBIAN_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    DOCKER_DEFAULT_PLATFORM,
    MIN_DOCKER_COMPOSE_VERSION,
    MIN_DOCKER_VERSION,
)
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.run_utils import (
    RunCommandResult,
    check_if_buildx_plugin_installed,
    run_command,
)

if TYPE_CHECKING:
    from airflow_breeze.params.common_build_params import CommonBuildParams
    from airflow_breeze.params.shell_params import ShellParams

# Those are volumes that are mounted when MOUNT_SELECTED is chosen (which is the default when
# entering Breeze. MOUNT_SELECTED prevents to mount the files that you can have accidentally added
# in your sources (or they were added automatically by pyproject.toml) to be mounted to container.
# This is important to get a "clean" environment for different python versions and to avoid
# unnecessary slow-downs when you are mounting files on MacOS (which has very slow filesystem)
# Any time you add a top-level folder in airflow that should also be added to container you should
# add it here.
VOLUMES_FOR_SELECTED_MOUNTS = [
    (".bash_aliases", "/root/.bash_aliases"),
    (".bash_history", "/root/.bash_history"),
    (".build", "/opt/airflow/.build"),
    (".dockerignore", "/opt/airflow/.dockerignore"),
    (".github", "/opt/airflow/.github"),
    (".inputrc", "/root/.inputrc"),
    (".rat-excludes", "/opt/airflow/.rat-excludes"),
    ("LICENSE", "/opt/airflow/LICENSE"),
    ("RELEASE_NOTES.rst", "/opt/airflow/RELEASE_NOTES.rst"),
    ("airflow-core", "/opt/airflow/airflow-core"),
    ("airflow-ctl", "/opt/airflow/airflow-ctl"),
    ("chart", "/opt/airflow/chart"),
    ("clients", "/opt/airflow/clients"),
    ("constraints", "/opt/airflow/constraints"),
    ("dags", "/opt/airflow/dags"),
    ("dev", "/opt/airflow/dev"),
    ("devel-common", "/opt/airflow/devel-common"),
    ("docker-stack-docs", "/opt/airflow/docker-stack-docs"),
    ("docker-tests", "/opt/airflow/docker-tests"),
    ("docs", "/opt/airflow/docs"),
    ("generated", "/opt/airflow/generated"),
    ("helm-tests", "/opt/airflow/helm-tests"),
    ("kubernetes-tests", "/opt/airflow/kubernetes-tests"),
    ("logs", "/root/airflow/logs"),
    ("providers", "/opt/airflow/providers"),
    ("providers-summary-docs", "/opt/airflow/providers-summary-docs"),
    ("pyproject.toml", "/opt/airflow/pyproject.toml"),
    ("scripts", "/opt/airflow/scripts"),
    ("scripts/docker/entrypoint_ci.sh", "/entrypoint"),
    ("shared", "/opt/airflow/shared"),
    ("task-sdk", "/opt/airflow/task-sdk"),
]


def check_docker_resources(airflow_image_name: str) -> RunCommandResult:
    """
    Check if we have enough resources to run docker. This is done via running script embedded in our image.


    :param airflow_image_name: name of the airflow image to use
    """
    return run_command(
        cmd=[
            "docker",
            "run",
            "-t",
            "--entrypoint",
            "/bin/bash",
            "-e",
            "PYTHONDONTWRITEBYTECODE=true",
            airflow_image_name,
            "-c",
            "python /opt/airflow/scripts/in_container/run_resource_check.py",
        ],
        text=True,
    )


def check_docker_permission_denied() -> bool:
    """
    Checks if we have permission to write to docker socket. By default, on Linux you need to add your user
    to docker group and some new users do not realize that. We help those users if we have
    permission to run docker commands.


    :return: True if permission is denied
    """
    permission_denied = False
    docker_permission_command = ["docker", "info"]
    command_result = run_command(
        docker_permission_command,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    if command_result.returncode != 0:
        permission_denied = True
        if command_result.stdout and "Got permission denied while trying to connect" in command_result.stdout:
            get_console().print(
                "ERROR: You have `permission denied` error when trying to communicate with docker."
            )
            get_console().print(
                "Most likely you need to add your user to `docker` group: \
                https://docs.docker.com/ engine/install/linux-postinstall/ ."
            )
    return permission_denied


def compare_version(current_version: str, min_version: str) -> bool:
    return version.parse(current_version) >= version.parse(min_version)


def check_docker_is_running():
    """
    Checks if docker is running. Suppressed Dockers stdout and stderr output.

    """
    response = run_command(
        ["docker", "info"],
        no_output_dump_on_exception=True,
        text=False,
        capture_output=True,
        check=False,
    )
    if response.returncode != 0:
        get_console().print(
            "[error]Docker is not running.[/]\n[warning]Please make sure Docker is installed and running.[/]"
        )
        sys.exit(1)


def check_docker_version(quiet: bool = False):
    """
    Checks if the docker compose version is as expected. including some specific modifications done by
    some vendors such as Microsoft. They might have modified version of docker-compose/docker in their
    cloud. In case docker compose version is wrong we continue but print warning for the user.

    """
    permission_denied = check_docker_permission_denied()
    if not permission_denied:
        docker_version_command = ["docker", "version", "--format", "{{.Client.Version}}"]
        docker_version = ""
        docker_version_result = run_command(
            docker_version_command,
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            check=False,
            dry_run_override=False,
        )
        if docker_version_result.returncode == 0:
            regex = re.compile(r"^(" + version.VERSION_PATTERN + r").*$", re.VERBOSE | re.IGNORECASE)
            docker_version = re.sub(regex, r"\1", docker_version_result.stdout.strip())
        if docker_version == "":
            get_console().print(
                f"""
[warning]Your version of docker is unknown. If the scripts fail, please make sure to[/]
[warning]install docker at least: {MIN_DOCKER_VERSION} version.[/]
"""
            )
            sys.exit(1)
        else:
            good_version = compare_version(docker_version, MIN_DOCKER_VERSION)
            if good_version:
                if not quiet:
                    get_console().print(f"[success]Good version of Docker: {docker_version}.[/]")
            else:
                get_console().print(
                    f"""
[error]Your version of docker is too old: {docker_version}.\n[/]
[warning]Please upgrade to at least {MIN_DOCKER_VERSION}.\n[/]
You can find installation instructions here: https://docs.docker.com/engine/install/
"""
                )
                sys.exit(1)


def check_container_engine(quiet: bool = False):
    """Checks if the container engine is Docker or podman."""
    response = run_command(
        ["docker", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    if response.returncode != 0:
        get_console().print(
            "[error]Could not determine the container engine.[/]\n"
            "[warning]Please ensure that Docker is installed and running.[/]"
        )
        sys.exit(1)
    run_command_output = "\n".join(
        " ".join(line.split()) for line in response.stdout.strip().lower().splitlines()
    )
    podman_engine_enabled = any(
        "client: podman engine" in line or "podman" in line for line in run_command_output.splitlines()
    )
    if podman_engine_enabled:
        get_console().print(
            "[error]Podman is not yet supported as a container engine in breeze.[/]\n"
            "[warning]Please switch to Docker.[/]"
        )
        sys.exit(1)


def check_remote_ghcr_io_commands():
    """Checks if you have permissions to pull an empty image from ghcr.io.

    Unfortunately, GitHub packages treat expired login as "no-access" even on
    public repos. We need to detect that situation and suggest user to log-out
    or if they are in CI environment to re-push their PR/close or reopen the PR.
    """
    response = run_command(
        ["docker", "pull", "ghcr.io/apache/airflow-hello-world"],
        no_output_dump_on_exception=True,
        text=False,
        capture_output=True,
        check=False,
    )
    if response.returncode != 0:
        if "no such host" in response.stderr.decode("utf-8"):
            get_console().print(
                "[error]\nYou seem to be offline. This command requires access to network.[/]\n"
            )
            sys.exit(2)
        get_console().print("[error]Response:[/]\n")
        get_console().print(response.stdout.decode("utf-8"))
        get_console().print(response.stderr.decode("utf-8"))
        if os.environ.get("CI"):
            get_console().print(
                "\n[error]We are extremely sorry but you've hit the rare case that the "
                "credentials you got from GitHub Actions to run are expired, and we cannot do much.[/]"
                "\n¯\\_(ツ)_/¯\n\n"
                "[warning]You have the following options now:\n\n"
                "  * Close and reopen the Pull Request of yours\n"
                "  * Rebase or amend your commit and push your branch again\n"
                "  * Ask in the PR to re-run the failed job\n\n"
            )
            sys.exit(1)
        else:
            get_console().print(
                "[error]\nYou seem to have expired permissions on ghcr.io.[/]\n"
                "[warning]Please logout. Run this command:[/]\n\n"
                "   docker logout ghcr.io\n\n"
            )
            sys.exit(1)


def check_docker_compose_version(quiet: bool = False):
    """Checks if the docker compose version is as expected.

    This includes specific modifications done by some vendors such as Microsoft.
    They might have modified version of docker-compose/docker in their cloud. In
    the case the docker compose version is wrong, we continue but print a
    warning for the user.
    """
    version_pattern = re.compile(r"(\d+)\.(\d+)\.(\d+)")
    docker_compose_version_command = ["docker", "compose", "version"]
    try:
        docker_compose_version_result = run_command(
            docker_compose_version_command,
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            dry_run_override=False,
        )
    except Exception:
        get_console().print(
            "[error]You either do not have docker-composer or have docker-compose v1 installed.[/]\n"
            "[warning]Breeze does not support docker-compose v1 any more as it has been replaced by v2.[/]\n"
            "Follow https://docs.docker.com/compose/migrate/ to migrate to v2"
        )
        sys.exit(1)
    if docker_compose_version_result.returncode == 0:
        docker_compose_version = docker_compose_version_result.stdout
        version_extracted = version_pattern.search(docker_compose_version)
        if version_extracted is not None:
            docker_compose_version = ".".join(version_extracted.groups())
            good_version = compare_version(docker_compose_version, MIN_DOCKER_COMPOSE_VERSION)
            if good_version:
                if not quiet:
                    get_console().print(
                        f"[success]Good version of docker-compose: {docker_compose_version}[/]"
                    )
            else:
                get_console().print(
                    f"""
[error]You have too old version of docker-compose: {docker_compose_version}!\n[/]
[warning]At least {MIN_DOCKER_COMPOSE_VERSION} needed! Please upgrade!\n[/]
See https://docs.docker.com/compose/install/ for installation instructions.\n
Make sure docker-compose you install is first on the PATH variable of yours.\n
"""
                )
                sys.exit(1)
    else:
        get_console().print(
            f"""
[error]Unknown docker-compose version.[/]
[warning]At least {MIN_DOCKER_COMPOSE_VERSION} needed! Please upgrade!\n[/]
See https://docs.docker.com/compose/install/ for installation instructions.\n
Make sure docker-compose you install is first on the PATH variable of yours.\n
"""
        )
        sys.exit(1)


def prepare_docker_build_cache_command(
    image_params: CommonBuildParams,
) -> list[str]:
    """
    Constructs docker build_cache command based on the parameters passed.
    :param image_params: parameters of the image


    :return: Command to run as list of string
    """
    final_command = []
    final_command.extend(["docker"])
    final_command.extend(
        ["buildx", "build", "--builder", get_and_use_docker_context(image_params.builder), "--progress=auto"]
    )
    final_command.extend(image_params.common_docker_build_flags)
    final_command.extend(["--pull"])
    final_command.extend(image_params.prepare_arguments_for_docker_build_command())
    final_command.extend(["--target", "main", "."])
    final_command.extend(
        ["-f", "Dockerfile" if isinstance(image_params, BuildProdParams) else "Dockerfile.ci"]
    )
    final_command.extend(["--platform", image_params.platform])
    final_command.extend(
        [f"--cache-to=type=registry,ref={image_params.get_cache(image_params.platform)},mode=max"]
    )
    return final_command


def prepare_base_build_command(image_params: CommonBuildParams) -> list[str]:
    """
    Prepare build command for docker build. Depending on whether we have buildx plugin installed or not,
    and whether we run cache preparation, there might be different results:

    * if buildx plugin is installed - `docker buildx` command is returned - using regular or cache builder
      depending on whether we build regular image or cache
    * if no buildx plugin is installed, and we do not prepare cache, regular docker `build` command is used.
    * if no buildx plugin is installed, and we prepare cache - we fail. Cache can only be done with buildx
    :param image_params: parameters of the image

    :return: command to use as docker build command
    """
    build_command_param = []
    is_buildx_available = check_if_buildx_plugin_installed()
    if is_buildx_available:
        build_command_param.extend(
            [
                "buildx",
                "build",
                "--push" if image_params.push else "--load",
            ]
        )
        if not image_params.docker_host:
            builder = get_and_use_docker_context(image_params.builder)
            build_command_param.extend(
                [
                    "--builder",
                    builder,
                ]
            )
            if builder != "default":
                build_command_param.append("--load")
    else:
        build_command_param.append("build")
    return build_command_param


def prepare_docker_build_command(
    image_params: CommonBuildParams,
) -> list[str]:
    """
    Constructs docker build command based on the parameters passed.
    :param image_params: parameters of the image

    :return: Command to run as list of string
    """
    build_command = prepare_base_build_command(
        image_params=image_params,
    )
    final_command = []
    final_command.extend(["docker"])
    final_command.extend(build_command)
    final_command.extend(image_params.common_docker_build_flags)
    final_command.extend(["--pull"])
    final_command.extend(image_params.prepare_arguments_for_docker_build_command())
    final_command.extend(["-t", image_params.airflow_image_name, "--target", "main", "."])
    final_command.extend(
        ["-f", "Dockerfile" if isinstance(image_params, BuildProdParams) else "Dockerfile.ci"]
    )
    final_command.extend(["--platform", image_params.platform])
    return final_command


def construct_docker_push_command(
    image_params: CommonBuildParams,
) -> list[str]:
    """
    Constructs docker push command based on the parameters passed.
    :param image_params: parameters of the image
    :return: Command to run as list of string
    """
    return ["docker", "push", image_params.airflow_image_name]


def build_cache(image_params: CommonBuildParams, output: Output | None) -> RunCommandResult:
    build_command_result: RunCommandResult = CompletedProcess(args=[], returncode=0)
    for platform in image_params.platforms:
        platform_image_params = copy.deepcopy(image_params)
        # override the platform in the copied params to only be single platform per run
        # as a workaround to https://github.com/docker/buildx/issues/1044
        platform_image_params.platform = platform
        cmd = prepare_docker_build_cache_command(image_params=platform_image_params)
        build_command_result = run_command(
            cmd,
            cwd=AIRFLOW_ROOT_PATH,
            output=output,
            check=False,
            text=True,
        )
        if build_command_result.returncode != 0:
            break
    return build_command_result


def make_sure_builder_configured(params: CommonBuildParams):
    if params.builder != "autodetect":
        cmd = ["docker", "buildx", "inspect", params.builder]
        buildx_command_result = run_command(cmd, text=True, check=False, dry_run_override=False)
        if buildx_command_result and buildx_command_result.returncode != 0:
            next_cmd = ["docker", "buildx", "create", "--name", params.builder]
            run_command(next_cmd, text=True, check=False, dry_run_override=False)


def set_value_to_default_if_not_set(env: dict[str, str], name: str, default: str):
    """Set value of name parameter to default (indexed by name) if not set.

    :param env: dictionary where to set the parameter
    :param name: name of parameter
    :param default: default value
    """
    if env.get(name) is None:
        env[name] = os.environ.get(name, default)


def prepare_broker_url(params, env_variables):
    """Prepare broker url for celery executor"""
    urls = env_variables["CELERY_BROKER_URLS"].split(",")
    url_map = {
        ALLOWED_CELERY_BROKERS[0]: urls[0],
        ALLOWED_CELERY_BROKERS[1]: urls[1],
    }
    if getattr(params, "celery_broker", None) and params.celery_broker in params.celery_broker in url_map:
        env_variables["AIRFLOW__CELERY__BROKER_URL"] = url_map[params.celery_broker]


def check_executable_entrypoint_permissions(quiet: bool = False):
    """
    Checks if the user has executable permissions on the entrypoints in checked-out airflow repository..
    """
    for entrypoint in SCRIPTS_DOCKER_PATH.glob("entrypoint*.sh"):
        if get_verbose() and not quiet:
            get_console().print(f"[info]Checking executable permissions on {entrypoint.as_posix()}[/]")
        if not os.access(entrypoint.as_posix(), os.X_OK):
            get_console().print(
                f"[error]You do not have executable permissions on {entrypoint}[/]\n"
                f"You likely checked out airflow repo on a filesystem that does not support executable "
                f"permissions (for example on a Windows filesystem that is mapped to Linux VM). Airflow "
                f"repository should only be checked out on a filesystem that is POSIX compliant."
            )
            sys.exit(1)
    if not quiet:
        get_console().print("[success]Executable permissions on entrypoints are OK[/]")


@lru_cache
def perform_environment_checks(quiet: bool = False):
    check_docker_is_running()
    check_container_engine(quiet)
    check_docker_version(quiet)
    check_docker_compose_version(quiet)
    check_executable_entrypoint_permissions(quiet)


def get_docker_syntax_version() -> str:
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    return (AIRFLOW_ROOT_PATH / "Dockerfile").read_text().splitlines()[0]


def warm_up_docker_builder(image_params_list: list[CommonBuildParams]):
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    platforms: set[str] = set()
    for image_params in image_params_list:
        platforms.add(image_params.platform)
    get_console().print(f"[info]Warming up the builder for platforms: {platforms}")
    for platform in platforms:
        docker_context = get_and_use_docker_context(image_params.builder)
        if docker_context == "default":
            return
        docker_syntax = get_docker_syntax_version()
        get_console().print(f"[info]Warming up the {docker_context} builder for syntax: {docker_syntax}")
        warm_up_image_param = copy.deepcopy(image_params_list[0])
        warm_up_image_param.push = False
        warm_up_image_param.platform = platform
        build_command = prepare_base_build_command(image_params=warm_up_image_param)
        warm_up_command = []
        warm_up_command.extend(["docker"])
        warm_up_command.extend(build_command)
        warm_up_command.extend(["--platform", platform, "-"])
        warm_up_command_result = run_command(
            warm_up_command,
            input=f"""{docker_syntax}
    FROM scratch
    LABEL description="test warmup image"
    """,
            cwd=AIRFLOW_ROOT_PATH,
            text=True,
            check=False,
        )
        if warm_up_command_result.returncode != 0:
            get_console().print(
                f"[warning]Warning {warm_up_command_result.returncode} when warming up builder:"
                f" {warm_up_command_result.stdout} {warm_up_command_result.stderr}"
            )


OWNERSHIP_CLEANUP_DOCKER_TAG = (
    f"python:{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}-slim-{ALLOWED_DEBIAN_VERSIONS[0]}"
)


def fix_ownership_using_docker(quiet: bool = True):
    if get_host_os() != "linux":
        # no need to even attempt fixing ownership on MacOS/Windows
        return
    cmd = [
        "docker",
        "run",
        "-v",
        f"{AIRFLOW_ROOT_PATH}:/opt/airflow/",
        "-e",
        f"HOST_OS={get_host_os()}",
        "-e",
        f"HOST_USER_ID={get_host_user_id()}",
        "-e",
        f"HOST_GROUP_ID={get_host_group_id()}",
        "-e",
        f"VERBOSE={str(get_verbose()).lower()}",
        "-e",
        f"DOCKER_IS_ROOTLESS={is_docker_rootless()}",
        "--rm",
        "-t",
        OWNERSHIP_CLEANUP_DOCKER_TAG,
        "/opt/airflow/scripts/in_container/run_fix_ownership.py",
    ]
    run_command(cmd, text=True, check=False, quiet=quiet)


def remove_docker_networks(networks: list[str] | None = None) -> None:
    """
    Removes specified docker networks. If no networks are specified, it removes all networks created by breeze.
    Any network with label "com.docker.compose.project=breeze" are removed when no networks are specified.
    Errors are ignored (not even printed in the output), so you can safely call it without checking
    if the networks exist.

    :param networks: list of networks to remove
    """
    if networks is None:
        run_command(
            ["docker", "network", "prune", "-f", "-a", "--filter", "label=com.docker.compose.project=breeze"],
            check=False,
            stderr=DEVNULL,
            quiet=True,
        )
    else:
        for network in networks:
            run_command(
                ["docker", "network", "rm", network],
                check=False,
                stderr=DEVNULL,
                quiet=True,
            )


def remove_docker_volumes(volumes: list[str] | None = None) -> None:
    """
    Removes specified docker volumes. If no volumes are specified, it removes all volumes created by breeze.
    Any volume with label "com.docker.compose.project=breeze" are removed when no volumes are specified.
    Errors are ignored (not even printed in the output), so you can safely call it without checking
    if the volumes exist.

    :param volumes: list of volumes to remove
    """
    if volumes is None:
        run_command(
            ["docker", "volume", "prune", "-f", "-a", "--filter", "label=com.docker.compose.project=breeze"],
            check=False,
            stderr=DEVNULL,
        )
    else:
        for volume in volumes:
            run_command(
                ["docker", "volume", "rm", volume],
                check=False,
                stderr=DEVNULL,
            )


# When you are using Docker Desktop (specifically on MacOS). the preferred context is "desktop-linux"
# because the docker socket to use is created in the .docker/ directory in the user's home directory
# and it does not require the user to belong to the "docker" group.
# The "rancher-desktop" context is the preferred context for the Rancher dockerd (moby) Container Engine.
# The "default" context is the traditional one that requires "/var/run/docker.sock" to be writeable by the
# user running the docker command.
PREFERRED_CONTEXTS = ["orbstack", "desktop-linux", "rancher-desktop", "default"]


def autodetect_docker_context():
    """
    Auto-detects which docker context to use.

    :return: name of the docker context to use
    """
    result = run_command(
        ["docker", "context", "ls", "--format=json"],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        get_console().print("[warning]Could not detect docker builder. Using default.[/]")
        return "default"
    try:
        context_dicts = json.loads(result.stdout)
        if isinstance(context_dicts, dict):
            context_dicts = [context_dicts]
    except json.decoder.JSONDecodeError:
        context_dicts = (json.loads(line) for line in result.stdout.splitlines() if line.strip())
    known_contexts = {info["Name"]: info for info in context_dicts}
    if not known_contexts:
        get_console().print("[warning]Could not detect docker builder. Using default.[/]")
        return "default"
    for preferred_context_name in PREFERRED_CONTEXTS:
        try:
            context = known_contexts[preferred_context_name]
        except KeyError:
            continue
        # On Windows, some contexts are used for WSL2. We don't want to use those.
        if context["DockerEndpoint"] == "npipe:////./pipe/dockerDesktopLinuxEngine":
            continue
        get_console().print(f"[info]Using {preferred_context_name!r} as context.[/]")
        return preferred_context_name
    fallback_context = next(iter(known_contexts))
    get_console().print(
        f"[warning]Could not use any of the preferred docker contexts {PREFERRED_CONTEXTS}.\n"
        f"Using {fallback_context} as context.[/]"
    )
    return fallback_context


def get_and_use_docker_context(context: str):
    if context == "autodetect":
        context = autodetect_docker_context()
    run_command(["docker", "context", "create", context], check=False)
    output = run_command(["docker", "context", "use", context], check=False, stdout=DEVNULL, stderr=DEVNULL)
    if output.returncode:
        get_console().print(f"[warning]Could no use context {context!r}. Continuing with current context[/]")
    return context


def get_docker_build_env(image_params: CommonBuildParams) -> dict[str, str]:
    env = os.environ.copy()
    env["DOCKER_BUILDKIT"] = "1"
    if image_params.docker_host:
        env["DOCKER_HOST"] = image_params.docker_host
    return env


def bring_compose_project_down(preserve_volumes: bool, shell_params: ShellParams):
    down_command_to_execute = ["docker", "compose"]
    if shell_params.project_name:
        down_command_to_execute.extend(["--project-name", shell_params.project_name])
    down_command_to_execute.extend(["down", "--remove-orphans"])
    if not preserve_volumes:
        down_command_to_execute.append("--volumes")
    run_command(
        down_command_to_execute,
        text=True,
        check=False,
        capture_output=shell_params.quiet,
        env=shell_params.env_variables_for_docker_commands,
    )


def execute_command_in_shell(
    shell_params: ShellParams,
    project_name: str,
    command: str | None = None,
    output: Output | None = None,
    signal_error: bool = True,
    preserve_backend: bool = False,
) -> RunCommandResult:
    """Executes command in shell.

    When you want to execute a script/bash command inside the CI container and want to use `enter_shell`
    for this purpose, the helper methods sets the following parameters of shell_params:

    * backend - to force sqlite backend (unless preserve_backend=True)
    * clean_sql_db=True - to clean the sqlite DB
    * forward_ports=False - to avoid forwarding ports from the container to the host - again that will
      allow to avoid clashes with other commands and opened breeze shell
    * project_name - to avoid name clashes with default "breeze" project name used
    * quiet=True - avoid displaying all "interactive" parts of Breeze: ASCIIART, CHEATSHEET, some diagnostics
    * skip_environment_initialization - to avoid initializing interactive environment
    * skip_image_upgrade_check - to avoid checking if the image is up to date

    if command is passed as parameter, extra_args - to pass the command to execute in the shell

    :param shell_params: shell parameters to use
    :param project_name: Name of the project to use. This avoids name clashes with default 'breeze"
        project name used - this way you will be able to run the command in parallel to regular
        "breeze" shell opened in parallel
    :param command: command to execute in the shell
    :param output: output configuration
    :param signal_error: whether to signal error
    :param preserve_backend: if True, preserve the backend specified in shell_params instead of forcing sqlite
    """
    if not preserve_backend:
        shell_params.backend = "sqlite"
    shell_params.forward_ports = False
    shell_params.project_name = project_name
    shell_params.quiet = True
    shell_params.skip_environment_initialization = True
    shell_params.skip_image_upgrade_check = True
    if get_verbose():
        if not preserve_backend:
            get_console().print("[warning]Sqlite DB is cleaned[/]")
        else:
            get_console().print(f"[info]Using backend: {shell_params.backend}[/]")
        get_console().print("[warning]Disabled port forwarding[/]")
        get_console().print(f"[warning]Project name set to: {project_name}[/]")
        get_console().print("[warning]Forced quiet mode[/]")
        get_console().print("[warning]Forced skipping environment initialization[/]")
        get_console().print("[warning]Forced skipping upgrade check[/]")
    if command:
        shell_params.extra_args = (command,)
        if get_verbose():
            get_console().print(f"[info]Command to execute: '{command}'[/]")
    return enter_shell(shell_params, output=output, signal_error=signal_error)


def enter_shell(
    shell_params: ShellParams, output: Output | None = None, signal_error: bool = True
) -> RunCommandResult:
    """
    Executes entering shell using the parameters passed as kwargs:

    * checks if docker version is good
    * checks if docker-compose version is good
    * updates kwargs with cached parameters
    * displays ASCIIART and CHEATSHEET unless disabled
    * build ShellParams from the updated kwargs
    * shuts down existing project
    * executes the command to drop the user to Breeze shell
    """
    perform_environment_checks(quiet=shell_params.quiet)
    fix_ownership_using_docker(quiet=shell_params.quiet)
    cleanup_python_generated_files()
    if read_from_cache_file("suppress_asciiart") is None and not shell_params.quiet:
        get_console().print(ASCIIART, style=ASCIIART_STYLE)
    if read_from_cache_file("suppress_cheatsheet") is None and not shell_params.quiet:
        get_console().print(CHEATSHEET, style=CHEATSHEET_STYLE)
    if shell_params.use_airflow_version:
        # in case you use specific version of Airflow, you want to bring airflow down automatically before
        # using it. This prevents the problem that if you have newer DB, airflow will not know how
        # to migrate to it and fail with "Can't locate revision identified by 'xxxx'".
        get_console().print(
            f"[warning]Bringing the project down as {shell_params.use_airflow_version} "
            f"airflow version is used[/]"
        )
        bring_compose_project_down(preserve_volumes=False, shell_params=shell_params)

    if shell_params.restart:
        bring_compose_project_down(preserve_volumes=False, shell_params=shell_params)
    if shell_params.include_mypy_volume:
        create_mypy_volume_if_needed()
    shell_params.print_badge_info()
    cmd = ["docker", "compose"]
    if shell_params.quiet:
        cmd.extend(["--progress", "quiet"])
    if shell_params.project_name:
        cmd.extend(["--project-name", shell_params.project_name])
    cmd.extend(["run", "--service-ports", "--rm"])
    if shell_params.tty == "disabled":
        cmd.append("--no-TTY")
    cmd.append("airflow")
    cmd_added = shell_params.command_passed
    if cmd_added is not None:
        cmd.extend(["-c", cmd_added])
    if "arm64" in DOCKER_DEFAULT_PLATFORM:
        if shell_params.backend == "mysql":
            get_console().print("\n[warn]MySQL use MariaDB client binaries on ARM architecture.[/]\n")

    if "openlineage" in shell_params.integration or "all" in shell_params.integration:
        if shell_params.backend != "postgres" or shell_params.postgres_version not in ["12", "13", "14"]:
            get_console().print(
                "\n[error]Only PostgreSQL 12, 13, and 14 are supported "
                "as a backend with OpenLineage integration via Breeze[/]\n"
            )
            sys.exit(1)

    command_result = run_command(
        cmd,
        text=True,
        check=False,
        env=shell_params.env_variables_for_docker_commands,
        output=output,
        output_outside_the_group=True,
    )
    if command_result.returncode == 0:
        return command_result
    if signal_error:
        get_console().print(f"[red]Error {command_result.returncode} returned[/]")
    if get_verbose():
        get_console().print(command_result.stderr)
    notify_on_unhealthy_backend_container(shell_params.project_name, shell_params.backend, output)
    return command_result


def notify_on_unhealthy_backend_container(project_name: str, backend: str, output: Output | None = None):
    """Put emphasis on unhealthy backend container and `breeze down` command for user."""
    if backend not in ["postgres", "mysql"] or os.environ.get("CI") == "true":
        return

    if _is_backend_container_unhealthy(project_name, backend):
        get_console(output=output).print(
            "[warning]The backend container is unhealthy. You might need to run `down` "
            "command to clean up:\n\n"
            "\tbreeze down[/]\n"
        )


def _is_backend_container_unhealthy(project_name: str, backend: str) -> bool:
    try:
        filter = f"name={project_name}-{backend}"
        search_response = run_command(
            ["docker", "ps", "--filter", filter, "--format={{.Names}}"],
            capture_output=True,
            check=False,
            text=True,
        )
        container_name = search_response.stdout.strip()

        # Skip the check if not found or multiple containers found
        if len(container_name.strip().splitlines()) != 1:
            return False

        inspect_response = run_command(
            ["docker", "inspect", "--format={{.State.Health.Status}}", container_name],
            capture_output=True,
            check=False,
            text=True,
        )
        if inspect_response.returncode == 0:
            return inspect_response.stdout.strip() == "unhealthy"
    # We don't want to misguide the user, so in case of any error we skip the check
    except Exception:
        pass

    return False


def is_docker_rootless() -> bool:
    try:
        response = run_command(
            ["docker", "info", "-f", "{{println .SecurityOptions}}"],
            capture_output=True,
            check=False,
            text=True,
            quiet=True,
        )
        if response.returncode == 0 and "rootless" in response.stdout.strip():
            get_console().print("[info]Docker is running in rootless mode.[/]\n")
            return True
    except FileNotFoundError:
        # we ignore if docker is missing
        pass
    return False


def check_airflow_cache_builder_configured():
    result_inspect_builder = run_command(["docker", "buildx", "inspect", "airflow_cache"], check=False)
    if result_inspect_builder.returncode != 0:
        get_console().print(
            "[error]Airflow Cache builder must be configured to "
            "build multi-platform images with multiple builders[/]"
        )
        get_console().print()
        get_console().print(
            "See https://github.com/apache/airflow/blob/main/dev/MANUALLY_BUILDING_IMAGES.md"
            " for instructions on setting it up."
        )
        sys.exit(1)


def check_regctl_installed():
    result_regctl = run_command(["regctl", "version"], check=False)
    if result_regctl.returncode != 0:
        get_console().print("[error]Regctl must be installed and on PATH to release the images[/]")
        get_console().print()
        get_console().print(
            "See https://github.com/regclient/regclient/blob/main/docs/regctl.md for installation info."
        )
        sys.exit(1)


def check_docker_buildx_plugin():
    result_docker_buildx = run_command(
        ["docker", "buildx", "version"],
        check=False,
        text=True,
        capture_output=True,
    )
    if result_docker_buildx.returncode != 0:
        get_console().print("[error]Docker buildx plugin must be installed to release the images[/]")
        get_console().print()
        get_console().print("See https://docs.docker.com/buildx/working-with-buildx/ for installation info.")
        sys.exit(1)
    from packaging.version import Version

    version = result_docker_buildx.stdout.splitlines()[0].split(" ")[1].lstrip("v")
    packaging_version = Version(version)
    if packaging_version < Version("0.13.0"):
        get_console().print("[error]Docker buildx plugin must be at least 0.13.0 to release the images[/]")
        get_console().print()
        get_console().print(
            "See https://github.com/docker/buildx?tab=readme-ov-file#installing for installation info."
        )
        sys.exit(1)
    else:
        get_console().print(f"[success]Docker buildx plugin is installed and in good version: {version}[/]")
