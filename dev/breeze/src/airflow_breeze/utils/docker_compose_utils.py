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
"""Utilities for managing Airflow docker-compose environments in tests."""

from __future__ import annotations

import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from shutil import copyfile

import yaml
from cryptography.fernet import Fernet

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.run_utils import run_command


def setup_airflow_docker_compose_environment(
    docker_compose_source: Path,
    tmp_dir: Path | None = None,
    env_vars: dict[str, str] | None = None,
    docker_compose_modifications: Callable[[dict, Path], dict] | None = None,
) -> tuple[Path, Path]:
    """Set up a temporary directory with docker-compose files for Airflow."""
    if tmp_dir is None:
        tmp_dir = Path(tempfile.mkdtemp(prefix="airflow-docker-compose-"))

    docker_compose_path = tmp_dir / "docker-compose.yaml"
    copyfile(docker_compose_source, docker_compose_path)

    for subdir in ("dags", "logs", "plugins", "config"):
        (tmp_dir / subdir).mkdir(exist_ok=True)

    env_vars = env_vars or {}

    if "FERNET_KEY" not in env_vars:
        env_vars["FERNET_KEY"] = Fernet.generate_key().decode()

    if "AIRFLOW_UID" not in env_vars:
        env_vars["AIRFLOW_UID"] = str(os.getuid())

    dot_env_file = tmp_dir / ".env"
    env_content = "\n".join([f"{key}={value}" for key, value in env_vars.items()])
    dot_env_file.write_text(env_content + "\n")

    if docker_compose_modifications:
        with open(docker_compose_path) as f:
            compose_config = yaml.safe_load(f)
        compose_config = docker_compose_modifications(compose_config, tmp_dir)
        with open(docker_compose_path, "w") as f:
            yaml.dump(compose_config, f, default_flow_style=False)

    return tmp_dir, dot_env_file


def start_docker_compose_and_wait_for_health(
    tmp_dir: Path,
    airflow_base_url: str = "http://localhost:8080",
    max_wait: int = 180,
    check_interval: int = 5,
) -> int:
    """Start docker-compose and wait for Airflow to be healthy."""
    health_check_url = f"{airflow_base_url}/api/v2/monitor/health"

    console_print("[info]Starting Airflow services with docker-compose...[/]")
    compose_up_result = run_command(
        ["docker", "compose", "up", "-d"], cwd=tmp_dir, check=False, verbose_override=True
    )
    if compose_up_result.returncode != 0:
        console_print("[error]Failed to start docker-compose[/]")
        return compose_up_result.returncode

    console_print(f"[info]Waiting for Airflow at {health_check_url}...[/]")
    elapsed = 0
    while elapsed < max_wait:
        try:
            response = urllib.request.urlopen(health_check_url, timeout=5)
            if response.status == 200:
                console_print("[success]Airflow is ready![/]")
                return 0
        except (urllib.error.URLError, urllib.error.HTTPError, Exception):
            time.sleep(check_interval)
            elapsed += check_interval
            if elapsed % 15 == 0:
                console_print(f"[info]Still waiting... ({elapsed}s/{max_wait}s)[/]")

    console_print(f"[error]Airflow did not become ready within {max_wait} seconds[/]")
    console_print("[info]Docker compose logs:[/]")
    run_command(["docker", "compose", "logs"], cwd=tmp_dir, check=False)
    return 1


def stop_docker_compose(tmp_dir: Path, remove_volumes: bool = True) -> None:
    """Stop and cleanup docker-compose services."""
    console_print("[info]Stopping docker-compose services...[/]")
    cmd = ["docker", "compose", "down"]
    if remove_volumes:
        cmd.append("-v")
    run_command(cmd, cwd=tmp_dir, check=False)
    console_print("[success]Docker-compose cleaned up.[/]")


def ensure_image_exists_and_build_if_needed(image_name: str, python: str) -> None:
    inspect_result = run_command(
        ["docker", "inspect", image_name], check=False, capture_output=True, text=True
    )
    if inspect_result.returncode == 0:
        return

    console_print(f"[info]Image {image_name} not found locally[/]")

    # Check if it looks like a Docker Hub image (apache/airflow:*)
    if image_name.startswith("apache/airflow:"):
        console_print(f"[info]Pulling image from Docker Hub: {image_name}[/]")
        pull_result = run_command(["docker", "pull", image_name], check=False)
        if pull_result.returncode == 0:
            console_print(f"[success]Successfully pulled {image_name}[/]")
            return
        console_print(f"[warning]Failed to pull {image_name}, will try to build[/]")

    console_print(f"[info]Building image with: breeze prod-image build --python {python}[/]")
    build_result = run_command(["breeze", "prod-image", "build", "--python", python], check=False)
    if build_result.returncode != 0:
        console_print("[error]Failed to build image[/]")
        sys.exit(1)

    # After building, check if the image now exists with the expected name.
    # breeze prod-image build tags the image with the correct name, so no fallback search is needed.
    re_inspect = run_command(["docker", "inspect", image_name], check=False, capture_output=True, text=True)
    if re_inspect.returncode == 0:
        console_print(f"[success]Image {image_name} is now available[/]")
        return

    console_print(
        f"[error]Image {image_name} not found after build. "
        f"Run 'breeze prod-image build --python {python}' manually to diagnose.[/]"
    )
    sys.exit(1)
