from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class EnvironmentContext:
    is_wsl: bool
    inside_container: bool
    docker_available: bool
    recommended_command: str


def detect_environment_context() -> EnvironmentContext:
    inside_container = os.path.exists("/.dockerenv")
    is_wsl_env = "WSL_DISTRO_NAME" in os.environ

    if inside_container:
        recommended = "pytest"
    else:
        recommended = "breeze shell"

    return EnvironmentContext(
        is_wsl=is_wsl_env,
        inside_container=inside_container,
        docker_available=True,
        recommended_command=recommended,
    )