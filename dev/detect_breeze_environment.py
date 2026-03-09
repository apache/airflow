#!/usr/bin/env python3
"""
Airflow Breeze Environment Detection Script

This script detects whether the current environment is running on the host
or inside a Breeze container, and provides information about the context.
"""

import os
import sys
from pathlib import Path


def is_inside_docker() -> bool:
    """Check if running inside a Docker container."""
    return os.path.isfile("/.dockerenv")


def is_breeze_container() -> bool:
    """Check if running inside a Breeze container."""
    return os.environ.get("BREEZE", "").lower() == "true"


def get_environment_context() -> dict:
    """Get the current environment context."""
    inside_docker = is_inside_docker()
    breeze_container = is_breeze_container()

    context = {
        "inside_docker": inside_docker,
        "breeze_container": breeze_container,
        "is_host": not inside_docker,
        "is_breeze": breeze_container,
        "can_run_host_commands": not inside_docker,
        "can_run_container_commands": breeze_container,
    }

    # Additional context
    context["airflow_root"] = os.environ.get("AIRFLOW_ROOT_PATH", str(Path.cwd()))
    context["python_version"] = f"{sys.version_info.major}.{sys.version_info.minor}"

    return context


def main():
    """Main entry point."""
    import json

    context = get_environment_context()

    if len(sys.argv) > 1 and sys.argv[1] == "--json":
        print(json.dumps(context, indent=2))
    else:
        print("Airflow Breeze Environment Context:")
        print(f"  Inside Docker: {context['inside_docker']}")
        print(f"  Breeze Container: {context['breeze_container']}")
        print(f"  Is Host: {context['is_host']}")
        print(f"  Is Breeze: {context['is_breeze']}")
        print(f"  Can run host commands: {context['can_run_host_commands']}")
        print(f"  Can run container commands: {context['can_run_container_commands']}")
        print(f"  Airflow Root: {context['airflow_root']}")
        print(f"  Python Version: {context['python_version']}")


if __name__ == "__main__":
    main()