#!/usr/bin/env python3
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

"""Generate mprocs configuration dynamically based on environment variables."""

from __future__ import annotations

import os
import sys
import tempfile

import yaml
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax


def get_env_bool(var_name: str, default: str = "false") -> bool:
    """Get environment variable as boolean."""
    return os.environ.get(var_name, default).lower() == "true"


def get_env(var_name: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.environ.get(var_name, default)


def generate_mprocs_config() -> str:
    """Generate mprocs YAML configuration based on environment variables."""
    procs = {}

    # Scheduler
    scheduler_cmd = "airflow scheduler"
    if get_env_bool("BREEZE_DEBUG_SCHEDULER"):
        port = get_env("BREEZE_DEBUG_SCHEDULER_PORT", "5678")
        scheduler_cmd = f"debugpy --listen 0.0.0.0:{port} --wait-for-client -m airflow scheduler"

    procs["scheduler"] = {
        "shell": scheduler_cmd,
        "restart": "always",
        "scrollback": 100000,
    }

    # API Server or Webserver (depending on Airflow version)
    use_airflow_version = get_env("USE_AIRFLOW_VERSION", "")
    if not use_airflow_version.startswith("2."):
        # API Server (Airflow 3.x+)
        if get_env_bool("BREEZE_DEBUG_APISERVER"):
            port = get_env("BREEZE_DEBUG_APISERVER_PORT", "5679")
            api_cmd = f"debugpy --listen 0.0.0.0:{port} --wait-for-client -m airflow api-server -d"
        else:
            dev_mode = get_env_bool("DEV_MODE")
            api_cmd = "airflow api-server -d" if dev_mode else "airflow api-server"

        procs["api_server"] = {
            "shell": api_cmd,
            "restart": "always",
            "scrollback": 100000,
        }
    else:
        # Webserver (Airflow 2.x)
        if get_env_bool("BREEZE_DEBUG_WEBSERVER"):
            port = get_env("BREEZE_DEBUG_WEBSERVER_PORT", "5680")
            web_cmd = f"debugpy --listen 0.0.0.0:{port} --wait-for-client -m airflow webserver"
        else:
            dev_mode = get_env_bool("DEV_MODE")
            web_cmd = "airflow webserver -d" if dev_mode else "airflow webserver"

        procs["webserver"] = {
            "shell": web_cmd,
            "restart": "always",
            "scrollback": 100000,
        }

    # Triggerer
    triggerer_cmd = "airflow triggerer"
    if get_env_bool("BREEZE_DEBUG_TRIGGERER"):
        port = get_env("BREEZE_DEBUG_TRIGGERER_PORT", "5681")
        triggerer_cmd = f"debugpy --listen 0.0.0.0:{port} --wait-for-client -m airflow triggerer"

    procs["triggerer"] = {
        "shell": triggerer_cmd,
        "restart": "always",
        "scrollback": 100000,
    }

    # Celery Worker (conditional)
    if get_env_bool("INTEGRATION_CELERY"):
        if get_env_bool("BREEZE_DEBUG_CELERY_WORKER"):
            port = get_env("BREEZE_DEBUG_CELERY_WORKER_PORT", "5682")
            celery_cmd = f"debugpy --listen 0.0.0.0:{port} --wait-for-client -m airflow celery worker"
        else:
            celery_cmd = "airflow celery worker"

        procs["celery_worker"] = {
            "shell": celery_cmd,
            "restart": "always",
            "scrollback": 100000,
        }

    # Flower (conditional)
    if get_env_bool("INTEGRATION_CELERY") and get_env_bool("CELERY_FLOWER"):
        if get_env_bool("BREEZE_DEBUG_FLOWER"):
            port = get_env("BREEZE_DEBUG_FLOWER_PORT", "5683")
            flower_cmd = f"debugpy --listen 0.0.0.0:{port} --wait-for-client -m airflow celery flower"
        else:
            flower_cmd = "airflow celery flower"

        procs["flower"] = {
            "shell": flower_cmd,
            "restart": "always",
            "scrollback": 100000,
        }

    # Edge Worker (conditional)
    executor = get_env("AIRFLOW__CORE__EXECUTOR", "")
    if executor == "airflow.providers.edge3.executors.edge_executor.EdgeExecutor":
        if get_env_bool("BREEZE_DEBUG_EDGE"):
            port = get_env("BREEZE_DEBUG_EDGE_PORT", "5684")
            edge_cmd = f"debugpy --listen 0.0.0.0:{port} --wait-for-client -m airflow edge worker --edge-hostname breeze --queues default"
        else:
            # Build command with environment cleanup
            edge_cmd_parts = [
                "unset AIRFLOW__DATABASE__SQL_ALCHEMY_CONN || true",
                "unset AIRFLOW__CELERY__RESULT_BACKEND || true",
                "unset POSTGRES_HOST_PORT || true",
                "unset BACKEND || true",
                "unset POSTGRES_VERSION || true",
                "export AIRFLOW__LOGGING__BASE_LOG_FOLDER=edge_logs",
                "airflow edge worker --edge-hostname breeze --queues default",
            ]
            edge_cmd = " && ".join(edge_cmd_parts)

        procs["edge_worker"] = {
            "shell": edge_cmd,
            "restart": "always",
            "scrollback": 100000,
        }

    # Dag Processor (conditional)
    if get_env_bool("STANDALONE_DAG_PROCESSOR"):
        if get_env_bool("BREEZE_DEBUG_DAG_PROCESSOR"):
            port = get_env("BREEZE_DEBUG_DAG_PROCESSOR_PORT", "5685")
            dag_proc_cmd = f"debugpy --listen 0.0.0.0:{port} --wait-for-client -m airflow dag-processor"
        else:
            dag_proc_cmd = "airflow dag-processor"

        procs["dag_processor"] = {
            "shell": dag_proc_cmd,
            "restart": "always",
            "scrollback": 100000,
        }

    procs["shell"] = {
        "shell": "bash",
        "restart": "always",
        "scrollback": 100000,
    }

    # Generate YAML output
    config_dict = {"procs": procs}
    return yaml.dump(config_dict, default_flow_style=False, sort_keys=False)


def main():
    # Set LocalExecutor if not set and backend is not sqlite
    backend = get_env("BACKEND", "")
    if backend != "sqlite" and not get_env("AIRFLOW__CORE__EXECUTOR"):
        os.environ["AIRFLOW__CORE__EXECUTOR"] = "LocalExecutor"

    # Generate and print configuration
    config = generate_mprocs_config()

    # Determine output path
    if len(sys.argv) > 1:
        output_path = sys.argv[1]
    else:
        temp_dir = tempfile.gettempdir()
        output_path = os.path.join(temp_dir, "mprocs.yaml")

    with open(output_path, "w") as f:
        f.write(config)

    # Use rich console for pretty output
    console = Console()

    console.print(
        f"\n[bold green]✓[/bold green] Generated mprocs configuration at: [cyan]{output_path}[/cyan]"
    )

    # Display configuration with syntax highlighting
    syntax = Syntax(config, "yaml", theme="monokai", line_numbers=False)
    panel = Panel(
        syntax, title="[bold yellow]Configuration Preview[/bold yellow]", border_style="blue", expand=False
    )
    console.print(panel)


if __name__ == "__main__":
    main()
