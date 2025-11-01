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
"""DagProcessor command."""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from airflow import settings
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.configuration import conf
from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job, run_job
from airflow.utils import cli as cli_utils
from airflow.utils.memray_utils import MemrayTraceComponents, enable_memray_trace
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


def _run_rust_processor():
    """Run the Rust DAG processor."""
    log.info("Starting experimental Rust DAG processor.")
    rust_project_dir = Path(__file__).parents[2] / "dag_processing_rust"

    # When running from a release, we'd have a binary, but for dev, we use cargo run.
    # cargo run will recompile if sources have changed.
    # We pass configuration options as command-line arguments to avoid PyO3 overhead in initialization.
    command = [
        "cargo",
        "run",
        "--manifest-path",
        str(rust_project_dir / "Cargo.toml"),
        # "--release",
        "--",
        "--processor-timeout",
        str(conf.getint("dag_processor", "dag_file_processor_timeout")),
        "--parsing-processes",
        str(conf.getint("dag_processor", "parsing_processes")),
        "--parsing-cleanup-interval",
        str(conf.getint("scheduler", "parsing_cleanup_interval")),
        "--min-file-process-interval",
        str(conf.getint("dag_processor", "min_file_process_interval")),
        "--stale-dag-threshold",
        str(conf.getint("dag_processor", "stale_dag_threshold")),
        "--print-stats-interval",
        str(conf.getint("dag_processor", "print_stats_interval")),
        "--max-callbacks-per-loop",
        str(conf.getint("dag_processor", "max_callbacks_per_loop")),
        "--base-log-dir",
        conf.get("logging", "dag_processor_child_process_log_directory"),
        "--bundle-refresh-check-interval",
        str(conf.getint("dag_processor", "bundle_refresh_check_interval")),
        "--file-parsing-sort-mode",
        conf.get("dag_processor", "file_parsing_sort_mode"),
        "--dags-folder",
        settings.DAGS_FOLDER,
    ]
    env = os.environ.copy()
    env["RUST_LOG"] = "debug"
    try:
        process = subprocess.Popen(command, env=env)
        process.wait()
    except KeyboardInterrupt:
        log.info("Terminating Rust DAG processor.")
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
    except Exception:
        log.exception("Error running Rust DAG processor.")
        raise


def _create_dag_processor_job_runner(args: Any) -> DagProcessorJobRunner:
    """Create DagFileProcessorProcess instance."""
    if args.bundle_name:
        cli_utils.validate_dag_bundle_arg(args.bundle_name)
    return DagProcessorJobRunner(
        job=Job(),
        processor=DagFileProcessorManager(
            max_runs=args.num_runs,
            bundle_names_to_parse=args.bundle_name,
        ),
    )


@enable_memray_trace(component=MemrayTraceComponents.dag_processor)
@cli_utils.action_cli
@providers_configuration_loaded
def dag_processor(args):
    """Start Airflow Dag Processor Job."""
    if conf.getboolean("dag_processor", "use_rust_dag_processor", fallback=False):
        callback = _run_rust_processor
    else:
        job_runner = _create_dag_processor_job_runner(args)
        callback = lambda: run_job(job=job_runner.job, execute_callable=job_runner._execute)

    run_command_with_daemon_option(
        args=args,
        process_name="dag-processor",
        callback=callback,
        should_setup_logging=True,
    )
