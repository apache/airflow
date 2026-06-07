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
import time
from typing import Any

from airflow.api_fastapi.auth.dag_processor_token import provision_dag_processor_token_file
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job
from airflow.utils import cli as cli_utils
from airflow.utils.memray_utils import MemrayTraceComponents, enable_memray_trace
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


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


def _run_dag_processor_job(job_runner: DagProcessorJobRunner) -> None:
    """
    Run the job, registering the liveness Job through the DAG Processing API.

    The DAG processor holds no metadata-DB connection, so its Job row is registered,
    heartbeated, and completed through the DAG Processing API instead of writing the
    database directly. The first call waits for the API server to be ready (the processor and
    server may start concurrently), the heartbeat is throttled to the job heartrate and never
    crashes the loop, and a failure to complete the Job does not mask a parsing-loop error.
    """
    client = job_runner.processor._dag_processing_client
    client.wait_until_ready()
    job_id = client.register_job(job_runner.job_type)

    heartrate = getattr(job_runner.job, "heartrate", 5.0) or 5.0
    last_heartbeat = 0.0

    def _heartbeat() -> None:
        nonlocal last_heartbeat
        now = time.monotonic()
        if now - last_heartbeat < heartrate:
            return
        try:
            client.job_heartbeat(job_id)
            last_heartbeat = now
        except Exception:
            # Liveness update failure must not crash the processor; the next loop retries.
            log.warning("DAG Processing API heartbeat failed; retrying next loop", exc_info=True)

    job_runner.processor.heartbeat = _heartbeat
    state = "success"
    try:
        job_runner._execute()
    except SystemExit:
        pass
    except Exception:
        state = "failed"
        raise
    finally:
        try:
            client.complete_job(job_id, state=state)
        except Exception:
            # Don't let a completion failure mask the original parsing-loop exception.
            log.warning("Failed to mark DAG processor Job %s as %s", job_id, state, exc_info=True)


@cli_utils.action_cli
@providers_configuration_loaded
def provision_dag_processor_token(args):
    """
    Mint the DAG processor's API token and write it to a file (trusted bootstrap step).

    Run by a trusted component (a deployment init step, not the processor itself), which holds the
    signing key. Writes to ``--output`` or, by default, ``[dag_processor] api_token_path``. The DAG
    processor then only reads that file. Re-run before ``[dag_processor] jwt_expiration_time``
    elapses to rotate the token in place.
    """
    path = provision_dag_processor_token_file(getattr(args, "output", None))
    print(f"DAG processor API token written to {path}")


@enable_memray_trace(component=MemrayTraceComponents.dag_processor)
@cli_utils.action_cli
@providers_configuration_loaded
def dag_processor(args):
    """Start Airflow Dag Processor Job."""
    job_runner = _create_dag_processor_job_runner(args)

    if cli_utils.should_enable_hot_reload(args):
        from airflow.cli.hot_reload import run_with_reloader

        run_with_reloader(
            lambda: _run_dag_processor_job(job_runner),
            process_name="dag-processor",
        )
        return

    run_command_with_daemon_option(
        args=args,
        process_name="dag-processor",
        callback=lambda: _run_dag_processor_job(job_runner),
        should_setup_logging=True,
    )
