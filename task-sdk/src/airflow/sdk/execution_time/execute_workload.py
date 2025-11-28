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

"""
Module for executing an Airflow task using the workload json provided by a input file.

Usage:
    python execute_workload.py <input_file>

Arguments:
    input_file (str): Path to the JSON file containing the workload definition.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from airflow.executors.workloads import ExecuteTask

log = structlog.get_logger(logger_name=__name__)


def execute_workload(workload: ExecuteTask) -> None:
    from airflow.executors import workloads
    from airflow.sdk.configuration import conf
    from airflow.sdk.execution_time.supervisor import supervise
    from airflow.sdk.log import configure_logging
    from airflow.settings import dispose_orm

    dispose_orm(do_log=False)

    configure_logging(output=sys.stdout.buffer, json_output=True)

    if not isinstance(workload, workloads.ExecuteTask):
        raise ValueError(f"Executor does not know how to handle {type(workload)}")

    log.info("Executing workload", workload=workload)

    base_url = conf.get("api", "base_url", fallback="/")
    # If it's a relative URL, use localhost:8080 as the default
    if base_url.startswith("/"):
        base_url = f"http://localhost:8080{base_url}"
    default_execution_api_server = f"{base_url.rstrip('/')}/execution/"
    server = conf.get("core", "execution_api_server_url", fallback=default_execution_api_server)
    log.info("Connecting to server:", server=server)

    supervise(
        # This is the "wrong" ti type, but it duck types the same. TODO: Create a protocol for this.
        ti=workload.ti,  # type: ignore[arg-type]
        dag_rel_path=workload.dag_rel_path,
        bundle_info=workload.bundle_info,
        token=workload.token,
        server=server,
        log_path=workload.log_path,
        sentry_integration=workload.sentry_integration,
        # Include the output of the task to stdout too, so that in process logs can be read from via the
        # kubeapi as pod logs.
        subprocess_logs_to_stdout=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Execute a workload in a Containerised executor using the task SDK."
    )

    # Create a mutually exclusive group to ensure that only one of the flags is set
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--json-path",
        help="Path to the input JSON file containing the execution workload payload.",
        type=str,
    )
    group.add_argument(
        "--json-string",
        help="The JSON string itself containing the execution workload payload.",
        type=str,
    )

    args = parser.parse_args()

    from pydantic import TypeAdapter

    from airflow.executors import workloads

    decoder = TypeAdapter[workloads.All](workloads.All)
    if args.json_path:
        try:
            with open(args.json_path) as file:
                input_data = file.read()
                workload = decoder.validate_json(input_data)
        except Exception as e:
            log.error("Failed to read file", error=str(e))
            sys.exit(1)

    elif args.json_string:
        try:
            workload = decoder.validate_json(args.json_string)
        except Exception as e:
            log.error("Failed to parse input JSON string", error=str(e))
            sys.exit(1)

    execute_workload(workload)


if __name__ == "__main__":
    main()
