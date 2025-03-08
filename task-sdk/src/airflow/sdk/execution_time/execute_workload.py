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

import structlog

log = structlog.get_logger(logger_name=__name__)


def execute_workload(input: str) -> None:
    from pydantic import TypeAdapter

    from airflow.configuration import conf
    from airflow.executors import workloads
    from airflow.sdk.execution_time.supervisor import supervise
    from airflow.sdk.log import configure_logging

    configure_logging(output=sys.stdout.buffer)

    decoder = TypeAdapter[workloads.All](workloads.All)
    workload = decoder.validate_json(input)

    if not isinstance(workload, workloads.ExecuteTask):
        raise ValueError(f"KubernetesExecutor does not know how to handle {type(workload)}")

    log.info("Executing workload in Kubernetes", workload=workload)

    supervise(
        # This is the "wrong" ti type, but it duck types the same. TODO: Create a protocol for this.
        ti=workload.ti,  # type: ignore[arg-type]
        dag_rel_path=workload.dag_rel_path,
        bundle_info=workload.bundle_info,
        token=workload.token,
        server=conf.get("core", "execution_api_server_url"),
        log_path=workload.log_path,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Execute a workload in a Containerised executor using the task SDK."
    )
    parser.add_argument(
        "input_file", help="Path to the input JSON file containing the execution workload payload."
    )

    args = parser.parse_args()

    with open(args.input_file) as file:
        input_data = file.read()

    execute_workload(input_data)


if __name__ == "__main__":
    main()
