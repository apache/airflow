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
"""In-sandbox entrypoint that executes an Airflow 3 task workload.

The ``SandboxExecutor`` ships the serialized ``ExecuteTask`` workload into the
sandbox (base64 in ``AIRFLOW_SANDBOX_WORKLOAD``, or a file path argument) and
runs ``python -m airflow.providers.sandbox.execution_time.run_workload``. Inside
the sandbox this reconstructs the workload and hands it to the Task SDK
``supervise()`` — the exact mechanism Celery/Edge executors use — which talks to
the Execution API server over HTTPS using the workload's short-lived JWT and
streams logs to remote storage.

This needs the sandbox image to have ``apache-airflow`` (Task SDK) installed and
network reach to the Execution API server.
"""

from __future__ import annotations

import base64
import os
import sys


def _load_raw() -> str:
    env = os.environ.get("AIRFLOW_SANDBOX_WORKLOAD")
    if env:
        return base64.b64decode(env).decode()
    if len(sys.argv) > 1:
        with open(sys.argv[1], encoding="utf-8") as fh:
            return fh.read()
    raise SystemExit("no workload: set AIRFLOW_SANDBOX_WORKLOAD or pass a file path")


def main() -> int:
    from airflow.executors import workloads
    from airflow.sdk.execution_time.supervisor import supervise

    workload = workloads.ExecuteTask.model_validate_json(_load_raw())

    server = os.environ.get("AIRFLOW__CORE__EXECUTION_API_SERVER_URL")
    if not server:
        try:
            from airflow.configuration import conf

            server = conf.get("core", "execution_api_server_url", fallback=None)
        except Exception:
            server = None

    return supervise(
        ti=workload.ti,
        bundle_info=workload.bundle_info,
        dag_rel_path=workload.dag_rel_path,
        token=workload.token,
        server=server,
        log_path=workload.log_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
