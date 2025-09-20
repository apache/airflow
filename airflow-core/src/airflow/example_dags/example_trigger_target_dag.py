#
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
Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
1. 1st DAG (example_trigger_controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
2. 2nd DAG (example_trigger_target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG
"""

from __future__ import annotations

import pendulum
from shlex import quote

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task


@task(task_id="run_this")
def run_this_func(dag_run=None):
    """
    Print the payload "message" passed to the DagRun conf attribute.

    :param dag_run: The DagRun object
    """
    print(f"Remotely received value of {dag_run.conf.get('message')} for key=message")


with DAG(
    dag_id="example_trigger_target_dag",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag:
    run_this = run_this_func()

    @task(task_id="safe_message")
    def safe_message(dag_run=None):
        """
        Safely quote user-provided message to prevent command injection.

        Uses shlex.quote() to produce a POSIX shell-safe literal that can be
        directly embedded in bash commands without risk of code execution.
        """
        # Extract message with robust null-checking
        msg = ""
        if dag_run and getattr(dag_run, "conf", None):
            msg = dag_run.conf.get("message", "") or ""
        # Return shell-safe quoted string
        return quote(msg)

    bash_task = BashOperator(
        task_id="bash_task",
        # Use printf for predictable cross-platform behavior
        # The quoted message is safely injected as a shell argument
        bash_command=("printf 'Here is the message: %s\\n' {{ ti.xcom_pull(task_ids='safe_message') }}"),
    )

    # Set up task dependencies
    safe_message() >> bash_task
