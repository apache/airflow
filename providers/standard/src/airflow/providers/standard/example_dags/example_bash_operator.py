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
"""Example DAG demonstrating the usage of the BashOperator."""

from __future__ import annotations

import datetime
import json

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="example_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    dag.doc_md = """
    ### Example BashOperator DAG

    This DAG demonstrates how to use the **BashOperator** to execute shell commands
    as part of an Apache Airflow workflow.

    **What this DAG shows:**
    - Defining tasks using `BashOperator`
    - Executing simple bash commands
    - Creating task dependencies, including loops and templated commands
    - Pushing several named XComs from one task with `multiple_outputs`
    - Pushing several named XComs, resilient to task failure, via the XCom directory

    This example is intended for beginners who want to understand how Airflow
    interacts with system-level commands using bash.
    """

    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo https://airflow.apache.org/",
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id=f"runme_{i}",
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    # [START howto_operator_bash_template]
    also_run_this = BashOperator(
        task_id="also_run_this",
        bash_command='echo "ti_key={{ task_instance_key_str }}"',
    )
    # [END howto_operator_bash_template]
    also_run_this >> run_this_last

    # [START howto_operator_bash_multiple_outputs]
    describe_dag_folder = BashOperator(
        task_id="describe_dag_folder",
        # The dict must be the last line the command writes: only that line is captured.
        bash_command="""
            set -e
            dag_folder="$AIRFLOW_HOME/dags"
            file_count=$(find "$dag_folder" -type f -name '*.py' 2>/dev/null | wc -l)
            printf '{"dag_folder": "%s", "file_count": %s}\\n' "$dag_folder" "$file_count"
        """,
        multiple_outputs=True,
        output_processor=json.loads,
    )

    # Each key of the pushed dict is available as its own XCom.
    show_dag_folder_stats = BashOperator(
        task_id="show_dag_folder_stats",
        bash_command=(
            "echo \"found {{ ti.xcom_pull(task_ids='describe_dag_folder', key='file_count') }}"
            " Dag file(s) under {{ ti.xcom_pull(task_ids='describe_dag_folder', key='dag_folder') }}\""
        ),
    )
    # [END howto_operator_bash_multiple_outputs]
    describe_dag_folder >> show_dag_folder_stats >> run_this_last

    # [START howto_operator_bash_xcom_dir]
    write_multiple_xcoms = BashOperator(
        task_id="write_multiple_xcoms",
        bash_command="""
            set -e
            echo "42" > "$AIRFLOW_XCOM_DIR/row_count"
            xcom push --json summary '{"rows": 42, "errors": 0}'
        """,
    )

    read_multiple_xcoms = BashOperator(
        task_id="read_multiple_xcoms",
        bash_command=(
            "echo \"row_count={{ ti.xcom_pull(task_ids='write_multiple_xcoms', key='row_count') }}"
            " summary={{ ti.xcom_pull(task_ids='write_multiple_xcoms', key='summary') }}\""
        ),
    )
    # [END howto_operator_bash_xcom_dir]
    write_multiple_xcoms >> read_multiple_xcoms >> run_this_last

# [START howto_operator_bash_skip]
this_will_skip = BashOperator(
    task_id="this_will_skip",
    bash_command='echo "hello world"; exit 99;',
    dag=dag,
)
# [END howto_operator_bash_skip]
this_will_skip >> run_this_last
