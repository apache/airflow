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
"""Example Airflow DAG to show triggering downstream DAG from upstream DAG.

This DAG assumes Airflow Connection with connection id `teradata_sp_call` already exists in locally. It
shows how to trigger downstream DAG using TriggerDagRunOperator and how to pass parameters to downstream
DAG from upstream DAG by differing upstream DAG waiting time to trigger instead of worker which increases
Airflow's scalability and can reduce cost. Upstream DAG will pause and resume only once the downstream DAG
has finished running."""
from __future__ import annotations

from pendulum import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.teradata.operators.teradata import TeradataOperator, TeradataStoredProcedureOperator

CONN_ID = "teradata_sp_call"

with DAG(
    dag_id="example_upstream_dag_wait_for_downstream_defer",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    start_date=datetime(2023, 1, 1),
    render_template_as_native_obj=True,
) as dag:
    t1 = TeradataOperator(
        task_id="t1",
        conn_id=CONN_ID,
        sql=r"""replace procedure examplestoredproc (in p1 integer, out p2 integer)
             begin
                set p2 = p1 + p1 ;
             end ;
            """,
    )

    t2 = TeradataStoredProcedureOperator(
        task_id="t2",
        conn_id=CONN_ID,
        procedure="examplestoredproc",
        parameters=[3, int],    # Input parameter and Output parameter
    )

    example_trigger = TriggerDagRunOperator(
        task_id="upstream_dag_task",
        trigger_dag_id="example_downstream_dag", # Downstream DAG DAG_ID
        wait_for_completion=True,  # Upstream DAG will wait for downstream DAG Completion.
        allowed_states=["success", "failed"],   # Trigger upstream DAG if downstream tag either success or fail.
        conf={"input_param1": "{{ ti.xcom_pull(task_ids='t2')[0][0] }}"},    # Parameters from upstream DAG to downstream DAG
        deferrable=True  # Defers upstream DAG waiting process to the trigger instead of Worker
    )

    t1 >> t2 >> example_trigger
