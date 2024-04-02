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

"""Example Airflow DAG to show how values can pass from one task to another task using xcom

This DAG assumes Airflow Connection with connection id `teradata_sp_call` already exists in locally. """

from __future__ import annotations

from pendulum import datetime

from airflow import DAG
from airflow.providers.teradata.operators.teradata import TeradataOperator, TeradataStoredProcedureOperator

CONN_ID = "teradata_sp_call"

with DAG(
    dag_id="example_tasks_comm_xcom",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    start_date=datetime(2023, 1, 1),
    render_template_as_native_obj=True,
) as dag:
    create_sp_in_inout = TeradataOperator(
        task_id="create_sp_in_inout",
        conn_id=CONN_ID,
        sql=r"""replace procedure examplestoredproc (in p1 integer, out p2 integer, out p3 integer)
         begin
            set p2 = p1 + p1 ;
            set p3 = p1 * p1;
         end ;
        """,
        dag=dag,
    )

    opr_sp_in_inout = TeradataStoredProcedureOperator(
        task_id="opr_sp_in_inout",
        conn_id=CONN_ID,
        procedure="examplestoredproc",
        parameters=[3, int, int],
        dag=dag,
    )

    create_sp_two_in = TeradataOperator(
        task_id="create_sp_two_in",
        conn_id=CONN_ID,
        sql=r"""replace procedure examplestoredproc (in p1 integer, in p2 integer, out p3 integer)
         begin
            set p3 = p1 * p2;
         end ;
        """,
        dag=dag,
    )

    opr_sp_in_inout_xcom = TeradataStoredProcedureOperator(
        task_id="opr_sp_in_inout_xcom",
        conn_id=CONN_ID,
        procedure="examplestoredproc",
        parameters=["{{ ti.xcom_pull(task_ids='opr_sp_in_inout')[0][0] }}", "{{ ti.xcom_pull(task_ids='opr_sp_in_inout')[0][1] }}", int],
        # Accessing previous task opr_sp_in_inout output parameter 1 and parameter 2
        dag=dag,
    )

    create_sp_in_inout >> opr_sp_in_inout >> create_sp_two_in >> opr_sp_in_inout_xcom


    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
