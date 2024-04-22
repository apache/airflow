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

from __future__ import annotations

from airflow import DAG
from airflow.providers.teradata.operators.teradata import TeradataOperator, TeradataStoredProcedureOperator

CONN_ID = "teradata_sp_call"

with DAG(
    dag_id="example_downstream_dag",
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
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
        parameters=["{{ dag_run.conf.get('input_param1') }}", int],
    )

    t1 >> t2


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
