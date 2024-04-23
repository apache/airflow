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
"""Example Airflow DAG to show Stored Procedure creation and execution on teradata database using
TeradataStoredProcedureOperator.

This DAG assumes Airflow Connection with connection id `teradata_sp_call` already exists in locally. It
shows how to create and execute Stored Procedure as tasks in airflow dags using
TeradataStoredProcedureOperator."""

from __future__ import annotations

from datetime import datetime

import pytest

from airflow import DAG

try:
    from airflow.providers.teradata.operators.teradata import (
        TeradataOperator,
        TeradataStoredProcedureOperator,
    )
except ImportError:
    pytest.skip("Teradata provider apache-airflow-provider-teradata not available", allow_module_level=True)

# [START howto_teradata_operator_for_sp]
CONN_ID = "teradata_sp_call"
DAG_ID = "example_teradata_call_sp"

with DAG(
    dag_id=DAG_ID,
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    default_args={"conn_id": CONN_ID},
    schedule="@once",
    start_date=datetime(2023, 1, 1),
) as dag:
    # [START howto_teradata_stored_procedure_operator_with_in_inout]
    create_sp_in_inout = TeradataOperator(
        task_id="create_sp_in_inout",
        sql=r"""REPLACE PROCEDURE TEST_PROCEDURE (IN val_in INTEGER, OUT val_out INTEGER)
                BEGIN
                    set val_out = val_in * 2;
                END;
            """,
    )

    opr_sp_in_inout = TeradataStoredProcedureOperator(
        task_id="opr_sp_in_inout",
        procedure="TEST_PROCEDURE",
        parameters=[3, int],
    )

    # [END howto_teradata_stored_procedure_operator_with_in_inout]

    # [START howto_teradata_stored_procedure_operator_with_in_out_dynamic_result]
    create_sp_param_dr = TeradataOperator(
        task_id="create_sp_param_dr",
        sql=r"""replace procedure examplestoredproc (in p1 integer, inout p2 integer, out p3 integer)
                dynamic result sets 2
                begin
                    declare cur1 cursor with return for select * from dbc.dbcinfo order by 1 ;
                    declare cur2 cursor with return for select infodata, infokey from dbc.dbcinfo order by 1 ;
                    open cur1 ;
                    open cur2 ;
                    set p2 = p1 + p2 ;
                    set p3 = p1 * p2 ;
                end ;
            """,
    )
    opr_sp_param_dr = TeradataStoredProcedureOperator(
        task_id="opr_sp_param_dr",
        procedure="examplestoredproc",
        parameters=[3, 2, int],
    )
    # [END howto_teradata_stored_procedure_operator_with_in_out_dynamic_result]

    # [START howto_teradata_stored_procedure_operator_drop]
    drop_sp = TeradataOperator(
        task_id="drop_sp",
        sql=r"""drop procedure examplestoredproc;
               """,
    )
    # [END howto_teradata_stored_procedure_operator_drop]
    (create_sp_in_inout >> opr_sp_in_inout >> create_sp_param_dr >> opr_sp_param_dr >> drop_sp)

    # [END howto_teradata_operator_for_sp]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
