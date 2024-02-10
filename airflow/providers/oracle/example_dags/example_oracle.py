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

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator

with DAG(
    max_active_runs=1,
    max_active_tasks=3,
    catchup=False,
    start_date=datetime(2023, 1, 1),
    dag_id="example_oracle",
) as dag:
    # [START howto_oracle_operator]

    opr_sql = SQLExecuteQueryOperator(
        task_id="task_sql", conn_id="oracle", sql="SELECT 1 FROM DUAL", autocommit=True
    )

    # [END howto_oracle_operator]

    # [START howto_oracle_stored_procedure_operator_with_list_inout]

    opr_stored_procedure_with_list_input_output = OracleStoredProcedureOperator(
        task_id="opr_stored_procedure_with_list_input_output",
        oracle_conn_id="oracle",
        procedure="TEST_PROCEDURE",
        parameters=[3, int],
    )

    # [END howto_oracle_stored_procedure_operator_with_list_inout]

    # [START howto_oracle_stored_procedure_operator_with_dict_inout]

    opr_stored_procedure_with_dict_input_output = OracleStoredProcedureOperator(
        task_id="opr_stored_procedure_with_dict_input_output",
        oracle_conn_id="oracle",
        procedure="TEST_PROCEDURE",
        parameters={"val_in": 3, "val_out": int},
    )

    # [END howto_oracle_stored_procedure_operator_with_dict_inout]
