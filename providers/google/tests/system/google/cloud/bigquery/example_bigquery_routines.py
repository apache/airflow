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
Example Airflow DAG for Google BigQuery service testing routine operations.

Exercises the full BigQuery routines lifecycle through Airflow: create a scalar SQL
UDF, a stored procedure, and a table-valued function; verify their existence; list
and fetch them; update one; and delete them all.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateRoutineOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteRoutineOperator,
    BigQueryGetRoutineOperator,
    BigQueryListRoutinesOperator,
    BigQueryUpdateRoutineOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryRoutineExistenceSensor

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or "default"
DAG_ID = "bigquery_routines"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
SCALAR_ROUTINE = f"scalar_udf_{ENV_ID}"
PROCEDURE_ROUTINE = f"procedure_{ENV_ID}"
TVF_ROUTINE = f"tvf_{ENV_ID}"

INT64_TYPE = {"typeKind": "INT64"}
STRING_TYPE = {"typeKind": "STRING"}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME, project_id=PROJECT_ID
    )

    # [START howto_operator_bigquery_create_scalar_routine]
    create_scalar_routine = BigQueryCreateRoutineOperator(
        task_id="create_scalar_routine",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=SCALAR_ROUTINE,
        routine_type="SCALAR_FUNCTION",
        language="SQL",
        arguments=[{"name": "x", "dataType": INT64_TYPE}],
        return_type=INT64_TYPE,
        definition_body="x + 1",
        description="Adds one to its argument.",
    )
    # [END howto_operator_bigquery_create_scalar_routine]

    # [START howto_operator_bigquery_create_procedure_routine]
    create_procedure = BigQueryCreateRoutineOperator(
        task_id="create_procedure",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=PROCEDURE_ROUTINE,
        routine_type="PROCEDURE",
        language="SQL",
        arguments=[
            {"name": "prefix", "dataType": STRING_TYPE, "argumentKind": "FIXED_TYPE"},
        ],
        definition_body="BEGIN SELECT CONCAT(prefix, ' world') AS greeting; END",
        description="Echoes a prefixed greeting.",
    )
    # [END howto_operator_bigquery_create_procedure_routine]

    # [START howto_operator_bigquery_create_tvf_routine]
    create_tvf = BigQueryCreateRoutineOperator(
        task_id="create_tvf",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=TVF_ROUTINE,
        routine_type="TABLE_VALUED_FUNCTION",
        language="SQL",
        arguments=[{"name": "n", "dataType": INT64_TYPE}],
        return_table_type={
            "columns": [
                {"name": "value", "type": INT64_TYPE},
            ]
        },
        definition_body="SELECT * FROM UNNEST(GENERATE_ARRAY(1, n)) AS value",
        description="Generates integers 1..n as a table.",
    )
    # [END howto_operator_bigquery_create_tvf_routine]

    # [START howto_sensor_bigquery_routine_existence]
    wait_for_routine = BigQueryRoutineExistenceSensor(
        task_id="wait_for_routine",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=SCALAR_ROUTINE,
        timeout=60,
        poke_interval=5,
    )
    # [END howto_sensor_bigquery_routine_existence]

    # [START howto_operator_bigquery_update_routine]
    update_routine = BigQueryUpdateRoutineOperator(
        task_id="update_routine",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=SCALAR_ROUTINE,
        routine_resource={"description": "Updated description for scalar UDF"},
        fields=["description"],
    )
    # [END howto_operator_bigquery_update_routine]

    # [START howto_operator_bigquery_get_routine]
    get_routine = BigQueryGetRoutineOperator(
        task_id="get_routine",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=SCALAR_ROUTINE,
    )
    # [END howto_operator_bigquery_get_routine]

    # [START howto_operator_bigquery_list_routines]
    list_routines = BigQueryListRoutinesOperator(
        task_id="list_routines",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
    )
    # [END howto_operator_bigquery_list_routines]

    # [START howto_operator_bigquery_delete_routine]
    delete_scalar_routine = BigQueryDeleteRoutineOperator(
        task_id="delete_scalar_routine",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=SCALAR_ROUTINE,
        ignore_if_missing=True,
    )
    # [END howto_operator_bigquery_delete_routine]

    delete_procedure = BigQueryDeleteRoutineOperator(
        task_id="delete_procedure",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=PROCEDURE_ROUTINE,
        ignore_if_missing=True,
    )
    delete_tvf = BigQueryDeleteRoutineOperator(
        task_id="delete_tvf",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        routine_id=TVF_ROUTINE,
        ignore_if_missing=True,
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_dataset
        # TEST BODY
        >> [create_scalar_routine, create_procedure, create_tvf]
        >> wait_for_routine
        >> update_routine
        >> get_routine
        >> list_routines
        >> [delete_scalar_routine, delete_procedure, delete_tvf]
        # TEST TEARDOWN
        >> delete_dataset
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
