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
Example Airflow DAG for Google BigQuery service.

The DAG checks how BigQueryValueCheckOperator works with a non-US dataset.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
NON_US_LOCATION = "asia-east1"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]

DAG_ID = "bq_value_check_location"
DATASET = f"ds_{DAG_ID}_{ENV_ID}"
TABLE = "ds_table"
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET}.{TABLE} VALUES "
    f"(42, 'monty python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)
default_args = {
    "execution_timeout": timedelta(minutes=10),
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    DAG_ID,
    schedule="@once",
    catchup=False,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    tags=["example", "bigquery"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET,
        location=NON_US_LOCATION,
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET,
        table_id=TABLE,
        schema_fields=SCHEMA,
        location=NON_US_LOCATION,
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=NON_US_LOCATION,
    )

    check_value = BigQueryValueCheckOperator(
        task_id="check_value",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        pass_value=2,
        use_legacy_sql=False,
        location=NON_US_LOCATION,
    )

    check_value_no_location = BigQueryValueCheckOperator(
        task_id="check_value_no_location",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        pass_value=2,
        use_legacy_sql=False,
        deferrable=False,
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_dataset
        >> create_table
        >> insert_query_job
        # TEST BODY
        >> check_value
        >> check_value_no_location
        # TEST TEARDOWN
        >> delete_dataset
    )

    from dev.tests_common.test_utils.system_tests import get_test_run
    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

    # Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
    test_run = get_test_run(dag)


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
