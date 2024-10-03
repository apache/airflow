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
Example Airflow DAG for Google BigQuery service testing dataset operations.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "bigquery_dataset"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:
    # [START howto_operator_bigquery_create_dataset]
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME)
    # [END howto_operator_bigquery_create_dataset]

    # [START howto_operator_bigquery_update_dataset]
    update_dataset = BigQueryUpdateDatasetOperator(
        task_id="update_dataset",
        dataset_id=DATASET_NAME,
        dataset_resource={"description": "Updated dataset"},
    )
    # [END howto_operator_bigquery_update_dataset]

    # [START howto_operator_bigquery_get_dataset]
    get_dataset = BigQueryGetDatasetOperator(task_id="get-dataset", dataset_id=DATASET_NAME)
    # [END howto_operator_bigquery_get_dataset]

    get_dataset_result = BashOperator(
        task_id="get_dataset_result",
        bash_command="echo \"{{ task_instance.xcom_pull('get-dataset')['id'] }}\"",
    )

    # [START howto_operator_bigquery_delete_dataset]
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )
    # [END howto_operator_bigquery_delete_dataset]
    delete_dataset.trigger_rule = TriggerRule.ALL_DONE

    (
        # TEST BODY
        create_dataset
        >> update_dataset
        >> get_dataset
        >> get_dataset_result
        # TEST TEARDOWN
        >> delete_dataset
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
