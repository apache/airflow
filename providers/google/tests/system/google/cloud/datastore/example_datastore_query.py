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
Airflow System Test DAG that verifies Datastore query operators.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreAllocateIdsOperator,
    CloudDatastoreBeginTransactionOperator,
    CloudDatastoreRunQueryOperator,
)
from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "datastore_query"

KEYS = [
    {
        "partitionId": {"projectId": PROJECT_ID, "namespaceId": ""},
        "path": {"kind": "airflow"},
    }
]

TRANSACTION_OPTIONS: dict[str, Any] = {"readWrite": {}}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["datastore", "example"],
) as dag:
    allocate_ids = CloudDatastoreAllocateIdsOperator(
        task_id="allocate_ids", partial_keys=KEYS, project_id=PROJECT_ID
    )

    begin_transaction_query = CloudDatastoreBeginTransactionOperator(
        task_id="begin_transaction_query",
        transaction_options=TRANSACTION_OPTIONS,
        project_id=PROJECT_ID,
    )

    # [START how_to_query_def]
    QUERY = {
        "partitionId": {"projectId": PROJECT_ID, "namespaceId": "query"},
        "readOptions": {"transaction": begin_transaction_query.output},
        "query": {},
    }
    # [END how_to_query_def]

    # [START how_to_run_query]
    run_query = CloudDatastoreRunQueryOperator(task_id="run_query", body=QUERY, project_id=PROJECT_ID)
    # [END how_to_run_query]

    allocate_ids >> begin_transaction_query >> run_query

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
