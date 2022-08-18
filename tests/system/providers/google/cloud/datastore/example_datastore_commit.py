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
Airflow System Test DAG that verifies Datastore commit operators.
"""

import os
from datetime import datetime
from typing import Any, Dict

from airflow import models
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreAllocateIdsOperator,
    CloudDatastoreBeginTransactionOperator,
    CloudDatastoreCommitOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "datastore_commit"

# [START how_to_keys_def]
KEYS = [
    {
        "partitionId": {"projectId": PROJECT_ID, "namespaceId": ""},
        "path": {"kind": "airflow"},
    }
]
# [END how_to_keys_def]

# [START how_to_transaction_def]
TRANSACTION_OPTIONS: Dict[str, Any] = {"readWrite": {}}
# [END how_to_transaction_def]


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["datastore", "example"],
) as dag:
    # [START how_to_allocate_ids]
    allocate_ids = CloudDatastoreAllocateIdsOperator(
        task_id="allocate_ids", partial_keys=KEYS, project_id=PROJECT_ID
    )
    # [END how_to_allocate_ids]

    # [START how_to_begin_transaction]
    begin_transaction_commit = CloudDatastoreBeginTransactionOperator(
        task_id="begin_transaction_commit",
        transaction_options=TRANSACTION_OPTIONS,
        project_id=PROJECT_ID,
    )
    # [END how_to_begin_transaction]

    # [START how_to_commit_def]
    COMMIT_BODY = {
        "mode": "TRANSACTIONAL",
        "mutations": [
            {
                "insert": {
                    "key": KEYS[0],
                    "properties": {"string": {"stringValue": "airflow is awesome!"}},
                }
            }
        ],
        "transaction": begin_transaction_commit.output,
    }
    # [END how_to_commit_def]

    # [START how_to_commit_task]
    commit_task = CloudDatastoreCommitOperator(task_id="commit_task", body=COMMIT_BODY, project_id=PROJECT_ID)
    # [END how_to_commit_task]

    allocate_ids >> begin_transaction_commit >> commit_task


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
