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
Airflow System Test DAG that verifies Datastore rollback operators.
"""

import os
from datetime import datetime
from typing import Any, Dict, cast

from airflow import models
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreBeginTransactionOperator,
    CloudDatastoreRollbackOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "datastore_rollback"

TRANSACTION_OPTIONS: Dict[str, Any] = {"readWrite": {}}


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["datastore", "example"],
) as dag:
    begin_transaction_to_rollback = CloudDatastoreBeginTransactionOperator(
        task_id="begin_transaction_to_rollback",
        transaction_options=TRANSACTION_OPTIONS,
        project_id=PROJECT_ID,
    )

    # [START how_to_rollback_transaction]
    rollback_transaction = CloudDatastoreRollbackOperator(
        task_id="rollback_transaction",
        transaction=cast(str, begin_transaction_to_rollback.output),
    )
    # [END how_to_rollback_transaction]

    begin_transaction_to_rollback >> rollback_transaction


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
