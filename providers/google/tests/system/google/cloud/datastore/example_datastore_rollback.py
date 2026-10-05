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
Airflow System Test DAG that verifies Datastore transaction operators.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.datastore import DatastoreHook
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreBeginTransactionOperator,
    CloudDatastoreRollbackOperator,
)

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

if TYPE_CHECKING:
    from airflow.sdk import Context

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "datastore_rollback"

TRANSACTION_OPTIONS: dict[str, Any] = {"readWrite": {}}


def begin_transaction_for_rollback(context: Context) -> None:
    task = cast("CloudDatastoreRollbackOperator", context["task"])

    hook = DatastoreHook(
        gcp_conn_id=task.gcp_conn_id,
        impersonation_chain=task.impersonation_chain,
    )

    task.transaction = hook.begin_transaction(
        transaction_options=TRANSACTION_OPTIONS,
        project_id=task.project_id,
    )

    task.log.info("Created Datastore transaction immediately before rollback")


def rollback_after_begin(context: Context, transaction: str) -> None:
    task = cast("CloudDatastoreBeginTransactionOperator", context["task"])

    hook = DatastoreHook(
        gcp_conn_id=task.gcp_conn_id,
        impersonation_chain=task.impersonation_chain,
    )

    hook.rollback(transaction=transaction, project_id=task.project_id)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["datastore", "example"],
) as dag:
    begin_transaction = CloudDatastoreBeginTransactionOperator(
        task_id="begin_transaction",
        transaction_options=TRANSACTION_OPTIONS,
        project_id=PROJECT_ID,
        post_execute=rollback_after_begin,
    )

    rollback_transaction = CloudDatastoreRollbackOperator(
        task_id="rollback_transaction",
        transaction="defined_in_begin_transaction_for_rollback",
        pre_execute=begin_transaction_for_rollback,
        project_id=PROJECT_ID,
    )

    # Each task owns its transaction so scheduler delays cannot expire a handle between tasks.
    begin_transaction >> rollback_transaction

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
