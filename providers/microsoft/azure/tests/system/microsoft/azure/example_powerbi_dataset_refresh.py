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

import os
from datetime import datetime

from airflow import DAG, settings

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models import Connection
from airflow.models.baseoperator import chain
from airflow.providers.microsoft.azure.operators.powerbi import PowerBIDatasetRefreshOperator

DAG_ID = "example_refresh_powerbi_dataset"
CONN_ID = "powerbi_default"

# Before running this system test, you should set following environment variables:
DATASET_ID = os.environ.get("DATASET_ID", "None")
GROUP_ID = os.environ.get("GROUP_ID", "None")
CLIENT_ID = os.environ.get("CLIENT_ID", None)
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", None)
TENANT_ID = os.environ.get("TENANT_ID", None)


@task
def create_connection(conn_id_name: str):
    conn = Connection(
        conn_id=conn_id_name,
        conn_type="powerbi",
        login=CLIENT_ID,
        password=CLIENT_SECRET,
        extra={"tenant_id": TENANT_ID},
    )
    if settings.Session is None:
        raise RuntimeError("Session not configured. Call configure_orm() first.")
    session = settings.Session()
    session.add(conn)
    session.commit()


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    tags=["example"],
) as dag:
    set_up_connection = create_connection(CONN_ID)

    # [START howto_operator_powerbi_refresh_async]
    refresh_powerbi_dataset = PowerBIDatasetRefreshOperator(
        conn_id="powerbi_default",
        task_id="refresh_powerbi_dataset",
        dataset_id=DATASET_ID,
        group_id=GROUP_ID,
        check_interval=30,
        timeout=120,
        request_body={
            "type": "full",
            "retryCount": 3,
            "commitMode": "transactional",
            "notifyOption": "MailOnFailure",
        },
    )
    # [END howto_operator_powerbi_refresh_async]

    chain(
        # TEST SETUP
        set_up_connection,
        # TEST BODY
        refresh_powerbi_dataset,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
