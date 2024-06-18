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

from datetime import datetime, timedelta

from airflow.models import DAG

# Ignore missing args provided by default_args
# mypy: disable-error-code="call-arg"
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.powerbi import PowerBIDatasetRefreshOperator
from airflow.utils.edgemodifier import Label

DAG_ID = "example_powerbi_dataset_refresh"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 8, 13),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    # [START howto_operator_powerbi_refresh_dataset]
    dataset_refresh = PowerBIDatasetRefreshOperator(
        powerbi_conn_id="powerbi_default",
        task_id="dataset_refresh",
        dataset_id="dataset-id",
        group_id="group-id",
    )
    # [END howto_operator_powerbi_refresh_dataset]

    # [START howto_operator_powerbi_refresh_dataset_async]
    dataset_refresh2 = PowerBIDatasetRefreshOperator(
        powerbi_conn_id="powerbi_default",
        task_id="dataset_refresh_async",
        dataset_id="dataset-id",
        group_id="group-id",
        wait_for_termination=False,
    )
    # [END howto_operator_powerbi_refresh_dataset_async]

    # [START howto_operator_powerbi_refresh_dataset_force_refresh]
    dataset_refresh3 = PowerBIDatasetRefreshOperator(
        powerbi_conn_id="powerbi_default",
        task_id="dataset_refresh_force_refresh",
        dataset_id="dataset-id",
        group_id="group-id",
        force_refresh=True,
    )
    # [END howto_operator_powerbi_refresh_dataset_force_refresh]

    begin >> Label("No async wait") >> dataset_refresh
    begin >> Label("Do async wait with force refresh") >> dataset_refresh2
    begin >> Label("Do async wait") >> dataset_refresh3 >> end

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
