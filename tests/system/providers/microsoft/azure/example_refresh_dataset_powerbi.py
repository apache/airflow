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
from airflow.providers.microsoft.azure.operators.powerbi import PowerBIDatasetRefreshOperator

DAG_ID = "example_refresh_powerbi_dataset"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    tags=["example"],
) as dag:
    # [START howto_operator_powerbi_refresh]
    refresh_powerbi_dataset = PowerBIDatasetRefreshOperator(
        conn_id="powerbi_default",
        task_id="refresh_powerbi_dataset",
        dataset_id="7bacf905-be8a-4f67-9512-71f4dc0c42dc",
        group_id="aaaebfa6-194a-4edd-8a36-a5e42200df2e",
        check_interval=30,
        wait_for_termination=True,
    )
    # [END howto_operator_powerbi_refresh]

    refresh_powerbi_dataset

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
