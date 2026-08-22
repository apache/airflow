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
from typing import TYPE_CHECKING, cast

from airflow.providers.microsoft.azure.operators.analysis_services import (
    AzureAnalysisServicesRefreshOperator,
)
from airflow.providers.microsoft.azure.sensors.analysis_services import AzureAnalysisServicesSensor
from airflow.sdk import DAG

if TYPE_CHECKING:
    from airflow.providers.microsoft.azure.hooks.analysis_services import RefreshType

DAG_ID = "example_azure_analysis_services"
SERVER_NAME = os.environ.get("AZURE_ANALYSIS_SERVICES_SERVER_NAME", "testserver")
DATABASE = os.environ.get("AZURE_ANALYSIS_SERVICES_DATABASE", "adventureworks")
REFRESH_TYPE = cast("RefreshType", os.environ.get("AZURE_ANALYSIS_SERVICES_REFRESH_TYPE", "calculate"))

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "azure", "analysis-services"],
) as dag:
    # [START howto_operator_azure_analysis_services_refresh]
    start_refresh_without_wait = AzureAnalysisServicesRefreshOperator(
        task_id="start_refresh_without_wait",
        server_name=SERVER_NAME,
        database=DATABASE,
        refresh_type=REFRESH_TYPE,
        wait_for_termination=False,
    )
    # [END howto_operator_azure_analysis_services_refresh]

    # [START howto_sensor_azure_analysis_services_refresh]
    wait_with_deferrable_sensor = AzureAnalysisServicesSensor(
        task_id="wait_with_deferrable_sensor",
        server_name=SERVER_NAME,
        database=DATABASE,
        refresh_id=start_refresh_without_wait.output,
        deferrable=True,
        poke_interval=10,
        timeout=600,
    )
    # [END howto_sensor_azure_analysis_services_refresh]

    # [START howto_operator_azure_analysis_services_deferrable]
    refresh_with_deferrable_operator = AzureAnalysisServicesRefreshOperator(
        task_id="refresh_with_deferrable_operator",
        server_name=SERVER_NAME,
        database=DATABASE,
        refresh_type=REFRESH_TYPE,
        deferrable=True,
        check_interval=10,
        timeout=600,
    )
    # [END howto_operator_azure_analysis_services_deferrable]

    start_refresh_without_wait >> wait_with_deferrable_sensor >> refresh_with_deferrable_operator

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
