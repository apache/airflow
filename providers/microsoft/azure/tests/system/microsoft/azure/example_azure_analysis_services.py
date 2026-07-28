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
Example DAG demonstrating Azure Analysis Services model refresh.

Before running this system test, set the following environment variables:

- ``SERVER_NAME``: The Analysis Services server name (e.g. ``myserver``)
- ``DATABASE``: The model (database) name (e.g. ``AdventureWorks``)
- ``CLIENT_ID``: Service principal client ID
- ``CLIENT_SECRET``: Service principal client secret
- ``TENANT_ID``: Azure tenant ID
- ``REGION_ENDPOINT``: Region endpoint (e.g. ``eastus.asazure.windows.net``)
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG, settings
from airflow.models import Connection

try:
    from airflow.sdk import task
except ImportError:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]

from airflow.providers.microsoft.azure.operators.analysis_services import (
    AzureAnalysisServicesRefreshOperator,
)

DAG_ID = "example_azure_analysis_services_refresh"
CONN_ID = "azure_analysis_services_example"

SERVER_NAME = os.environ.get("SERVER_NAME", "myserver")
DATABASE = os.environ.get("DATABASE", "AdventureWorks")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
TENANT_ID = os.environ.get("TENANT_ID")
REGION_ENDPOINT = os.environ.get("REGION_ENDPOINT", "eastus.asazure.windows.net")


@task
def create_connection(conn_id: str) -> None:
    conn = Connection(
        conn_id=conn_id,
        conn_type="azure_analysis_services",
        host=REGION_ENDPOINT,
        login=CLIENT_ID,
        password=CLIENT_SECRET,
        extra={"tenantId": TENANT_ID},
    )
    if settings.Session is None:
        raise RuntimeError("Session not configured. Call configure_orm() first.")
    session = settings.Session()
    session.add(conn)
    session.commit()


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "azure", "analysis-services"],
) as dag:
    setup = create_connection(CONN_ID)

    # [START howto_operator_azure_analysis_services_refresh]
    refresh = AzureAnalysisServicesRefreshOperator(
        task_id="refresh_model",
        server_name=SERVER_NAME,
        database=DATABASE,
        azure_analysis_services_conn_id=CONN_ID,
        refresh_type="full",
    )
    # [END howto_operator_azure_analysis_services_refresh]

    # [START howto_operator_azure_analysis_services_refresh_deferrable]
    refresh_deferrable = AzureAnalysisServicesRefreshOperator(
        task_id="refresh_model_deferrable",
        server_name=SERVER_NAME,
        database=DATABASE,
        azure_analysis_services_conn_id=CONN_ID,
        refresh_type="full",
        deferrable=True,
    )
    # [END howto_operator_azure_analysis_services_refresh_deferrable]

    setup >> refresh >> refresh_deferrable


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
