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

from airflow import models
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator

DAG_ID = "example_sharepoint_site"

with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    tags=["example"],
) as dag:
    # [START howto_operator_graph_site]
    site_task = MSGraphAsyncOperator(
        task_id="news_site",
        conn_id="msgraph_api",
        url="sites/850v1v.sharepoint.com:/sites/news",
        result_processor=lambda context, response: response["id"].split(",")[1],  # only keep site_id
    )
    # [END howto_operator_graph_site]

    # [START howto_operator_graph_site_pages]
    site_pages_task = MSGraphAsyncOperator(
        task_id="news_pages",
        conn_id="msgraph_api",
        api_version="beta",
        url=(
            "sites/%s/pages"
            % "{{ ti.xcom_pull(task_ids='news_site') }}"
        ),
    )
    # [END howto_operator_graph_site_pages]

    site_task >> site_pages_task

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
