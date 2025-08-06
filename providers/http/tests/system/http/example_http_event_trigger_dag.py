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
"""Example DAG that uses an AssetWatcher with HttpEventTrigger"""

# [START howto_http_event_trigger]
from __future__ import annotations

import datetime
import os

from asgiref.sync import sync_to_async

from airflow.providers.http.triggers.http import HttpEventTrigger
from airflow.sdk import Asset, AssetWatcher, Variable, dag, task

token = os.getenv("GITHUB_TOKEN")

headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28",
}


async def check_github_api_response(response):
    data = response.json()
    release_id = str(data["id"])
    get_variable_sync = sync_to_async(Variable.get)
    previous_release_id = await get_variable_sync(key="release_id_var", default=None)
    if release_id == previous_release_id:
        return False
    release_name = data["name"]
    release_html_url = data["html_url"]
    set_variable_sync = sync_to_async(Variable.set)
    await set_variable_sync(key="release_id_var", value=str(release_id))
    await set_variable_sync(key="release_name_var", value=release_name)
    await set_variable_sync(key="release_html_url_var", value=release_html_url)
    return True


trigger = HttpEventTrigger(
    endpoint="repos/apache/airflow/releases/latest",
    method="GET",
    http_conn_id="http_default",
    headers=headers,
    response_check_path="dags.check_airflow_releases.check_github_api_response",
)

asset = Asset(
    "airflow_releases_asset", watchers=[AssetWatcher(name="airflow_releases_watcher", trigger=trigger)]
)


@dag(start_date=datetime.datetime(2024, 10, 1), schedule=asset, catchup=False)
def check_airflow_releases():
    @task()
    def print_airflow_release_info():
        release_name = Variable.get("release_name_var")
        release_html_url = Variable.get("release_html_url_var")
        print(f"{release_name} has been released. Check it out at {release_html_url}")

    print_airflow_release_info()


check_airflow_releases()
# [END howto_http_event_trigger]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
