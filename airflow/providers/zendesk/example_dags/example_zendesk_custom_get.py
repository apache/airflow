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

from datetime import datetime
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.zendesk.hooks.zendesk import ZendeskHook


def zendesk_custom_get_request() -> List[Dict]:
    hook = ZendeskHook()
    response = hook.get(
        url="https://yourdomain.zendesk.com/api/v2/organizations.json",
    )
    return [org.to_dict() for org in response]


with DAG(
    dag_id="zendesk_custom_get_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    fetch_organizations = PythonOperator(
        task_id="trigger_zendesk_hook",
        python_callable=zendesk_custom_get_request,
    )
