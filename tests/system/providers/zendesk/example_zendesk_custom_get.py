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

import os
from datetime import datetime
from typing import Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.providers.zendesk.hooks.zendesk import ZendeskHook

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "zendesk_custom_get_dag"


@task
def fetch_organizations() -> List[Dict]:
    hook = ZendeskHook()
    response = hook.get(
        url="https://yourdomain.zendesk.com/api/v2/organizations.json",
    )
    return [org.to_dict() for org in response]


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    fetch_organizations()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
