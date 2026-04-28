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
"""Example DAG demonstrating the Akeyless provider.

Before running, create an Airflow Connection:
  - Connection ID: ``akeyless_default``
  - Connection Type: ``akeyless``
  - Host: ``https://api.akeyless.io``
  - Login: your Akeyless Access ID
  - Password: your Akeyless Access Key
  - Extra: ``{"access_type": "api_key"}``
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

AKEYLESS_CONN_ID = "akeyless_default"


def _get_static_secret():
    from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

    hook = AkeylessHook(akeyless_conn_id=AKEYLESS_CONN_ID)
    value = hook.get_secret_value("/example/my-secret")
    print(f"Secret retrieved (length={len(value) if value else 0})")
    return value is not None


def _list_secrets():
    from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

    hook = AkeylessHook(akeyless_conn_id=AKEYLESS_CONN_ID)
    items = hook.list_items("/example")
    print(f"Found {len(items)} items under /example")
    return len(items)


def _get_dynamic_secret():
    from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

    hook = AkeylessHook(akeyless_conn_id=AKEYLESS_CONN_ID)
    creds = hook.get_dynamic_secret_value("/example/dynamic-db-producer")
    if creds:
        print(f"Dynamic secret generated with keys: {list(creds.keys())}")
    return creds is not None


with DAG(
    dag_id="example_akeyless",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "akeyless"],
) as dag:
    get_secret = PythonOperator(task_id="get_static_secret", python_callable=_get_static_secret)
    list_items = PythonOperator(task_id="list_secrets", python_callable=_list_secrets)
    get_dynamic = PythonOperator(task_id="get_dynamic_secret", python_callable=_get_dynamic_secret)

    get_secret >> list_items >> get_dynamic


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
