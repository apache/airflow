# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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

import pendulum

from airflow.decorators import dag, task

AKEYLESS_CONN_ID = "akeyless_default"


@dag(
    dag_id="example_akeyless",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "akeyless"],
)
def example_akeyless():
    @task
    def get_static_secret():
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook(akeyless_conn_id=AKEYLESS_CONN_ID)
        value = hook.get_secret_value("/example/my-secret")
        print(f"Secret retrieved (length={len(value) if value else 0})")
        return value is not None

    @task
    def list_secrets():
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook(akeyless_conn_id=AKEYLESS_CONN_ID)
        items = hook.list_items("/example")
        print(f"Found {len(items)} items under /example")
        return len(items)

    @task
    def get_dynamic_secret():
        from airflow.providers.akeyless.hooks.akeyless import AkeylessHook

        hook = AkeylessHook(akeyless_conn_id=AKEYLESS_CONN_ID)
        creds = hook.get_dynamic_secret_value("/example/dynamic-db-producer")
        if creds:
            print(f"Dynamic secret generated with keys: {list(creds.keys())}")
        return creds is not None

    get_static_secret() >> list_secrets() >> get_dynamic_secret()


example_akeyless()
