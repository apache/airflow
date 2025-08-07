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
from __future__ import annotations

import ipaddress

# [START dag_decorator_usage]
from typing import TYPE_CHECKING, Any

import httpx
import pendulum

from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

if TYPE_CHECKING:
    from airflow.sdk import Context


class GetRequestOperator(BaseOperator):
    """Custom operator to send GET request to provided url"""

    template_fields = ("url",)

    def __init__(self, *, url: str, **kwargs):
        super().__init__(**kwargs)
        self.url = url

    def execute(self, context: Context):
        return httpx.get(self.url).json()


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_dag_decorator(url: str = "http://httpbin.org/get"):
    """
    DAG to get IP address and echo it via BashOperator.

    :param url: URL to get IP address from. Defaults to "http://httpbin.org/get".
    """
    get_ip = GetRequestOperator(task_id="get_ip", url=url)

    @task(multiple_outputs=True)
    def prepare_command(raw_json: dict[str, Any]) -> dict[str, str]:
        external_ip = raw_json["origin"]
        try:
            ipaddress.ip_address(external_ip)
            return {
                "command": f"echo 'Seems like today your server executing Airflow is connected from IP {external_ip}'",
            }
        except ValueError:
            raise ValueError(f"Invalid IP address: '{external_ip}'.")

    command_info = prepare_command(get_ip.output)

    BashOperator(task_id="echo_ip_info", bash_command=command_info["command"])


example_dag = example_dag_decorator()
# [END dag_decorator_usage]
