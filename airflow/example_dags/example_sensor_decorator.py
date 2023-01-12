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

"""Example DAG demonstrating the usage of the sensor decorator."""

from __future__ import annotations

# [START tutorial]
# [START import_module]
import pendulum

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue


# [END import_module]


# [START instantiate_dag]
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_sensor_decorator():
    # [END instantiate_dag]

    # [START wait_function]
    # Using a sensor operator to wait for the upstream data to be ready.
    @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
    def wait_for_upstream() -> PokeReturnValue:
        return PokeReturnValue(is_done=True, xcom_value="xcom_value")

    # [END wait_function]

    # [START dummy_function]
    @task
    def dummy_operator() -> None:
        pass

    # [END dummy_function]

    # [START main_flow]
    wait_for_upstream() >> dummy_operator()
    # [END main_flow]


# [START dag_invocation]
tutorial_etl_dag = example_sensor_decorator()
# [END dag_invocation]

# [END tutorial]
