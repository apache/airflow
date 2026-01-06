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
"""
Example DAG demonstrating the usage of the sensor decorator.
"""

from __future__ import annotations

# [START tutorial]
# [START import_module]
import pendulum

from airflow.sdk import PokeReturnValue, dag, task

# [END import_module]


# [START instantiate_dag]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    doc_md="""
    ### Sensor Decorator: Data-Driven Task Dependencies

    Sensors are used to delay downstream execution until an external condition
    or data dependency is satisfied. Unlike purely time-based scheduling,
    sensors actively evaluate external state, making them a key building block
    for data-driven workflows.

    **When to use sensors versus time-based scheduling:**
    - Use sensors when downstream tasks depend on external data availability
      or system state rather than elapsed time
    - Time-based schedules are appropriate when execution timing is the primary
      driver; sensors are appropriate when data readiness is the driver
    - Combining both patterns enables data-aware scheduling
      (for example, “run daily, but only after upstream data is ready”)

    **Runtime behavior and resource considerations:**
    - Sensors block downstream task execution but do not block DAG scheduling;
      the DAG run remains active until the sensor succeeds or times out
    - *Poke mode* (default): the task occupies an executor slot while polling,
      which can increase resource usage for long waits
    - *Reschedule mode*: the task is deferred between polls, reducing resource
      consumption and improving scalability for long-running sensors
    - On timeout, downstream task behavior depends on trigger rules and
      failure-handling configuration

    **Data passing:**
    - Sensors can return XCom values via `PokeReturnValue`, allowing downstream
      tasks to consume metadata discovered during sensing without re-querying
      the external system

    📖 **Related documentation**  
    https://airflow.apache.org/docs/apache-airflow/stable/howto/operator.html#sensors
    """,
)
def example_sensor_decorator():
    # [END instantiate_dag]

    # [START wait_function]
    # Using a sensor decorator to wait for upstream data to be ready.
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
