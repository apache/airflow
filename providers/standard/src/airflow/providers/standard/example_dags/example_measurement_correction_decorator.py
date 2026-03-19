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

"""Example Dag demonstrating a simple measurement correction workflow"""

from __future__ import annotations

from airflow.sdk import dag, task
import pendulum


# [START example_measurement_correction_decorator]
@dag(
    dag_id="example_measurement_correction_decorator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
)
def measurement_correction_decorator():
    """
    A tutorial Dag showing how to:
    1. Read a raw measurement
    2. Validate the measurement
    3. Apply a correction factor
    4. Log the corrected result
    """

    # Task 1: Read a raw measurement
    @task
    def read_measurement() -> int:
        return 100

    # Task 2: Validate the measurement
    @task
    def validate_measurement(value: int) -> int:
        if value < 0:
            raise ValueError("Measurement must be positive")
        return value

    # Task 3: Apply a correction factor
    @task
    def apply_correction(value: int) -> float:
        correction_factor = 1.1
        return value * correction_factor

    # Task 4: Log the final corrected result
    @task
    def store_result(value: float) -> None:
        print(f"Corrected measurement: {value}")

    raw_value = read_measurement()
    validated_value = validate_measurement(raw_value)
    corrected_value = apply_correction(validated_value)
    store_result(corrected_value)


measurement_correction_decorator()
# [END example_measurement_correction_decorator]