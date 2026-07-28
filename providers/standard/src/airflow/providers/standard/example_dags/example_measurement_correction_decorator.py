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
Tutorial example Dag: measurement correction storyline (TaskFlow).

This Dag is part of the "Examples Refurbish" storyline and is meant to be a
short, didactic walkthrough of a typical scientific or industrial data
pipeline. It demonstrates the TaskFlow API on a self-contained workflow that:

1. reads a raw measurement,
2. validates it,
3. applies a correction factor,
4. stores the corrected result.

The Dag has no external dependencies (no connections, no datasets, no hooks),
so it parses and runs out of the box. Pair it with
``example_measurement_correction_operator.py`` to compare TaskFlow and the
classic ``PythonOperator`` style on the exact same storyline.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import dag, task

DAG_DOC_MD = """
### Measurement correction (TaskFlow)

Tutorial Dag showing a minimal "read, validate, correct, store" measurement
pipeline implemented with the TaskFlow API.

**Storyline**

1. `read_measurement` produces a raw value coming from a fictional sensor.
2. `validate_measurement` rejects negative values.
3. `apply_correction` multiplies the value by a calibration factor.
4. `store_result` logs the corrected value (the real Dag would persist it).

**When to use this example**

- Learning the TaskFlow API on a single, linear pipeline.
- As a reference shape for new "tutorial" example Dags following the
  [example Dag review checklist](
  https://github.com/apache/airflow/blob/main/contributing-docs/28_example_dag_review_checklist.rst).
"""


# [START example_measurement_correction_decorator]
@dag(
    dag_id="example_measurement_correction_decorator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
    doc_md=DAG_DOC_MD,
)
def measurement_correction_decorator():
    """Tutorial Dag: read, validate, correct, and store a measurement."""

    @task
    def read_measurement() -> int:
        """Return the raw measurement coming from the source system.

        In a real pipeline this would fetch the value from a sensor, an
        external API, or an upstream dataset. For the tutorial it returns a
        constant so the Dag is fully self-contained.
        """
        return 100

    @task
    def validate_measurement(value: int) -> int:
        """Reject obviously invalid measurements.

        Raises ``ValueError`` when ``value`` is negative, so the failure is
        visible as a failed task in the UI instead of silently corrupting
        downstream data.
        """
        if value < 0:
            raise ValueError("Measurement must be positive")
        return value

    @task
    def apply_correction(value: int) -> float:
        """Apply the calibration factor to a validated measurement.

        The factor (``1.1``) is hard-coded here for brevity; a real pipeline
        would read it from a Variable, a Connection extra, or a config file.
        """
        correction_factor = 1.1
        return value * correction_factor

    @task
    def store_result(value: float) -> None:
        """Persist the corrected measurement.

        The tutorial implementation logs the value via ``print`` so the
        result is visible in the task logs. A production Dag would write it
        to a database, an object store, or publish it to a downstream
        system.
        """
        print(f"Corrected measurement: {value}")

    raw_value = read_measurement()
    validated_value = validate_measurement(raw_value)
    corrected_value = apply_correction(validated_value)
    store_result(corrected_value)


measurement_correction_decorator()
# [END example_measurement_correction_decorator]
