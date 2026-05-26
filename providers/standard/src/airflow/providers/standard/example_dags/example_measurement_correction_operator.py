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
Tutorial example Dag: measurement correction storyline (PythonOperator).

Classic, ``PythonOperator`` based counterpart of
``example_measurement_correction_decorator.py``. It runs the same
"read, validate, correct, store" pipeline so that learners can compare the
TaskFlow API and the operator-based style on identical business logic.

The Dag has no external dependencies (no connections, no datasets, no hooks),
so it parses and runs out of the box and is suitable as a tutorial or as a
documentation snippet.
"""

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DAG_DOC_MD = """
### Measurement correction (PythonOperator)

Tutorial Dag showing a minimal "read, validate, correct, store" measurement
pipeline implemented with classic ``PythonOperator`` tasks and XCom for
inter-task communication.

**Storyline**

1. `read_measurement` returns a raw value.
2. `validate_measurement` pulls it from XCom and rejects negative values.
3. `apply_correction` multiplies it by a calibration factor.
4. `store_result` logs the corrected value.

**When to use this example**

- Comparing the TaskFlow API with the classic operator style on the same
  storyline (see ``example_measurement_correction_decorator.py``).
- As a reference shape for new "tutorial" example Dags following the
  [example Dag review checklist](
  https://github.com/apache/airflow/blob/main/contributing-docs/28_example_dag_review_checklist.rst).
"""


def read_measurement(**context):
    """Return the raw measurement value pushed to XCom.

    The returned value becomes the task's XCom payload so the downstream
    tasks can pull it with ``ti.xcom_pull``.
    """
    return 100


def validate_measurement(**context):
    """Validate the upstream measurement pulled from XCom.

    Raises ``ValueError`` when the value is negative so the failure is
    visible as a failed task in the UI instead of silently corrupting
    downstream data.
    """
    value = context["ti"].xcom_pull(task_ids="read_measurement")
    if value < 0:
        raise ValueError("Measurement must be positive")
    return value


def apply_correction(**context):
    """Apply the calibration factor to a validated measurement.

    The factor (``1.1``) is hard-coded for brevity; a production pipeline
    would source it from a Variable, a Connection extra, or a config file.
    """
    value = context["ti"].xcom_pull(task_ids="validate_measurement")
    return value * 1.1


def store_result(**context):
    """Persist the corrected measurement.

    The tutorial implementation logs the value via ``print`` so the result
    is visible in the task logs. A production Dag would write it to a
    database, an object store, or publish it to a downstream system.
    """
    value = context["ti"].xcom_pull(task_ids="apply_correction")
    print(f"Corrected measurement: {value}")


# [START example_measurement_correction_operator]
with DAG(
    dag_id="example_measurement_correction_operator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
    doc_md=DAG_DOC_MD,
) as dag:
    read = PythonOperator(
        task_id="read_measurement",
        python_callable=read_measurement,
    )

    validate = PythonOperator(
        task_id="validate_measurement",
        python_callable=validate_measurement,
    )

    correct = PythonOperator(
        task_id="apply_correction",
        python_callable=apply_correction,
    )

    store = PythonOperator(
        task_id="store_result",
        python_callable=store_result,
    )

    read >> validate >> correct >> store
# [END example_measurement_correction_operator]
