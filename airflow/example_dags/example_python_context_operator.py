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
Example DAG demonstrating the usage of the PythonOperator with `get_current_context()` to get the current context.

Also, demonstrates the usage of the classic Python operators.
"""

from __future__ import annotations

import sys

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
)

SOME_EXTERNAL_PYTHON = sys.executable

with DAG(
    dag_id="example_python_context_operator",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    # [START get_current_context]
    def print_context() -> str:
        """Print the Airflow context."""
        from pprint import pprint

        from airflow.providers.standard.operators.python import get_current_context

        context = get_current_context()
        pprint(context)
        return "Whatever you return gets printed in the logs"

    print_the_context = PythonOperator(task_id="print_the_context", python_callable=print_context)
    # [END get_current_context]

    # [START get_current_context_venv]
    def print_context_venv() -> str:
        """Print the Airflow context in venv."""
        from pprint import pprint

        from airflow.providers.standard.operators.python import get_current_context

        context = get_current_context()
        pprint(context)
        return "Whatever you return gets printed in the logs"

    print_the_context_venv = PythonVirtualenvOperator(
        task_id="print_the_context_venv", python_callable=print_context_venv, use_airflow_context=True
    )
    # [END get_current_context_venv]

    # [START get_current_context_external]
    def print_context_external() -> str:
        """Print the Airflow context in external python."""
        from pprint import pprint

        from airflow.providers.standard.operators.python import get_current_context

        context = get_current_context()
        pprint(context)
        return "Whatever you return gets printed in the logs"

    print_the_context_external = ExternalPythonOperator(
        task_id="print_the_context_external",
        python_callable=print_context_external,
        python=SOME_EXTERNAL_PYTHON,
        use_airflow_context=True,
    )
    # [END get_current_context_external]

    _ = print_the_context >> [print_the_context_venv, print_the_context_external]
