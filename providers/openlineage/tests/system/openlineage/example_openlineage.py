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

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.operator import OpenLineageTestOperator


def do_nothing():
    pass


default_args = {"start_date": datetime(2021, 1, 1), "retries": 1}

# Instantiate the DAG
with DAG(
    "openlineage_basic_dag",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    nothing_task = PythonOperator(task_id="do_nothing_task", python_callable=do_nothing)

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=str(Path(__file__).parent / "example_openlineage.json"),
    )

    nothing_task >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
