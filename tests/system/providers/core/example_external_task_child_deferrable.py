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

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskMarker

with DAG(
    dag_id="wait_for_dag_child",
    start_date=datetime(2022, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "async", "core"],
) as dag:
    wait_for_task = ExternalTaskMarker(
        task_id="wait_for_task",
        external_dag_id="example_external_task",
        external_task_id="wait_for_task",
    )


from tests.system.utils import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
