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
Simple DAG with setup and teardown operators.

It checks:
    - if setup and teardown information is included in DAG event for all tasks
    - if setup and teardown information is included in task events for AF2
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def do_nothing():
    pass


DAG_ID = "openlineage_setup_teardown_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    create_cluster = PythonOperator(task_id="create_cluster", python_callable=do_nothing)
    run_query = PythonOperator(task_id="run_query", python_callable=do_nothing)
    run_query2 = PythonOperator(task_id="run_query2", python_callable=do_nothing)
    delete_cluster = PythonOperator(task_id="delete_cluster", python_callable=do_nothing)

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    (
        create_cluster
        >> run_query
        >> run_query2
        >> delete_cluster.as_teardown(setups=create_cluster)
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
