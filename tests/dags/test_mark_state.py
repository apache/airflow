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

import time
from datetime import datetime
from time import sleep

from airflow.models.dag import DAG
from airflow.providers.standard.core.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import utcnow

DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    "owner": "airflow",
    "start_date": DEFAULT_DATE,
}


dag_id = "test_mark_state"
dag = DAG(dag_id=dag_id, schedule=None, default_args=args)


def success_callback(context):
    assert context["dag_run"].dag_id == dag_id


def sleep_execution():
    time.sleep(1)


def slow_execution():
    import re

    re.match(r"(a?){30}a{30}", "a" * 30)


def test_mark_success_no_kill(ti):
    assert ti.state == State.RUNNING
    # Simulate marking this successful in the UI
    with create_session() as session:
        ti.state = State.SUCCESS
        ti.end_date = utcnow()
        session.merge(ti)
        session.commit()
        # The below code will not run as heartbeat will detect change of state
        sleep(10)


PythonOperator(
    task_id="test_mark_success_no_kill",
    python_callable=test_mark_success_no_kill,
    dag=dag,
    on_success_callback=success_callback,
)


def check_failure(context):
    assert context["dag_run"].dag_id == dag_id
    assert context["exception"] == "task marked as failed externally"


def test_mark_failure_externally(ti):
    assert State.RUNNING == ti.state
    with create_session() as session:
        ti.log.info("Marking TI as failed 'externally'")
        ti.state = State.FAILED
        session.merge(ti)
        session.commit()

    sleep(10)
    msg = "This should not happen -- the state change should be noticed and the task should get killed"
    raise RuntimeError(msg)


PythonOperator(
    task_id="test_mark_failure_externally",
    python_callable=test_mark_failure_externally,
    on_failure_callback=check_failure,
    dag=dag,
)


def test_mark_skipped_externally(ti):
    assert State.RUNNING == ti.state
    sleep(0.1)  # for timeout
    with create_session() as session:
        ti.log.info("Marking TI as failed 'externally'")
        ti.state = State.SKIPPED
        session.merge(ti)
        session.commit()

    sleep(10)
    msg = "This should not happen -- the state change should be noticed and the task should get killed"
    raise RuntimeError(msg)


PythonOperator(task_id="test_mark_skipped_externally", python_callable=test_mark_skipped_externally, dag=dag)

PythonOperator(task_id="dummy", python_callable=lambda: True, dag=dag)

PythonOperator(task_id="slow_execution", python_callable=slow_execution, dag=dag)

PythonOperator(task_id="sleep_execution", python_callable=sleep_execution, dag=dag)
