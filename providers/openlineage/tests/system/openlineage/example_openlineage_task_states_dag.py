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
DAG with tasks whose events only taskInstance's `state` tells apart (requires Airflow 3+).

It checks:
    - a task that skips itself emits START with `state` running, and COMPLETE with `state` skipped, which
      a successful task's COMPLETE would carry as success
      (Airflow 3.0 calls no listener when a task skips itself, so it emits no COMPLETE there, and its lone
      START isn't checked)
    - a task that raises AirflowFailException with retries left emits START with `state` running, and FAIL
      with `state` failed at `try_number` 1 and `max_tries` 2: it isn't retried although
      `try_number <= max_tries`
    - taskInstance's `dag_version_id` of every event, from Airflow 3.1 on
    - every task event's identity, as `system.openlineage.expected_events` describes it, and each task in the Dag's job facet
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.sdk import AirflowFailException, AirflowSkipException
from airflow.sdk import task

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_task_states_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:

    @task
    def skip_itself():
        raise AirflowSkipException("Intentional skip")

    @task(retries=2)
    def fail_without_retry():
        raise AirflowFailException("Intentional failure that skips the retries left")

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        trigger_rule="all_done",
    )

    [skip_itself(), fail_without_retry()] >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
