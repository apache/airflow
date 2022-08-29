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
This is an example dag that performs two refresh operations on a Tableau Workbook aka Extract. The first one
waits until it succeeds. The second does not wait since this is an asynchronous operation and we don't know
when the operation actually finishes. That's why we have another task that checks only that.
"""
import os
from datetime import datetime, timedelta
from typing import cast

from airflow import DAG
from airflow.providers.tableau.operators.tableau import TableauOperator
from airflow.providers.tableau.sensors.tableau import TableauJobStatusSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_tableau_refresh_workbook"

with DAG(
    dag_id=DAG_ID,
    dagrun_timeout=timedelta(hours=2),
    schedule=None,
    start_date=datetime(2021, 1, 1),
    default_args={'site_id': 'my_site'},
    tags=['example'],
) as dag:
    # Refreshes a workbook and waits until it succeeds.
    task_refresh_workbook_blocking = TableauOperator(
        resource='workbooks',
        method='refresh',
        find='MyWorkbook',
        match_with='name',
        blocking_refresh=True,
        task_id='refresh_tableau_workbook_blocking',
    )
    # Refreshes a workbook and does not wait until it succeeds.
    task_refresh_workbook_non_blocking = TableauOperator(
        resource='workbooks',
        method='refresh',
        find='MyWorkbook',
        match_with='name',
        blocking_refresh=False,
        task_id='refresh_tableau_workbook_non_blocking',
    )
    # The following task queries the status of the workbook refresh job until it succeeds.
    task_check_job_status = TableauJobStatusSensor(
        job_id=cast(str, task_refresh_workbook_non_blocking.output),
        task_id='check_tableau_job_status',
    )

    # Task dependency created via XComArgs:
    #   task_refresh_workbook_non_blocking >> task_check_job_status


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
