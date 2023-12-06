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

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.yeedu.operators.yeedu import YeeduJobRunOperator
from tests.system.utils.watcher import watcher

# Define default_args dictionary to specify the default parameters for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    "dag_system_test",
    default_args=default_args,
    description="DAG to submit Spark job to Yeedu",
    schedule_interval="@once",  # You can modify the schedule_interval as needed
    start_date=datetime(2023, 1, 1),
)

# YeeduOperator task to submit the Spark job
yeedu_task = YeeduJobRunOperator(
    task_id="dag_system_test",
    job_conf_id=1,
    token="",
    hostname="",
    workspace_id=3,
    dag=dag,
)

# This test needs watcher in order to properly mark success/failure
# when "tearDown" task with trigger rule is part of the DAG
list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
