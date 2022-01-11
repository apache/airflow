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

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
from airflow.utils.edgemodifier import Label


@dag(
    dag_id="example_dbt_cloud",
    default_args={"dbt_cloud_conn_id": "dbt", "account_id": 39151},
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
)
def example_dbt_cloud():
    [begin, end] = [DummyOperator(task_id=x) for x in ["begin", "end"]]

    # [START howto_operator_adf_run_pipeline]
    trigger_job_run1 = DbtCloudRunJobOperator(
        task_id="trigger_job_run1",
        job_id=48617,
        wait_for_termination=True,
        check_interval=10,
        timeout=20,
    )
    # [END howto_operator_adf_run_pipeline]

    # [START howto_operator_adf_run_pipeline_async]
    trigger_job_run2 = DbtCloudRunJobOperator(
        task_id="trigger_job_run2", job_id=48617, additional_run_config={"threads_override": 8}
    )

    job_run_sensor = DbtCloudJobRunSensor(
        task_id="job_run_sensor", run_id=trigger_job_run2.output, timeout=30
    )
    # [END howto_operator_adf_run_pipeline_async]

    begin >> Label("No async wait") >> trigger_job_run1
    begin >> Label("Do async wait with sensor") >> trigger_job_run2
    [trigger_job_run1, job_run_sensor] >> end

    # Task dependency created via `XComArgs`:
    # trigger_job_run2 >> job_run_sensor


dag = example_dbt_cloud()
