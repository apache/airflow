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
Example Airflow DAG that shows how to use Google Dataprep.
"""

from airflow import models
from airflow.providers.google.cloud.operators.dataprep import DataprepGetJobsForJobGroupOperator
from airflow.utils import dates

JOB_ID = 6269792

with models.DAG(
    "example_dataprep",
    schedule_interval=None,  # Override to match your needs
    start_date=dates.days_ago(1)
) as dag:

    # [START how_to_dataprep_get_jobs_for_job_group_operator]
    get_jobs_for_job_group = DataprepGetJobsForJobGroupOperator(
        task_id="get_jobs_for_job_group", job_id=JOB_ID
    )
    # [END how_to_dataprep_get_jobs_for_job_group_operator]
