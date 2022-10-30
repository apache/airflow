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
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataprep import (
    DataprepGetJobGroupOperator,
    DataprepGetJobsForJobGroupOperator,
    DataprepRunJobGroupOperator,
)

DATAPREP_JOB_ID = int(os.environ.get("DATAPREP_JOB_ID", 12345677))
DATAPREP_JOB_RECIPE_ID = int(os.environ.get("DATAPREP_JOB_RECIPE_ID", 12345677))
DATAPREP_BUCKET = os.environ.get("DATAPREP_BUCKET", "gs://INVALID BUCKET NAME/name@email.com")

DATA = {
    "wrangledDataset": {"id": DATAPREP_JOB_RECIPE_ID},
    "overrides": {
        "execution": "dataflow",
        "profiler": False,
        "writesettings": [
            {
                "path": DATAPREP_BUCKET,
                "action": "create",
                "format": "csv",
                "compression": "none",
                "header": False,
                "asSingleFile": False,
            }
        ],
    },
}


with models.DAG(
    "example_dataprep",
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    catchup=False,
) as dag:
    # [START how_to_dataprep_run_job_group_operator]
    run_job_group = DataprepRunJobGroupOperator(task_id="run_job_group", body_request=DATA)
    # [END how_to_dataprep_run_job_group_operator]

    # [START how_to_dataprep_get_jobs_for_job_group_operator]
    get_jobs_for_job_group = DataprepGetJobsForJobGroupOperator(
        task_id="get_jobs_for_job_group", job_id=DATAPREP_JOB_ID
    )
    # [END how_to_dataprep_get_jobs_for_job_group_operator]

    # [START how_to_dataprep_get_job_group_operator]
    get_job_group = DataprepGetJobGroupOperator(
        task_id="get_job_group",
        job_group_id=DATAPREP_JOB_ID,
        embed="",
        include_deleted=False,
    )
    # [END how_to_dataprep_get_job_group_operator]

    run_job_group >> [get_jobs_for_job_group, get_job_group]
