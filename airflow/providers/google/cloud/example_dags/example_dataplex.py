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
Example Airflow DAG that shows how to use Dataplex.
"""

import datetime
import os

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateTaskOperator,
    DataplexDeleteTaskOperator,
    DataplexGetTaskOperator,
    DataplexListTasksOperator,
)
from airflow.providers.google.cloud.sensors.dataplex import DataplexTaskStateSensor

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "INVALID PROJECT ID")
REGION = os.environ.get("GCP_REGION", "INVALID REGION")
LAKE_ID = os.environ.get("GCP_LAKE_ID", "INVALID LAKE ID")
SERVICE_ACC = os.environ.get("GCP_DATAPLEX_SERVICE_ACC", "XYZ@developer.gserviceaccount.com")
BUCKET = os.environ.get("GCP_DATAPLEX_BUCKET", "INVALID BUCKET NAME")
SPARK_FILE_NAME = os.environ.get("SPARK_FILE_NAME", "INVALID FILE NAME")
SPARK_FILE_FULL_PATH = f"gs://{BUCKET}/{SPARK_FILE_NAME}"
DATAPLEX_TASK_ID = "task001"
TRIGGER_SPEC_TYPE = "ON_DEMAND"

# [START howto_dataplex_configuration]
EXAMPLE_TASK_BODY = {
    "trigger_spec": {"type_": TRIGGER_SPEC_TYPE},
    "execution_spec": {"service_account": SERVICE_ACC},
    "spark": {"python_script_file": SPARK_FILE_FULL_PATH},
}
# [END howto_dataplex_configuration]

with models.DAG(
    "example_dataplex",
    start_date=datetime.datetime(2021, 1, 1),
) as dag:
    # [START howto_dataplex_create_task_operator]
    create_dataplex_task = DataplexCreateTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_TASK_BODY,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="create_dataplex_task",
    )
    # [END howto_dataplex_create_task_operator]

    # [START howto_dataplex_async_create_task_operator]
    create_dataplex_task_async = DataplexCreateTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_TASK_BODY,
        dataplex_task_id=DATAPLEX_TASK_ID,
        asynchronous=True,
        task_id="create_dataplex_task_async",
    )
    # [END howto_dataplex_async_create_task_operator]

    # [START howto_dataplex_delete_task_operator]
    delete_dataplex_task = DataplexDeleteTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="delete_dataplex_task",
    )
    # [END howto_dataplex_delete_task_operator]

    # [START howto_dataplex_list_tasks_operator]
    list_dataplex_task = DataplexListTasksOperator(
        project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, task_id="list_dataplex_task"
    )
    # [END howto_dataplex_list_tasks_operator]

    # [START howto_dataplex_get_task_operator]
    get_dataplex_task = DataplexGetTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="get_dataplex_task",
    )
    # [END howto_dataplex_get_task_operator]

    # [START howto_dataplex_task_state_sensor]
    dataplex_task_state = DataplexTaskStateSensor(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="dataplex_task_state",
    )
    # [END howto_dataplex_task_state_sensor]

    chain(
        create_dataplex_task,
        get_dataplex_task,
        list_dataplex_task,
        delete_dataplex_task,
        create_dataplex_task_async,
        dataplex_task_state,
    )
