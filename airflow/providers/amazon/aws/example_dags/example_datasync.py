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

import json
import re
from datetime import datetime
from os import getenv

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.datasync import DataSyncOperator

TASK_ARN = getenv("TASK_ARN", "my_aws_datasync_task_arn")
SOURCE_LOCATION_URI = getenv("SOURCE_LOCATION_URI", "smb://hostname/directory/")
DESTINATION_LOCATION_URI = getenv("DESTINATION_LOCATION_URI", "s3://mybucket/prefix")
CREATE_TASK_KWARGS = json.loads(getenv("CREATE_TASK_KWARGS", '{"Name": "Created by Airflow"}'))
CREATE_SOURCE_LOCATION_KWARGS = json.loads(getenv("CREATE_SOURCE_LOCATION_KWARGS", '{}'))
default_destination_location_kwargs = """\
{"S3BucketArn": "arn:aws:s3:::mybucket",
    "S3Config": {"BucketAccessRoleArn":
    "arn:aws:iam::11112223344:role/r-11112223344-my-bucket-access-role"}
}"""
CREATE_DESTINATION_LOCATION_KWARGS = json.loads(
    getenv("CREATE_DESTINATION_LOCATION_KWARGS", re.sub(r"[\s+]", '', default_destination_location_kwargs))
)
UPDATE_TASK_KWARGS = json.loads(getenv("UPDATE_TASK_KWARGS", '{"Name": "Updated by Airflow"}'))

with models.DAG(
    "example_datasync",
    schedule_interval=None,  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_datasync_specific_task]
    # Execute a specific task
    datasync_specific_task = DataSyncOperator(task_id="datasync_specific_task", task_arn=TASK_ARN)
    # [END howto_operator_datasync_specific_task]

    # [START howto_operator_datasync_search_task]
    # Search and execute a task
    datasync_search_task = DataSyncOperator(
        task_id="datasync_search_task",
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI,
    )
    # [END howto_operator_datasync_search_task]

    # [START howto_operator_datasync_create_task]
    # Create a task (the task does not exist)
    datasync_create_task = DataSyncOperator(
        task_id="datasync_create_task",
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI,
        create_task_kwargs=CREATE_TASK_KWARGS,
        create_source_location_kwargs=CREATE_SOURCE_LOCATION_KWARGS,
        create_destination_location_kwargs=CREATE_DESTINATION_LOCATION_KWARGS,
        update_task_kwargs=UPDATE_TASK_KWARGS,
        delete_task_after_execution=True,
    )
    # [END howto_operator_datasync_create_task]

    chain(
        datasync_specific_task,
        datasync_search_task,
        datasync_create_task,
    )
