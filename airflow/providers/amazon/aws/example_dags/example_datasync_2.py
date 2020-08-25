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
This is an example dag for using `AWSDataSyncOperator` in a more complex manner.

- Try to get a TaskArn. If one exists, update it.
- If no tasks exist, try to create a new DataSync Task.
    - If source and destination locations dont exist for the new task, create them first
- If many tasks exist, raise an Exception
- After getting or creating a DataSync Task, run it

This DAG relies on the following environment variables:

* SOURCE_LOCATION_URI - Source location URI, usually on premisis SMB or NFS
* DESTINATION_LOCATION_URI - Destination location URI, usually S3
* CREATE_TASK_KWARGS - Passed to boto3.create_task(**kwargs)
* CREATE_SOURCE_LOCATION_KWARGS - Passed to boto3.create_location(**kwargs)
* CREATE_DESTINATION_LOCATION_KWARGS - Passed to boto3.create_location(**kwargs)
* UPDATE_TASK_KWARGS - Passed to boto3.update_task(**kwargs)
"""

import json
import re
from os import getenv

from airflow import models
from airflow.providers.amazon.aws.operators.datasync import AWSDataSyncOperator
from airflow.utils.dates import days_ago

# [START howto_operator_datasync_2_args]
SOURCE_LOCATION_URI = getenv("SOURCE_LOCATION_URI", "smb://hostname/directory/")

DESTINATION_LOCATION_URI = getenv("DESTINATION_LOCATION_URI", "s3://mybucket/prefix")

default_create_task_kwargs = '{"Name": "Created by Airflow"}'
CREATE_TASK_KWARGS = json.loads(getenv("CREATE_TASK_KWARGS", default_create_task_kwargs))

default_create_source_location_kwargs = "{}"
CREATE_SOURCE_LOCATION_KWARGS = json.loads(
    getenv("CREATE_SOURCE_LOCATION_KWARGS", default_create_source_location_kwargs)
)

bucket_access_role_arn = "arn:aws:iam::11112223344:role/r-11112223344-my-bucket-access-role"
default_destination_location_kwargs = """\
{"S3BucketArn": "arn:aws:s3:::mybucket",
    "S3Config": {"BucketAccessRoleArn":
    "arn:aws:iam::11112223344:role/r-11112223344-my-bucket-access-role"}
}"""
CREATE_DESTINATION_LOCATION_KWARGS = json.loads(
    getenv("CREATE_DESTINATION_LOCATION_KWARGS", re.sub(r"[\s+]", '', default_destination_location_kwargs))
)

default_update_task_kwargs = '{"Name": "Updated by Airflow"}'
UPDATE_TASK_KWARGS = json.loads(getenv("UPDATE_TASK_KWARGS", default_update_task_kwargs))

# [END howto_operator_datasync_2_args]

with models.DAG(
    "example_datasync_2",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    # [START howto_operator_datasync_2]
    datasync_task = AWSDataSyncOperator(
        aws_conn_id="aws_default",
        task_id="datasync_task",
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI,
        create_task_kwargs=CREATE_TASK_KWARGS,
        create_source_location_kwargs=CREATE_SOURCE_LOCATION_KWARGS,
        create_destination_location_kwargs=CREATE_DESTINATION_LOCATION_KWARGS,
        update_task_kwargs=UPDATE_TASK_KWARGS,
        delete_task_after_execution=True,
    )
    # [END howto_operator_datasync_2]
