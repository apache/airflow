# -*- coding: utf-8 -*-
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
This is an example dag for using some of the the AWS DataSync operators in a simple manner.

This DAG gets an AWS TaskArn for a specified source and destination, and then attempts to execute it.
It assumes there is a single task returned and does not do error checking (eg if multiple tasks were found).

Specific operators used:
* `AWSDataSyncGetTasksOperator`
* `AWSDataSyncTaskOperator`

This DAG relies on the following environment variables:

* SOURCE_LOCATION_URI - Source location URI, usually on premisis SMB or NFS
* DESTINATION_LOCATION_URI - Destination location URI, usually S3
"""

from os import getenv

import airflow
from airflow import models
from airflow.providers.amazon.aws.operators.datasync import (
    AWSDataSyncGetTasksOperator,
    AWSDataSyncTaskOperator,
)

# [START howto_operator_datasync_simple_args]
SOURCE_LOCATION_URI = getenv("SOURCE_LOCATION_URI", "smb://hostname/directory/")

DESTINATION_LOCATION_URI = getenv("DESTINATION_LOCATION_URI", "s3://mybucket/prefix")
# [END howto_operator_datasync_simple_args]

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

with models.DAG(
    "example_datasync_simple",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
) as dag:

    # [START howto_operator_datasync_simple_get_tasks]
    get_tasks = AWSDataSyncGetTasksOperator(
        aws_conn_id="aws_default",
        task_id="get_tasks",
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI,
    )
    # [END howto_operator_datasync_simple_get_tasks]

    # [START howto_operator_datasync_simple_run_task]
    run_task = AWSDataSyncTaskOperator(
        task_id="run_task", task_arn="{{ ti.xcom_pull(task_ids='get_tasks')[0] }}",
    )
    # [END howto_operator_datasync_simple_run_task]

    # [START howto_operator_datasync_simple_dependencies]
    get_tasks >> run_task
    # [END howto_operator_datasync_simple_dependencies]
