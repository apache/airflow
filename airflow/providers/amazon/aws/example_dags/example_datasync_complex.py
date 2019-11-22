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
This is an example dag for using some of the the AWS DataSync operators in a more complex manner.

- Try to get a TaskArn. If one exists, update it.
- If no tasks exist, try to create a new DataSync Task.
    - If source and destination locations dont exist for the new task, create them first
- If many tasks exist, raise an Exception
- After getting or creating a DataSync Task, run it

Specific operators used:
* `AWSDataSyncCreateTaskOperator`
* `AWSDataSyncGetTasksOperator`
* `AWSDataSyncTaskOperator`
* `AWSDataSyncUpdateTaskOperator`

This DAG relies on the following environment variables:

* SOURCE_LOCATION_URI - Source location URI, usually on premisis SMB or NFS
* DESTINATION_LOCATION_URI - Destination location URI, usually S3
* CREATE_TASK_KWARGS - Passed to boto3.create_task(**kwargs)
* CREATE_SOURCE_LOCATION_KWARGS - Passed to boto3.create_location(**kwargs)
* CREATE_DESTINATION_LOCATION_KWARGS - Passed to boto3.create_location(**kwargs)
* UPDATE_TASK_KWARGS - Passed to boto3.update_task(**kwargs)
"""

import json
from os import getenv

import airflow
from airflow import models
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.datasync import (
    AWSDataSyncCreateTaskOperator,
    AWSDataSyncGetTasksOperator,
    AWSDataSyncTaskOperator,
    AWSDataSyncUpdateTaskOperator,
)

# [START howto_operator_datasync_complex_args]
SOURCE_LOCATION_URI = getenv("SOURCE_LOCATION_URI", "smb://hostname/directory/")

DESTINATION_LOCATION_URI = getenv("DESTINATION_LOCATION_URI", "s3://mybucket/prefix")

default_create_task_kwargs = '{"Name": "Created by Airflow"}'
CREATE_TASK_KWARGS = json.loads(
    getenv("CREATE_TASK_KWARGS", default_create_task_kwargs)
)

default_create_source_location_kwargs = "{}"
CREATE_SOURCE_LOCATION_KWARGS = json.loads(
    getenv("CREATE_SOURCE_LOCATION_KWARGS", default_create_source_location_kwargs)
)

bucket_access_role_arn = (
    "arn:aws:iam::11112223344:role/r-11112223344-my-bucket-access-role"
)
default_destination_location_kwargs = """\
{"S3BucketArn": "arn:aws:s3:::mybucket",
    "S3Config": {"BucketAccessRoleArn": bucket_access_role_arn}
}"""
CREATE_DESTINATION_LOCATION_KWARGS = json.loads(
    getenv("CREATE_DESTINATION_LOCATION_KWARGS", default_destination_location_kwargs)
)

default_update_task_kwargs = '{"Name": "Updated by Airflow"}'
UPDATE_TASK_KWARGS = json.loads(
    getenv("UPDATE_TASK_KWARGS", default_update_task_kwargs)
)

default_args = {"start_date": airflow.utils.dates.days_ago(1)}
# [END howto_operator_datasync_complex_args]


# [START howto_operator_datasync_complex_decide_function]
def decide(**kwargs):
    """
    Decide whether to create a new DataSync Task or update an existing one, depending
    on what we managed to get from AWS DataSync.
    """
    # Get the task instance (dag_id + task_id + execution_date)
    ti = kwargs["ti"]
    datasync_tasks = ti.xcom_pull(key=None, task_ids="get_tasks")
    # Got no tasks so lets create one next and then run it
    if not datasync_tasks:
        return "create_task"
    # Got one task so lets update it next and then run it
    if len(datasync_tasks) == 1:
        return "update_task"
    # Got more than one task - Error
    # Or we could just randomly select a task from the suitable tasks
    raise AirflowException("Got too many datasync tasks! %s" % datasync_tasks)


# [END howto_operator_datasync_complex_decide_function]


# [START howto_operator_datasync_complex_join_function]
def join(**kwargs):
    """
    After creating or updating a task, we need to collect the task that was
    created or updated and pass it along to be executed
    """
    # Get the task instance (dag_id + task_id + execution_date)
    ti = kwargs["ti"]
    # The decide_task will output which task (create/update) to run
    decision = ti.xcom_pull(key=None, task_ids="decide_task")
    # Get the result from the create/update task and pass it along
    return ti.xcom_pull(key=None, task_ids=decision)


# [END howto_operator_datasync_complex_join_function]


with models.DAG(
    "example_datasync_complex",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
) as dag:

    # [START howto_operator_datasync_complex_get_tasks]
    get_tasks = AWSDataSyncGetTasksOperator(
        aws_conn_id="aws_default",
        task_id="get_tasks",
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI,
    )
    # [END howto_operator_datasync_complex_get_tasks]

    # [START howto_operator_datasync_complex_decide_task]
    decide_task = BranchPythonOperator(
        task_id="decide_task", provide_context=True, python_callable=decide
    )
    # [END howto_operator_datasync_complex_decide_task]

    # [START howto_operator_datasync_complex_create_task]
    create_task = AWSDataSyncCreateTaskOperator(
        aws_conn_id="aws_default",
        task_id="create_task",
        source_location_uri=SOURCE_LOCATION_URI,
        destination_location_uri=DESTINATION_LOCATION_URI,
        create_task_kwargs=CREATE_TASK_KWARGS,
        create_source_location_kwargs=CREATE_SOURCE_LOCATION_KWARGS,
        create_destination_location_kwargs=CREATE_DESTINATION_LOCATION_KWARGS,
    )
    # [END howto_operator_datasync_complex_create_task]

    # [START howto_operator_datasync_complex_update_task]
    update_task = AWSDataSyncUpdateTaskOperator(
        aws_conn_id="aws_default",
        task_id="update_task",
        task_arn="{{ ti.xcom_pull(task_ids='get_tasks')[0] }}",
        update_task_kwargs=UPDATE_TASK_KWARGS,
    )
    # [END howto_operator_datasync_complex_update_task]

    # [START howto_operator_datasync_complex_join_task]
    join_task = PythonOperator(
        task_id="join_task",
        provide_context=True,
        python_callable=join,
        trigger_rule="none_failed",
    )
    # [END howto_operator_datasync_complex_join_task]

    # [START howto_operator_datasync_complex_run_task]
    run_task = AWSDataSyncTaskOperator(
        task_id="run_task", task_arn="{{ ti.xcom_pull(task_ids='join_task') }}"
    )
    # [END howto_operator_datasync_complex_run_task]

    # [START howto_operator_datasync_complex_dependencies]
    get_tasks >> decide_task
    decide_task >> [create_task, update_task]
    [create_task, update_task] >> join_task
    join_task >> run_task
    # [END howto_operator_datasync_complex_dependencies]
