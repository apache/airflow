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
Example Airflow DAG for Google Cloud Storage sensors.
"""

import os
from datetime import datetime
from pathlib import Path

from airflow import models
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "gcs_sensor"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "example"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    # [START howto_sensor_gcs_upload_session_complete_task]
    gcs_upload_session_complete = GCSUploadSessionCompleteSensor(
        bucket=BUCKET_NAME,
        prefix=FILE_NAME,
        inactivity_period=15,
        min_objects=1,
        allow_delete=True,
        previous_objects=set(),
        task_id="gcs_upload_session_complete_task",
    )
    # [END howto_sensor_gcs_upload_session_complete_task]

    # [START howto_sensor_object_update_exists_task]
    gcs_update_object_exists = GCSObjectUpdateSensor(
        bucket=BUCKET_NAME,
        object=FILE_NAME,
        task_id="gcs_object_update_sensor_task",
    )
    # [END howto_sensor_object_update_exists_task]

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=UPLOAD_FILE_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )

    # [START howto_sensor_object_exists_task]
    gcs_object_exists = GCSObjectExistenceSensor(
        bucket=BUCKET_NAME,
        object=FILE_NAME,
        mode='poke',
        task_id="gcs_object_exists_task",
    )
    # [END howto_sensor_object_exists_task]

    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
        bucket=BUCKET_NAME,
        prefix=FILE_NAME[:5],
        mode='poke',
        task_id="gcs_object_with_prefix_exists_task",
    )
    # [END howto_sensor_object_with_prefix_exists_task]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    sleep = BashOperator(task_id='sleep', bash_command='sleep 5')

    chain(
        # TEST SETUP
        create_bucket,
        sleep,
        upload_file,
        # TEST BODY
        [gcs_object_exists, gcs_object_with_prefix_exists],
        # TEST TEARDOWN
        delete_bucket,
    )
    chain(
        create_bucket,
        # TEST BODY
        gcs_upload_session_complete,
        gcs_update_object_exists,
        delete_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
