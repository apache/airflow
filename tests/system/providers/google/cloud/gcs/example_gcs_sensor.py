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

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceAsyncSensor,
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "gcs_sensor"

RESOURCES_BUCKET_NAME = "airflow-system-tests-resources"
DESTINATION_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE_PATH = f"gcs/{FILE_NAME}"


def workaround_in_debug_executor(cls):
    """
    DebugExecutor change sensor mode from poke to reschedule. Some sensors don't work correctly
    in reschedule mode. They are decorated with `poke_mode_only` decorator to fail when mode is changed.
    This method creates dummy property to overwrite it and force poke method to always return True.
    """
    cls.mode = dummy_mode_property()
    cls.poke = lambda self, context: True


def dummy_mode_property():
    def mode_getter(self):
        return self._mode

    def mode_setter(self, value):
        self._mode = value

    return property(mode_getter, mode_setter)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "example"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=DESTINATION_BUCKET_NAME, project_id=PROJECT_ID
    )

    workaround_in_debug_executor(GCSUploadSessionCompleteSensor)

    # [START howto_sensor_gcs_upload_session_complete_task]
    gcs_upload_session_complete = GCSUploadSessionCompleteSensor(
        bucket=DESTINATION_BUCKET_NAME,
        prefix=FILE_NAME,
        inactivity_period=15,
        min_objects=1,
        allow_delete=True,
        previous_objects=set(),
        task_id="gcs_upload_session_complete_task",
    )
    # [END howto_sensor_gcs_upload_session_complete_task]

    # [START howto_sensor_gcs_upload_session_async_task]
    gcs_upload_session_async_complete = GCSUploadSessionCompleteSensor(
        bucket=DESTINATION_BUCKET_NAME,
        prefix=FILE_NAME,
        inactivity_period=15,
        min_objects=1,
        allow_delete=True,
        previous_objects=set(),
        task_id="gcs_upload_session_async_complete",
        deferrable=True,
    )
    # [END howto_sensor_gcs_upload_session_async_task]

    # [START howto_sensor_object_update_exists_task]
    gcs_update_object_exists = GCSObjectUpdateSensor(
        bucket=DESTINATION_BUCKET_NAME,
        object=FILE_NAME,
        task_id="gcs_object_update_sensor_task",
    )
    # [END howto_sensor_object_update_exists_task]

    # [START howto_sensor_object_update_exists_task_async]
    gcs_update_object_exists_async = GCSObjectUpdateSensor(
        bucket=DESTINATION_BUCKET_NAME,
        object=FILE_NAME,
        task_id="gcs_object_update_sensor_task_async",
        deferrable=True,
    )
    # [END howto_sensor_object_update_exists_task_async]

    copy_file = GCSToGCSOperator(
        task_id="copy_example_gcs_file",
        source_bucket=RESOURCES_BUCKET_NAME,
        source_object=UPLOAD_FILE_PATH,
        destination_bucket=DESTINATION_BUCKET_NAME,
        destination_object=FILE_NAME,
        exact_match=True,
    )

    # [START howto_sensor_object_exists_task]
    gcs_object_exists = GCSObjectExistenceSensor(
        bucket=DESTINATION_BUCKET_NAME,
        object=FILE_NAME,
        task_id="gcs_object_exists_task",
    )
    # [END howto_sensor_object_exists_task]

    # [START howto_sensor_object_exists_task_async]
    gcs_object_exists_async = GCSObjectExistenceAsyncSensor(
        bucket=DESTINATION_BUCKET_NAME,
        object=FILE_NAME,
        task_id="gcs_object_exists_task_async",
    )
    # [END howto_sensor_object_exists_task_async]

    # [START howto_sensor_object_exists_task_defered]
    gcs_object_exists_defered = GCSObjectExistenceSensor(
        bucket=DESTINATION_BUCKET_NAME, object=FILE_NAME, task_id="gcs_object_exists_defered", deferrable=True
    )
    # [END howto_sensor_object_exists_task_defered]

    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
        bucket=DESTINATION_BUCKET_NAME,
        prefix=FILE_NAME[:5],
        task_id="gcs_object_with_prefix_exists_task",
    )
    # [END howto_sensor_object_with_prefix_exists_task]

    # [START howto_sensor_object_with_prefix_exists_task_async]
    gcs_object_with_prefix_exists_async = GCSObjectsWithPrefixExistenceSensor(
        bucket=DESTINATION_BUCKET_NAME,
        prefix=FILE_NAME[:5],
        task_id="gcs_object_with_prefix_exists_task_async",
        deferrable=True,
    )
    # [END howto_sensor_object_with_prefix_exists_task_async]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=DESTINATION_BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        create_bucket,
        copy_file,
        # TEST BODY
        [
            gcs_object_exists,
            gcs_object_exists_defered,
            gcs_object_exists_async,
            gcs_object_with_prefix_exists,
            gcs_object_with_prefix_exists_async,
        ],
        # TEST TEARDOWN
        delete_bucket,
    )
    chain(
        create_bucket,
        # TEST BODY
        gcs_upload_session_complete,
        gcs_update_object_exists,
        gcs_update_object_exists_async,
        delete_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
