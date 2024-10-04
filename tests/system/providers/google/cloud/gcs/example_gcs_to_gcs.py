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
Example Airflow DAG for Google Cloud Storage GCSSynchronizeBucketsOperator and
GCSToGCSOperator operators.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSListObjectsOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "gcs_to_gcs"

BUCKET_NAME_SRC = f"bucket_{DAG_ID}_{ENV_ID}"
BUCKET_NAME_DST = f"bucket_dst_{DAG_ID}_{ENV_ID}"
RANDOM_FILE_NAME = OBJECT_1 = OBJECT_2 = "random.bin"
HOME = "/home/airflow/gcs"
PREFIX = f"{HOME}/data/{DAG_ID}_{ENV_ID}/"


def _assert_copied_files_exist(ti):
    objects = ti.xcom_pull(task_ids=["list_objects"], key="return_value")[0]

    assert PREFIX + OBJECT_1 in objects
    assert f"{PREFIX}subdir/{OBJECT_1}" in objects
    assert f"backup/{OBJECT_1}" in objects
    assert f"backup_{OBJECT_1}" in objects


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "example"],
) as dag:

    @task
    def create_workdir() -> str:
        """
        Task creates working directory. The logic behind this task is a workaround that provides sustainable
        execution in Composer environment: local files can be safely shared among tasks if they are located
        within '/home/airflow/gcs/data/' folder which is mounted to GCS bucket under the hood
        (https://cloud.google.com/composer/docs/composer-2/cloud-storage).
        Otherwise, worker nodes don't share local path and thus files created by one task aren't guaranteed
        to be accessible be others.
        """
        workdir = PREFIX if os.path.exists(HOME) else HOME
        os.makedirs(PREFIX)
        return workdir

    create_workdir_task = create_workdir()

    generate_random_file = BashOperator(
        task_id="generate_random_file",
        bash_command=f"cat /dev/urandom | head -c $((1 * 1024 * 1024)) > {PREFIX + RANDOM_FILE_NAME}",
    )

    create_bucket_src = GCSCreateBucketOperator(
        task_id="create_bucket_src",
        bucket_name=BUCKET_NAME_SRC,
        project_id=PROJECT_ID,
    )

    create_bucket_dst = GCSCreateBucketOperator(
        task_id="create_bucket_dst",
        bucket_name=BUCKET_NAME_DST,
        project_id=PROJECT_ID,
    )

    upload_file_src = LocalFilesystemToGCSOperator(
        task_id="upload_file_src",
        src=PREFIX + RANDOM_FILE_NAME,
        dst=PREFIX + RANDOM_FILE_NAME,
        bucket=BUCKET_NAME_SRC,
    )

    upload_file_src_sub = LocalFilesystemToGCSOperator(
        task_id="upload_file_src_sub",
        src=PREFIX + RANDOM_FILE_NAME,
        dst=f"{PREFIX}subdir/{RANDOM_FILE_NAME}",
        bucket=BUCKET_NAME_SRC,
    )

    upload_file_dst = LocalFilesystemToGCSOperator(
        task_id="upload_file_dst",
        src=PREFIX + RANDOM_FILE_NAME,
        dst=PREFIX + RANDOM_FILE_NAME,
        bucket=BUCKET_NAME_DST,
    )

    upload_file_dst_sub = LocalFilesystemToGCSOperator(
        task_id="upload_file_dst_sub",
        src=PREFIX + RANDOM_FILE_NAME,
        dst=f"{PREFIX}subdir/{RANDOM_FILE_NAME}",
        bucket=BUCKET_NAME_DST,
    )

    # [START howto_synch_bucket]
    sync_bucket = GCSSynchronizeBucketsOperator(
        task_id="sync_bucket", source_bucket=BUCKET_NAME_SRC, destination_bucket=BUCKET_NAME_DST
    )
    # [END howto_synch_bucket]

    # [START howto_synch_full_bucket]
    sync_full_bucket = GCSSynchronizeBucketsOperator(
        task_id="sync_full_bucket",
        source_bucket=BUCKET_NAME_SRC,
        destination_bucket=BUCKET_NAME_DST,
        delete_extra_files=True,
        allow_overwrite=True,
    )
    # [END howto_synch_full_bucket]

    # [START howto_synch_to_subdir]
    sync_to_subdirectory = GCSSynchronizeBucketsOperator(
        task_id="sync_to_subdirectory",
        source_bucket=BUCKET_NAME_SRC,
        destination_bucket=BUCKET_NAME_DST,
        destination_object="subdir/",
    )
    # [END howto_synch_to_subdir]

    # [START howto_sync_from_subdir]
    sync_from_subdirectory = GCSSynchronizeBucketsOperator(
        task_id="sync_from_subdirectory",
        source_bucket=BUCKET_NAME_SRC,
        source_object="subdir/",
        destination_bucket=BUCKET_NAME_DST,
    )
    # [END howto_sync_from_subdir]

    # [START howto_operator_gcs_to_gcs_single_file]
    copy_single_file = GCSToGCSOperator(
        task_id="copy_single_gcs_file",
        source_bucket=BUCKET_NAME_SRC,
        source_object=PREFIX + OBJECT_1,
        destination_bucket=BUCKET_NAME_DST,  # If not supplied the source_bucket value will be used
        destination_object="backup_" + OBJECT_1,  # If not supplied the source_object value will be used
        exact_match=True,
    )
    # [END howto_operator_gcs_to_gcs_single_file]

    # [START howto_operator_gcs_to_gcs_without_wildcard]
    copy_files = GCSToGCSOperator(
        task_id="copy_files",
        source_bucket=BUCKET_NAME_SRC,
        source_object=PREFIX + "subdir/",
        destination_bucket=BUCKET_NAME_DST,
        destination_object="backup/",
    )
    # [END howto_operator_gcs_to_gcs_without_wildcard]

    # [START howto_operator_gcs_to_gcs_match_glob]
    copy_files_with_match_glob = GCSToGCSOperator(
        task_id="copy_files_with_match_glob",
        source_bucket=BUCKET_NAME_SRC,
        source_object="data/",
        destination_bucket=BUCKET_NAME_DST,
        destination_object="backup/",
        match_glob="**/*.txt",
    )
    # [END howto_operator_gcs_to_gcs_match_glob]

    # [START howto_operator_gcs_to_gcs_list]
    copy_files_with_list = GCSToGCSOperator(
        task_id="copy_files_with_list",
        source_bucket=BUCKET_NAME_SRC,
        source_objects=[
            PREFIX + OBJECT_1,
            PREFIX + OBJECT_2,
        ],  # Instead of files each element could be a wildcard expression
        destination_bucket=BUCKET_NAME_DST,
        destination_object="backup/",
    )
    # [END howto_operator_gcs_to_gcs_list]

    # [START howto_operator_gcs_to_gcs_single_file_move]
    move_single_file = GCSToGCSOperator(
        task_id="move_single_file",
        source_bucket=BUCKET_NAME_SRC,
        source_object=PREFIX + OBJECT_1,
        destination_bucket=BUCKET_NAME_DST,
        destination_object="backup_" + OBJECT_1,
        exact_match=True,
        move_object=True,
    )
    # [END howto_operator_gcs_to_gcs_single_file_move]

    # [START howto_operator_gcs_to_gcs_list_move]
    move_files_with_list = GCSToGCSOperator(
        task_id="move_files_with_list",
        source_bucket=BUCKET_NAME_SRC,
        source_objects=[PREFIX + OBJECT_1, PREFIX + OBJECT_2],
        destination_bucket=BUCKET_NAME_DST,
        destination_object="backup/",
    )
    # [END howto_operator_gcs_to_gcs_list_move]

    list_objects = GCSListObjectsOperator(task_id="list_objects", bucket=BUCKET_NAME_DST)
    assert_copied_files_exist = PythonOperator(
        task_id="assert_copied_files_exist",
        python_callable=_assert_copied_files_exist,
    )

    delete_bucket_src = GCSDeleteBucketOperator(
        task_id="delete_bucket_src", bucket_name=BUCKET_NAME_SRC, trigger_rule=TriggerRule.ALL_DONE
    )
    delete_bucket_dst = GCSDeleteBucketOperator(
        task_id="delete_bucket_dst", bucket_name=BUCKET_NAME_DST, trigger_rule=TriggerRule.ALL_DONE
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def delete_work_dir(create_workdir_result: str) -> None:
        shutil.rmtree(create_workdir_result)

    chain(
        # TEST SETUP
        create_workdir_task,
        generate_random_file,
        [create_bucket_src, create_bucket_dst],
        [upload_file_src, upload_file_src_sub],
        [upload_file_dst, upload_file_dst_sub],
        # TEST BODY
        sync_bucket,
        sync_full_bucket,
        sync_to_subdirectory,
        sync_from_subdirectory,
        copy_single_file,
        copy_files,
        copy_files_with_match_glob,
        copy_files_with_list,
        move_single_file,
        move_files_with_list,
        list_objects,
        assert_copied_files_exist,
        # TEST TEARDOWN
        [delete_bucket_src, delete_bucket_dst, delete_work_dir(create_workdir_task)],
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
