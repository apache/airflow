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
Example Airflow DAG for Google Cloud Storage to SFTP transfer operators.
"""

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "example_gcs_to_sftp"

SFTP_CONN_ID = "ssh_default"
BUCKET_SRC = os.environ.get("GCP_GCS_BUCKET_1_SRC", "test-gcs-sftp")
OBJECT_SRC_1 = "parent-1.bin"
OBJECT_SRC_2 = "dir-1/parent-2.bin"
OBJECT_SRC_3 = "dir-2/*"
DESTINATION_PATH_1 = "/tmp/single-file/"
DESTINATION_PATH_2 = "/tmp/dest-dir-1/"
DESTINATION_PATH_3 = "/tmp/dest-dir-2/"

with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'gcs'],
) as dag:
    # [START howto_operator_gcs_to_sftp_copy_single_file]
    copy_file_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="file-copy-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        source_bucket=BUCKET_SRC,
        source_object=OBJECT_SRC_1,
        destination_path=DESTINATION_PATH_1,
    )
    # [END howto_operator_gcs_to_sftp_copy_single_file]

    check_copy_file_from_gcs_to_sftp = SFTPSensor(
        task_id="check-file-copy-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        timeout=60,
        path=os.path.join(DESTINATION_PATH_1, OBJECT_SRC_1),
    )

    # [START howto_operator_gcs_to_sftp_move_single_file_destination]
    move_file_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="file-move-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        source_bucket=BUCKET_SRC,
        source_object=OBJECT_SRC_2,
        destination_path=DESTINATION_PATH_1,
        move_object=True,
    )
    # [END howto_operator_gcs_to_sftp_move_single_file_destination]

    check_move_file_from_gcs_to_sftp = SFTPSensor(
        task_id="check-file-move-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        timeout=60,
        path=os.path.join(DESTINATION_PATH_1, OBJECT_SRC_2),
    )

    # [START howto_operator_gcs_to_sftp_copy_directory]
    copy_dir_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="dir-copy-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        source_bucket=BUCKET_SRC,
        source_object=OBJECT_SRC_3,
        destination_path=DESTINATION_PATH_2,
    )
    # [END howto_operator_gcs_to_sftp_copy_directory]

    check_copy_dir_from_gcs_to_sftp = SFTPSensor(
        task_id="check-dir-copy-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        timeout=60,
        path=os.path.join(DESTINATION_PATH_2, "dir-2", OBJECT_SRC_1),
    )

    # [START howto_operator_gcs_to_sftp_move_specific_files]
    move_dir_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="dir-move-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        source_bucket=BUCKET_SRC,
        source_object=OBJECT_SRC_3,
        destination_path=DESTINATION_PATH_3,
        keep_directory_structure=False,
    )
    # [END howto_operator_gcs_to_sftp_move_specific_files]

    check_move_dir_from_gcs_to_sftp = SFTPSensor(
        task_id="check-dir-move-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        timeout=60,
        path=os.path.join(DESTINATION_PATH_3, OBJECT_SRC_1),
    )

    (
        # TEST BODY
        copy_file_from_gcs_to_sftp
        >> check_copy_file_from_gcs_to_sftp
        >> move_file_from_gcs_to_sftp
        >> check_move_file_from_gcs_to_sftp
        >> copy_dir_from_gcs_to_sftp
        >> check_copy_dir_from_gcs_to_sftp
        >> move_dir_from_gcs_to_sftp
        >> check_move_dir_from_gcs_to_sftp
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
