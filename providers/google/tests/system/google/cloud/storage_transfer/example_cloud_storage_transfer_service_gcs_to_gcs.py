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
Example Airflow DAG that demonstrates interactions with Google Cloud Transfer.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceGCSToGCSOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "example_transfer_gcs_to_gcs"

BUCKET_NAME_SRC = f"src-bucket-{DAG_ID}-{ENV_ID}".replace("_", "-")
BUCKET_NAME_DST = f"dst-bucket-{DAG_ID}-{ENV_ID}".replace("_", "-")
FILE_NAME = "transfer_service_gcs_to_gcs_file"
FILE_URI = f"gs://{BUCKET_NAME_SRC}/{FILE_NAME}"

CURRENT_FOLDER = Path(__file__).parent
FILE_LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources" / FILE_NAME)


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "transfer", "gcs-to-gcs"],
) as dag:
    create_bucket_src = GCSCreateBucketOperator(
        task_id="create_bucket_src",
        bucket_name=BUCKET_NAME_SRC,
        project_id=PROJECT_ID,
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=FILE_LOCAL_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME_SRC,
    )

    create_bucket_dst = GCSCreateBucketOperator(
        task_id="create_bucket_dst",
        bucket_name=BUCKET_NAME_DST,
        project_id=PROJECT_ID,
    )

    # [START howto_operator_transfer_gcs_to_gcs]
    transfer_gcs_to_gcs = CloudDataTransferServiceGCSToGCSOperator(
        task_id="transfer_gcs_to_gcs",
        source_bucket=BUCKET_NAME_SRC,
        source_path=FILE_URI,
        destination_bucket=BUCKET_NAME_DST,
        destination_path=FILE_URI,
        wait=True,
    )
    # [END howto_operator_transfer_gcs_to_gcs]

    # [START howto_operator_transfer_gcs_to_gcs_defered]
    transfer_gcs_to_gcs_defered = CloudDataTransferServiceGCSToGCSOperator(
        task_id="transfer_gcs_to_gcs_defered",
        source_bucket=BUCKET_NAME_SRC,
        source_path=FILE_URI,
        destination_bucket=BUCKET_NAME_DST,
        destination_path=FILE_URI,
        wait=True,
        deferrable=True,
    )
    # [END howto_operator_transfer_gcs_to_gcs_defered]

    delete_bucket_dst = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME_DST, trigger_rule=TriggerRule.ALL_DONE
    )

    delete_bucket_src = GCSDeleteBucketOperator(
        task_id="delete_bucket_src", bucket_name=BUCKET_NAME_SRC, trigger_rule=TriggerRule.ALL_DONE
    )

    # TEST SETUP
    create_bucket_src >> upload_file
    [create_bucket_dst, upload_file] >> transfer_gcs_to_gcs
    (
        [create_bucket_dst, create_bucket_src >> upload_file]
        # TEST BODY
        >> transfer_gcs_to_gcs
        >> transfer_gcs_to_gcs_defered
        # TEST TEARDOWN
        >> [delete_bucket_src, delete_bucket_dst]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
