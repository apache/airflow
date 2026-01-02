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
Example Airflow DAG that show how to use various Dataproc Metastore
operators to manage a service.
"""

from __future__ import annotations

import datetime
import os

from google.protobuf.field_mask_pb2 import FieldMask

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateMetadataImportOperator,
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteServiceOperator,
    DataprocMetastoreExportMetadataOperator,
    DataprocMetastoreGetServiceOperator,
    DataprocMetastoreUpdateServiceOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dataproc_metastore"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")

SERVICE_ID = f"{DAG_ID}-service-{ENV_ID}".replace("_", "-")
METADATA_IMPORT_ID = f"{DAG_ID}-metadata-{ENV_ID}".replace("_", "-")

REGION = "us-central1"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
TIMEOUT = 2400
DB_TYPE = "MYSQL"
DESTINATION_GCS_FOLDER = f"gs://{BUCKET_NAME}/>"
HIVE_FILE = "hive.sql"
GCS_URI = f"gs://{BUCKET_NAME}/dataproc/{HIVE_FILE}"

# Service definition
# Docs: https://cloud.google.com/dataproc-metastore/docs/reference/rest/v1/projects.locations.services#Service
# [START how_to_cloud_dataproc_metastore_create_service]
SERVICE = {
    "name": "test-service",
}
# [END how_to_cloud_dataproc_metastore_create_service]

# [START how_to_cloud_dataproc_metastore_create_metadata_import]
METADATA_IMPORT = {
    "name": "test-metadata-import",
    "database_dump": {
        "gcs_uri": GCS_URI,
        "database_type": DB_TYPE,
    },
}
# [END how_to_cloud_dataproc_metastore_create_metadata_import]

# Update service
# [START how_to_cloud_dataproc_metastore_update_service]
SERVICE_TO_UPDATE = {
    "labels": {
        "mylocalmachine": "mylocalmachine",
        "systemtest": "systemtest",
    }
}
UPDATE_MASK = FieldMask(paths=["labels"])
# [END how_to_cloud_dataproc_metastore_update_service]

with DAG(
    DAG_ID,
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "dataproc", "metastore"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    move_file = GCSSynchronizeBucketsOperator(
        task_id="move_file",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="dataproc/hive",
        destination_bucket=BUCKET_NAME,
        destination_object="dataproc",
        recursive=True,
    )

    # [START how_to_cloud_dataproc_metastore_create_service_operator]
    create_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_service",
        region=REGION,
        project_id=PROJECT_ID,
        service=SERVICE,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_create_service_operator]

    # [START how_to_cloud_dataproc_metastore_get_service_operator]
    get_service = DataprocMetastoreGetServiceOperator(
        task_id="get_service",
        region=REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_get_service_operator]

    # [START how_to_cloud_dataproc_metastore_update_service_operator]
    update_service = DataprocMetastoreUpdateServiceOperator(
        task_id="update_service",
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        region=REGION,
        service=SERVICE_TO_UPDATE,
        update_mask=UPDATE_MASK,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_update_service_operator]

    # [START how_to_cloud_dataproc_metastore_create_metadata_import_operator]
    import_metadata = DataprocMetastoreCreateMetadataImportOperator(
        task_id="import_metadata",
        project_id=PROJECT_ID,
        region=REGION,
        service_id=SERVICE_ID,
        metadata_import=METADATA_IMPORT,
        metadata_import_id=METADATA_IMPORT_ID,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_create_metadata_import_operator]

    # [START how_to_cloud_dataproc_metastore_export_metadata_operator]
    export_metadata = DataprocMetastoreExportMetadataOperator(
        task_id="export_metadata",
        destination_gcs_folder=DESTINATION_GCS_FOLDER,
        project_id=PROJECT_ID,
        region=REGION,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_export_metadata_operator]

    # [START how_to_cloud_dataproc_metastore_delete_service_operator]
    delete_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_service",
        region=REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_delete_service_operator]

    delete_service.trigger_rule = TriggerRule.ALL_DONE
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        create_bucket
        >> move_file
        >> create_service
        >> get_service
        >> update_service
        >> import_metadata
        >> export_metadata
        >> delete_service
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
