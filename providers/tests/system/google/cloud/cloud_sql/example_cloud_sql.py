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
Example Airflow DAG that creates, patches and deletes a Cloud SQL instance, and also
creates, patches and deletes a database inside the instance, in Google Cloud.

"""

from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import urlsplit

from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCloneInstanceOperator,
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceDatabaseOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExportInstanceOperator,
    CloudSQLImportInstanceOperator,
    CloudSQLInstancePatchOperator,
    CloudSQLPatchInstanceDatabaseOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSObjectCreateAclEntryOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = (
    os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
)
DAG_ID = "cloudsql"

INSTANCE_NAME = f"{DAG_ID}-{ENV_ID}-instance".replace("_", "-")
DB_NAME = f"{DAG_ID}-{ENV_ID}-db".replace("_", "-")

BUCKET_NAME = f"{DAG_ID}_{ENV_ID}_bucket".replace("-", "_")
FILE_NAME = f"{DAG_ID}_{ENV_ID}_exportImportTestFile".replace("-", "_")
FILE_NAME_DEFERRABLE = f"{DAG_ID}_{ENV_ID}_def_exportImportTestFile".replace("-", "_")
FILE_URI = f"gs://{BUCKET_NAME}/{FILE_NAME}"
FILE_URI_DEFERRABLE = f"gs://{BUCKET_NAME}/{FILE_NAME_DEFERRABLE}"

CLONED_INSTANCE_NAME = f"{INSTANCE_NAME}-clone"

# Bodies below represent Cloud SQL instance resources:
# https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances

# [START howto_operator_cloudsql_create_body]
body = {
    "name": INSTANCE_NAME,
    "settings": {
        "tier": "db-n1-standard-1",
        "backupConfiguration": {
            "binaryLogEnabled": True,
            "enabled": True,
            "startTime": "05:00",
        },
        "activationPolicy": "ALWAYS",
        "dataDiskSizeGb": 30,
        "dataDiskType": "PD_SSD",
        "databaseFlags": [],
        "ipConfiguration": {
            "ipv4Enabled": True,
            "requireSsl": True,
        },
        "locationPreference": {"zone": "europe-west4-a"},
        "maintenanceWindow": {"hour": 5, "day": 7, "updateTrack": "canary"},
        "pricingPlan": "PER_USE",
        "storageAutoResize": True,
        "storageAutoResizeLimit": 0,
        "userLabels": {"my-key": "my-value"},
    },
    "databaseVersion": "MYSQL_5_7",
    "region": "europe-west4",
}
# [END howto_operator_cloudsql_create_body]

# [START howto_operator_cloudsql_patch_body]
patch_body = {
    "name": INSTANCE_NAME,
    "settings": {
        "dataDiskSizeGb": 35,
        "maintenanceWindow": {"hour": 3, "day": 6, "updateTrack": "canary"},
        "userLabels": {"my-key-patch": "my-value-patch"},
    },
}
# [END howto_operator_cloudsql_patch_body]
# [START howto_operator_cloudsql_export_body]
export_body = {
    "exportContext": {
        "fileType": "sql",
        "uri": FILE_URI,
        "sqlExportOptions": {"schemaOnly": False},
        "offload": True,
    }
}
export_body_deferrable = {
    "exportContext": {
        "fileType": "sql",
        "uri": FILE_URI_DEFERRABLE,
        "sqlExportOptions": {"schemaOnly": False},
        "offload": True,
    }
}
# [END howto_operator_cloudsql_export_body]
# [START howto_operator_cloudsql_import_body]
import_body = {"importContext": {"fileType": "sql", "uri": FILE_URI}}
# [END howto_operator_cloudsql_import_body]
# [START howto_operator_cloudsql_db_create_body]
db_create_body = {"instance": INSTANCE_NAME, "name": DB_NAME, "project": PROJECT_ID}
# [END howto_operator_cloudsql_db_create_body]
# [START howto_operator_cloudsql_db_patch_body]
db_patch_body = {"charset": "utf16", "collation": "utf16_general_ci"}
# [END howto_operator_cloudsql_db_patch_body]

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "cloud_sql"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        resource={"predefined_acl": "public_read_write"},
    )

    # ############################################## #
    # ### INSTANCES SET UP ######################### #
    # ############################################## #

    # [START howto_operator_cloudsql_create]
    sql_instance_create_task = CloudSQLCreateInstanceOperator(
        body=body, instance=INSTANCE_NAME, task_id="sql_instance_create_task"
    )
    # [END howto_operator_cloudsql_create]

    # ############################################## #
    # ### MODIFYING INSTANCE AND ITS DATABASE ###### #
    # ############################################## #

    # [START howto_operator_cloudsql_patch]
    sql_instance_patch_task = CloudSQLInstancePatchOperator(
        body=patch_body, instance=INSTANCE_NAME, task_id="sql_instance_patch_task"
    )
    # [END howto_operator_cloudsql_patch]

    # [START howto_operator_cloudsql_db_create]
    sql_db_create_task = CloudSQLCreateInstanceDatabaseOperator(
        body=db_create_body, instance=INSTANCE_NAME, task_id="sql_db_create_task"
    )
    # [END howto_operator_cloudsql_db_create]

    # [START howto_operator_cloudsql_db_patch]
    sql_db_patch_task = CloudSQLPatchInstanceDatabaseOperator(
        body=db_patch_body,
        instance=INSTANCE_NAME,
        database=DB_NAME,
        task_id="sql_db_patch_task",
    )
    # [END howto_operator_cloudsql_db_patch]

    # ############################################## #
    # ### EXPORTING & IMPORTING SQL ################ #
    # ############################################## #
    file_url_split = urlsplit(FILE_URI)

    # For export & import to work we need to add the Cloud SQL instance's Service Account
    # write access to the destination GCS bucket.
    service_account_email = XComArg(sql_instance_create_task, key="service_account_email")

    # [START howto_operator_cloudsql_export_gcs_permissions]
    sql_gcp_add_bucket_permission_task = GCSBucketCreateAclEntryOperator(
        entity=f"user-{service_account_email}",
        role="WRITER",
        bucket=file_url_split[1],  # netloc (bucket)
        task_id="sql_gcp_add_bucket_permission_task",
    )
    # [END howto_operator_cloudsql_export_gcs_permissions]

    # [START howto_operator_cloudsql_export]
    sql_export_task = CloudSQLExportInstanceOperator(
        body=export_body, instance=INSTANCE_NAME, task_id="sql_export_task"
    )
    # [END howto_operator_cloudsql_export]

    # [START howto_operator_cloudsql_export_async]
    sql_export_def_task = CloudSQLExportInstanceOperator(
        body=export_body_deferrable,
        instance=INSTANCE_NAME,
        task_id="sql_export_def_task",
        deferrable=True,
    )
    # [END howto_operator_cloudsql_export_async]

    # For import to work we need to add the Cloud SQL instance's Service Account
    # read access to the target GCS object.
    # [START howto_operator_cloudsql_import_gcs_permissions]
    sql_gcp_add_object_permission_task = GCSObjectCreateAclEntryOperator(
        entity=f"user-{service_account_email}",
        role="READER",
        bucket=file_url_split[1],  # netloc (bucket)
        object_name=file_url_split[2][1:],  # path (strip first '/')
        task_id="sql_gcp_add_object_permission_task",
    )
    # [END howto_operator_cloudsql_import_gcs_permissions]

    # [START howto_operator_cloudsql_import]
    sql_import_task = CloudSQLImportInstanceOperator(
        body=import_body, instance=INSTANCE_NAME, task_id="sql_import_task"
    )
    # [END howto_operator_cloudsql_import]

    # ############################################## #
    # ### CLONE AN INSTANCE ######################## #
    # ############################################## #
    # [START howto_operator_cloudsql_clone]
    sql_instance_clone = CloudSQLCloneInstanceOperator(
        instance=INSTANCE_NAME,
        destination_instance_name=CLONED_INSTANCE_NAME,
        task_id="sql_instance_clone",
    )
    # [END howto_operator_cloudsql_clone]

    # ############################################## #
    # ### DELETING A DATABASE FROM AN INSTANCE ##### #
    # ############################################## #

    # [START howto_operator_cloudsql_db_delete]
    sql_db_delete_task = CloudSQLDeleteInstanceDatabaseOperator(
        instance=INSTANCE_NAME,
        database=DB_NAME,
        task_id="sql_db_delete_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_cloudsql_db_delete]

    # ############################################## #
    # ### INSTANCES TEAR DOWN ###################### #
    # ############################################## #

    sql_instance_clone_delete_task = CloudSQLDeleteInstanceOperator(
        instance=CLONED_INSTANCE_NAME,
        task_id="sql_instance_clone_delete_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_cloudsql_delete]
    sql_instance_delete_task = CloudSQLDeleteInstanceOperator(
        instance=INSTANCE_NAME,
        task_id="sql_instance_delete_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_cloudsql_delete]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> sql_instance_create_task
        >> sql_instance_patch_task
        >> sql_db_create_task
        >> sql_db_patch_task
        >> sql_gcp_add_bucket_permission_task
        >> sql_export_task
        >> sql_export_def_task
        >> sql_gcp_add_object_permission_task
        >> sql_import_task
        >> sql_instance_clone
        >> sql_db_delete_task
        >> sql_instance_clone_delete_task
        >> sql_instance_delete_task
        # TEST TEARDOWN
        >> delete_bucket
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
