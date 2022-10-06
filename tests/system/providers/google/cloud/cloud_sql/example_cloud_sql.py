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

This DAG relies on the following OS environment variables
https://airflow.apache.org/concepts.html#variables
* GCP_PROJECT_ID - Google Cloud project for the Cloud SQL instance.
* INSTANCE_NAME - Name of the Cloud SQL instance.
* DB_NAME - Name of the database inside a Cloud SQL instance.
"""
from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import urlsplit

from airflow import models
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.cloud_sql import (
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

ENV_ID = os.environ.get('SYSTEM_TESTS_ENV_ID')
PROJECT_ID = os.environ.get('SYSTEM_TESTS_GCP_PROJECT')
DAG_ID = "cloudsql"

INSTANCE_NAME = f"{DAG_ID}-{ENV_ID}-instance"
DB_NAME = f"{DAG_ID}-{ENV_ID}-db"

BUCKET_NAME = f"{DAG_ID}_{ENV_ID}_bucket"
FILE_NAME = f"{DAG_ID}_{ENV_ID}_exportImportTestFile"
FILE_URI = f"gs://{BUCKET_NAME}/{FILE_NAME}"

FAILOVER_REPLICA_NAME = f"{INSTANCE_NAME}-failover-replica"
READ_REPLICA_NAME = f"{INSTANCE_NAME}-read-replica"


# Bodies below represent Cloud SQL instance resources:
# https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances

# [START howto_operator_cloudsql_create_body]
body = {
    "name": INSTANCE_NAME,
    "settings": {
        "tier": "db-n1-standard-1",
        "backupConfiguration": {"binaryLogEnabled": True, "enabled": True, "startTime": "05:00"},
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
        "replicationType": "ASYNCHRONOUS",
        "storageAutoResize": True,
        "storageAutoResizeLimit": 0,
        "userLabels": {"my-key": "my-value"},
    },
    "failoverReplica": {"name": FAILOVER_REPLICA_NAME},
    "databaseVersion": "MYSQL_5_7",
    "region": "europe-west4",
}
# [END howto_operator_cloudsql_create_body]

# [START howto_operator_cloudsql_create_replica]
read_replica_body = {
    "name": READ_REPLICA_NAME,
    "settings": {
        "tier": "db-n1-standard-1",
    },
    "databaseVersion": "MYSQL_5_7",
    "region": "europe-west4",
    "masterInstanceName": INSTANCE_NAME,
}
# [END howto_operator_cloudsql_create_replica]


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

with models.DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "cloud_sql"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    # ############################################## #
    # ### INSTANCES SET UP ######################### #
    # ############################################## #

    # [START howto_operator_cloudsql_create]
    sql_instance_create_task = CloudSQLCreateInstanceOperator(
        body=body, instance=INSTANCE_NAME, task_id='sql_instance_create_task'
    )
    # [END howto_operator_cloudsql_create]

    sql_instance_read_replica_create = CloudSQLCreateInstanceOperator(
        body=read_replica_body,
        instance=READ_REPLICA_NAME,
        task_id='sql_instance_read_replica_create',
    )

    # ############################################## #
    # ### MODIFYING INSTANCE AND ITS DATABASE ###### #
    # ############################################## #

    # [START howto_operator_cloudsql_patch]
    sql_instance_patch_task = CloudSQLInstancePatchOperator(
        body=patch_body, instance=INSTANCE_NAME, task_id='sql_instance_patch_task'
    )
    # [END howto_operator_cloudsql_patch]

    # [START howto_operator_cloudsql_db_create]
    sql_db_create_task = CloudSQLCreateInstanceDatabaseOperator(
        body=db_create_body, instance=INSTANCE_NAME, task_id='sql_db_create_task'
    )
    # [END howto_operator_cloudsql_db_create]

    # [START howto_operator_cloudsql_db_patch]
    sql_db_patch_task = CloudSQLPatchInstanceDatabaseOperator(
        body=db_patch_body,
        instance=INSTANCE_NAME,
        database=DB_NAME,
        task_id='sql_db_patch_task',
    )
    # [END howto_operator_cloudsql_db_patch]

    # ############################################## #
    # ### EXPORTING & IMPORTING SQL ################ #
    # ############################################## #
    file_url_split = urlsplit(FILE_URI)

    # For export & import to work we need to add the Cloud SQL instance's Service Account
    # write access to the destination GCS bucket.
    service_account_email = XComArg(sql_instance_create_task, key='service_account_email')

    # [START howto_operator_cloudsql_export_gcs_permissions]
    sql_gcp_add_bucket_permission_task = GCSBucketCreateAclEntryOperator(
        entity=f"user-{service_account_email}",
        role="WRITER",
        bucket=file_url_split[1],  # netloc (bucket)
        task_id='sql_gcp_add_bucket_permission_task',
    )
    # [END howto_operator_cloudsql_export_gcs_permissions]

    # [START howto_operator_cloudsql_export]
    sql_export_task = CloudSQLExportInstanceOperator(
        body=export_body, instance=INSTANCE_NAME, task_id='sql_export_task'
    )
    # [END howto_operator_cloudsql_export]

    # For import to work we need to add the Cloud SQL instance's Service Account
    # read access to the target GCS object.
    # [START howto_operator_cloudsql_import_gcs_permissions]
    sql_gcp_add_object_permission_task = GCSObjectCreateAclEntryOperator(
        entity=f"user-{service_account_email}",
        role="READER",
        bucket=file_url_split[1],  # netloc (bucket)
        object_name=file_url_split[2][1:],  # path (strip first '/')
        task_id='sql_gcp_add_object_permission_task',
    )
    # [END howto_operator_cloudsql_import_gcs_permissions]

    # [START howto_operator_cloudsql_import]
    sql_import_task = CloudSQLImportInstanceOperator(
        body=import_body, instance=INSTANCE_NAME, task_id='sql_import_task'
    )
    # [END howto_operator_cloudsql_import]

    # ############################################## #
    # ### DELETING A DATABASE FROM AN INSTANCE ##### #
    # ############################################## #

    # [START howto_operator_cloudsql_db_delete]
    sql_db_delete_task = CloudSQLDeleteInstanceDatabaseOperator(
        instance=INSTANCE_NAME, database=DB_NAME, task_id='sql_db_delete_task'
    )
    # [END howto_operator_cloudsql_db_delete]
    sql_db_delete_task.trigger_rule = TriggerRule.ALL_DONE

    # ############################################## #
    # ### INSTANCES TEAR DOWN ###################### #
    # ############################################## #

    # [START howto_operator_cloudsql_replicas_delete]
    sql_instance_failover_replica_delete_task = CloudSQLDeleteInstanceOperator(
        instance=FAILOVER_REPLICA_NAME,
        task_id='sql_instance_failover_replica_delete_task',
    )

    sql_instance_read_replica_delete_task = CloudSQLDeleteInstanceOperator(
        instance=READ_REPLICA_NAME, task_id='sql_instance_read_replica_delete_task'
    )
    # [END howto_operator_cloudsql_replicas_delete]
    sql_instance_failover_replica_delete_task.trigger_rule = TriggerRule.ALL_DONE
    sql_instance_read_replica_delete_task.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_cloudsql_delete]
    sql_instance_delete_task = CloudSQLDeleteInstanceOperator(
        instance=INSTANCE_NAME, task_id='sql_instance_delete_task'
    )
    # [END howto_operator_cloudsql_delete]
    sql_instance_delete_task.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> sql_instance_create_task
        >> sql_instance_read_replica_create
        >> sql_instance_patch_task
        >> sql_db_create_task
        >> sql_db_patch_task
        >> sql_gcp_add_bucket_permission_task
        >> sql_export_task
        >> sql_gcp_add_object_permission_task
        >> sql_import_task
        >> sql_db_delete_task
        >> sql_instance_failover_replica_delete_task
        >> sql_instance_read_replica_delete_task
        >> sql_instance_delete_task
        # TEST TEARDOWN
        >> delete_bucket
    )

    # Task dependencies created via `XComArgs`:
    #   sql_instance_create_task >> sql_gcp_add_bucket_permission_task
    #   sql_instance_create_task >> sql_gcp_add_object_permission_task

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
