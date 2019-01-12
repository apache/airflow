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
Example Airflow DAG that creates, patches and deletes a Cloud SQL instance, and also
creates, patches and deletes a database inside the instance, in Google Cloud Platform.

This DAG relies on the following OS environment variables
https://airflow.apache.org/concepts.html#variables
* GCP_PROJECT_ID - Google Cloud Platform project for the Cloud SQL instance.
* INSTANCE_NAME - Name of the Cloud SQL instance.
* DB_NAME - Name of the database inside a Cloud SQL instance.
"""

import os

import airflow
from airflow import models
from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceCreateOperator, \
    CloudSqlInstancePatchOperator, CloudSqlInstanceDeleteOperator, \
    CloudSqlInstanceDatabaseCreateOperator, CloudSqlInstanceDatabasePatchOperator, \
    CloudSqlInstanceDatabaseDeleteOperator, CloudSqlInstanceExportOperator, \
    CloudSqlInstanceImportOperator
from airflow.contrib.operators.gcs_acl_operator import \
    GoogleCloudStorageBucketCreateAclEntryOperator, \
    GoogleCloudStorageObjectCreateAclEntryOperator

from six.moves.urllib.parse import urlsplit

# [START howto_operator_cloudsql_arguments]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
INSTANCE_NAME = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME', 'test-mysql')
INSTANCE_NAME2 = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME2', 'test-mysql2')
DB_NAME = os.environ.get('GCSQL_MYSQL_DATABASE_NAME', 'testdb')
# [END howto_operator_cloudsql_arguments]

# [START howto_operator_cloudsql_export_import_arguments]
EXPORT_URI = os.environ.get('GCSQL_MYSQL_EXPORT_URI', 'gs://bucketName/fileName')
IMPORT_URI = os.environ.get('GCSQL_MYSQL_IMPORT_URI', 'gs://bucketName/fileName')
# [END howto_operator_cloudsql_export_import_arguments]

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
            "startTime": "05:00"
        },
        "activationPolicy": "ALWAYS",
        "dataDiskSizeGb": 30,
        "dataDiskType": "PD_SSD",
        "databaseFlags": [],
        "ipConfiguration": {
            "ipv4Enabled": True,
            "requireSsl": True,
        },
        "locationPreference": {
            "zone": "europe-west4-a"
        },
        "maintenanceWindow": {
            "hour": 5,
            "day": 7,
            "updateTrack": "canary"
        },
        "pricingPlan": "PER_USE",
        "replicationType": "ASYNCHRONOUS",
        "storageAutoResize": False,
        "storageAutoResizeLimit": 0,
        "userLabels": {
            "my-key": "my-value"
        }
    },
    "databaseVersion": "MYSQL_5_7",
    "region": "europe-west4",
}
# [END howto_operator_cloudsql_create_body]

body2 = {
    "name": INSTANCE_NAME2,
    "settings": {
        "tier": "db-n1-standard-1",
    },
    "databaseVersion": "MYSQL_5_7",
    "region": "europe-west4",
}

# [START howto_operator_cloudsql_patch_body]
patch_body = {
    "name": INSTANCE_NAME,
    "settings": {
        "dataDiskSizeGb": 35,
        "maintenanceWindow": {
            "hour": 3,
            "day": 6,
            "updateTrack": "canary"
        },
        "userLabels": {
            "my-key-patch": "my-value-patch"
        }
    }
}
# [END howto_operator_cloudsql_patch_body]
# [START howto_operator_cloudsql_export_body]
export_body = {
    "exportContext": {
        "fileType": "sql",
        "uri": EXPORT_URI,
        "sqlExportOptions": {
            "schemaOnly": False
        }
    }
}
# [END howto_operator_cloudsql_export_body]
# [START howto_operator_cloudsql_import_body]
import_body = {
    "importContext": {
        "fileType": "sql",
        "uri": IMPORT_URI
    }
}
# [END howto_operator_cloudsql_import_body]
# [START howto_operator_cloudsql_db_create_body]
db_create_body = {
    "instance": INSTANCE_NAME,
    "name": DB_NAME,
    "project": GCP_PROJECT_ID
}
# [END howto_operator_cloudsql_db_create_body]
# [START howto_operator_cloudsql_db_patch_body]
db_patch_body = {
    "charset": "utf16",
    "collation": "utf16_general_ci"
}
# [END howto_operator_cloudsql_db_patch_body]

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}

with models.DAG(
    'example_gcp_sql',
    default_args=default_args,
    schedule_interval=None  # Override to match your needs
) as dag:

    def next_dep(task, prev):
        prev >> task
        return task

    # ############################################## #
    # ### INSTANCES SET UP ######################### #
    # ############################################## #

    # [START howto_operator_cloudsql_create]
    sql_instance_create_task = CloudSqlInstanceCreateOperator(
        project_id=GCP_PROJECT_ID,
        body=body,
        instance=INSTANCE_NAME,
        task_id='sql_instance_create_task'
    )
    # [END howto_operator_cloudsql_create]
    prev_task = sql_instance_create_task

    sql_instance_create_2_task = CloudSqlInstanceCreateOperator(
        project_id=GCP_PROJECT_ID,
        body=body2,
        instance=INSTANCE_NAME2,
        task_id='sql_instance_create_2_task'
    )
    prev_task = next_dep(sql_instance_create_2_task, prev_task)

    # ############################################## #
    # ### MODIFYING INSTANCE AND ITS DATABASE ###### #
    # ############################################## #

    # [START howto_operator_cloudsql_patch]
    sql_instance_patch_task = CloudSqlInstancePatchOperator(
        project_id=GCP_PROJECT_ID,
        body=patch_body,
        instance=INSTANCE_NAME,
        task_id='sql_instance_patch_task'
    )
    # [END howto_operator_cloudsql_patch]
    prev_task = next_dep(sql_instance_patch_task, prev_task)

    # [START howto_operator_cloudsql_db_create]
    sql_db_create_task = CloudSqlInstanceDatabaseCreateOperator(
        project_id=GCP_PROJECT_ID,
        body=db_create_body,
        instance=INSTANCE_NAME,
        task_id='sql_db_create_task'
    )
    # [END howto_operator_cloudsql_db_create]
    prev_task = next_dep(sql_db_create_task, prev_task)

    # [START howto_operator_cloudsql_db_patch]
    sql_db_patch_task = CloudSqlInstanceDatabasePatchOperator(
        project_id=GCP_PROJECT_ID,
        body=db_patch_body,
        instance=INSTANCE_NAME,
        database=DB_NAME,
        task_id='sql_db_patch_task'
    )
    # [END howto_operator_cloudsql_db_patch]
    prev_task = next_dep(sql_db_patch_task, prev_task)

    # ############################################## #
    # ### EXPORTING SQL FROM INSTANCE 1 ############ #
    # ############################################## #
    export_url_split = urlsplit(EXPORT_URI)

    # For export to work we need to add the Cloud SQL instance's Service Account
    # write access to the destination GCS bucket.
    # [START howto_operator_cloudsql_export_gcs_permissions]
    sql_gcp_add_bucket_permission_task = GoogleCloudStorageBucketCreateAclEntryOperator(
        entity="user-{{ task_instance.xcom_pull("
               "'sql_instance_create_task', key='service_account_email') "
               "}}",
        role="WRITER",
        bucket=export_url_split[1],  # netloc (bucket)
        task_id='sql_gcp_add_bucket_permission_task'
    )
    # [END howto_operator_cloudsql_export_gcs_permissions]
    prev_task = next_dep(sql_gcp_add_bucket_permission_task, prev_task)

    # [START howto_operator_cloudsql_export]
    sql_export_task = CloudSqlInstanceExportOperator(
        project_id=GCP_PROJECT_ID,
        body=export_body,
        instance=INSTANCE_NAME,
        task_id='sql_export_task'
    )
    # [END howto_operator_cloudsql_export]
    prev_task = next_dep(sql_export_task, prev_task)

    # ############################################## #
    # ### IMPORTING SQL TO INSTANCE 2 ############## #
    # ############################################## #
    import_url_split = urlsplit(IMPORT_URI)

    # For import to work we need to add the Cloud SQL instance's Service Account
    # read access to the target GCS object.
    # [START howto_operator_cloudsql_import_gcs_permissions]
    sql_gcp_add_object_permission_task = GoogleCloudStorageObjectCreateAclEntryOperator(
        entity="user-{{ task_instance.xcom_pull("
               "'sql_instance_create_2_task', key='service_account_email')"
               " }}",
        role="READER",
        bucket=import_url_split[1],  # netloc (bucket)
        object_name=import_url_split[2][1:],  # path (strip first '/')
        task_id='sql_gcp_add_object_permission_task',
    )
    prev_task = next_dep(sql_gcp_add_object_permission_task, prev_task)

    # For import to work we also need to add the Cloud SQL instance's Service Account
    # write access to the whole bucket!.
    sql_gcp_add_bucket_permission_2_task = GoogleCloudStorageBucketCreateAclEntryOperator(
        entity="user-{{ task_instance.xcom_pull("
               "'sql_instance_create_2_task', key='service_account_email') "
               "}}",
        role="WRITER",
        bucket=import_url_split[1],  # netloc
        task_id='sql_gcp_add_bucket_permission_2_task',
    )
    # [END howto_operator_cloudsql_import_gcs_permissions]
    prev_task = next_dep(sql_gcp_add_bucket_permission_2_task, prev_task)

    # [START howto_operator_cloudsql_import]
    sql_import_task = CloudSqlInstanceImportOperator(
        project_id=GCP_PROJECT_ID,
        body=import_body,
        instance=INSTANCE_NAME2,
        task_id='sql_import_task'
    )
    # [END howto_operator_cloudsql_import]
    prev_task = next_dep(sql_import_task, prev_task)

    # ############################################## #
    # ### DELETING A DATABASE FROM AN INSTANCE ##### #
    # ############################################## #

    # [START howto_operator_cloudsql_db_delete]
    sql_db_delete_task = CloudSqlInstanceDatabaseDeleteOperator(
        project_id=GCP_PROJECT_ID,
        instance=INSTANCE_NAME,
        database=DB_NAME,
        task_id='sql_db_delete_task'
    )
    # [END howto_operator_cloudsql_db_delete]
    prev_task = next_dep(sql_db_delete_task, prev_task)

    # ############################################## #
    # ### INSTANCES TEAR DOWN ###################### #
    # ############################################## #

    # [START howto_operator_cloudsql_delete]
    sql_instance_delete_task = CloudSqlInstanceDeleteOperator(
        project_id=GCP_PROJECT_ID,
        instance=INSTANCE_NAME,
        task_id='sql_instance_delete_task'
    )
    # [END howto_operator_cloudsql_delete]
    prev_task = next_dep(sql_instance_delete_task, prev_task)

    sql_instance_delete_2_task = CloudSqlInstanceDeleteOperator(
        project_id=GCP_PROJECT_ID,
        instance=INSTANCE_NAME2,
        task_id='sql_instance_delete_2_task'
    )
    prev_task = next_dep(sql_instance_delete_2_task, prev_task)
