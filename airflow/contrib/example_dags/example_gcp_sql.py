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
Example Airflow DAG that deploys, updates, patches and deletes a Cloud SQL instance
in Google Cloud Platform.

This DAG relies on the following Airflow variables
https://airflow.apache.org/concepts.html#variables
* PROJECT_ID - Google Cloud Platform project for the Cloud SQL instance.
* INSTANCE_NAME - Name of the Cloud SQL instance.
"""

import datetime

import airflow
from airflow import models

from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceCreateOperator, \
    CloudSqlInstancePatchOperator, CloudSqlInstanceDeleteOperator

# [START howto_operator_cloudsql_arguments]
PROJECT_ID = models.Variable.get('PROJECT_ID', '')
INSTANCE_NAME = models.Variable.get('INSTANCE_NAME', '')
# [END howto_operator_cloudsql_arguments]

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

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}

with models.DAG(
    'example_gcp_sql',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    # [START howto_operator_cloudsql_create]
    sql_instance_create_task = CloudSqlInstanceCreateOperator(
        project_id=PROJECT_ID,
        body=body,
        instance=INSTANCE_NAME,
        task_id='sql_instance_create_task'
    )
    # [END howto_operator_cloudsql_create]
    # [START howto_operator_cloudsql_patch]
    sql_instance_patch_task = CloudSqlInstancePatchOperator(
        project_id=PROJECT_ID,
        body=patch_body,
        instance=INSTANCE_NAME,
        task_id='sql_instance_patch_task'
    )
    # [END howto_operator_cloudsql_patch]
    # [START howto_operator_cloudsql_delete]
    sql_instance_delete_task = CloudSqlInstanceDeleteOperator(
        project_id=PROJECT_ID,
        instance=INSTANCE_NAME,
        task_id='sql_instance_delete_task'
    )
    # [END howto_operator_cloudsql_delete]

    sql_instance_create_task >> sql_instance_patch_task >> sql_instance_delete_task
