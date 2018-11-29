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
Example Airflow DAG that creates a new ACL entry on the specified bucket and object.

This DAG relies on the following OS environment variables

* GCS_ACL_BUCKET - Name of a bucket.
* GCS_ACL_OBJECT - Name of the object. For information about how to URL encode object
    names to be path safe, see:
    https://cloud.google.com/storage/docs/json_api/#encoding
* GCS_ACL_ENTITY - The entity holding the permission.
* GCS_ACL_BUCKET_ROLE - The access permission for the entity for the bucket.
* GCS_ACL_OBJECT_ROLE - The access permission for the entity for the object.
"""
import os

import airflow
from airflow import models
from airflow.contrib.operators.gcs_acl_operator import \
    GoogleCloudStorageBucketCreateAclEntryOperator, \
    GoogleCloudStorageObjectCreateAclEntryOperator

# [START howto_operator_gcs_acl_args_common]
GCS_ACL_BUCKET = os.environ.get('GCS_ACL_BUCKET', 'example-bucket')
GCS_ACL_OBJECT = os.environ.get('GCS_ACL_OBJECT', 'example-object')
GCS_ACL_ENTITY = os.environ.get('GCS_ACL_ENTITY', 'example-entity')
GCS_ACL_BUCKET_ROLE = os.environ.get('GCS_ACL_BUCKET_ROLE', 'example-bucket-role')
GCS_ACL_OBJECT_ROLE = os.environ.get('GCS_ACL_OBJECT_ROLE', 'example-object-role')
# [END howto_operator_gcs_acl_args_common]

default_args = {
    'start_date': airflow.utils.dates.days_ago(1)
}

with models.DAG(
    'example_gcs_acl',
    default_args=default_args,
    schedule_interval=None  # Change to match your use case
) as dag:
    # [START howto_operator_gcs_bucket_create_acl_entry_task]
    gcs_bucket_create_acl_entry_task = GoogleCloudStorageBucketCreateAclEntryOperator(
        bucket=GCS_ACL_BUCKET,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_BUCKET_ROLE,
        task_id="gcs_bucket_create_acl_entry_task"
    )
    # [END howto_operator_gcs_bucket_create_acl_entry_task]
    # [START howto_operator_gcs_object_create_acl_entry_task]
    gcs_object_create_acl_entry_task = GoogleCloudStorageObjectCreateAclEntryOperator(
        bucket=GCS_ACL_BUCKET,
        object_name=GCS_ACL_OBJECT,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_OBJECT_ROLE,
        task_id="gcs_object_create_acl_entry_task"
    )
    # [END howto_operator_gcs_object_create_acl_entry_task]

    gcs_bucket_create_acl_entry_task >> gcs_object_create_acl_entry_task
