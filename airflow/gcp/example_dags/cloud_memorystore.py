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
import os

from google.cloud.redis_v1.gapic.enums import Instance, FailoverInstanceRequest

from airflow import models
from airflow.contrib.operators.gcs_acl_operator import GoogleCloudStorageBucketCreateAclEntryOperator
from airflow.gcp.operators.cloud_memorystore import (CloudMemorystoreCreateInstanceOperator,
                                                     CloudMemorystoreDeleteInstanceOperator,
                                                     CloudMemorystoreExportInstanceOperator,
                                                     CloudMemorystoreFailoverInstanceOperator,
                                                     CloudMemorystoreGetInstanceOperator,
                                                     CloudMemorystoreImportInstanceOperator,
                                                     CloudMemorystoreListInstancesOperator,
                                                     CloudMemorystoreUpdateInstanceOperator)
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates


GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')

INSTANCE_NAME = os.environ.get('GCP_MEMORYSTORE_INSTANCE_NAME', 'test-memorystore-1')
INSTANCE_NAME_2 = os.environ.get('GCP_MEMORYSTORE_INSTANCE_NAME2', 'test-memorystore-2')

BUCKET_NAME = os.environ.get('GCP_MEMORYSTORE_BUCKET_NAME', 'test-memorystore-2')

FIRST_INSTANCE = {
    'tier': Instance.Tier.BASIC,
    'memory_size_gb': 1
}

SECOND_INSTANCE = {
    'tier': Instance.Tier.STANDARD_HA,
    'memory_size_gb': 3
}


default_args = {
    'start_date': dates.days_ago(1)
}

with models.DAG(
    'gcp_cloud_memorystore',
    default_args=default_args,
    schedule_interval=None  # Override to match your needs
) as dag:
    create_instance = CloudMemorystoreCreateInstanceOperator(
        task_id='create-instance',
        location='europe-north1',
        instance_id=INSTANCE_NAME,
        instance=FIRST_INSTANCE,
        project_id=GCP_PROJECT_ID,
    )

    create_instance_2 = CloudMemorystoreCreateInstanceOperator(
        task_id='create-instance-2',
        location='europe-north1',
        instance_id=INSTANCE_NAME_2,
        instance=SECOND_INSTANCE,
        project_id=GCP_PROJECT_ID,
    )

    delete_instance = CloudMemorystoreDeleteInstanceOperator(
        task_id='delete-instance',
        location='europe-north1',
        instance=INSTANCE_NAME,
        project_id=GCP_PROJECT_ID,
    )

    get_instance = CloudMemorystoreGetInstanceOperator(
        task_id='get-instance',
        location='europe-north1',
        instance=INSTANCE_NAME,
        project_id=GCP_PROJECT_ID,
    )

    get_instance_result = BashOperator(
        task_id="get-instance-result",
        bash_command="echo \"{{ task_instance.xcom_pull('get-instance') }}\"",
    )

    failover_instance = CloudMemorystoreFailoverInstanceOperator(
        task_id='failover-instance',
        location='europe-north1',
        instance=INSTANCE_NAME_2,
        data_protection_mode=FailoverInstanceRequest.DataProtectionMode.LIMITED_DATA_LOSS,
        project_id=GCP_PROJECT_ID,
    )

    list_instance = CloudMemorystoreListInstancesOperator(
        task_id='list-instances',
        location='-',
        page_size=100,
        project_id=GCP_PROJECT_ID,
    )

    list_instance_result = BashOperator(
        task_id="get-instance-result",
        bash_command="echo \"{{ task_instance.xcom_pull('list-instances') }}\"",
    )

    # update_instance = CloudMemorystoreUpdateInstanceOperator(
    #     task_id='update-instance',
    #     update_mask={
    #         'paths': ['memory_size_gb']
    #     },
    #     instance={'memory_size_gb': 2},
    # )

    set_acl_permission = GoogleCloudStorageBucketCreateAclEntryOperator(
        bucket=BUCKET_NAME,
        # entity="{{ task_instance.xcom_pull('get-instance')['persistenceIamIdentity'] }}",
        entity="33987476006-compute@developer.gserviceaccount.com",
        role='OWNER',
        task_id="gcs-set-acl-permission"
    )

    export_instance = CloudMemorystoreExportInstanceOperator(
        task_id='export-instance',
        location='europe-north1',
        instance=INSTANCE_NAME,
        output_config={
            'gcs_destination': {
                'uri': "gs://{}/my_export.rdb".format(BUCKET_NAME)
            },
        },
        project_id=GCP_PROJECT_ID,
    )

    # import_instance = CloudMemorystoreImportInstanceOperator(
    #     task_id='import-instance',
    #     location=TEST_LOCATION,
    #     instance=TEST_INSTANCE_NAME,
    #     input_config=TEST_INPUT_CONFIG,
    #     project_id=GCP_PROJECT_ID,
    #     retry=TEST_RETRY,
    #     timeout=TEST_TIMEOUT,
    #     metadata=TEST_METADATA,
    #     gcp_conn_id=TEST_GCP_CONN_ID,
    # )
