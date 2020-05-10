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
Example Airflow DAG for Google Cloud Memorystore service.
"""
import os
from urllib.parse import urlparse

from google.cloud.redis_v1.gapic.enums import FailoverInstanceRequest, Instance

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_memorystore import (
    CloudMemorystoreCreateInstanceAndImportOperator, CloudMemorystoreCreateInstanceOperator,
    CloudMemorystoreDeleteInstanceOperator, CloudMemorystoreExportAndDeleteInstanceOperator,
    CloudMemorystoreExportInstanceOperator, CloudMemorystoreFailoverInstanceOperator,
    CloudMemorystoreGetInstanceOperator, CloudMemorystoreImportOperator,
    CloudMemorystoreListInstancesOperator, CloudMemorystoreScaleInstanceOperator,
    CloudMemorystoreUpdateInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSBucketCreateAclEntryOperator
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

INSTANCE_NAME = os.environ.get("GCP_MEMORYSTORE_INSTANCE_NAME", "test-memorystore")
INSTANCE_NAME_2 = os.environ.get("GCP_MEMORYSTORE_INSTANCE_NAME2", "test-memorystore-2")
INSTANCE_NAME_3 = os.environ.get("GCP_MEMORYSTORE_INSTANCE_NAME3", "test-memorystore-3")

EXPORT_GCS_URL = os.environ.get("GCP_MEMORYSTORE_EXPORT_GCS_URL", "gs://test-memorystore/my-export.rdb")
EXPORT_GCS_URL_PARTS = urlparse(EXPORT_GCS_URL)
BUCKET_NAME = EXPORT_GCS_URL_PARTS.netloc

# [START howto_operator_instance]
FIRST_INSTANCE = {"tier": Instance.Tier.BASIC, "memory_size_gb": 1}
# [END howto_operator_instance]

SECOND_INSTANCE = {"tier": Instance.Tier.STANDARD_HA, "memory_size_gb": 3}


with models.DAG(
    "gcp_cloud_memorystore",
    schedule_interval=None,  # Override to match your needs
    start_date=dates.days_ago(1),
    tags=['example'],
) as dag:
    # [START howto_operator_create_instance]
    create_instance = CloudMemorystoreCreateInstanceOperator(
        task_id="create-instance",
        location="europe-north1",
        instance_id=INSTANCE_NAME,
        instance=FIRST_INSTANCE,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_create_instance]

    # [START howto_operator_create_instance_result]
    create_instance_result = BashOperator(
        task_id="create-instance-result",
        bash_command="echo \"{{ task_instance.xcom_pull('create-instance') }}\"",
    )
    # [END howto_operator_create_instance_result]

    create_instance_2 = CloudMemorystoreCreateInstanceOperator(
        task_id="create-instance-2",
        location="europe-north1",
        instance_id=INSTANCE_NAME_2,
        instance=SECOND_INSTANCE,
        project_id=GCP_PROJECT_ID,
    )

    # [START howto_operator_get_instance]
    get_instance = CloudMemorystoreGetInstanceOperator(
        task_id="get-instance",
        location="europe-north1",
        instance=INSTANCE_NAME,
        project_id=GCP_PROJECT_ID,
        do_xcom_push=True,
    )
    # [END howto_operator_get_instance]

    # [START howto_operator_get_instance_result]
    get_instance_result = BashOperator(
        task_id="get-instance-result", bash_command="echo \"{{ task_instance.xcom_pull('get-instance') }}\""
    )
    # [END howto_operator_get_instance_result]

    # [START howto_operator_failover_instance]
    failover_instance = CloudMemorystoreFailoverInstanceOperator(
        task_id="failover-instance",
        location="europe-north1",
        instance=INSTANCE_NAME_2,
        data_protection_mode=FailoverInstanceRequest.DataProtectionMode.LIMITED_DATA_LOSS,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_failover_instance]

    # [START howto_operator_list_instances]
    list_instances = CloudMemorystoreListInstancesOperator(
        task_id="list-instances", location="-", page_size=100, project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_list_instances]

    # [START howto_operator_list_instances_result]
    list_instances_result = BashOperator(
        task_id="list-instances-result", bash_command="echo \"{{ task_instance.xcom_pull('get-instance') }}\""
    )
    # [END howto_operator_list_instances_result]

    # [START howto_operator_update_instance]
    update_instance = CloudMemorystoreUpdateInstanceOperator(
        task_id="update-instance",
        location="europe-north1",
        instance_id=INSTANCE_NAME,
        project_id=GCP_PROJECT_ID,
        update_mask={"paths": ["memory_size_gb"]},
        instance={"memory_size_gb": 2},
    )
    # [END howto_operator_update_instance]

    # [START howto_operator_set_acl_permission]
    set_acl_permission = GCSBucketCreateAclEntryOperator(
        task_id="gcs-set-acl-permission",
        bucket=BUCKET_NAME,
        entity="user-{{ task_instance.xcom_pull('get-instance')['persistenceIamIdentity']"
        ".split(':', 2)[1] }}",
        role="OWNER",
    )
    # [END howto_operator_set_acl_permission]

    # [START howto_operator_export_instance]
    export_instance = CloudMemorystoreExportInstanceOperator(
        task_id="export-instance",
        location="europe-north1",
        instance=INSTANCE_NAME,
        output_config={"gcs_destination": {"uri": EXPORT_GCS_URL}},
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_export_instance]

    # [START howto_operator_import_instance]
    import_instance = CloudMemorystoreImportOperator(
        task_id="import-instance",
        location="europe-north1",
        instance=INSTANCE_NAME_2,
        input_config={"gcs_source": {"uri": EXPORT_GCS_URL}},
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_import_instance]

    # [START howto_operator_delete_instance]
    delete_instance = CloudMemorystoreDeleteInstanceOperator(
        task_id="delete-instance", location="europe-north1", instance=INSTANCE_NAME, project_id=GCP_PROJECT_ID
    )
    # [END howto_operator_delete_instance]

    delete_instance_2 = CloudMemorystoreDeleteInstanceOperator(
        task_id="delete-instance-2",
        location="europe-north1",
        instance=INSTANCE_NAME_2,
        project_id=GCP_PROJECT_ID,
    )

    # [END howto_operator_create_instance_and_import]
    create_instance_and_import = CloudMemorystoreCreateInstanceAndImportOperator(
        task_id="create-instance-and-import",
        location="europe-north1",
        instance_id=INSTANCE_NAME_3,
        instance=FIRST_INSTANCE,
        input_config={"gcs_source": {"uri": EXPORT_GCS_URL}},
        project_id=GCP_PROJECT_ID,
    )
    # [START howto_operator_create_instance_and_import]

    # [START howto_operator_scale_instance]
    scale_instance = CloudMemorystoreScaleInstanceOperator(
        task_id="scale-instance",
        location="europe-north1",
        instance_id=INSTANCE_NAME_3,
        project_id=GCP_PROJECT_ID,
        memory_size_gb=3,
    )
    # [END howto_operator_scale_instance]

    # [END howto_operator_export_and_delete_instance]
    export_and_delete_instance = CloudMemorystoreExportAndDeleteInstanceOperator(
        task_id="export-and-delete-instance",
        location="europe-north1",
        instance=INSTANCE_NAME_3,
        output_config={"gcs_destination": {"uri": EXPORT_GCS_URL}},
        project_id=GCP_PROJECT_ID,
    )
    # [START howto_operator_export_and_delete_instance]

    create_instance >> get_instance >> get_instance_result
    create_instance >> update_instance
    create_instance >> create_instance_result
    create_instance >> export_instance
    create_instance_2 >> import_instance
    create_instance >> list_instances >> list_instances_result
    list_instances >> delete_instance
    export_instance >> update_instance
    update_instance >> delete_instance
    get_instance >> set_acl_permission >> export_instance
    export_instance >> import_instance
    export_instance >> delete_instance
    import_instance >> delete_instance_2
    create_instance_2 >> failover_instance
    failover_instance >> delete_instance_2

    export_instance >> create_instance_and_import >> scale_instance >> export_and_delete_instance
