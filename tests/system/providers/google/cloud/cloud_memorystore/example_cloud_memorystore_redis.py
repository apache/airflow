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
from datetime import datetime

from google.cloud.redis_v1 import FailoverInstanceRequest, Instance

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_memorystore import (
    CloudMemorystoreCreateInstanceAndImportOperator,
    CloudMemorystoreCreateInstanceOperator,
    CloudMemorystoreDeleteInstanceOperator,
    CloudMemorystoreExportAndDeleteInstanceOperator,
    CloudMemorystoreExportInstanceOperator,
    CloudMemorystoreFailoverInstanceOperator,
    CloudMemorystoreGetInstanceOperator,
    CloudMemorystoreImportOperator,
    CloudMemorystoreListInstancesOperator,
    CloudMemorystoreScaleInstanceOperator,
    CloudMemorystoreUpdateInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "cloud_memorystore_redis"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
LOCATION = "europe-north1"
MEMORYSTORE_REDIS_INSTANCE_NAME = f"{ENV_ID}-redis-1"
MEMORYSTORE_REDIS_INSTANCE_NAME_2 = f"{ENV_ID}-redis-2"
MEMORYSTORE_REDIS_INSTANCE_NAME_3 = f"{ENV_ID}-redis-3"

EXPORT_GCS_URL = f"gs://{BUCKET_NAME}/my-export.rdb"

# [START howto_operator_instance]
FIRST_INSTANCE = {"tier": Instance.Tier.BASIC, "memory_size_gb": 1}
# [END howto_operator_instance]

SECOND_INSTANCE = {"tier": Instance.Tier.STANDARD_HA, "memory_size_gb": 3}


with models.DAG(
    DAG_ID,
    schedule='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    # [START howto_operator_create_instance]
    create_instance = CloudMemorystoreCreateInstanceOperator(
        task_id="create-instance",
        location=LOCATION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME,
        instance=FIRST_INSTANCE,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_create_instance]

    # [START howto_operator_create_instance_result]
    create_instance_result = BashOperator(
        task_id="create-instance-result",
        bash_command=f"echo {create_instance.output}",
    )
    # [END howto_operator_create_instance_result]

    create_instance_2 = CloudMemorystoreCreateInstanceOperator(
        task_id="create-instance-2",
        location=LOCATION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME_2,
        instance=SECOND_INSTANCE,
        project_id=PROJECT_ID,
    )

    # [START howto_operator_get_instance]
    get_instance = CloudMemorystoreGetInstanceOperator(
        task_id="get-instance",
        location=LOCATION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME,
        project_id=PROJECT_ID,
        do_xcom_push=True,
    )
    # [END howto_operator_get_instance]

    # [START howto_operator_get_instance_result]
    get_instance_result = BashOperator(
        task_id="get-instance-result", bash_command=f"echo {get_instance.output}"
    )
    # [END howto_operator_get_instance_result]

    # [START howto_operator_failover_instance]
    failover_instance = CloudMemorystoreFailoverInstanceOperator(
        task_id="failover-instance",
        location=LOCATION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME_2,
        data_protection_mode=FailoverInstanceRequest.DataProtectionMode(
            FailoverInstanceRequest.DataProtectionMode.LIMITED_DATA_LOSS
        ),
        project_id=PROJECT_ID,
    )
    # [END howto_operator_failover_instance]

    # [START howto_operator_list_instances]
    list_instances = CloudMemorystoreListInstancesOperator(
        task_id="list-instances", location="-", page_size=100, project_id=PROJECT_ID
    )
    # [END howto_operator_list_instances]

    # [START howto_operator_list_instances_result]
    list_instances_result = BashOperator(
        task_id="list-instances-result", bash_command=f"echo {get_instance.output}"
    )
    # [END howto_operator_list_instances_result]

    # [START howto_operator_update_instance]
    update_instance = CloudMemorystoreUpdateInstanceOperator(
        task_id="update-instance",
        location=LOCATION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME,
        project_id=PROJECT_ID,
        update_mask={"paths": ["memory_size_gb"]},
        instance={"memory_size_gb": 2},
    )
    # [END howto_operator_update_instance]

    # [START howto_operator_set_acl_permission]
    set_acl_permission = GCSBucketCreateAclEntryOperator(
        task_id="gcs-set-acl-permission",
        bucket=BUCKET_NAME,
        entity="user-{{ task_instance.xcom_pull('get-instance')['persistence_iam_identity']"
        ".split(':', 2)[1] }}",
        role="OWNER",
    )
    # [END howto_operator_set_acl_permission]

    # [START howto_operator_export_instance]
    export_instance = CloudMemorystoreExportInstanceOperator(
        task_id="export-instance",
        location=LOCATION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME,
        output_config={"gcs_destination": {"uri": EXPORT_GCS_URL}},
        project_id=PROJECT_ID,
    )
    # [END howto_operator_export_instance]

    # [START howto_operator_import_instance]
    import_instance = CloudMemorystoreImportOperator(
        task_id="import-instance",
        location=LOCATION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME_2,
        input_config={"gcs_source": {"uri": EXPORT_GCS_URL}},
        project_id=PROJECT_ID,
    )
    # [END howto_operator_import_instance]

    # [START howto_operator_delete_instance]
    delete_instance = CloudMemorystoreDeleteInstanceOperator(
        task_id="delete-instance",
        location=LOCATION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_delete_instance]
    delete_instance.trigger_rule = TriggerRule.ALL_DONE

    delete_instance_2 = CloudMemorystoreDeleteInstanceOperator(
        task_id="delete-instance-2",
        location=LOCATION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME_2,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [END howto_operator_create_instance_and_import]
    create_instance_and_import = CloudMemorystoreCreateInstanceAndImportOperator(
        task_id="create-instance-and-import",
        location=LOCATION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME_3,
        instance=FIRST_INSTANCE,
        input_config={"gcs_source": {"uri": EXPORT_GCS_URL}},
        project_id=PROJECT_ID,
    )
    # [START howto_operator_create_instance_and_import]

    # [START howto_operator_scale_instance]
    scale_instance = CloudMemorystoreScaleInstanceOperator(
        task_id="scale-instance",
        location=LOCATION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME_3,
        project_id=PROJECT_ID,
        memory_size_gb=3,
    )
    # [END howto_operator_scale_instance]

    # [END howto_operator_export_and_delete_instance]
    export_and_delete_instance = CloudMemorystoreExportAndDeleteInstanceOperator(
        task_id="export-and-delete-instance",
        location=LOCATION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME_3,
        output_config={"gcs_destination": {"uri": EXPORT_GCS_URL}},
        project_id=PROJECT_ID,
    )
    # [START howto_operator_export_and_delete_instance]
    export_and_delete_instance.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        create_bucket
        >> create_instance
        >> create_instance_result
        >> get_instance
        >> get_instance_result
        >> set_acl_permission
        >> export_instance
        >> update_instance
        >> list_instances
        >> list_instances_result
        >> create_instance_2
        >> failover_instance
        >> import_instance
        >> delete_instance
        >> delete_instance_2
        >> create_instance_and_import
        >> scale_instance
        >> export_and_delete_instance
        >> delete_bucket
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
