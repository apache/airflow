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
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs import AzureFileShareToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = 'azure_fileshare_to_gcs_example'

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
AZURE_SHARE_NAME = os.environ.get('AZURE_SHARE_NAME', 'test-azure-share')
AZURE_DIRECTORY_NAME = "test-azure-dir"

with DAG(
    dag_id=DAG_ID,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'azure'],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    # [START howto_operator_azure_fileshare_to_gcs_basic]
    sync_azure_files_with_gcs = AzureFileShareToGCSOperator(
        task_id='sync_azure_files_with_gcs',
        share_name=AZURE_SHARE_NAME,
        dest_gcs=BUCKET_NAME,
        directory_name=AZURE_DIRECTORY_NAME,
        replace=False,
        gzip=True,
        google_impersonation_chain=None,
    )
    # [END howto_operator_azure_fileshare_to_gcs_basic]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> sync_azure_files_with_gcs
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
