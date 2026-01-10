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
Example Airflow DAG that shows how to use DataFusion.
"""

from __future__ import annotations

import os
from datetime import datetime

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.datafusion import DataFusionHook
from airflow.providers.google.cloud.operators.datafusion import (
    CloudDataFusionCreateInstanceOperator,
    CloudDataFusionCreatePipelineOperator,
    CloudDataFusionDeleteInstanceOperator,
    CloudDataFusionDeletePipelineOperator,
    CloudDataFusionGetInstanceOperator,
    CloudDataFusionListPipelinesOperator,
    CloudDataFusionRestartInstanceOperator,
    CloudDataFusionStartPipelineOperator,
    CloudDataFusionStopPipelineOperator,
    CloudDataFusionUpdateInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.datafusion import CloudDataFusionPipelineStateSensor

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

# [START howto_data_fusion_env_variables]
SERVICE_ACCOUNT = os.environ.get("GCP_DATAFUSION_SERVICE_ACCOUNT")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
LOCATION = "europe-north1"
DAG_ID = "datafusion"
INSTANCE_NAME = f"df-{ENV_ID}".replace("_", "-")
INSTANCE = {
    "type": "BASIC",
    "displayName": INSTANCE_NAME,
    "dataprocServiceAccount": SERVICE_ACCOUNT,
}

BUCKET_NAME_1 = f"bucket1-{DAG_ID}-{ENV_ID}".replace("_", "-")
BUCKET_NAME_2 = f"bucket2-{DAG_ID}-{ENV_ID}".replace("_", "-")
BUCKET_NAME_1_URI = f"gs://{BUCKET_NAME_1}"
BUCKET_NAME_2_URI = f"gs://{BUCKET_NAME_2}"

PIPELINE_NAME = f"pipe-{ENV_ID}".replace("_", "-")
PIPELINE = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "{{ task_instance.xcom_pull(task_ids='get_artifacts_versions')['cdap-data-pipeline'] }}",
        "scope": "SYSTEM",
    },
    "description": "Data Pipeline Application",
    "name": PIPELINE_NAME,
    "config": {
        "resources": {"memoryMB": 2048, "virtualCores": 1},
        "driverResources": {"memoryMB": 2048, "virtualCores": 1},
        "connections": [{"from": "GCS", "to": "GCS2"}],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCSFile",
                    "type": "batchsource",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "{{ task_instance.xcom_pull(task_ids='get_artifacts_versions')\
                            ['google-cloud'] }}",
                        "scope": "SYSTEM",
                    },
                    "properties": {
                        "project": "auto-detect",
                        "format": "text",
                        "skipHeader": "false",
                        "serviceFilePath": "auto-detect",
                        "filenameOnly": "false",
                        "recursive": "false",
                        "encrypted": "false",
                        "schema": '{"type":"record","name":"textfile","fields":[{"name"\
                            :"offset","type":"long"},{"name":"body","type":"string"}]}',
                        "path": BUCKET_NAME_1_URI,
                        "referenceName": "foo_bucket",
                        "useConnection": "false",
                        "serviceAccountType": "filePath",
                        "sampleSize": "1000",
                        "fileEncoding": "UTF-8",
                    },
                },
                "outputSchema": '{"type":"record","name":"textfile","fields"\
                    :[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                "id": "GCS",
            },
            {
                "name": "GCS2",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS2",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "{{ task_instance.xcom_pull(task_ids='get_artifacts_versions')\
                            ['google-cloud'] }}",
                        "scope": "SYSTEM",
                    },
                    "properties": {
                        "project": "auto-detect",
                        "suffix": "yyyy-MM-dd-HH-mm",
                        "format": "json",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "schema": '{"type":"record","name":"textfile","fields":[{"name"\
                            :"offset","type":"long"},{"name":"body","type":"string"}]}',
                        "referenceName": "bar",
                        "path": BUCKET_NAME_2_URI,
                        "serviceAccountType": "filePath",
                        "contentType": "application/octet-stream",
                    },
                },
                "outputSchema": '{"type":"record","name":"textfile","fields"\
                    :[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                "inputSchema": [
                    {
                        "name": "GCS",
                        "schema": '{"type":"record","name":"textfile","fields":[{"name"\
                            :"offset","type":"long"},{"name":"body","type":"string"}]}',
                    }
                ],
                "id": "GCS2",
            },
        ],
        "schedule": "0 * * * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "description": "Data Pipeline Application",
        "maxConcurrentRuns": 1,
    },
}
# [END howto_data_fusion_env_variables]

CloudDataFusionCreatePipelineOperator.template_fields = (
    *CloudDataFusionCreatePipelineOperator.template_fields,
    "pipeline",
)

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "datafusion"],
) as dag:
    create_bucket1 = GCSCreateBucketOperator(
        task_id="create_bucket1",
        bucket_name=BUCKET_NAME_1,
        project_id=PROJECT_ID,
    )

    create_bucket2 = GCSCreateBucketOperator(
        task_id="create_bucket2",
        bucket_name=BUCKET_NAME_2,
        project_id=PROJECT_ID,
    )

    # [START howto_cloud_data_fusion_create_instance_operator]
    create_instance = CloudDataFusionCreateInstanceOperator(
        location=LOCATION,
        instance_name=INSTANCE_NAME,
        instance=INSTANCE,
        task_id="create_instance",
    )
    # [END howto_cloud_data_fusion_create_instance_operator]

    # [START howto_cloud_data_fusion_get_instance_operator]
    get_instance = CloudDataFusionGetInstanceOperator(
        location=LOCATION, instance_name=INSTANCE_NAME, task_id="get_instance"
    )
    # [END howto_cloud_data_fusion_get_instance_operator]

    # [START howto_cloud_data_fusion_restart_instance_operator]
    restart_instance = CloudDataFusionRestartInstanceOperator(
        location=LOCATION, instance_name=INSTANCE_NAME, task_id="restart_instance"
    )
    # [END howto_cloud_data_fusion_restart_instance_operator]

    # [START howto_cloud_data_fusion_update_instance_operator]
    update_instance = CloudDataFusionUpdateInstanceOperator(
        location=LOCATION,
        instance_name=INSTANCE_NAME,
        instance=INSTANCE,
        update_mask="",
        task_id="update_instance",
    )
    # [END howto_cloud_data_fusion_update_instance_operator]

    @task(task_id="get_artifacts_versions")
    def get_artifacts_versions(ti=None):
        hook = DataFusionHook()
        instance_url = ti.xcom_pull(task_ids="get_instance", key="return_value")["apiEndpoint"]
        artifacts = hook.get_instance_artifacts(instance_url=instance_url, namespace="default")
        return {item["name"]: item["version"] for item in artifacts}

    # [START howto_cloud_data_fusion_create_pipeline]
    create_pipeline = CloudDataFusionCreatePipelineOperator(
        location=LOCATION,
        pipeline_name=PIPELINE_NAME,
        pipeline=PIPELINE,
        instance_name=INSTANCE_NAME,
        task_id="create_pipeline",
    )
    # [END howto_cloud_data_fusion_create_pipeline]

    # [START howto_cloud_data_fusion_list_pipelines]
    list_pipelines = CloudDataFusionListPipelinesOperator(
        location=LOCATION, instance_name=INSTANCE_NAME, task_id="list_pipelines"
    )
    # [END howto_cloud_data_fusion_list_pipelines]

    # [START howto_cloud_data_fusion_start_pipeline]
    start_pipeline = CloudDataFusionStartPipelineOperator(
        location=LOCATION,
        pipeline_name=PIPELINE_NAME,
        instance_name=INSTANCE_NAME,
        pipeline_timeout=1000,
        task_id="start_pipeline",
    )
    # [END howto_cloud_data_fusion_start_pipeline]

    # [START howto_cloud_data_fusion_start_pipeline_def]
    start_pipeline_def = CloudDataFusionStartPipelineOperator(
        location=LOCATION,
        pipeline_name=PIPELINE_NAME,
        instance_name=INSTANCE_NAME,
        task_id="start_pipeline_def",
        deferrable=True,
    )
    # [END howto_cloud_data_fusion_start_pipeline_def]

    # [START howto_cloud_data_fusion_start_pipeline_async]
    start_pipeline_async = CloudDataFusionStartPipelineOperator(
        location=LOCATION,
        pipeline_name=PIPELINE_NAME,
        instance_name=INSTANCE_NAME,
        asynchronous=True,
        task_id="start_pipeline_async",
    )
    # [END howto_cloud_data_fusion_start_pipeline_async]

    # [START howto_cloud_data_fusion_start_pipeline_sensor]
    start_pipeline_sensor = CloudDataFusionPipelineStateSensor(
        task_id="pipeline_state_sensor",
        pipeline_name=PIPELINE_NAME,
        pipeline_id=start_pipeline_async.output,
        expected_statuses=["COMPLETED"],
        failure_statuses=["FAILED"],
        instance_name=INSTANCE_NAME,
        location=LOCATION,
    )
    # [END howto_cloud_data_fusion_start_pipeline_sensor]

    # [START howto_cloud_data_fusion_stop_pipeline]
    stop_pipeline = CloudDataFusionStopPipelineOperator(
        location=LOCATION,
        pipeline_name=PIPELINE_NAME,
        instance_name=INSTANCE_NAME,
        task_id="stop_pipeline",
    )
    # [END howto_cloud_data_fusion_stop_pipeline]

    # [START howto_cloud_data_fusion_delete_pipeline]
    delete_pipeline = CloudDataFusionDeletePipelineOperator(
        location=LOCATION,
        pipeline_name=PIPELINE_NAME,
        instance_name=INSTANCE_NAME,
        task_id="delete_pipeline",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_cloud_data_fusion_delete_pipeline]

    # [START howto_cloud_data_fusion_delete_instance_operator]
    delete_instance = CloudDataFusionDeleteInstanceOperator(
        location=LOCATION,
        instance_name=INSTANCE_NAME,
        task_id="delete_instance",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_cloud_data_fusion_delete_instance_operator]

    delete_bucket1 = GCSDeleteBucketOperator(
        task_id="delete_bucket1", bucket_name=BUCKET_NAME_1, trigger_rule=TriggerRule.ALL_DONE
    )
    delete_bucket2 = GCSDeleteBucketOperator(
        task_id="delete_bucket2", bucket_name=BUCKET_NAME_1, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        [create_bucket1, create_bucket2]
        # TEST BODY
        >> create_instance
        >> get_instance
        >> get_artifacts_versions()
        >> restart_instance
        >> update_instance
        >> create_pipeline
        >> list_pipelines
        >> start_pipeline_def
        >> start_pipeline_async
        >> start_pipeline_sensor
        >> start_pipeline
        >> stop_pipeline
        >> delete_pipeline
        >> delete_instance
        # TEST TEARDOWN
        >> [delete_bucket1, delete_bucket2]
    )

    from tests_common.test_utils.watcher import watcher

# This test needs watcher in order to properly mark success/failure
# when "tearDown" task with trigger rule is part of the DAG
list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
