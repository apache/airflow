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

from airflow import models
from airflow.operators.bash import BashOperator
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
from airflow.utils import dates
from airflow.utils.state import State

# [START howto_data_fusion_env_variables]
LOCATION = "europe-north1"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE = {"type": "BASIC", "displayName": INSTANCE_NAME}

BUCKET1 = "gs://test-bucket--2h83r23r"
BUCKET2 = "gs://test-bucket--2d23h83r23r"
PIPELINE_NAME = "airflow_test"
PIPELINE = {
    "name": "test-pipe",
    "description": "Data Pipeline Application",
    "artifact": {"name": "cdap-data-pipeline", "version": "6.1.2", "scope": "SYSTEM"},
    "config": {
        "resources": {"memoryMB": 2048, "virtualCores": 1},
        "driverResources": {"memoryMB": 2048, "virtualCores": 1},
        "connections": [{"from": "GCS", "to": "GCS2"}],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": True,
        "stageLoggingEnabled": False,
        "stages": [
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCSFile",
                    "type": "batchsource",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.14.2",
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
                        "schema": '{"type":"record","name":"etlSchemaBody","fields":'
                        '[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                        "path": BUCKET1,
                        "referenceName": "foo_bucket",
                    },
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": '{"type":"record","name":"etlSchemaBody","fields":'
                        '[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                    }
                ],
            },
            {
                "name": "GCS2",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS2",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.14.2",
                        "scope": "SYSTEM",
                    },
                    "properties": {
                        "project": "auto-detect",
                        "suffix": "yyyy-MM-dd-HH-mm",
                        "format": "json",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "schema": '{"type":"record","name":"etlSchemaBody","fields":'
                        '[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                        "referenceName": "bar",
                        "path": BUCKET2,
                    },
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": '{"type":"record","name":"etlSchemaBody","fields":'
                        '[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                    }
                ],
                "inputSchema": [
                    {
                        "name": "GCS",
                        "schema": '{"type":"record","name":"etlSchemaBody","fields":'
                        '[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                    }
                ],
            },
        ],
        "schedule": "0 * * * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "maxConcurrentRuns": 1,
    },
}
# [END howto_data_fusion_env_variables]


with models.DAG(
    "example_data_fusion",
    schedule_interval=None,  # Override to match your needs
    start_date=dates.days_ago(1),
) as dag:
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
        update_mask="instance.displayName",
        task_id="update_instance",
    )
    # [END howto_cloud_data_fusion_update_instance_operator]

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
        task_id="start_pipeline",
    )
    # [END howto_cloud_data_fusion_start_pipeline]

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
    )
    # [END howto_cloud_data_fusion_delete_pipeline]

    # [START howto_cloud_data_fusion_delete_instance_operator]
    delete_instance = CloudDataFusionDeleteInstanceOperator(
        location=LOCATION, instance_name=INSTANCE_NAME, task_id="delete_instance"
    )
    # [END howto_cloud_data_fusion_delete_instance_operator]

    # Add sleep before creating pipeline
    sleep = BashOperator(task_id="sleep", bash_command="sleep 60")

    create_instance >> get_instance >> restart_instance >> update_instance >> sleep
    sleep >> create_pipeline >> list_pipelines >> start_pipeline >> stop_pipeline >> delete_pipeline
    delete_pipeline >> delete_instance

if __name__ == "__main__":
    dag.clear(dag_run_state=State.NONE)
    dag.run()
