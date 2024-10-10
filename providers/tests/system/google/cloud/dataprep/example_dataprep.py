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
Example Airflow DAG that shows how to use Google Dataprep.

This DAG relies on the following OS environment variables

* SYSTEM_TESTS_DATAPREP_TOKEN - Dataprep API access token.
  For generating it please use instruction
  https://docs.trifacta.com/display/DP/Manage+API+Access+Tokens#:~:text=Enable%20individual%20access-,Generate%20New%20Token,-Via%20UI.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow import models
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.hooks.dataprep import GoogleDataprepHook
from airflow.providers.google.cloud.operators.dataprep import (
    DataprepCopyFlowOperator,
    DataprepDeleteFlowOperator,
    DataprepGetJobGroupOperator,
    DataprepGetJobsForJobGroupOperator,
    DataprepRunFlowOperator,
    DataprepRunJobGroupOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.dataprep import DataprepJobGroupIsFinishedSensor
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "dataprep"

CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}".replace("-", "_")
DATAPREP_TOKEN = os.environ.get("SYSTEM_TESTS_DATAPREP_TOKEN", "")
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
GCS_BUCKET_NAME = f"dataprep-bucket-{DAG_ID}-{ENV_ID}"
GCS_BUCKET_PATH = f"gs://{GCS_BUCKET_NAME}/task_results/"

DATASET_URI = "gs://airflow-system-tests-resources/dataprep/dataset-00000.parquet"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}".replace("-", "_")
DATASET_WRANGLED_NAME = f"wrangled_{DATASET_NAME}"
DATASET_WRANGLED_ID = "{{ task_instance.xcom_pull('create_wrangled_dataset')['id'] }}"

FLOW_ID = "{{ task_instance.xcom_pull('create_flow')['id'] }}"
FLOW_COPY_ID = "{{ task_instance.xcom_pull('copy_flow')['id'] }}"
RECIPE_NAME = DATASET_WRANGLED_NAME
WRITE_SETTINGS = {
    "writesettings": [
        {
            "path": GCS_BUCKET_PATH + f"adhoc_{RECIPE_NAME}.csv",
            "action": "create",
            "format": "csv",
        },
    ],
}

log = logging.getLogger(__name__)


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    catchup=False,
    tags=["example", "dataprep"],
    render_template_as_native_obj=True,
) as dag:
    create_bucket_task = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=GCS_BUCKET_NAME,
        project_id=GCP_PROJECT_ID,
    )

    @task
    def create_connection(connection_id: str) -> None:
        connection = Connection(
            conn_id=connection_id,
            description="Example Dataprep connection",
            conn_type="dataprep",
            extra={"token": DATAPREP_TOKEN},
        )
        session = Session()
        log.info("Removing connection %s if it exists", connection_id)
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()

        session.add(connection)
        session.commit()
        log.info("Connection created: '%s'", connection_id)

    create_connection_task = create_connection(connection_id=CONNECTION_ID)

    @task
    def create_imported_dataset():
        hook = GoogleDataprepHook(dataprep_conn_id=CONNECTION_ID)
        response = hook.create_imported_dataset(
            body_request={
                "uri": DATASET_URI,
                "name": DATASET_NAME,
            }
        )
        return response

    create_imported_dataset_task = create_imported_dataset()

    @task
    def create_flow():
        hook = GoogleDataprepHook(dataprep_conn_id=CONNECTION_ID)
        response = hook.create_flow(
            body_request={
                "name": f"test_flow_{DAG_ID}_{ENV_ID}",
                "description": "Test flow",
            }
        )
        return response

    create_flow_task = create_flow()

    @task
    def create_wrangled_dataset(flow, imported_dataset):
        hook = GoogleDataprepHook(dataprep_conn_id=CONNECTION_ID)
        response = hook.create_wrangled_dataset(
            body_request={
                "importedDataset": {"id": imported_dataset["id"]},
                "flow": {"id": flow["id"]},
                "name": DATASET_WRANGLED_NAME,
            }
        )
        return response

    create_wrangled_dataset_task = create_wrangled_dataset(create_flow_task, create_imported_dataset_task)

    @task
    def create_output(wrangled_dataset):
        hook = GoogleDataprepHook(dataprep_conn_id=CONNECTION_ID)
        response = hook.create_output_object(
            body_request={
                "execution": "dataflow",
                "profiler": False,
                "flowNodeId": wrangled_dataset["id"],
            }
        )
        return response

    create_output_task = create_output(create_wrangled_dataset_task)

    @task
    def create_write_settings(output):
        hook = GoogleDataprepHook(dataprep_conn_id=CONNECTION_ID)
        response = hook.create_write_settings(
            body_request={
                "path": GCS_BUCKET_PATH + f"adhoc_{RECIPE_NAME}.csv",
                "action": "create",
                "format": "csv",
                "outputObjectId": output["id"],
            }
        )
        return response

    create_write_settings_task = create_write_settings(create_output_task)

    # [START how_to_dataprep_copy_flow_operator]
    copy_task = DataprepCopyFlowOperator(
        task_id="copy_flow",
        dataprep_conn_id=CONNECTION_ID,
        project_id=GCP_PROJECT_ID,
        flow_id=FLOW_ID,
        name=f"copy_{DATASET_NAME}",
    )
    # [END how_to_dataprep_copy_flow_operator]

    # [START how_to_dataprep_run_job_group_operator]
    run_job_group_task = DataprepRunJobGroupOperator(
        task_id="run_job_group",
        dataprep_conn_id=CONNECTION_ID,
        project_id=GCP_PROJECT_ID,
        body_request={
            "wrangledDataset": {"id": DATASET_WRANGLED_ID},
            "overrides": WRITE_SETTINGS,
        },
    )
    # [END how_to_dataprep_run_job_group_operator]

    # [START how_to_dataprep_dataprep_run_flow_operator]
    run_flow_task = DataprepRunFlowOperator(
        task_id="run_flow",
        dataprep_conn_id=CONNECTION_ID,
        project_id=GCP_PROJECT_ID,
        flow_id=FLOW_COPY_ID,
        body_request={},
    )
    # [END how_to_dataprep_dataprep_run_flow_operator]

    # [START how_to_dataprep_get_job_group_operator]
    get_job_group_task = DataprepGetJobGroupOperator(
        task_id="get_job_group",
        dataprep_conn_id=CONNECTION_ID,
        project_id=GCP_PROJECT_ID,
        job_group_id="{{ task_instance.xcom_pull('run_flow')['data'][0]['id'] }}",
        embed="",
        include_deleted=False,
    )
    # [END how_to_dataprep_get_job_group_operator]

    # [START how_to_dataprep_get_jobs_for_job_group_operator]
    get_jobs_for_job_group_task = DataprepGetJobsForJobGroupOperator(
        task_id="get_jobs_for_job_group",
        dataprep_conn_id=CONNECTION_ID,
        job_group_id="{{ task_instance.xcom_pull('run_flow')['data'][0]['id'] }}",
    )
    # [END how_to_dataprep_get_jobs_for_job_group_operator]

    # [START how_to_dataprep_job_group_finished_sensor]
    check_flow_status_sensor = DataprepJobGroupIsFinishedSensor(
        task_id="check_flow_status",
        dataprep_conn_id=CONNECTION_ID,
        job_group_id="{{ task_instance.xcom_pull('run_flow')['data'][0]['id'] }}",
    )
    # [END how_to_dataprep_job_group_finished_sensor]

    # [START how_to_dataprep_job_group_finished_sensor]
    check_job_group_status_sensor = DataprepJobGroupIsFinishedSensor(
        task_id="check_job_group_status",
        dataprep_conn_id=CONNECTION_ID,
        job_group_id="{{ task_instance.xcom_pull('run_job_group')['id'] }}",
    )
    # [END how_to_dataprep_job_group_finished_sensor]

    # [START how_to_dataprep_delete_flow_operator]
    delete_flow_task = DataprepDeleteFlowOperator(
        task_id="delete_flow",
        dataprep_conn_id=CONNECTION_ID,
        flow_id="{{ task_instance.xcom_pull('copy_flow')['id'] }}",
    )
    # [END how_to_dataprep_delete_flow_operator]
    delete_flow_task.trigger_rule = TriggerRule.ALL_DONE

    delete_flow_task_original = DataprepDeleteFlowOperator(
        task_id="delete_flow_original",
        dataprep_conn_id=CONNECTION_ID,
        flow_id="{{ task_instance.xcom_pull('create_flow')['id'] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def delete_dataset(dataset):
        hook = GoogleDataprepHook(dataprep_conn_id=CONNECTION_ID)
        hook.delete_imported_dataset(dataset_id=dataset["id"])

    delete_dataset_task = delete_dataset(create_imported_dataset_task)

    delete_bucket_task = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(task_id="delete_connection")
    def delete_connection(connection_id: str) -> None:
        session = Session()
        log.info("Removing connection %s", connection_id)
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()
        session.commit()

    delete_connection_task = delete_connection(connection_id=CONNECTION_ID)

    chain(
        # TEST SETUP
        create_bucket_task,
        create_connection_task,
        [create_imported_dataset_task, create_flow_task],
        create_wrangled_dataset_task,
        create_output_task,
        create_write_settings_task,
        # TEST BODY
        copy_task,
        [run_job_group_task, run_flow_task],
        [get_job_group_task, get_jobs_for_job_group_task],
        [check_flow_status_sensor, check_job_group_status_sensor],
        # TEST TEARDOWN
        delete_dataset_task,
        [delete_flow_task, delete_flow_task_original],
        [delete_bucket_task, delete_connection_task],
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
