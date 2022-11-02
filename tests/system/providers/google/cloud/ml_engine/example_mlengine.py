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
Example Airflow DAG for Google ML Engine service.
"""
from __future__ import annotations

import os
import pathlib
from datetime import datetime
from math import ceil

from airflow import models
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.mlengine import (
    MLEngineCreateModelOperator,
    MLEngineCreateVersionOperator,
    MLEngineDeleteModelOperator,
    MLEngineDeleteVersionOperator,
    MLEngineGetModelOperator,
    MLEngineListVersionsOperator,
    MLEngineSetDefaultVersionOperator,
    MLEngineStartBatchPredictionJobOperator,
    MLEngineStartTrainingJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.utils import mlengine_operator_utils
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

DAG_ID = "example_gcp_mlengine"
PREDICT_FILE_NAME = "predict.json"
MODEL_NAME = f"example_mlengine_model_{ENV_ID}"
BUCKET_NAME = f"example_mlengine_bucket_{ENV_ID}"
BUCKET_PATH = f"gs://{BUCKET_NAME}"
JOB_DIR = f"{BUCKET_PATH}/job-dir"
SAVED_MODEL_PATH = f"{JOB_DIR}/"
PREDICTION_INPUT = f"{BUCKET_PATH}/{PREDICT_FILE_NAME}"
PREDICTION_OUTPUT = f"{BUCKET_PATH}/prediction_output/"
TRAINER_URI = "gs://system-tests-resources/example_gcp_mlengine/trainer-0.1.tar.gz"
TRAINER_PY_MODULE = "trainer.task"
SUMMARY_TMP = f"{BUCKET_PATH}/tmp/"
SUMMARY_STAGING = f"{BUCKET_PATH}/staging/"

BASE_DIR = pathlib.Path(__file__).parent.resolve()
PATH_TO_PREDICT_FILE = BASE_DIR / PREDICT_FILE_NAME


def generate_model_predict_input_data() -> list[int]:
    return [i for i in range(0, 201, 10)]


with models.DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "ml_engine"],
    params={"model_name": MODEL_NAME},
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create-bucket",
        bucket_name=BUCKET_NAME,
    )

    @task(task_id="write-predict-data-file")
    def write_predict_file(path_to_file: str):
        predict_data = generate_model_predict_input_data()
        with open(path_to_file, "w") as file:
            for predict_value in predict_data:
                file.write(f'{{"input_layer": [{predict_value}]}}\n')

    write_data = write_predict_file(path_to_file=PATH_TO_PREDICT_FILE)

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload-predict-file",
        src=[PATH_TO_PREDICT_FILE],
        dst=PREDICT_FILE_NAME,
        bucket=BUCKET_NAME,
    )

    # [START howto_operator_gcp_mlengine_training]
    training = MLEngineStartTrainingJobOperator(
        task_id="training",
        project_id=PROJECT_ID,
        region="us-central1",
        job_id="training-job-{{ ts_nodash }}-{{ params.model_name }}",
        runtime_version="1.15",
        python_version="3.7",
        job_dir=JOB_DIR,
        package_uris=[TRAINER_URI],
        training_python_module=TRAINER_PY_MODULE,
        training_args=[],
        labels={"job_type": "training"},
    )
    # [END howto_operator_gcp_mlengine_training]

    # [START howto_operator_gcp_mlengine_create_model]
    create_model = MLEngineCreateModelOperator(
        task_id="create-model",
        project_id=PROJECT_ID,
        model={
            "name": MODEL_NAME,
        },
    )
    # [END howto_operator_gcp_mlengine_create_model]

    # [START howto_operator_gcp_mlengine_get_model]
    get_model = MLEngineGetModelOperator(
        task_id="get-model",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
    )
    # [END howto_operator_gcp_mlengine_get_model]

    # [START howto_operator_gcp_mlengine_print_model]
    get_model_result = BashOperator(
        bash_command=f"echo {get_model.output}",
        task_id="get-model-result",
    )
    # [END howto_operator_gcp_mlengine_print_model]

    # [START howto_operator_gcp_mlengine_create_version1]
    create_version_v1 = MLEngineCreateVersionOperator(
        task_id="create-version-v1",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        version={
            "name": "v1",
            "description": "First-version",
            "deployment_uri": JOB_DIR,
            "runtime_version": "1.15",
            "machineType": "mls1-c1-m2",
            "framework": "TENSORFLOW",
            "pythonVersion": "3.7",
        },
    )
    # [END howto_operator_gcp_mlengine_create_version1]

    # [START howto_operator_gcp_mlengine_create_version2]
    create_version_v2 = MLEngineCreateVersionOperator(
        task_id="create-version-v2",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        version={
            "name": "v2",
            "description": "Second version",
            "deployment_uri": JOB_DIR,
            "runtime_version": "1.15",
            "machineType": "mls1-c1-m2",
            "framework": "TENSORFLOW",
            "pythonVersion": "3.7",
        },
    )
    # [END howto_operator_gcp_mlengine_create_version2]

    # [START howto_operator_gcp_mlengine_default_version]
    set_defaults_version = MLEngineSetDefaultVersionOperator(
        task_id="set-default-version",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        version_name="v2",
    )
    # [END howto_operator_gcp_mlengine_default_version]

    # [START howto_operator_gcp_mlengine_list_versions]
    list_version = MLEngineListVersionsOperator(
        task_id="list-version",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
    )
    # [END howto_operator_gcp_mlengine_list_versions]

    # [START howto_operator_gcp_mlengine_print_versions]
    list_version_result = BashOperator(
        bash_command=f"echo {list_version.output}",
        task_id="list-version-result",
    )
    # [END howto_operator_gcp_mlengine_print_versions]

    # [START howto_operator_gcp_mlengine_get_prediction]
    prediction = MLEngineStartBatchPredictionJobOperator(
        task_id="prediction",
        project_id=PROJECT_ID,
        job_id="prediction-{{ ts_nodash }}-{{ params.model_name }}",
        region="us-central1",
        model_name=MODEL_NAME,
        data_format="TEXT",
        input_paths=[PREDICTION_INPUT],
        output_path=PREDICTION_OUTPUT,
        labels={"job_type": "prediction"},
    )
    # [END howto_operator_gcp_mlengine_get_prediction]

    # [START howto_operator_gcp_mlengine_delete_version]
    delete_version_v1 = MLEngineDeleteVersionOperator(
        task_id="delete-version-v1",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        version_name="v1",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gcp_mlengine_delete_version]

    delete_version_v2 = MLEngineDeleteVersionOperator(
        task_id="delete-version-v2",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        version_name="v2",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_gcp_mlengine_delete_model]
    delete_model = MLEngineDeleteModelOperator(
        task_id="delete-model",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gcp_mlengine_delete_model]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete-bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_gcp_mlengine_get_metric]
    def get_metric_fn_and_keys():
        """
        Gets metric function and keys used to generate summary
        """

        def normalize_value(inst: dict):
            val = float(inst["output_layer"][0])
            return tuple([val])  # returns a tuple.

        return normalize_value, ["val"]  # key order must match.

    # [END howto_operator_gcp_mlengine_get_metric]

    # [START howto_operator_gcp_mlengine_validate_error]
    def validate_err_and_count(summary: dict) -> dict:
        """
        Validate summary result
        """
        summary = summary.get("val", 0)
        initial_values = generate_model_predict_input_data()
        initial_summary = sum(initial_values) / len(initial_values)

        multiplier = ceil(summary / initial_summary)
        if multiplier != 2:
            raise ValueError(f"Multiplier is not equal 2; multiplier: {multiplier}")
        return summary

    # [END howto_operator_gcp_mlengine_validate_error]

    # [START howto_operator_gcp_mlengine_evaluate]
    evaluate_prediction, evaluate_summary, evaluate_validation = mlengine_operator_utils.create_evaluate_ops(
        task_prefix="evaluate-ops",
        data_format="TEXT",
        input_paths=[PREDICTION_INPUT],
        prediction_path=PREDICTION_OUTPUT,
        metric_fn_and_keys=get_metric_fn_and_keys(),
        validate_fn=validate_err_and_count,
        batch_prediction_job_id="evaluate-ops-{{ ts_nodash }}-{{ params.model_name }}",
        project_id=PROJECT_ID,
        region="us-central1",
        dataflow_options={
            "project": PROJECT_ID,
            "tempLocation": SUMMARY_TMP,
            "stagingLocation": SUMMARY_STAGING,
        },
        model_name=MODEL_NAME,
        version_name="v1",
        py_interpreter="python3",
    )
    # [END howto_operator_gcp_mlengine_evaluate]

    # TEST SETUP
    create_bucket >> write_data >> upload_file
    upload_file >> [prediction, evaluate_prediction]
    create_bucket >> training >> create_version_v1

    # TEST BODY
    create_model >> get_model >> [get_model_result, delete_model]
    create_model >> create_version_v1 >> create_version_v2 >> set_defaults_version >> list_version

    create_version_v1 >> prediction
    create_version_v1 >> evaluate_prediction
    create_version_v2 >> prediction

    list_version >> [list_version_result, delete_version_v1]
    prediction >> delete_version_v1

    # TEST TEARDOWN
    evaluate_validation >> delete_version_v1 >> delete_version_v2 >> delete_model >> delete_bucket

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
