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
import os
from typing import Dict

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.mlengine import (
    MLEngineCreateModelOperator, MLEngineCreateVersionOperator, MLEngineDeleteModelOperator,
    MLEngineDeleteVersionOperator, MLEngineGetModelOperator, MLEngineListVersionsOperator,
    MLEngineSetDefaultVersionOperator, MLEngineStartBatchPredictionJobOperator,
    MLEngineStartTrainingJobOperator,
)
from airflow.providers.google.cloud.utils import mlengine_operator_utils
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

MODEL_NAME = os.environ.get("GCP_MLENGINE_MODEL_NAME", "model_name")

SAVED_MODEL_PATH = os.environ.get("GCP_MLENGINE_SAVED_MODEL_PATH", "gs://test-airflow-mlengine/saved-model/")
JOB_DIR = os.environ.get("GCP_MLENGINE_JOB_DIR", "gs://test-airflow-mlengine/keras-job-dir")
PREDICTION_INPUT = os.environ.get("GCP_MLENGINE_PREDICTION_INPUT",
                                  "gs://test-airflow-mlengine/prediction_input.json")
PREDICTION_OUTPUT = os.environ.get("GCP_MLENGINE_PREDICTION_OUTPUT",
                                   "gs://test-airflow-mlengine/prediction_output")
TRAINER_URI = os.environ.get("GCP_MLENGINE_TRAINER_URI", "gs://test-airflow-mlengine/trainer.tar.gz")
TRAINER_PY_MODULE = os.environ.get("GCP_MLENGINE_TRAINER_TRAINER_PY_MODULE", "trainer.task")

SUMMARY_TMP = os.environ.get("GCP_MLENGINE_DATAFLOW_TMP", "gs://test-airflow-mlengine/tmp/")
SUMMARY_STAGING = os.environ.get("GCP_MLENGINE_DATAFLOW_STAGING", "gs://test-airflow-mlengine/staging/")

default_args = {
    "params": {
        "model_name": MODEL_NAME
    }
}

with models.DAG(
    "example_gcp_mlengine",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag:
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
        bash_command="echo \"{{ task_instance.xcom_pull('get-model') }}\"",
        task_id="get-model-result",
    )
    # [END howto_operator_gcp_mlengine_print_model]

    # [START howto_operator_gcp_mlengine_create_version1]
    create_version = MLEngineCreateVersionOperator(
        task_id="create-version",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        version={
            "name": "v1",
            "description": "First-version",
            "deployment_uri": '{}/keras_export/'.format(JOB_DIR),
            "runtime_version": "1.15",
            "machineType": "mls1-c1-m2",
            "framework": "TENSORFLOW",
            "pythonVersion": "3.7"
        }
    )
    # [END howto_operator_gcp_mlengine_create_version1]

    # [START howto_operator_gcp_mlengine_create_version2]
    create_version_2 = MLEngineCreateVersionOperator(
        task_id="create-version-2",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        version={
            "name": "v2",
            "description": "Second version",
            "deployment_uri": SAVED_MODEL_PATH,
            "runtime_version": "1.15",
            "machineType": "mls1-c1-m2",
            "framework": "TENSORFLOW",
            "pythonVersion": "3.7"
        }
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
        bash_command="echo \"{{ task_instance.xcom_pull('list-version') }}\"",
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
    delete_version = MLEngineDeleteVersionOperator(
        task_id="delete-version",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        version_name="v1"
    )
    # [END howto_operator_gcp_mlengine_delete_version]

    # [START howto_operator_gcp_mlengine_delete_model]
    delete_model = MLEngineDeleteModelOperator(
        task_id="delete-model",
        project_id=PROJECT_ID,
        model_name=MODEL_NAME,
        delete_contents=True
    )
    # [END howto_operator_gcp_mlengine_delete_model]

    training >> create_version
    training >> create_version_2
    create_model >> get_model >> [get_model_result, delete_model]
    create_model >> create_version >> create_version_2 >> set_defaults_version >> list_version
    create_version >> prediction
    create_version_2 >> prediction
    prediction >> delete_version
    list_version >> list_version_result
    list_version >> delete_version
    delete_version >> delete_model

    # [START howto_operator_gcp_mlengine_get_metric]
    def get_metric_fn_and_keys():
        """
        Gets metric function and keys used to generate summary
        """
        def normalize_value(inst: Dict):
            val = float(inst['dense_4'][0])
            return tuple([val])  # returns a tuple.
        return normalize_value, ['val']  # key order must match.
    # [END howto_operator_gcp_mlengine_get_metric]

    # [START howto_operator_gcp_mlengine_validate_error]
    def validate_err_and_count(summary: Dict) -> Dict:
        """
        Validate summary result
        """
        if summary['val'] > 1:
            raise ValueError('Too high val>1; summary={}'.format(summary))
        if summary['val'] < 0:
            raise ValueError('Too low val<0; summary={}'.format(summary))
        if summary['count'] != 20:
            raise ValueError('Invalid value val != 20; summary={}'.format(summary))
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
            'project': PROJECT_ID,
            'tempLocation': SUMMARY_TMP,
            'stagingLocation': SUMMARY_STAGING,
        },
        model_name=MODEL_NAME,
        version_name="v1",
        py_interpreter="python3",
    )
    # [END howto_operator_gcp_mlengine_evaluate]

    create_model >> create_version >> evaluate_prediction
    evaluate_validation >> delete_version
