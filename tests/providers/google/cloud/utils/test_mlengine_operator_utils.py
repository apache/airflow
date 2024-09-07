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
from __future__ import annotations

import base64
import json
from datetime import datetime
from unittest import mock

import dill
import pytest

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.utils.mlengine_operator_utils import create_evaluate_ops
from airflow.providers.standard.core.operators.python import PythonOperator

TASK_PREFIX = "test-task-prefix"
TASK_PREFIX_PREDICTION = TASK_PREFIX + "-prediction"
TASK_PREFIX_SUMMARY = TASK_PREFIX + "-summary"
TASK_PREFIX_VALIDATION = TASK_PREFIX + "-validation"
DATA_FORMAT = "TEXT"
INPUT_PATHS = [
    "gs://path/to/input/file.json",
    "gs://path/to/input/file2.json",
    "gs://path/to/input/file3.json",
]
PREDICTION_PATH = "gs://path/to/output/predictions.json"
BATCH_PREDICTION_JOB_ID = "test-batch-prediction-job-id"
PROJECT_ID = "test-project-id"
REGION = "test-region"
DATAFLOW_OPTIONS = {
    "project": "my-gcp-project",
    "zone": "us-central1-f",
    "stagingLocation": "gs://bucket/tmp/dataflow/staging/",
}
MODEL_URI = "gs://path/to/model/model"
MODEL_NAME = "test-model-name"
VERSION_NAME = "test-version-name"
DAG_DEFAULT_ARGS = {
    "project_id": PROJECT_ID,
    "region": REGION,
    "model_name": MODEL_NAME,
    "version_name": VERSION_NAME,
    "dataflow_default_options": DATAFLOW_OPTIONS,
}
TEST_DAG = DAG(dag_id="test-dag-id", start_date=datetime(2000, 1, 1), default_args=DAG_DEFAULT_ARGS)


def get_metric_fn_and_keys():
    import math

    def error_and_squared_error(inst):
        label = float(inst["input_label"])
        classes = float(inst["classes"])
        err = abs(classes - label)
        squared_err = math.pow(classes - label, 2)
        return err, squared_err

    return error_and_squared_error, ["err", "mse"]


METRIC_FN, METRIC_KEYS = get_metric_fn_and_keys()
METRIC_KEYS_EXPECTED = ",".join(METRIC_KEYS)


def validate_err_and_count(summary):
    if summary["err"] > 0.2:
        raise ValueError(f"Too high err>0.2; summary={summary}")
    if summary["mse"] > 0.05:
        raise ValueError(f"Too high mse>0.05; summary={summary}")
    if summary["count"] < 1000:
        raise ValueError(f"Too few instances<1000; summary={summary}")
    return summary


class TestMlengineOperatorUtils:
    @mock.patch.object(PythonOperator, "set_upstream")
    @mock.patch.object(BeamRunPythonPipelineOperator, "set_upstream")
    def test_create_evaluate_ops(self, mock_beam_pipeline, mock_python):
        with pytest.warns(AirflowProviderDeprecationWarning):
            result = create_evaluate_ops(
                task_prefix=TASK_PREFIX,
                data_format=DATA_FORMAT,
                input_paths=INPUT_PATHS,
                prediction_path=PREDICTION_PATH,
                metric_fn_and_keys=get_metric_fn_and_keys(),
                validate_fn=validate_err_and_count,
                batch_prediction_job_id=BATCH_PREDICTION_JOB_ID,
                project_id=PROJECT_ID,
                region=REGION,
                dataflow_options=DATAFLOW_OPTIONS,
                model_uri=MODEL_URI,
            )

        evaluate_prediction, evaluate_summary, evaluate_validation = result

        mock_beam_pipeline.assert_called_once_with(evaluate_prediction)
        mock_python.assert_called_once_with(evaluate_summary)

        # importing apache_beam elsewhere modifies the metrics. In order to avoid metrics being modified
        # by apache_beam import happening after importing this test, we retrieve the metrics here rather than
        # at the top of the file.
        METRIC_FN_ENCODED = base64.b64encode(dill.dumps(METRIC_FN, recurse=True)).decode()

        assert TASK_PREFIX_PREDICTION == evaluate_prediction.task_id
        assert PROJECT_ID == evaluate_prediction.project_id
        assert BATCH_PREDICTION_JOB_ID == evaluate_prediction.job_id
        assert REGION == evaluate_prediction.region
        assert DATA_FORMAT == evaluate_prediction._data_format
        assert INPUT_PATHS == evaluate_prediction.input_paths
        assert PREDICTION_PATH == evaluate_prediction.output_path
        assert MODEL_URI == evaluate_prediction.uri

        assert TASK_PREFIX_SUMMARY == evaluate_summary.task_id
        assert DATAFLOW_OPTIONS == evaluate_summary.default_pipeline_options
        assert PREDICTION_PATH == evaluate_summary.pipeline_options["prediction_path"]
        assert METRIC_FN_ENCODED == evaluate_summary.pipeline_options["metric_fn_encoded"]
        assert METRIC_KEYS_EXPECTED == evaluate_summary.pipeline_options["metric_keys"]

        assert TASK_PREFIX_VALIDATION == evaluate_validation.task_id
        assert PREDICTION_PATH == evaluate_validation.templates_dict["prediction_path"]

    @mock.patch.object(PythonOperator, "set_upstream")
    @mock.patch.object(BeamRunPythonPipelineOperator, "set_upstream")
    def test_create_evaluate_ops_model_and_version_name(self, mock_beam_pipeline, mock_python):
        with pytest.warns(AirflowProviderDeprecationWarning):
            result = create_evaluate_ops(
                task_prefix=TASK_PREFIX,
                data_format=DATA_FORMAT,
                input_paths=INPUT_PATHS,
                prediction_path=PREDICTION_PATH,
                metric_fn_and_keys=get_metric_fn_and_keys(),
                validate_fn=validate_err_and_count,
                batch_prediction_job_id=BATCH_PREDICTION_JOB_ID,
                project_id=PROJECT_ID,
                region=REGION,
                dataflow_options=DATAFLOW_OPTIONS,
                model_name=MODEL_NAME,
                version_name=VERSION_NAME,
            )

        evaluate_prediction, evaluate_summary, evaluate_validation = result

        mock_beam_pipeline.assert_called_once_with(evaluate_prediction)
        mock_python.assert_called_once_with(evaluate_summary)

        # importing apache_beam elsewhere modifies the metrics. In order to avoid metrics being modified
        # by apache_beam import happening after importing this test, we retrieve the metrics here rather than
        # at the top of the file.
        METRIC_FN_ENCODED = base64.b64encode(dill.dumps(METRIC_FN, recurse=True)).decode()

        assert TASK_PREFIX_PREDICTION == evaluate_prediction.task_id
        assert PROJECT_ID == evaluate_prediction.project_id
        assert BATCH_PREDICTION_JOB_ID == evaluate_prediction.job_id
        assert REGION == evaluate_prediction.region
        assert DATA_FORMAT == evaluate_prediction._data_format
        assert INPUT_PATHS == evaluate_prediction.input_paths
        assert PREDICTION_PATH == evaluate_prediction.output_path
        assert MODEL_NAME == evaluate_prediction.model_name
        assert VERSION_NAME == evaluate_prediction.version_name

        assert TASK_PREFIX_SUMMARY == evaluate_summary.task_id
        assert DATAFLOW_OPTIONS == evaluate_summary.default_pipeline_options
        assert PREDICTION_PATH == evaluate_summary.pipeline_options["prediction_path"]
        assert METRIC_FN_ENCODED == evaluate_summary.pipeline_options["metric_fn_encoded"]
        assert METRIC_KEYS_EXPECTED == evaluate_summary.pipeline_options["metric_keys"]

        assert TASK_PREFIX_VALIDATION == evaluate_validation.task_id
        assert PREDICTION_PATH == evaluate_validation.templates_dict["prediction_path"]

    @mock.patch.object(PythonOperator, "set_upstream")
    @mock.patch.object(BeamRunPythonPipelineOperator, "set_upstream")
    def test_create_evaluate_ops_dag(self, mock_dataflow, mock_python):
        with pytest.warns(AirflowProviderDeprecationWarning):
            result = create_evaluate_ops(
                task_prefix=TASK_PREFIX,
                data_format=DATA_FORMAT,
                input_paths=INPUT_PATHS,
                prediction_path=PREDICTION_PATH,
                metric_fn_and_keys=get_metric_fn_and_keys(),
                validate_fn=validate_err_and_count,
                batch_prediction_job_id=BATCH_PREDICTION_JOB_ID,
                dag=TEST_DAG,
            )

        evaluate_prediction, evaluate_summary, evaluate_validation = result

        mock_dataflow.assert_called_once_with(evaluate_prediction)
        mock_python.assert_called_once_with(evaluate_summary)

        # importing apache_beam elsewhere modifies the metrics. In order to avoid metrics being modified
        # by apache_beam import happening after importing this test, we retrieve the metrics here rather than
        # at the top of the file.
        METRIC_FN_ENCODED = base64.b64encode(dill.dumps(METRIC_FN, recurse=True)).decode()

        assert TASK_PREFIX_PREDICTION == evaluate_prediction.task_id
        assert PROJECT_ID == evaluate_prediction.project_id
        assert BATCH_PREDICTION_JOB_ID == evaluate_prediction.job_id
        assert REGION == evaluate_prediction.region
        assert DATA_FORMAT == evaluate_prediction._data_format
        assert INPUT_PATHS == evaluate_prediction.input_paths
        assert PREDICTION_PATH == evaluate_prediction.output_path
        assert MODEL_NAME == evaluate_prediction.model_name
        assert VERSION_NAME == evaluate_prediction.version_name

        assert TASK_PREFIX_SUMMARY == evaluate_summary.task_id
        assert DATAFLOW_OPTIONS == evaluate_summary.default_pipeline_options
        assert PREDICTION_PATH == evaluate_summary.pipeline_options["prediction_path"]
        assert METRIC_FN_ENCODED == evaluate_summary.pipeline_options["metric_fn_encoded"]
        assert METRIC_KEYS_EXPECTED == evaluate_summary.pipeline_options["metric_keys"]

        assert TASK_PREFIX_VALIDATION == evaluate_validation.task_id
        assert PREDICTION_PATH == evaluate_validation.templates_dict["prediction_path"]

    @pytest.mark.db_test
    @mock.patch.object(GCSHook, "download")
    @mock.patch.object(PythonOperator, "set_upstream")
    @mock.patch.object(BeamRunPythonPipelineOperator, "set_upstream")
    def test_apply_validate_fn(self, mock_beam_pipeline, mock_python, mock_download):
        with pytest.warns(AirflowProviderDeprecationWarning):
            result = create_evaluate_ops(
                task_prefix=TASK_PREFIX,
                data_format=DATA_FORMAT,
                input_paths=INPUT_PATHS,
                prediction_path=PREDICTION_PATH,
                metric_fn_and_keys=get_metric_fn_and_keys(),
                validate_fn=validate_err_and_count,
                batch_prediction_job_id=BATCH_PREDICTION_JOB_ID,
                project_id=PROJECT_ID,
                region=REGION,
                dataflow_options=DATAFLOW_OPTIONS,
                model_uri=MODEL_URI,
            )

        _, _, evaluate_validation = result

        mock_download.return_value = json.dumps({"err": 0.3, "mse": 0.04, "count": 1100}).encode("utf-8")
        templates_dict = {"prediction_path": PREDICTION_PATH}
        with pytest.raises(ValueError) as ctx:
            evaluate_validation.python_callable(templates_dict=templates_dict)

        assert "Too high err>0.2; summary={'err': 0.3, 'mse': 0.04, 'count': 1100}" == str(ctx.value)
        mock_download.assert_called_once_with("path", "to/output/predictions.json/prediction.summary.json")

        invalid_prediction_paths = ["://path/to/output/predictions.json", "gs://", ""]

        for path in invalid_prediction_paths:
            templates_dict = {"prediction_path": path}
            with pytest.raises(ValueError) as ctx:
                evaluate_validation.python_callable(templates_dict=templates_dict)
            assert "Wrong format prediction_path:" == str(ctx.value)[:29]

    def test_invalid_task_prefix(self):
        invalid_task_prefix_values = ["test-task-prefix&", "~test-task-prefix", "test-task(-prefix"]

        for invalid_task_prefix_value in invalid_task_prefix_values:
            with pytest.raises(AirflowException):
                create_evaluate_ops(
                    task_prefix=invalid_task_prefix_value,
                    data_format=DATA_FORMAT,
                    input_paths=INPUT_PATHS,
                    prediction_path=PREDICTION_PATH,
                    metric_fn_and_keys=get_metric_fn_and_keys(),
                    validate_fn=validate_err_and_count,
                )

    def test_non_callable_metric_fn(self):
        with pytest.raises(AirflowException):
            create_evaluate_ops(
                task_prefix=TASK_PREFIX,
                data_format=DATA_FORMAT,
                input_paths=INPUT_PATHS,
                prediction_path=PREDICTION_PATH,
                metric_fn_and_keys=("error_and_squared_error", ["err", "mse"]),
                validate_fn=validate_err_and_count,
            )

    def test_non_callable_validate_fn(self):
        with pytest.raises(AirflowException):
            create_evaluate_ops(
                task_prefix=TASK_PREFIX,
                data_format=DATA_FORMAT,
                input_paths=INPUT_PATHS,
                prediction_path=PREDICTION_PATH,
                metric_fn_and_keys=get_metric_fn_and_keys(),
                validate_fn="validate_err_and_count",
            )
