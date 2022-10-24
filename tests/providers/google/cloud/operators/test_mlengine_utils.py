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

import datetime
import unittest
from unittest import mock
from unittest.mock import ANY, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.providers.google.cloud.utils import mlengine_operator_utils
from airflow.version import version

DEFAULT_DATE = datetime.datetime(2017, 6, 6)
TEST_VERSION = f"v{version.replace('.', '-').replace('+', '-')}"


class TestCreateEvaluateOps(unittest.TestCase):
    INPUT_MISSING_ORIGIN = {
        "dataFormat": "TEXT",
        "inputPaths": ["gs://legal-bucket/fake-input-path/*"],
        "outputPath": "gs://legal-bucket/fake-output-path",
        "region": "us-east1",
        "versionName": "projects/test-project/models/test_model/versions/test_version",
    }
    SUCCESS_MESSAGE_MISSING_INPUT = {
        "jobId": "eval_test_prediction",
        "predictionOutput": {
            "outputPath": "gs://fake-output-path",
            "predictionCount": 5000,
            "errorCount": 0,
            "nodeHours": 2.78,
        },
        "state": "SUCCEEDED",
    }

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
                "end_date": DEFAULT_DATE,
                "project_id": "test-project",
                "region": "us-east1",
                "model_name": "test_model",
                "version_name": "test_version",
            },
            schedule="@daily",
        )
        self.metric_fn = lambda x: (0.1,)
        self.metric_fn_encoded = mlengine_operator_utils.base64.b64encode(
            mlengine_operator_utils.dill.dumps(self.metric_fn, recurse=True)
        ).decode()

    def test_successful_run(self):
        input_with_model = self.INPUT_MISSING_ORIGIN.copy()

        pred, summary, validate = mlengine_operator_utils.create_evaluate_ops(
            task_prefix="eval-test",
            batch_prediction_job_id="eval-test-prediction",
            data_format=input_with_model["dataFormat"],
            input_paths=input_with_model["inputPaths"],
            prediction_path=input_with_model["outputPath"],
            metric_fn_and_keys=(self.metric_fn, ["err"]),
            validate_fn=(lambda x: f"err={x['err']:.1f}"),
            dag=self.dag,
            py_interpreter="python3",
        )

        with patch("airflow.providers.google.cloud.operators.mlengine.MLEngineHook") as mock_mlengine_hook:
            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message["predictionInput"] = input_with_model
            hook_instance = mock_mlengine_hook.return_value
            hook_instance.create_job.return_value = success_message
            result = pred.execute(None)
            mock_mlengine_hook.assert_called_once_with(
                "google_cloud_default",
                None,
                impersonation_chain=None,
            )
            hook_instance.create_job.assert_called_once_with(
                project_id="test-project",
                job={
                    "jobId": "eval_test_prediction",
                    "predictionInput": input_with_model,
                },
                use_existing_job_fn=ANY,
            )
            assert success_message["predictionOutput"] == result

        with patch("airflow.providers.apache.beam.operators.beam.BeamHook") as mock_beam_hook:
            beam_hook_instance = mock_beam_hook.return_value
            summary.execute(None)
            mock_beam_hook.assert_called_once_with(runner="DirectRunner")
            beam_hook_instance.start_python_pipeline.assert_called_once_with(
                variables={
                    "prediction_path": "gs://legal-bucket/fake-output-path",
                    "labels": {"airflow-version": TEST_VERSION},
                    "metric_keys": "err",
                    "metric_fn_encoded": self.metric_fn_encoded,
                },
                py_file=mock.ANY,
                py_options=[],
                py_interpreter="python3",
                py_requirements=["apache-beam[gcp]>=2.14.0"],
                py_system_site_packages=False,
                process_line_callback=mock.ANY,
            )

        with patch("airflow.providers.google.cloud.utils.mlengine_operator_utils.GCSHook") as mock_gcs_hook:
            hook_instance = mock_gcs_hook.return_value
            hook_instance.download.return_value = b'{"err": 0.9, "count": 9}'
            result = validate.execute({})
            hook_instance.download.assert_called_once_with(
                "legal-bucket", "fake-output-path/prediction.summary.json"
            )
            assert "err=0.9" == result

    def test_failures(self):
        def create_test_dag(dag_id):
            dag = DAG(
                dag_id,
                default_args={
                    "owner": "airflow",
                    "start_date": DEFAULT_DATE,
                    "end_date": DEFAULT_DATE,
                    "project_id": "test-project",
                    "region": "us-east1",
                },
                schedule="@daily",
            )
            return dag

        input_with_model = self.INPUT_MISSING_ORIGIN.copy()
        other_params_but_models = {
            "task_prefix": "eval-test",
            "batch_prediction_job_id": "eval-test-prediction",
            "data_format": input_with_model["dataFormat"],
            "input_paths": input_with_model["inputPaths"],
            "prediction_path": input_with_model["outputPath"],
            "metric_fn_and_keys": (self.metric_fn, ["err"]),
            "validate_fn": (lambda x: f"err={x['err']:.1f}"),
        }

        with pytest.raises(AirflowException, match="Missing model origin"):
            mlengine_operator_utils.create_evaluate_ops(
                dag=create_test_dag("test_dag_1"), **other_params_but_models
            )

        with pytest.raises(AirflowException, match="Ambiguous model origin"):
            mlengine_operator_utils.create_evaluate_ops(
                dag=create_test_dag("test_dag_2"),
                model_uri="abc",
                model_name="cde",
                **other_params_but_models,
            )

        with pytest.raises(AirflowException, match="Ambiguous model origin"):
            mlengine_operator_utils.create_evaluate_ops(
                dag=create_test_dag("test_dag_3"),
                model_uri="abc",
                version_name="vvv",
                **other_params_but_models,
            )

        with pytest.raises(AirflowException, match="`metric_fn` param must be callable"):
            params = other_params_but_models.copy()
            params["metric_fn_and_keys"] = (None, ["abc"])
            mlengine_operator_utils.create_evaluate_ops(
                dag=create_test_dag("test_dag_4"), model_uri="gs://blah", **params
            )

        with pytest.raises(AirflowException, match="`validate_fn` param must be callable"):
            params = other_params_but_models.copy()
            params["validate_fn"] = None
            mlengine_operator_utils.create_evaluate_ops(
                dag=create_test_dag("test_dag_5"), model_uri="gs://blah", **params
            )
