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

import datetime
import unittest
from unittest.mock import ANY, patch

import mock

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.providers.google.cloud.utils import mlengine_operator_utils
from airflow.version import version

DEFAULT_DATE = datetime.datetime(2017, 6, 6)
TEST_VERSION = 'v{}'.format(version.replace('.', '-').replace('+', '-'))


class TestCreateEvaluateOps(unittest.TestCase):
    INPUT_MISSING_ORIGIN = {
        'dataFormat': 'TEXT',
        'inputPaths': ['gs://legal-bucket/fake-input-path/*'],
        'outputPath': 'gs://legal-bucket/fake-output-path',
        'region': 'us-east1',
        'versionName': 'projects/test-project/models/test_model/versions/test_version',
    }
    SUCCESS_MESSAGE_MISSING_INPUT = {
        'jobId': 'eval_test_prediction',
        'predictionOutput': {
            'outputPath': 'gs://fake-output-path',
            'predictionCount': 5000,
            'errorCount': 0,
            'nodeHours': 2.78
        },
        'state': 'SUCCEEDED'
    }

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
                'project_id': 'test-project',
                'region': 'us-east1',
                'model_name': 'test_model',
                'version_name': 'test_version',
            },
            schedule_interval='@daily')
        self.metric_fn = lambda x: (0.1,)
        self.metric_fn_encoded = mlengine_operator_utils.base64.b64encode(
            mlengine_operator_utils.dill.dumps(self.metric_fn, recurse=True)).decode()

    def test_successful_run(self):
        input_with_model = self.INPUT_MISSING_ORIGIN.copy()

        pred, summary, validate = mlengine_operator_utils.create_evaluate_ops(
            task_prefix='eval-test',
            batch_prediction_job_id='eval-test-prediction',
            data_format=input_with_model['dataFormat'],
            input_paths=input_with_model['inputPaths'],
            prediction_path=input_with_model['outputPath'],
            metric_fn_and_keys=(self.metric_fn, ['err']),
            validate_fn=(lambda x: 'err=%.1f' % x['err']),
            dag=self.dag,
            py_interpreter="python3",
        )

        with patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook') as mock_mlengine_hook:
            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message['predictionInput'] = input_with_model
            hook_instance = mock_mlengine_hook.return_value
            hook_instance.create_job.return_value = success_message
            result = pred.execute(None)
            mock_mlengine_hook.assert_called_once_with('google_cloud_default', None)
            hook_instance.create_job.assert_called_once_with(
                project_id='test-project',
                job={
                    'jobId': 'eval_test_prediction',
                    'predictionInput': input_with_model,
                },
                use_existing_job_fn=ANY)
            self.assertEqual(success_message['predictionOutput'], result)

        with patch('airflow.providers.google.cloud.operators.dataflow.DataflowHook') as mock_dataflow_hook:
            hook_instance = mock_dataflow_hook.return_value
            hook_instance.start_python_dataflow.return_value = None
            summary.execute(None)
            mock_dataflow_hook.assert_called_once_with(
                gcp_conn_id='google_cloud_default', delegate_to=None, poll_sleep=10)
            hook_instance.start_python_dataflow.assert_called_once_with(
                job_name='{{task.task_id}}',
                variables={
                    'prediction_path': 'gs://legal-bucket/fake-output-path',
                    'labels': {'airflow-version': TEST_VERSION},
                    'metric_keys': 'err',
                    'metric_fn_encoded': self.metric_fn_encoded,
                },
                dataflow=mock.ANY,
                py_options=[],
                py_requirements=['apache-beam[gcp]>=2.14.0'],
                py_interpreter='python3',
                py_system_site_packages=False,
                on_new_job_id_callback=ANY,
                project_id='test-project',
            )

        with patch('airflow.providers.google.cloud.utils.mlengine_operator_utils.GCSHook') as mock_gcs_hook:
            hook_instance = mock_gcs_hook.return_value
            hook_instance.download.return_value = '{"err": 0.9, "count": 9}'
            result = validate.execute({})
            hook_instance.download.assert_called_once_with(
                'legal-bucket', 'fake-output-path/prediction.summary.json')
            self.assertEqual('err=0.9', result)

    def test_failures(self):
        dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
                'project_id': 'test-project',
                'region': 'us-east1',
            },
            schedule_interval='@daily')

        input_with_model = self.INPUT_MISSING_ORIGIN.copy()
        other_params_but_models = {
            'task_prefix': 'eval-test',
            'batch_prediction_job_id': 'eval-test-prediction',
            'data_format': input_with_model['dataFormat'],
            'input_paths': input_with_model['inputPaths'],
            'prediction_path': input_with_model['outputPath'],
            'metric_fn_and_keys': (self.metric_fn, ['err']),
            'validate_fn': (lambda x: 'err=%.1f' % x['err']),
            'dag': dag,
        }

        with self.assertRaisesRegex(AirflowException, 'Missing model origin'):
            mlengine_operator_utils.create_evaluate_ops(**other_params_but_models)

        with self.assertRaisesRegex(AirflowException, 'Ambiguous model origin'):
            mlengine_operator_utils.create_evaluate_ops(model_uri='abc', model_name='cde',
                                                        **other_params_but_models)

        with self.assertRaisesRegex(AirflowException, 'Ambiguous model origin'):
            mlengine_operator_utils.create_evaluate_ops(model_uri='abc', version_name='vvv',
                                                        **other_params_but_models)

        with self.assertRaisesRegex(AirflowException, '`metric_fn` param must be callable'):
            params = other_params_but_models.copy()
            params['metric_fn_and_keys'] = (None, ['abc'])
            mlengine_operator_utils.create_evaluate_ops(model_uri='gs://blah', **params)

        with self.assertRaisesRegex(AirflowException, '`validate_fn` param must be callable'):
            params = other_params_but_models.copy()
            params['validate_fn'] = None
            mlengine_operator_utils.create_evaluate_ops(model_uri='gs://blah', **params)
