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

import copy
import datetime
import unittest

import httplib2
from googleapiclient.errors import HttpError
from mock import ANY, patch

from airflow import DAG, configuration
from airflow.contrib.operators.mlengine_operator import (MLEngineBatchPredictionOperator,
                                                         MLEngineTrainingOperator,
                                                         MLEngineVersionOperator)
from airflow.exceptions import AirflowException

DEFAULT_DATE = datetime.datetime(2017, 6, 6)


class MLEngineBatchPredictionOperatorTest(unittest.TestCase):
    INPUT_MISSING_ORIGIN = {
        'dataFormat': 'TEXT',
        'inputPaths': ['gs://legal-bucket/fake-input-path/*'],
        'outputPath': 'gs://legal-bucket/fake-output-path',
        'region': 'us-east1',
    }
    SUCCESS_MESSAGE_MISSING_INPUT = {
        'jobId': 'test_prediction',
        'predictionOutput': {
            'outputPath': 'gs://fake-output-path',
            'predictionCount': 5000,
            'errorCount': 0,
            'nodeHours': 2.78
        },
        'state': 'SUCCEEDED'
    }
    BATCH_PREDICTION_DEFAULT_ARGS = {
        'project_id': 'test-project',
        'job_id': 'test_prediction',
        'region': 'us-east1',
        'data_format': 'TEXT',
        'input_paths': ['gs://legal-bucket-dash-Capital/legal-input-path/*'],
        'output_path':
        'gs://12_legal_bucket_underscore_number/legal-output-path',
        'task_id': 'test-prediction'
    }

    def setUp(self):
        super(MLEngineBatchPredictionOperatorTest, self).setUp()
        configuration.load_test_config()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
            },
            schedule_interval='@daily')

    def testSuccessWithModel(self):
        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:

            input_with_model = self.INPUT_MISSING_ORIGIN.copy()
            input_with_model['modelName'] = \
                'projects/test-project/models/test_model'
            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message['predictionInput'] = input_with_model

            hook_instance = mock_hook.return_value
            hook_instance.get_job.side_effect = HttpError(
                resp=httplib2.Response({
                    'status': 404
                }), content=b'some bytes')
            hook_instance.create_job.return_value = success_message

            prediction_task = MLEngineBatchPredictionOperator(
                job_id='test_prediction',
                project_id='test-project',
                region=input_with_model['region'],
                data_format=input_with_model['dataFormat'],
                input_paths=input_with_model['inputPaths'],
                output_path=input_with_model['outputPath'],
                model_name=input_with_model['modelName'].split('/')[-1],
                dag=self.dag,
                task_id='test-prediction')
            prediction_output = prediction_task.execute(None)

            mock_hook.assert_called_with('google_cloud_default', None)
            hook_instance.create_job.assert_called_once_with(
                'test-project', {
                    'jobId': 'test_prediction',
                    'predictionInput': input_with_model
                }, ANY)
            self.assertEqual(success_message['predictionOutput'],
                             prediction_output)

    def testSuccessWithVersion(self):
        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:

            input_with_version = self.INPUT_MISSING_ORIGIN.copy()
            input_with_version['versionName'] = \
                'projects/test-project/models/test_model/versions/test_version'
            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message['predictionInput'] = input_with_version

            hook_instance = mock_hook.return_value
            hook_instance.get_job.side_effect = HttpError(
                resp=httplib2.Response({
                    'status': 404
                }), content=b'some bytes')
            hook_instance.create_job.return_value = success_message

            prediction_task = MLEngineBatchPredictionOperator(
                job_id='test_prediction',
                project_id='test-project',
                region=input_with_version['region'],
                data_format=input_with_version['dataFormat'],
                input_paths=input_with_version['inputPaths'],
                output_path=input_with_version['outputPath'],
                model_name=input_with_version['versionName'].split('/')[-3],
                version_name=input_with_version['versionName'].split('/')[-1],
                dag=self.dag,
                task_id='test-prediction')
            prediction_output = prediction_task.execute(None)

            mock_hook.assert_called_with('google_cloud_default', None)
            hook_instance.create_job.assert_called_with(
                'test-project', {
                    'jobId': 'test_prediction',
                    'predictionInput': input_with_version
                }, ANY)
            self.assertEqual(success_message['predictionOutput'],
                             prediction_output)

    def testSuccessWithURI(self):
        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:

            input_with_uri = self.INPUT_MISSING_ORIGIN.copy()
            input_with_uri['uri'] = 'gs://my_bucket/my_models/savedModel'
            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message['predictionInput'] = input_with_uri

            hook_instance = mock_hook.return_value
            hook_instance.get_job.side_effect = HttpError(
                resp=httplib2.Response({
                    'status': 404
                }), content=b'some bytes')
            hook_instance.create_job.return_value = success_message

            prediction_task = MLEngineBatchPredictionOperator(
                job_id='test_prediction',
                project_id='test-project',
                region=input_with_uri['region'],
                data_format=input_with_uri['dataFormat'],
                input_paths=input_with_uri['inputPaths'],
                output_path=input_with_uri['outputPath'],
                uri=input_with_uri['uri'],
                dag=self.dag,
                task_id='test-prediction')
            prediction_output = prediction_task.execute(None)

            mock_hook.assert_called_with('google_cloud_default', None)
            hook_instance.create_job.assert_called_with(
                'test-project', {
                    'jobId': 'test_prediction',
                    'predictionInput': input_with_uri
                }, ANY)
            self.assertEqual(success_message['predictionOutput'],
                             prediction_output)

    def testInvalidModelOrigin(self):
        # Test that both uri and model is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['uri'] = 'gs://fake-uri/saved_model'
        task_args['model_name'] = 'fake_model'
        with self.assertRaises(AirflowException) as context:
            MLEngineBatchPredictionOperator(**task_args).execute(None)
        self.assertEqual('Ambiguous model origin: Both uri and '
                         'model/version name are provided.',
                         str(context.exception))

        # Test that both uri and model/version is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['uri'] = 'gs://fake-uri/saved_model'
        task_args['model_name'] = 'fake_model'
        task_args['version_name'] = 'fake_version'
        with self.assertRaises(AirflowException) as context:
            MLEngineBatchPredictionOperator(**task_args).execute(None)
        self.assertEqual('Ambiguous model origin: Both uri and '
                         'model/version name are provided.',
                         str(context.exception))

        # Test that a version is given without a model
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['version_name'] = 'bare_version'
        with self.assertRaises(AirflowException) as context:
            MLEngineBatchPredictionOperator(**task_args).execute(None)
        self.assertEqual('Missing model: Batch prediction expects a model '
                         'name when a version name is provided.',
                         str(context.exception))

        # Test that none of uri, model, model/version is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        with self.assertRaises(AirflowException) as context:
            MLEngineBatchPredictionOperator(**task_args).execute(None)
        self.assertEqual(
            'Missing model origin: Batch prediction expects a '
            'model, a model & version combination, or a URI to a savedModel.',
            str(context.exception))

    def testHttpError(self):
        http_error_code = 403

        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:
            input_with_model = self.INPUT_MISSING_ORIGIN.copy()
            input_with_model['modelName'] = \
                'projects/experimental/models/test_model'

            hook_instance = mock_hook.return_value
            hook_instance.create_job.side_effect = HttpError(
                resp=httplib2.Response({
                    'status': http_error_code
                }),
                content=b'Forbidden')

            with self.assertRaises(HttpError) as context:
                prediction_task = MLEngineBatchPredictionOperator(
                    job_id='test_prediction',
                    project_id='test-project',
                    region=input_with_model['region'],
                    data_format=input_with_model['dataFormat'],
                    input_paths=input_with_model['inputPaths'],
                    output_path=input_with_model['outputPath'],
                    model_name=input_with_model['modelName'].split('/')[-1],
                    dag=self.dag,
                    task_id='test-prediction')
                prediction_task.execute(None)

                mock_hook.assert_called_with('google_cloud_default', None)
                hook_instance.create_job.assert_called_with(
                    'test-project', {
                        'jobId': 'test_prediction',
                        'predictionInput': input_with_model
                    }, ANY)

            self.assertEqual(http_error_code, context.exception.resp.status)

    def testFailedJobError(self):
        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:
            hook_instance = mock_hook.return_value
            hook_instance.create_job.return_value = {
                'state': 'FAILED',
                'errorMessage': 'A failure message'
            }
            task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
            task_args['uri'] = 'a uri'

            with self.assertRaises(RuntimeError) as context:
                MLEngineBatchPredictionOperator(**task_args).execute(None)

            self.assertEqual('A failure message', str(context.exception))


class MLEngineTrainingOperatorTest(unittest.TestCase):
    TRAINING_DEFAULT_ARGS = {
        'project_id': 'test-project',
        'job_id': 'test_training',
        'package_uris': ['gs://some-bucket/package1'],
        'training_python_module': 'trainer',
        'training_args': '--some_arg=\'aaa\'',
        'region': 'us-east1',
        'scale_tier': 'STANDARD_1',
        'task_id': 'test-training'
    }
    TRAINING_INPUT = {
        'jobId': 'test_training',
        'trainingInput': {
            'scaleTier': 'STANDARD_1',
            'packageUris': ['gs://some-bucket/package1'],
            'pythonModule': 'trainer',
            'args': '--some_arg=\'aaa\'',
            'region': 'us-east1'
        }
    }

    def testSuccessCreateTrainingJob(self):
        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:
            success_response = self.TRAINING_INPUT.copy()
            success_response['state'] = 'SUCCEEDED'
            hook_instance = mock_hook.return_value
            hook_instance.create_job.return_value = success_response

            training_op = MLEngineTrainingOperator(
                **self.TRAINING_DEFAULT_ARGS)
            training_op.execute(None)

            mock_hook.assert_called_with(
                gcp_conn_id='google_cloud_default', delegate_to=None)
            # Make sure only 'create_job' is invoked on hook instance
            self.assertEqual(len(hook_instance.mock_calls), 1)
            hook_instance.create_job.assert_called_with(
                'test-project', self.TRAINING_INPUT, ANY)

    def testSuccessCreateTrainingJobWithOptionalArgs(self):
        training_input = copy.deepcopy(self.TRAINING_INPUT)
        training_input['trainingInput']['runtimeVersion'] = '1.6'
        training_input['trainingInput']['pythonVersion'] = '3.5'
        training_input['trainingInput']['jobDir'] = 'gs://some-bucket/jobs/test_training'

        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:
            success_response = self.TRAINING_INPUT.copy()
            success_response['state'] = 'SUCCEEDED'
            hook_instance = mock_hook.return_value
            hook_instance.create_job.return_value = success_response

            training_op = MLEngineTrainingOperator(
                runtime_version='1.6',
                python_version='3.5',
                job_dir='gs://some-bucket/jobs/test_training',
                **self.TRAINING_DEFAULT_ARGS)
            training_op.execute(None)

            mock_hook.assert_called_with(gcp_conn_id='google_cloud_default',
                                         delegate_to=None)
            # Make sure only 'create_job' is invoked on hook instance
            self.assertEqual(len(hook_instance.mock_calls), 1)
            hook_instance.create_job.assert_called_with(
                'test-project', training_input, ANY)

    def testHttpError(self):
        http_error_code = 403
        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:
            hook_instance = mock_hook.return_value
            hook_instance.create_job.side_effect = HttpError(
                resp=httplib2.Response({
                    'status': http_error_code
                }),
                content=b'Forbidden')

            with self.assertRaises(HttpError) as context:
                training_op = MLEngineTrainingOperator(
                    **self.TRAINING_DEFAULT_ARGS)
                training_op.execute(None)

            mock_hook.assert_called_with(
                gcp_conn_id='google_cloud_default', delegate_to=None)
            # Make sure only 'create_job' is invoked on hook instance
            self.assertEqual(len(hook_instance.mock_calls), 1)
            hook_instance.create_job.assert_called_with(
                'test-project', self.TRAINING_INPUT, ANY)
            self.assertEqual(http_error_code, context.exception.resp.status)

    def testFailedJobError(self):
        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:
            failure_response = self.TRAINING_INPUT.copy()
            failure_response['state'] = 'FAILED'
            failure_response['errorMessage'] = 'A failure message'
            hook_instance = mock_hook.return_value
            hook_instance.create_job.return_value = failure_response

            with self.assertRaises(RuntimeError) as context:
                training_op = MLEngineTrainingOperator(
                    **self.TRAINING_DEFAULT_ARGS)
                training_op.execute(None)

            mock_hook.assert_called_with(
                gcp_conn_id='google_cloud_default', delegate_to=None)
            # Make sure only 'create_job' is invoked on hook instance
            self.assertEqual(len(hook_instance.mock_calls), 1)
            hook_instance.create_job.assert_called_with(
                'test-project', self.TRAINING_INPUT, ANY)
            self.assertEqual('A failure message', str(context.exception))


class MLEngineVersionOperatorTest(unittest.TestCase):
    VERSION_DEFAULT_ARGS = {
        'project_id': 'test-project',
        'model_name': 'test-model',
        'task_id': 'test-version'
    }
    VERSION_INPUT = {
        'name': 'v1',
        'deploymentUri': 'gs://some-bucket/jobs/test_training/model.pb',
        'runtimeVersion': '1.6'
    }

    def testSuccessCreateVersion(self):
        with patch('airflow.contrib.operators.mlengine_operator.MLEngineHook') \
                as mock_hook:
            success_response = {'name': 'some-name', 'done': True}
            hook_instance = mock_hook.return_value
            hook_instance.create_version.return_value = success_response

            training_op = MLEngineVersionOperator(
                version=self.VERSION_INPUT,
                **self.VERSION_DEFAULT_ARGS)
            training_op.execute(None)

            mock_hook.assert_called_with(gcp_conn_id='google_cloud_default',
                                         delegate_to=None)
            # Make sure only 'create_version' is invoked on hook instance
            self.assertEqual(len(hook_instance.mock_calls), 1)
            hook_instance.create_version.assert_called_with(
                'test-project', 'test-model', self.VERSION_INPUT)


if __name__ == '__main__':
    unittest.main()
