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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
from apiclient import errors
import httplib2
import unittest

from airflow import configuration, DAG
from airflow.contrib.operators import cloudml_operator
from airflow.contrib.operators.cloudml_operator import CloudMLBatchPredictionOperator
from airflow.contrib.operators.cloudml_operator import CloudMLTrainingOperator
from airflow.contrib.operators.cloudml_operator import create_evaluate_ops

from mock import ANY
from mock import patch

DEFAULT_DATE = datetime.datetime(2017, 6, 6)


class CreateEvaluateOpsTest(unittest.TestCase):

    INPUT_MISSING_ORIGIN = {
        'dataFormat': 'TEXT',
        'inputPaths': ['gs://legal-bucket/fake-input-path/*'],
        'outputPath': 'gs://legal-bucket/fake-output-path',
        'region': 'us-east1',
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
        super(CreateEvaluateOpsTest, self).setUp()
        configuration.load_test_config()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
            },
            schedule_interval='@daily')

    def testSuccessfulRun(self):
        input_with_model = self.INPUT_MISSING_ORIGIN.copy()
        input_with_model['modelName'] = (
            'projects/test-project/models/test_model')
        metric_fn = lambda x: (0.1,)
        metric_fn_encoded = cloudml_operator.base64.b64encode(
            cloudml_operator.dill.dumps(metric_fn, recurse=True))

        pred, summary, validate = create_evaluate_ops(
            task_prefix='eval-test',
            project_id='test-project',
            job_id='eval-test-prediction',
            region=input_with_model['region'],
            data_format=input_with_model['dataFormat'],
            input_paths=input_with_model['inputPaths'],
            prediction_path=input_with_model['outputPath'],
            model_name=input_with_model['modelName'].split('/')[-1],
            metric_fn_and_keys=(metric_fn, ['err']),
            validate_fn=(lambda x: 'err=%.1f' % x['err']),
            dataflow_options=None,
            dag=self.dag)

        with patch('airflow.contrib.operators.cloudml_operator.'
                   'CloudMLHook') as mock_cloudml_hook:

            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message['predictionInput'] = input_with_model
            hook_instance = mock_cloudml_hook.return_value
            hook_instance.create_job.return_value = success_message
            pred.execute(None)
            mock_cloudml_hook.assert_called_with('google_cloud_default', None)
            hook_instance.create_job.assert_called_with(
                'test-project',
                {
                    'jobId': 'eval_test_prediction',
                    'predictionInput': input_with_model
                })

        with patch('airflow.contrib.operators.dataflow_operator.'
                   'DataFlowHook') as mock_dataflow_hook:

            hook_instance = mock_dataflow_hook.return_value
            hook_instance.start_python_dataflow.return_value = None
            summary.execute(None)
            mock_dataflow_hook.assert_called_with(
                gcp_conn_id='google_cloud_default', delegate_to=None)
            hook_instance.start_python_dataflow.assert_called_with(
                'eval-test-summary',
                {
                    'prediction_path': 'gs://legal-bucket/fake-output-path',
                    'metric_keys': 'err',
                    'metric_fn_encoded': metric_fn_encoded,
                },
                'airflow.contrib.operators.cloudml_prediction_summary',
                ['-m'])

        with patch('airflow.contrib.operators.cloudml_operator.'
                   'GoogleCloudStorageHook') as mock_gcs_hook:

            hook_instance = mock_gcs_hook.return_value
            hook_instance.download.return_value = '{"err": 0.9, "count": 9}'
            result = validate.execute({})
            hook_instance.download.assert_called_with(
                'legal-bucket', 'fake-output-path/prediction.summary.json')
            self.assertEqual('err=0.9', result)


class CloudMLBatchPredictionOperatorTest(unittest.TestCase):
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
        super(CloudMLBatchPredictionOperatorTest, self).setUp()
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
        with patch('airflow.contrib.operators.cloudml_operator.CloudMLHook') \
                as mock_hook:

            input_with_model = self.INPUT_MISSING_ORIGIN.copy()
            input_with_model['modelName'] = \
                'projects/test-project/models/test_model'
            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message['predictionInput'] = input_with_model

            hook_instance = mock_hook.return_value
            hook_instance.get_job.side_effect = errors.HttpError(
                resp=httplib2.Response({
                    'status': 404
                }), content=b'some bytes')
            hook_instance.create_job.return_value = success_message

            prediction_task = CloudMLBatchPredictionOperator(
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
                'test-project',
                {
                    'jobId': 'test_prediction',
                    'predictionInput': input_with_model
                }, ANY)
            self.assertEquals(
                success_message['predictionOutput'],
                prediction_output)

    def testSuccessWithVersion(self):
        with patch('airflow.contrib.operators.cloudml_operator.CloudMLHook') \
                as mock_hook:

            input_with_version = self.INPUT_MISSING_ORIGIN.copy()
            input_with_version['versionName'] = \
                'projects/test-project/models/test_model/versions/test_version'
            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message['predictionInput'] = input_with_version

            hook_instance = mock_hook.return_value
            hook_instance.get_job.side_effect = errors.HttpError(
                resp=httplib2.Response({
                    'status': 404
                }), content=b'some bytes')
            hook_instance.create_job.return_value = success_message

            prediction_task = CloudMLBatchPredictionOperator(
                job_id='test_prediction', project_id='test-project',
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
                'test-project',
                {
                    'jobId': 'test_prediction',
                    'predictionInput': input_with_version
                }, ANY)
            self.assertEquals(
                success_message['predictionOutput'],
                prediction_output)

    def testSuccessWithURI(self):
        with patch('airflow.contrib.operators.cloudml_operator.CloudMLHook') \
                as mock_hook:

            input_with_uri = self.INPUT_MISSING_ORIGIN.copy()
            input_with_uri['uri'] = 'gs://my_bucket/my_models/savedModel'
            success_message = self.SUCCESS_MESSAGE_MISSING_INPUT.copy()
            success_message['predictionInput'] = input_with_uri

            hook_instance = mock_hook.return_value
            hook_instance.get_job.side_effect = errors.HttpError(
                resp=httplib2.Response({
                    'status': 404
                }), content=b'some bytes')
            hook_instance.create_job.return_value = success_message

            prediction_task = CloudMLBatchPredictionOperator(
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
                'test-project',
                {
                    'jobId': 'test_prediction',
                    'predictionInput': input_with_uri
                }, ANY)
            self.assertEquals(
                success_message['predictionOutput'],
                prediction_output)

    def testInvalidModelOrigin(self):
        # Test that both uri and model is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['uri'] = 'gs://fake-uri/saved_model'
        task_args['model_name'] = 'fake_model'
        with self.assertRaises(ValueError) as context:
            CloudMLBatchPredictionOperator(**task_args).execute(None)
        self.assertEquals('Ambiguous model origin.', str(context.exception))

        # Test that both uri and model/version is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['uri'] = 'gs://fake-uri/saved_model'
        task_args['model_name'] = 'fake_model'
        task_args['version_name'] = 'fake_version'
        with self.assertRaises(ValueError) as context:
            CloudMLBatchPredictionOperator(**task_args).execute(None)
        self.assertEquals('Ambiguous model origin.', str(context.exception))

        # Test that a version is given without a model
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['version_name'] = 'bare_version'
        with self.assertRaises(ValueError) as context:
            CloudMLBatchPredictionOperator(**task_args).execute(None)
        self.assertEquals(
            'Missing model origin.',
            str(context.exception))

        # Test that none of uri, model, model/version is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        with self.assertRaises(ValueError) as context:
            CloudMLBatchPredictionOperator(**task_args).execute(None)
        self.assertEquals(
            'Missing model origin.',
            str(context.exception))

    def testHttpError(self):
        http_error_code = 403

        with patch('airflow.contrib.operators.cloudml_operator.CloudMLHook') \
                as mock_hook:
            input_with_model = self.INPUT_MISSING_ORIGIN.copy()
            input_with_model['modelName'] = \
                'projects/experimental/models/test_model'

            hook_instance = mock_hook.return_value
            hook_instance.create_job.side_effect = errors.HttpError(
                resp=httplib2.Response({
                    'status': http_error_code
                }), content=b'Forbidden')

            with self.assertRaises(errors.HttpError) as context:
                prediction_task = CloudMLBatchPredictionOperator(
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
                    'test-project',
                    {
                        'jobId': 'test_prediction',
                        'predictionInput': input_with_model
                    }, ANY)

            self.assertEquals(http_error_code, context.exception.resp.status)

    def testFailedJobError(self):
        with patch('airflow.contrib.operators.cloudml_operator.CloudMLHook') \
                as mock_hook:
            hook_instance = mock_hook.return_value
            hook_instance.create_job.return_value = {
                'state': 'FAILED',
                'errorMessage': 'A failure message'
            }
            task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
            task_args['uri'] = 'a uri'

            with self.assertRaises(RuntimeError) as context:
                CloudMLBatchPredictionOperator(**task_args).execute(None)

            self.assertEquals('A failure message', str(context.exception))


class CloudMLTrainingOperatorTest(unittest.TestCase):
    TRAINING_DEFAULT_ARGS = {
        'project_name': 'test-project',
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
        with patch('airflow.contrib.operators.cloudml_operator.CloudMLHook') \
                as mock_hook:
            success_response = self.TRAINING_INPUT.copy()
            success_response['state'] = 'SUCCEEDED'
            hook_instance = mock_hook.return_value
            hook_instance.create_job.return_value = success_response

            training_op = CloudMLTrainingOperator(**self.TRAINING_DEFAULT_ARGS)
            training_op.execute(None)

            mock_hook.assert_called_with(gcp_conn_id='google_cloud_default',
                                         delegate_to=None)
            # Make sure only 'create_job' is invoked on hook instance
            self.assertEquals(len(hook_instance.mock_calls), 1)
            hook_instance.create_job.assert_called_with(
                'test-project', self.TRAINING_INPUT, ANY)

    def testHttpError(self):
        http_error_code = 403
        with patch('airflow.contrib.operators.cloudml_operator.CloudMLHook') \
                as mock_hook:
            hook_instance = mock_hook.return_value
            hook_instance.create_job.side_effect = errors.HttpError(
                resp=httplib2.Response({
                    'status': http_error_code
                }), content=b'Forbidden')

            with self.assertRaises(errors.HttpError) as context:
                training_op = CloudMLTrainingOperator(
                    **self.TRAINING_DEFAULT_ARGS)
                training_op.execute(None)

            mock_hook.assert_called_with(
                gcp_conn_id='google_cloud_default', delegate_to=None)
            # Make sure only 'create_job' is invoked on hook instance
            self.assertEquals(len(hook_instance.mock_calls), 1)
            hook_instance.create_job.assert_called_with(
                'test-project', self.TRAINING_INPUT, ANY)
            self.assertEquals(http_error_code, context.exception.resp.status)

    def testFailedJobError(self):
        with patch('airflow.contrib.operators.cloudml_operator.CloudMLHook') \
                as mock_hook:
            failure_response = self.TRAINING_INPUT.copy()
            failure_response['state'] = 'FAILED'
            failure_response['errorMessage'] = 'A failure message'
            hook_instance = mock_hook.return_value
            hook_instance.create_job.return_value = failure_response

            with self.assertRaises(RuntimeError) as context:
                training_op = CloudMLTrainingOperator(
                    **self.TRAINING_DEFAULT_ARGS)
                training_op.execute(None)

            mock_hook.assert_called_with(
                gcp_conn_id='google_cloud_default', delegate_to=None)
            # Make sure only 'create_job' is invoked on hook instance
            self.assertEquals(len(hook_instance.mock_calls), 1)
            hook_instance.create_job.assert_called_with(
                'test-project', self.TRAINING_INPUT, ANY)
            self.assertEquals('A failure message', str(context.exception))


if __name__ == '__main__':
    unittest.main()
