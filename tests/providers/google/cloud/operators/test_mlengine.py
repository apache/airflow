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
from unittest.mock import ANY, MagicMock, patch

import httplib2
from googleapiclient.errors import HttpError

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.mlengine import (
    AIPlatformConsoleLink, MLEngineCreateModelOperator, MLEngineCreateVersionOperator,
    MLEngineDeleteModelOperator, MLEngineDeleteVersionOperator, MLEngineGetModelOperator,
    MLEngineListVersionsOperator, MLEngineManageModelOperator, MLEngineManageVersionOperator,
    MLEngineSetDefaultVersionOperator, MLEngineStartBatchPredictionJobOperator,
    MLEngineStartTrainingJobOperator, MLEngineTrainingJobFailureOperator,
)
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.dates import days_ago

DEFAULT_DATE = datetime.datetime(2017, 6, 6)

TEST_DAG_ID = "test-mlengine-operators"
TEST_PROJECT_ID = "test-project-id"
TEST_MODEL_NAME = "test-model-name"
TEST_VERSION_NAME = "test-version"
TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_DELEGATE_TO = "test-delegate-to"
TEST_MODEL = {
    'name': TEST_MODEL_NAME,
}
TEST_VERSION = {
    'name': 'v1',
    'deploymentUri': 'gs://some-bucket/jobs/test_training/model.pb',
    'runtimeVersion': '1.6'
}


class TestMLEngineBatchPredictionOperator(unittest.TestCase):
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
        super().setUp()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE,
                'end_date': DEFAULT_DATE,
            },
            schedule_interval='@daily')

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_with_model(self, mock_hook):
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

        prediction_task = MLEngineStartBatchPredictionJobOperator(
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

        mock_hook.assert_called_once_with('google_cloud_default', None)
        hook_instance.create_job.assert_called_once_with(
            project_id='test-project',
            job={
                'jobId': 'test_prediction',
                'predictionInput': input_with_model
            },
            use_existing_job_fn=ANY
        )
        self.assertEqual(success_message['predictionOutput'], prediction_output)

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_with_version(self, mock_hook):
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

        prediction_task = MLEngineStartBatchPredictionJobOperator(
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

        mock_hook.assert_called_once_with('google_cloud_default', None)
        hook_instance.create_job.assert_called_once_with(
            project_id='test-project',
            job={
                'jobId': 'test_prediction',
                'predictionInput': input_with_version
            },
            use_existing_job_fn=ANY
        )
        self.assertEqual(success_message['predictionOutput'], prediction_output)

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_with_uri(self, mock_hook):
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

        prediction_task = MLEngineStartBatchPredictionJobOperator(
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

        mock_hook.assert_called_once_with('google_cloud_default', None)
        hook_instance.create_job.assert_called_once_with(
            project_id='test-project',
            job={
                'jobId': 'test_prediction',
                'predictionInput': input_with_uri
            },
            use_existing_job_fn=ANY
        )
        self.assertEqual(success_message['predictionOutput'], prediction_output)

    def test_invalid_model_origin(self):
        # Test that both uri and model is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['uri'] = 'gs://fake-uri/saved_model'
        task_args['model_name'] = 'fake_model'
        with self.assertRaises(AirflowException) as context:
            MLEngineStartBatchPredictionJobOperator(**task_args).execute(None)
        self.assertEqual('Ambiguous model origin: Both uri and '
                         'model/version name are provided.',
                         str(context.exception))

        # Test that both uri and model/version is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['uri'] = 'gs://fake-uri/saved_model'
        task_args['model_name'] = 'fake_model'
        task_args['version_name'] = 'fake_version'
        with self.assertRaises(AirflowException) as context:
            MLEngineStartBatchPredictionJobOperator(**task_args).execute(None)
        self.assertEqual('Ambiguous model origin: Both uri and '
                         'model/version name are provided.',
                         str(context.exception))

        # Test that a version is given without a model
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['version_name'] = 'bare_version'
        with self.assertRaises(AirflowException) as context:
            MLEngineStartBatchPredictionJobOperator(**task_args).execute(None)
        self.assertEqual('Missing model: Batch prediction expects a model '
                         'name when a version name is provided.',
                         str(context.exception))

        # Test that none of uri, model, model/version is given
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        with self.assertRaises(AirflowException) as context:
            MLEngineStartBatchPredictionJobOperator(**task_args).execute(None)
        self.assertEqual(
            'Missing model origin: Batch prediction expects a '
            'model, a model & version combination, or a URI to a savedModel.',
            str(context.exception))

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_http_error(self, mock_hook):
        http_error_code = 403
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
            prediction_task = MLEngineStartBatchPredictionJobOperator(
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

            mock_hook.assert_called_once_with('google_cloud_default', None)
            hook_instance.create_job.assert_called_once_with(
                'test-project', {
                    'jobId': 'test_prediction',
                    'predictionInput': input_with_model
                }, ANY)

        self.assertEqual(http_error_code, context.exception.resp.status)

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_failed_job_error(self, mock_hook):
        hook_instance = mock_hook.return_value
        hook_instance.create_job.return_value = {
            'state': 'FAILED',
            'errorMessage': 'A failure message'
        }
        task_args = self.BATCH_PREDICTION_DEFAULT_ARGS.copy()
        task_args['uri'] = 'a uri'

        with self.assertRaises(RuntimeError) as context:
            MLEngineStartBatchPredictionJobOperator(**task_args).execute(None)

        self.assertEqual('A failure message', str(context.exception))


class TestMLEngineTrainingOperator(unittest.TestCase):
    TRAINING_DEFAULT_ARGS = {
        'project_id': 'test-project',
        'job_id': 'test_training',
        'package_uris': ['gs://some-bucket/package1'],
        'training_python_module': 'trainer',
        'training_args': '--some_arg=\'aaa\'',
        'region': 'us-east1',
        'scale_tier': 'STANDARD_1',
        'task_id': 'test-training',
        'start_date': days_ago(1)
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

    def setUp(self):
        self.dag = DAG(TEST_DAG_ID, default_args=self.TRAINING_DEFAULT_ARGS)

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_create_training_job(self, mock_hook):
        success_response = self.TRAINING_INPUT.copy()
        success_response['state'] = 'SUCCEEDED'
        hook_instance = mock_hook.return_value
        hook_instance.create_job.return_value = success_response

        training_op = MLEngineStartTrainingJobOperator(
            **self.TRAINING_DEFAULT_ARGS)
        training_op.execute(MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id='google_cloud_default', delegate_to=None)
        # Make sure only 'create_job' is invoked on hook instance
        self.assertEqual(len(hook_instance.mock_calls), 1)
        hook_instance.create_job.assert_called_once_with(
            project_id='test-project', job=self.TRAINING_INPUT, use_existing_job_fn=ANY)

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_create_training_job_with_optional_args(self, mock_hook):
        training_input = copy.deepcopy(self.TRAINING_INPUT)
        training_input['trainingInput']['runtimeVersion'] = '1.6'
        training_input['trainingInput']['pythonVersion'] = '3.5'
        training_input['trainingInput']['jobDir'] = 'gs://some-bucket/jobs/test_training'

        success_response = self.TRAINING_INPUT.copy()
        success_response['state'] = 'SUCCEEDED'
        hook_instance = mock_hook.return_value
        hook_instance.create_job.return_value = success_response

        training_op = MLEngineStartTrainingJobOperator(
            runtime_version='1.6',
            python_version='3.5',
            job_dir='gs://some-bucket/jobs/test_training',
            **self.TRAINING_DEFAULT_ARGS)
        training_op.execute(MagicMock())

        mock_hook.assert_called_once_with(gcp_conn_id='google_cloud_default', delegate_to=None)
        # Make sure only 'create_job' is invoked on hook instance
        self.assertEqual(len(hook_instance.mock_calls), 1)
        hook_instance.create_job.assert_called_once_with(
            project_id='test-project', job=training_input, use_existing_job_fn=ANY)

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_http_error(self, mock_hook):
        http_error_code = 403
        hook_instance = mock_hook.return_value
        hook_instance.create_job.side_effect = HttpError(
            resp=httplib2.Response({
                'status': http_error_code
            }),
            content=b'Forbidden')

        with self.assertRaises(HttpError) as context:
            training_op = MLEngineStartTrainingJobOperator(
                **self.TRAINING_DEFAULT_ARGS)
            training_op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id='google_cloud_default', delegate_to=None)
        # Make sure only 'create_job' is invoked on hook instance
        self.assertEqual(len(hook_instance.mock_calls), 1)
        hook_instance.create_job.assert_called_once_with(
            project_id='test-project', job=self.TRAINING_INPUT, use_existing_job_fn=ANY)
        self.assertEqual(http_error_code, context.exception.resp.status)

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_failed_job_error(self, mock_hook):
        failure_response = self.TRAINING_INPUT.copy()
        failure_response['state'] = 'FAILED'
        failure_response['errorMessage'] = 'A failure message'
        hook_instance = mock_hook.return_value
        hook_instance.create_job.return_value = failure_response

        with self.assertRaises(RuntimeError) as context:
            training_op = MLEngineStartTrainingJobOperator(
                **self.TRAINING_DEFAULT_ARGS)
            training_op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id='google_cloud_default', delegate_to=None)
        # Make sure only 'create_job' is invoked on hook instance
        self.assertEqual(len(hook_instance.mock_calls), 1)
        hook_instance.create_job.assert_called_once_with(
            project_id='test-project', job=self.TRAINING_INPUT, use_existing_job_fn=ANY)
        self.assertEqual('A failure message', str(context.exception))

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_console_extra_link(self, mock_hook):
        training_op = MLEngineStartTrainingJobOperator(
            **self.TRAINING_DEFAULT_ARGS)

        ti = TaskInstance(
            task=training_op,
            execution_date=DEFAULT_DATE,
        )

        job_id = self.TRAINING_DEFAULT_ARGS['job_id']
        project_id = self.TRAINING_DEFAULT_ARGS['project_id']
        gcp_metadata = {
            "job_id": job_id,
            "project_id": project_id,
        }
        ti.xcom_push(key='gcp_metadata', value=gcp_metadata)

        self.assertEqual(
            f"https://console.cloud.google.com/ai-platform/jobs/{job_id}?project={project_id}",
            training_op.get_extra_links(DEFAULT_DATE, AIPlatformConsoleLink.name),
        )

        self.assertEqual(
            '',
            training_op.get_extra_links(datetime.datetime(2019, 1, 1), AIPlatformConsoleLink.name),
        )

    def test_console_extra_link_serialized_field(self):
        with self.dag:
            training_op = MLEngineStartTrainingJobOperator(**self.TRAINING_DEFAULT_ARGS)
        serialized_dag = SerializedDAG.to_dict(self.dag)
        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[self.TRAINING_DEFAULT_ARGS['task_id']]

        # Check Serialized version of operator link
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [{"airflow.providers.google.cloud.operators.mlengine.AIPlatformConsoleLink": {}}]
        )

        # Check DeSerialized version of operator link
        self.assertIsInstance(list(simple_task.operator_extra_links)[0], AIPlatformConsoleLink)

        job_id = self.TRAINING_DEFAULT_ARGS['job_id']
        project_id = self.TRAINING_DEFAULT_ARGS['project_id']
        gcp_metadata = {
            "job_id": job_id,
            "project_id": project_id,
        }

        ti = TaskInstance(
            task=training_op,
            execution_date=DEFAULT_DATE,
        )
        ti.xcom_push(key='gcp_metadata', value=gcp_metadata)

        self.assertEqual(
            f"https://console.cloud.google.com/ai-platform/jobs/{job_id}?project={project_id}",
            simple_task.get_extra_links(DEFAULT_DATE, AIPlatformConsoleLink.name),
        )

        self.assertEqual(
            '',
            simple_task.get_extra_links(datetime.datetime(2019, 1, 1), AIPlatformConsoleLink.name),
        )


class TestMLEngineTrainingJobFailureOperator(unittest.TestCase):

    TRAINING_DEFAULT_ARGS = {
        'project_id': 'test-project',
        'job_id': 'test_training',
        'task_id': 'test-training'
    }

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_cancel_training_job(self, mock_hook):
        success_response = {}
        hook_instance = mock_hook.return_value
        hook_instance.cancel_job.return_value = success_response

        cancel_training_op = MLEngineTrainingJobFailureOperator(
            **self.TRAINING_DEFAULT_ARGS)
        cancel_training_op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id='google_cloud_default', delegate_to=None)
        # Make sure only 'cancel_job' is invoked on hook instance
        self.assertEqual(len(hook_instance.mock_calls), 1)
        hook_instance.cancel_job.assert_called_once_with(
            project_id=self.TRAINING_DEFAULT_ARGS['project_id'], job_id=self.TRAINING_DEFAULT_ARGS['job_id'])

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_http_error(self, mock_hook):
        http_error_code = 403
        hook_instance = mock_hook.return_value
        hook_instance.cancel_job.side_effect = HttpError(
            resp=httplib2.Response({
                'status': http_error_code
            }),
            content=b'Forbidden')

        with self.assertRaises(HttpError) as context:
            cancel_training_op = MLEngineTrainingJobFailureOperator(
                **self.TRAINING_DEFAULT_ARGS)
            cancel_training_op.execute(None)

        mock_hook.assert_called_once_with(
            gcp_conn_id='google_cloud_default', delegate_to=None)
        # Make sure only 'create_job' is invoked on hook instance
        self.assertEqual(len(hook_instance.mock_calls), 1)
        hook_instance.cancel_job.assert_called_once_with(
            project_id=self.TRAINING_DEFAULT_ARGS['project_id'], job_id=self.TRAINING_DEFAULT_ARGS['job_id'])
        self.assertEqual(http_error_code, context.exception.resp.status)


class TestMLEngineModelOperator(unittest.TestCase):

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_create_model(self, mock_hook):
        task = MLEngineManageModelOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model=TEST_MODEL,
            operation="create",
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO
        )

        task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.create_model.assert_called_once_with(
            project_id=TEST_PROJECT_ID, model=TEST_MODEL
        )

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_get_model(self, mock_hook):
        task = MLEngineManageModelOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model=TEST_MODEL,
            operation="get",
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO
        )

        result = task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.get_model.assert_called_once_with(
            project_id=TEST_PROJECT_ID, model_name=TEST_MODEL_NAME
        )
        self.assertEqual(mock_hook.return_value.get_model.return_value, result)

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_fail(self, mock_hook):
        task = MLEngineManageModelOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model=TEST_MODEL,
            operation="invalid",
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO
        )
        with self.assertRaises(ValueError):
            task.execute(None)


class TestMLEngineCreateModelOperator(unittest.TestCase):

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_create_model(self, mock_hook):
        task = MLEngineCreateModelOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model=TEST_MODEL,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO
        )

        task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.create_model.assert_called_once_with(
            project_id=TEST_PROJECT_ID, model=TEST_MODEL
        )


class TestMLEngineGetModelOperator(unittest.TestCase):

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_get_model(self, mock_hook):
        task = MLEngineGetModelOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO
        )

        result = task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.get_model.assert_called_once_with(
            project_id=TEST_PROJECT_ID, model_name=TEST_MODEL_NAME
        )
        self.assertEqual(mock_hook.return_value.get_model.return_value, result)


class TestMLEngineDeleteModelOperator(unittest.TestCase):

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_delete_model(self, mock_hook):
        task = MLEngineDeleteModelOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            delete_contents=True
        )

        task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.delete_model.assert_called_once_with(
            project_id=TEST_PROJECT_ID, model_name=TEST_MODEL_NAME, delete_contents=True
        )


class TestMLEngineVersionOperator(unittest.TestCase):
    VERSION_DEFAULT_ARGS = {
        'project_id': 'test-project',
        'model_name': 'test-model',
        'task_id': 'test-version'
    }

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success_create_version(self, mock_hook):
        success_response = {'name': 'some-name', 'done': True}
        hook_instance = mock_hook.return_value
        hook_instance.create_version.return_value = success_response

        training_op = MLEngineManageVersionOperator(
            version=TEST_VERSION,
            **self.VERSION_DEFAULT_ARGS)
        training_op.execute(None)

        mock_hook.assert_called_once_with(gcp_conn_id='google_cloud_default', delegate_to=None)
        # Make sure only 'create_version' is invoked on hook instance
        self.assertEqual(len(hook_instance.mock_calls), 1)
        hook_instance.create_version.assert_called_once_with(
            project_id='test-project', model_name='test-model', version_spec=TEST_VERSION)


class TestMLEngineCreateVersion(unittest.TestCase):

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success(self, mock_hook):
        task = MLEngineCreateVersionOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            version=TEST_VERSION,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )

        task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.create_version.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            version_spec=TEST_VERSION
        )

    def test_missing_model_name(self):
        with self.assertRaises(AirflowException):
            MLEngineCreateVersionOperator(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                model_name=None,
                version=TEST_VERSION,
                gcp_conn_id=TEST_GCP_CONN_ID,
                delegate_to=TEST_DELEGATE_TO,
            )

    def test_missing_version(self):
        with self.assertRaises(AirflowException):
            MLEngineCreateVersionOperator(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                model_name=TEST_MODEL_NAME,
                version=None,
                gcp_conn_id=TEST_GCP_CONN_ID,
                delegate_to=TEST_DELEGATE_TO,
            )


class TestMLEngineSetDefaultVersion(unittest.TestCase):

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success(self, mock_hook):
        task = MLEngineSetDefaultVersionOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            version_name=TEST_VERSION_NAME,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )

        task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.set_default_version.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            version_name=TEST_VERSION_NAME
        )

    def test_missing_model_name(self):
        with self.assertRaises(AirflowException):
            MLEngineSetDefaultVersionOperator(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                model_name=None,
                version_name=TEST_VERSION_NAME,
                gcp_conn_id=TEST_GCP_CONN_ID,
                delegate_to=TEST_DELEGATE_TO,
            )

    def test_missing_version_name(self):
        with self.assertRaises(AirflowException):
            MLEngineSetDefaultVersionOperator(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                model_name=TEST_MODEL_NAME,
                version_name=None,
                gcp_conn_id=TEST_GCP_CONN_ID,
                delegate_to=TEST_DELEGATE_TO,
            )


class TestMLEngineListVersions(unittest.TestCase):

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success(self, mock_hook):
        task = MLEngineListVersionsOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )

        task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.list_versions.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
        )

    def test_missing_model_name(self):
        with self.assertRaises(AirflowException):
            MLEngineListVersionsOperator(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                model_name=None,
                gcp_conn_id=TEST_GCP_CONN_ID,
                delegate_to=TEST_DELEGATE_TO,
            )


class TestMLEngineDeleteVersion(unittest.TestCase):

    @patch('airflow.providers.google.cloud.operators.mlengine.MLEngineHook')
    def test_success(self, mock_hook):
        task = MLEngineDeleteVersionOperator(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            version_name=TEST_VERSION_NAME,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
        )

        task.execute(None)

        mock_hook.assert_called_once_with(delegate_to=TEST_DELEGATE_TO, gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.delete_version.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            model_name=TEST_MODEL_NAME,
            version_name=TEST_VERSION_NAME
        )

    def test_missing_version_name(self):
        with self.assertRaises(AirflowException):
            MLEngineDeleteVersionOperator(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                model_name=TEST_MODEL_NAME,
                version_name=None,
                gcp_conn_id=TEST_GCP_CONN_ID,
                delegate_to=TEST_DELEGATE_TO,
            )

    def test_missing_model_name(self):
        with self.assertRaises(AirflowException):
            MLEngineDeleteVersionOperator(
                task_id="task-id",
                project_id=TEST_PROJECT_ID,
                model_name=None,
                version_name=TEST_VERSION_NAME,
                gcp_conn_id=TEST_GCP_CONN_ID,
                delegate_to=TEST_DELEGATE_TO,
            )


if __name__ == '__main__':
    unittest.main()
