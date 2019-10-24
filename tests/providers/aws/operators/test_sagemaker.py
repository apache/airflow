# -*- coding: utf-8 -*-

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

import unittest

from airflow.exceptions import AirflowException
from airflow.providers.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.aws.operators.sagemaker import (
    SageMakerBaseOperator, SageMakerEndpointConfigOperator, SageMakerEndpointOperator, SageMakerModelOperator,
    SageMakerTrainingOperator, SageMakerTransformOperator, SageMakerTuningOperator,
)
from tests.compat import mock


class TestSageMakerBaseOperator(unittest.TestCase):
    def setUp(self):
        self.sagemaker = SageMakerBaseOperator(
            task_id="test_sagemaker_operator",
            aws_conn_id="sagemaker_test_id",
            config={"key1": "1", "key2": {"key3": "3", "key4": "4"}, "key5": [{"key6": "6"}, {"key6": "7"}]},
        )

    def test_parse_integer(self):
        self.sagemaker.integer_fields = [["key1"], ["key2", "key3"], ["key2", "key4"], ["key5", "key6"]]
        self.sagemaker.parse_config_integers()
        self.assertEqual(
            self.sagemaker.config,
            {"key1": 1, "key2": {"key3": 3, "key4": 4}, "key5": [{"key6": 6}, {"key6": 7}]},
        )


class TestSageMakerEndpointConfigOperator(unittest.TestCase):
    model_name = 'test-model-name'
    config_name = 'test-config-name'
    create_endpoint_config_params = {
        'EndpointConfigName': config_name,
        'ProductionVariants': [
            {
                'VariantName': 'AllTraffic',
                'ModelName': model_name,
                'InitialInstanceCount': '1',
                'InstanceType': 'ml.c4.xlarge'
            }
        ]
    }

    def setUp(self):
        self.sagemaker = SageMakerEndpointConfigOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=self.create_endpoint_config_params
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        for variant in self.sagemaker.config['ProductionVariants']:
            self.assertEqual(variant['InitialInstanceCount'],
                             int(variant['InitialInstanceCount']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    def test_execute(self, mock_model, mock_client):
        mock_model.return_value = {
            'EndpointConfigArn': 'testarn',
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(self.create_endpoint_config_params)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    def test_execute_with_failure(self, mock_model, mock_client):
        mock_model.return_value = {
            'EndpointConfigArn': 'testarn',
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


class TestSageMakerEndpointOperator(unittest.TestCase):
    role = 'arn:aws:iam:role/test-role'
    bucket = 'test-bucket'
    image = 'test-image'
    output_url = 's3://{}/test/output'.format(bucket)
    model_name = 'test-model-name'
    config_name = 'test-endpoint-config-name'
    endpoint_name = 'test-endpoint-name'

    create_model_params = {
        'ModelName': model_name,
        'PrimaryContainer': {
            'Image': image,
            'ModelDataUrl': output_url,
        },
        'ExecutionRoleArn': role
    }

    create_endpoint_config_params = {
        'EndpointConfigName': config_name,
        'ProductionVariants': [
            {
                'VariantName': 'AllTraffic',
                'ModelName': model_name,
                'InitialInstanceCount': '1',
                'InstanceType': 'ml.c4.xlarge'
            }
        ]
    }

    create_endpoint_params = {
        'EndpointName': endpoint_name,
        'EndpointConfigName': config_name
    }

    config = {
        'Model': create_model_params,
        'EndpointConfig': create_endpoint_config_params,
        'Endpoint': create_endpoint_params
    }

    def setUp(self):
        self.sagemaker = SageMakerEndpointOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=self.config,
            wait_for_completion=False,
            check_interval=5,
            operation='create'
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        for variant in self.sagemaker.config['EndpointConfig']['ProductionVariants']:
            self.assertEqual(variant['InitialInstanceCount'],
                             int(variant['InitialInstanceCount']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    def test_execute(self, mock_endpoint, mock_endpoint_config,
                     mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'testarn',
                                      'ResponseMetadata':
                                      {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(self.create_model_params)
        mock_endpoint_config.assert_called_once_with(self.create_endpoint_config_params)
        mock_endpoint.assert_called_once_with(self.create_endpoint_params,
                                              wait_for_completion=False,
                                              check_interval=5,
                                              max_ingestion_time=None
                                              )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    def test_execute_with_failure(self, mock_endpoint, mock_endpoint_config,
                                  mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'testarn',
                                      'ResponseMetadata':
                                      {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


class TestSageMakerModelOperator(unittest.TestCase):
    role = 'arn:aws:iam:role/test-role'
    bucket = 'test-bucket'
    model_name = 'test-model-name'
    image = 'test-image'
    output_url = 's3://{}/test/output'.format(bucket)
    create_model_params = {
        'ModelName': model_name,
        'PrimaryContainer': {
            'Image': image,
            'ModelDataUrl': output_url,
        },
        'ExecutionRoleArn': role
    }

    def setUp(self):
        self.sagemaker = SageMakerModelOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=self.create_model_params
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    def test_execute(self, mock_model, mock_client):
        mock_model.return_value = {'ModelArn': 'testarn',
                                   'ResponseMetadata':
                                       {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(self.create_model_params)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    def test_execute_with_failure(self, mock_model, mock_client):
        mock_model.return_value = {'ModelArn': 'testarn',
                                   'ResponseMetadata':
                                       {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


# noinspection PyUnusedLocal
# pylint: disable=unused-argument
class TestSageMakerTrainingOperator(unittest.TestCase):
    role = 'arn:aws:iam:role/test-role'
    bucket = 'test-bucket'
    key = 'test/data'
    data_url = 's3://{}/{}'.format(bucket, key)
    job_name = 'test-job-name'
    image = 'test-image'
    output_url = 's3://{}/test/output'.format(bucket)
    create_training_params = {
        "AlgorithmSpecification": {"TrainingImage": image, "TrainingInputMode": "File"},
        "RoleArn": role,
        "OutputDataConfig": {"S3OutputPath": output_url},
        "ResourceConfig": {"InstanceCount": "2", "InstanceType": "ml.c4.8xlarge", "VolumeSizeInGB": "50"},
        "TrainingJobName": job_name,
        "HyperParameters": {"k": "10", "feature_dim": "784", "mini_batch_size": "500", "force_dense": "True"},
        "StoppingCondition": {"MaxRuntimeInSeconds": "3600"},
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": data_url,
                        "S3DataDistributionType": "FullyReplicated",
                    }
                },
                "CompressionType": "None",
                "RecordWrapperType": "None",
            }
        ],
    }

    def setUp(self):
        self.sagemaker = SageMakerTrainingOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=self.create_training_params,
            wait_for_completion=False,
            check_interval=5
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        self.assertEqual(self.sagemaker.config['ResourceConfig']['InstanceCount'],
                         int(self.sagemaker.config['ResourceConfig']['InstanceCount']))
        self.assertEqual(self.sagemaker.config['ResourceConfig']['VolumeSizeInGB'],
                         int(self.sagemaker.config['ResourceConfig']['VolumeSizeInGB']))
        self.assertEqual(self.sagemaker.config['StoppingCondition']['MaxRuntimeInSeconds'],
                         int(self.sagemaker.config['StoppingCondition']['MaxRuntimeInSeconds']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_training_job')
    def test_execute(self, mock_training, mock_client):
        mock_training.return_value = {'TrainingJobArn': 'testarn',
                                      'ResponseMetadata':
                                          {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_training.assert_called_once_with(self.create_training_params,
                                              wait_for_completion=False,
                                              print_log=True,
                                              check_interval=5,
                                              max_ingestion_time=None
                                              )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_training_job')
    def test_execute_with_failure(self, mock_training, mock_client):
        mock_training.return_value = {'TrainingJobArn': 'testarn',
                                      'ResponseMetadata':
                                          {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)
# pylint: enable=unused-argument


class TestSageMakerTransformOperator(unittest.TestCase):
    role = "arn:aws:iam:role/test-role"
    bucket = "test-bucket"
    key = "test/data"
    data_url = "s3://{}/{}".format(bucket, key)
    job_name = "test-job-name"
    model_name = "test-model-name"
    image = "test-image"
    output_url = "s3://{}/test/output".format(bucket)
    create_transform_params = {
        "TransformJobName": job_name,
        "ModelName": model_name,
        "MaxConcurrentTransforms": "12",
        "MaxPayloadInMB": "6",
        "BatchStrategy": "MultiRecord",
        "TransformInput": {"DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": data_url}}},
        "TransformOutput": {"S3OutputPath": output_url},
        "TransformResources": {"InstanceType": "ml.m4.xlarge", "InstanceCount": "3"},
    }

    create_model_params = {
        "ModelName": model_name,
        "PrimaryContainer": {"Image": image, "ModelDataUrl": output_url},
        "ExecutionRoleArn": role,
    }

    config = {"Model": create_model_params, "Transform": create_transform_params}

    def setUp(self):
        self.sagemaker = SageMakerTransformOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=self.config,
            wait_for_completion=False,
            check_interval=5
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        test_config = self.sagemaker.config['Transform']
        self.assertEqual(test_config['TransformResources']['InstanceCount'],
                         int(test_config['TransformResources']['InstanceCount']))
        self.assertEqual(test_config['MaxConcurrentTransforms'],
                         int(test_config['MaxConcurrentTransforms']))
        self.assertEqual(test_config['MaxPayloadInMB'],
                         int(test_config['MaxPayloadInMB']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_execute(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {'TransformJobArn': 'testarn',
                                       'ResponseMetadata':
                                       {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(self.create_model_params)
        mock_transform.assert_called_once_with(self.create_transform_params,
                                               wait_for_completion=False,
                                               check_interval=5,
                                               max_ingestion_time=None
                                               )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_execute_with_failure(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {'TransformJobArn': 'testarn',
                                       'ResponseMetadata':
                                       {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


class TestSageMakerTuningOperator(unittest.TestCase):
    role = "arn:aws:iam:role/test-role"
    bucket = "test-bucket"
    key = "test/data"
    data_url = "s3://{}/{}".format(bucket, key)
    job_name = "test-job-name"
    image = "test-image"
    output_url = "s3://{}/test/output".format(bucket)
    create_tuning_params = {
        "HyperParameterTuningJobName": job_name,
        "HyperParameterTuningJobConfig": {
            "Strategy": "Bayesian",
            "HyperParameterTuningJobObjective": {"Type": "Maximize", "MetricName": "test_metric"},
            "ResourceLimits": {"MaxNumberOfTrainingJobs": "123", "MaxParallelTrainingJobs": "123"},
            "ParameterRanges": {"IntegerParameterRanges": [{"Name": "k", "MinValue": "2", "MaxValue": "10"}]},
        },
        "TrainingJobDefinition": {
            "StaticHyperParameters": {
                "k": "10",
                "feature_dim": "784",
                "mini_batch_size": "500",
                "force_dense": "True",
            },
            "AlgorithmSpecification": {"TrainingImage": image, "TrainingInputMode": "File"},
            "RoleArn": role,
            "InputDataConfig": [
                {
                    "ChannelName": "train",
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": data_url,
                            "S3DataDistributionType": "FullyReplicated",
                        }
                    },
                    "CompressionType": "None",
                    "RecordWrapperType": "None",
                }
            ],
            "OutputDataConfig": {"S3OutputPath": output_url},
            "ResourceConfig": {"InstanceCount": "2", "InstanceType": "ml.c4.8xlarge", "VolumeSizeInGB": "50"},
            "StoppingCondition": dict(MaxRuntimeInSeconds=60 * 60),
        },
    }

    def setUp(self):
        self.sagemaker = SageMakerTuningOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_conn',
            config=self.create_tuning_params,
            wait_for_completion=False,
            check_interval=5
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        self.assertEqual(self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']
                         ['InstanceCount'],
                         int(self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']
                             ['InstanceCount']))
        self.assertEqual(self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']
                         ['VolumeSizeInGB'],
                         int(self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']
                             ['VolumeSizeInGB']))
        self.assertEqual(self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits']
                         ['MaxNumberOfTrainingJobs'],
                         int(self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits']
                             ['MaxNumberOfTrainingJobs']))
        self.assertEqual(self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits']
                         ['MaxParallelTrainingJobs'],
                         int(self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits']
                             ['MaxParallelTrainingJobs']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn',
                                    'ResponseMetadata':
                                    {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_tuning.assert_called_once_with(self.create_tuning_params,
                                            wait_for_completion=False,
                                            check_interval=5,
                                            max_ingestion_time=None
                                            )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute_with_failure(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn',
                                    'ResponseMetadata':
                                    {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


if __name__ == '__main__':
    unittest.main()
