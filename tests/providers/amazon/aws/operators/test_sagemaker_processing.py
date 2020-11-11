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
from unittest import mock

from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker_processing import SageMakerProcessingOperator

job_name = 'test-job-name'

create_processing_params = {
    "AppSpecification": {
        "ContainerArguments": ["container_arg"],
        "ContainerEntrypoint": ["container_entrypoint"],
        "ImageUri": "{{ image_uri }}",
    },
    "Environment": {"{{ key }}": "{{ value }}"},
    "ExperimentConfig": {
        "ExperimentName": "ExperimentName",
        "TrialComponentDisplayName": "TrialComponentDisplayName",
        "TrialName": "TrialName",
    },
    "ProcessingInputs": [
        {
            "InputName": "AnalyticsInputName",
            "S3Input": {
                "LocalPath": "{{ Local Path }}",
                "S3CompressionType": "None",
                "S3DataDistributionType": "FullyReplicated",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
                "S3Uri": "{{ S3Uri }}",
            },
        }
    ],
    "ProcessingJobName": job_name,
    "ProcessingOutputConfig": {
        "KmsKeyId": "KmsKeyID",
        "Outputs": [
            {
                "OutputName": "AnalyticsOutputName",
                "S3Output": {
                    "LocalPath": "{{ Local Path }}",
                    "S3UploadMode": "EndOfJob",
                    "S3Uri": "{{ S3Uri }}",
                },
            }
        ],
    },
    "ProcessingResources": {
        "ClusterConfig": {
            "InstanceCount": 2,
            "InstanceType": "ml.p2.xlarge",
            "VolumeSizeInGB": 30,
            "VolumeKmsKeyId": "{{ kms_key }}",
        }
    },
    "RoleArn": "arn:aws:iam::0122345678910:role/SageMakerPowerUser",
    "Tags": [{"{{ key }}": "{{ value }}"}],
}

create_processing_params_with_stopping_condition = create_processing_params.copy()
create_processing_params_with_stopping_condition.update(StoppingCondition={"MaxRuntimeInSeconds": 3600})


class TestSageMakerProcessingOperator(unittest.TestCase):
    def setUp(self):
        self.processing_config_kwargs = dict(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            wait_for_completion=False,
            check_interval=5,
        )

    @parameterized.expand(
        [
            (
                create_processing_params,
                [
                    ['ProcessingResources', 'ClusterConfig', 'InstanceCount'],
                    ['ProcessingResources', 'ClusterConfig', 'VolumeSizeInGB'],
                ],
            ),
            (
                create_processing_params_with_stopping_condition,
                [
                    ['ProcessingResources', 'ClusterConfig', 'InstanceCount'],
                    ['ProcessingResources', 'ClusterConfig', 'VolumeSizeInGB'],
                    ['StoppingCondition', 'MaxRuntimeInSeconds'],
                ],
            ),
        ]
    )
    def test_integer_fields_are_set(self, config, expected_fields):
        sagemaker = SageMakerProcessingOperator(**self.processing_config_kwargs, config=config)
        assert sagemaker.integer_fields == expected_fields

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(
        SageMakerHook,
        'create_processing_job',
        return_value={'ProcessingJobArn': 'testarn', 'ResponseMetadata': {'HTTPStatusCode': 200}},
    )
    def test_execute(self, mock_processing, mock_client):
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=create_processing_params
        )
        sagemaker.execute(None)
        mock_processing.assert_called_once_with(
            create_processing_params, wait_for_completion=False, check_interval=5, max_ingestion_time=None
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(
        SageMakerHook,
        'create_processing_job',
        return_value={'ProcessingJobArn': 'testarn', 'ResponseMetadata': {'HTTPStatusCode': 404}},
    )
    def test_execute_with_failure(self, mock_processing, mock_client):
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=create_processing_params
        )
        self.assertRaises(AirflowException, sagemaker.execute, None)

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "list_processing_jobs", return_value=[{"ProcessingJobName": job_name}])
    @mock.patch.object(
        SageMakerHook, "create_processing_job", return_value={"ResponseMetadata": {"HTTPStatusCode": 200}}
    )
    def test_execute_with_existing_job_increment(
        self, mock_create_processing_job, mock_list_processing_jobs, mock_client
    ):
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=create_processing_params
        )
        sagemaker.action_if_job_exists = "increment"
        sagemaker.execute(None)

        expected_config = create_processing_params.copy()
        # Expect to see ProcessingJobName suffixed with "-2" because we return one existing job
        expected_config["ProcessingJobName"] = f"{job_name}-2"
        mock_create_processing_job.assert_called_once_with(
            expected_config,
            wait_for_completion=False,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "list_processing_jobs", return_value=[{"ProcessingJobName": job_name}])
    @mock.patch.object(
        SageMakerHook, "create_processing_job", return_value={"ResponseMetadata": {"HTTPStatusCode": 200}}
    )
    def test_execute_with_existing_job_fail(
        self, mock_create_processing_job, mock_list_processing_jobs, mock_client
    ):
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=create_processing_params
        )
        sagemaker.action_if_job_exists = "fail"
        self.assertRaises(AirflowException, sagemaker.execute, None)

    @mock.patch.object(SageMakerHook, "get_conn")
    def test_action_if_job_exists_validation(self, mock_client):
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=create_processing_params
        )
        self.assertRaises(AirflowException, sagemaker.__init__, action_if_job_exists="not_fail_or_increment")
