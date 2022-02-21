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

from datetime import datetime
from os import environ

from airflow import DAG
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
    SageMakerModelOperator,
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)

model_name = "sample_model"
training_job_name = 'sample_training'
image_uri = environ.get('ECR_IMAGE_URI', '123456789012.dkr.ecr.us-east-1.amazonaws.com/repo_name')
s3_bucket = environ.get('BUCKET_NAME', 'test-airflow-12345')
role = environ.get('SAGEMAKER_ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')

sagemaker_processing_job_config = {
    "ProcessingJobName": "sample_processing_job",
    "ProcessingInputs": [
        {
            "InputName": "input",
            "AppManaged": False,
            "S3Input": {
                "S3Uri": f"s3://{s3_bucket}/preprocessing/input/",
                "LocalPath": "/opt/ml/processing/input/",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
                "S3DataDistributionType": "FullyReplicated",
                "S3CompressionType": "None",
            },
        },
    ],
    "ProcessingOutputConfig": {
        "Outputs": [
            {
                "OutputName": "output",
                "S3Output": {
                    "S3Uri": f"s3://{s3_bucket}/preprocessing/output/",
                    "LocalPath": "/opt/ml/processing/output/",
                    "S3UploadMode": "EndOfJob",
                },
                "AppManaged": False,
            }
        ]
    },
    "ProcessingResources": {
        "ClusterConfig": {
            "InstanceCount": 1,
            "InstanceType": "ml.m5.large",
            "VolumeSizeInGB": 5,
        }
    },
    "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
    "AppSpecification": {
        "ImageUri": f"{image_uri}",
        "ContainerEntrypoint": ["python3", "./preprocessing.py"],
    },
    "RoleArn": f"{role}",
}

sagemaker_training_job_config = {
    "AlgorithmSpecification": {
        "TrainingImage": f"{image_uri}",
        "TrainingInputMode": "File",
    },
    "InputDataConfig": [
        {
            "ChannelName": "config",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"s3://{s3_bucket}/config/",
                    "S3DataDistributionType": "FullyReplicated",
                }
            },
            "CompressionType": "None",
            "RecordWrapperType": "None",
        },
    ],
    "OutputDataConfig": {
        "KmsKeyId": "",
        "S3OutputPath": f"s3://{s3_bucket}/training/",
    },
    "ResourceConfig": {
        "InstanceType": "ml.m5.large",
        "InstanceCount": 1,
        "VolumeSizeInGB": 5,
    },
    "StoppingCondition": {"MaxRuntimeInSeconds": 6000},
    "RoleArn": f"{role}",
    "EnableNetworkIsolation": False,
    "EnableInterContainerTrafficEncryption": False,
    "EnableManagedSpotTraining": False,
    "TrainingJobName": training_job_name,
}

sagemaker_create_model_config = {
    "ModelName": model_name,
    "Containers": [
        {
            "Image": f"{image_uri}",
            "Mode": "SingleModel",
            "ModelDataUrl": f"s3://{s3_bucket}/training/{training_job_name}/output/model.tar.gz",
        }
    ],
    "ExecutionRoleArn": f"{role}",
    "EnableNetworkIsolation": False,
}

sagemaker_inference_config = {
    "TransformJobName": "sample_transform_job",
    "ModelName": model_name,
    "TransformInput": {
        "DataSource": {
            "S3DataSource": {
                "S3DataType": "S3Prefix",
                "S3Uri": f"s3://{s3_bucket}/config/config_date.yml",
            }
        },
        "ContentType": "application/x-yaml",
        "CompressionType": "None",
        "SplitType": "None",
    },
    "TransformOutput": {"S3OutputPath": f"s3://{s3_bucket}/inferencing/output/"},
    "TransformResources": {"InstanceType": "ml.m5.large", "InstanceCount": 1},
}

# [START howto_operator_sagemaker]
with DAG(
    "sample_sagemaker_dag",
    schedule_interval=None,
    start_date=datetime(2022, 2, 21),
    catchup=False,
) as dag:
    sagemaker_processing_task = SageMakerProcessingOperator(
        config=sagemaker_processing_job_config,
        aws_conn_id="aws_default",
        task_id="sagemaker_preprocessing_task",
    )

    training_task = SageMakerTrainingOperator(
        config=sagemaker_training_job_config, aws_conn_id="aws_default", task_id="sagemaker_training_task"
    )

    model_delete_task = SageMakerDeleteModelOperator(
        task_id="sagemaker_delete_model_task",
        model_name=model_name,
        aws_conn_id="aws_default",
        dag=dag,
    )
    model_create_task = SageMakerModelOperator(
        config=sagemaker_create_model_config, aws_conn_id="aws_default", task_id="sagemaker_create_model_task"
    )

    inference_task = SageMakerTransformOperator(
        config=sagemaker_inference_config, aws_conn_id="aws_default", task_id="sagemaker_inference_task"
    )

    sagemaker_processing_task >> training_task >> model_delete_task >> model_create_task >> inference_task
    # [END howto_operator_sagemaker]
