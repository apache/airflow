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

MODEL_NAME = "sample_model"
TRAINING_JOB_NAME = "sample_training"
IMAGE_URI = environ.get("ECR_IMAGE_URI", "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo_name")
S3_BUCKET = environ.get("BUCKET_NAME", "test-airflow-12345")
ROLE = environ.get("SAGEMAKER_ROLE_ARN", "arn:aws:iam::123456789012:role/role_name")

SAGEMAKER_PROCESSING_JOB_CONFIG = {
    "ProcessingJobName": "sample_processing_job",
    "ProcessingInputs": [
        {
            "InputName": "input",
            "AppManaged": False,
            "S3Input": {
                "S3Uri": f"s3://{S3_BUCKET}/preprocessing/input/",
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
                    "S3Uri": f"s3://{S3_BUCKET}/preprocessing/output/",
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
        "ImageUri": f"{IMAGE_URI}",
        "ContainerEntrypoint": ["python3", "./preprocessing.py"],
    },
    "RoleArn": f"{ROLE}",
}

SAGEMAKER_TRAINING_JOB_CONFIG = {
    "AlgorithmSpecification": {
        "TrainingImage": f"{IMAGE_URI}",
        "TrainingInputMode": "File",
    },
    "InputDataConfig": [
        {
            "ChannelName": "config",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"s3://{S3_BUCKET}/config/",
                    "S3DataDistributionType": "FullyReplicated",
                }
            },
            "CompressionType": "None",
            "RecordWrapperType": "None",
        },
    ],
    "OutputDataConfig": {
        "KmsKeyId": "",
        "S3OutputPath": f"s3://{S3_BUCKET}/training/",
    },
    "ResourceConfig": {
        "InstanceType": "ml.m5.large",
        "InstanceCount": 1,
        "VolumeSizeInGB": 5,
    },
    "StoppingCondition": {"MaxRuntimeInSeconds": 6000},
    "RoleArn": f"{ROLE}",
    "EnableNetworkIsolation": False,
    "EnableInterContainerTrafficEncryption": False,
    "EnableManagedSpotTraining": False,
    "TrainingJobName": TRAINING_JOB_NAME,
}

SAGEMAKER_CREATE_MODEL_CONFIG = {
    "ModelName": MODEL_NAME,
    "Containers": [
        {
            "Image": f"{IMAGE_URI}",
            "Mode": "SingleModel",
            "ModelDataUrl": f"s3://{S3_BUCKET}/training/{TRAINING_JOB_NAME}/output/model.tar.gz",
        }
    ],
    "ExecutionRoleArn": f"{ROLE}",
    "EnableNetworkIsolation": False,
}

SAGEMAKER_INFERENCE_CONFIG = {
    "TransformJobName": "sample_transform_job",
    "ModelName": MODEL_NAME,
    "TransformInput": {
        "DataSource": {
            "S3DataSource": {
                "S3DataType": "S3Prefix",
                "S3Uri": f"s3://{S3_BUCKET}/config/config_date.yml",
            }
        },
        "ContentType": "application/x-yaml",
        "CompressionType": "None",
        "SplitType": "None",
    },
    "TransformOutput": {"S3OutputPath": f"s3://{S3_BUCKET}/inferencing/output/"},
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
        config=SAGEMAKER_PROCESSING_JOB_CONFIG,
        aws_conn_id="aws_default",
        task_id="sagemaker_preprocessing_task",
    )

    training_task = SageMakerTrainingOperator(
        config=SAGEMAKER_TRAINING_JOB_CONFIG, aws_conn_id="aws_default", task_id="sagemaker_training_task"
    )

    model_create_task = SageMakerModelOperator(
        config=SAGEMAKER_CREATE_MODEL_CONFIG, aws_conn_id="aws_default", task_id="sagemaker_create_model_task"
    )

    inference_task = SageMakerTransformOperator(
        config=SAGEMAKER_INFERENCE_CONFIG, aws_conn_id="aws_default", task_id="sagemaker_inference_task"
    )

    model_delete_task = SageMakerDeleteModelOperator(
        task_id="sagemaker_delete_model_task", config={'ModelName': MODEL_NAME}, aws_conn_id="aws_default"
    )

    sagemaker_processing_task >> training_task >> model_create_task >> inference_task >> model_delete_task
    # [END howto_operator_sagemaker]
