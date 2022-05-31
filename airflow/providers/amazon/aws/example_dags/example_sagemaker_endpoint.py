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
import json
import os
from datetime import datetime

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
    SageMakerEndpointConfigOperator,
    SageMakerEndpointOperator,
    SageMakerModelOperator,
    SageMakerTrainingOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerEndpointSensor

# Project name will be used in naming the S3 buckets and various tasks.
# The dataset used in this example is identifying varieties of the Iris flower.
PROJECT_NAME = 'iris'
TIMESTAMP = '{{ ts_nodash }}'

S3_BUCKET = os.getenv('S3_BUCKET', 'S3_bucket')
ROLE_ARN = os.getenv(
    'SAGEMAKER_ROLE_ARN',
    'arn:aws:iam::1234567890:role/service-role/AmazonSageMaker-ExecutionRole',
)
INPUT_DATA_S3_KEY = f'{PROJECT_NAME}/processed-input-data'
TRAINING_OUTPUT_S3_KEY = f'{PROJECT_NAME}/training-results'

MODEL_NAME = f'{PROJECT_NAME}-KNN-model'
ENDPOINT_NAME = f'{PROJECT_NAME}-endpoint'
# Job names can't be reused, so appending a timestamp ensures it is unique.
ENDPOINT_CONFIG_JOB_NAME = f'{PROJECT_NAME}-endpoint-config-{TIMESTAMP}'
TRAINING_JOB_NAME = f'{PROJECT_NAME}-train-{TIMESTAMP}'

# For an example of how to obtain the following train and test data, please see
# https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/example_dags/example_sagemaker.py
TRAIN_DATA = '0,4.9,2.5,4.5,1.7\n1,7.0,3.2,4.7,1.4\n0,7.3,2.9,6.3,1.8\n2,5.1,3.5,1.4,0.2\n'
SAMPLE_TEST_DATA = '6.4,3.2,4.5,1.5'

# The URI of an Amazon-provided docker image for handling KNN model training.  This is a public ECR
# repo cited in public SageMaker documentation, so the account number does not need to be redacted.
# For more info see: https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-west-2.html#knn-us-west-2.title
KNN_IMAGE_URI = '174872318107.dkr.ecr.us-west-2.amazonaws.com/knn'

# Define configs for processing, training, model creation, and batch transform jobs
TRAINING_CONFIG = {
    'TrainingJobName': TRAINING_JOB_NAME,
    'RoleArn': ROLE_ARN,
    'AlgorithmSpecification': {
        "TrainingImage": KNN_IMAGE_URI,
        "TrainingInputMode": "File",
    },
    'HyperParameters': {
        'predictor_type': 'classifier',
        'feature_dim': '4',
        'k': '3',
        'sample_size': '6',
    },
    'InputDataConfig': [
        {
            'ChannelName': 'train',
            'CompressionType': 'None',
            'ContentType': 'text/csv',
            'DataSource': {
                'S3DataSource': {
                    'S3DataDistributionType': 'FullyReplicated',
                    'S3DataType': 'S3Prefix',
                    'S3Uri': f's3://{S3_BUCKET}/{INPUT_DATA_S3_KEY}/train.csv',
                }
            },
        }
    ],
    'OutputDataConfig': {'S3OutputPath': f's3://{S3_BUCKET}/{TRAINING_OUTPUT_S3_KEY}/'},
    'ResourceConfig': {
        'InstanceCount': 1,
        'InstanceType': 'ml.m5.large',
        'VolumeSizeInGB': 1,
    },
    'StoppingCondition': {'MaxRuntimeInSeconds': 6 * 60},
}

MODEL_CONFIG = {
    'ModelName': MODEL_NAME,
    'ExecutionRoleArn': ROLE_ARN,
    'PrimaryContainer': {
        'Mode': 'SingleModel',
        'Image': KNN_IMAGE_URI,
        'ModelDataUrl': f's3://{S3_BUCKET}/{TRAINING_OUTPUT_S3_KEY}/{TRAINING_JOB_NAME}/output/model.tar.gz',
    },
}

ENDPOINT_CONFIG_CONFIG = {
    'EndpointConfigName': ENDPOINT_CONFIG_JOB_NAME,
    'ProductionVariants': [
        {
            'VariantName': f'{PROJECT_NAME}-demo',
            'ModelName': MODEL_NAME,
            'InstanceType': 'ml.t2.medium',
            'InitialInstanceCount': 1,
        },
    ],
}

DEPLOY_ENDPOINT_CONFIG = {
    'EndpointName': ENDPOINT_NAME,
    'EndpointConfigName': ENDPOINT_CONFIG_JOB_NAME,
}


@task
def call_endpoint():
    runtime = boto3.Session().client('sagemaker-runtime')

    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType='text/csv',
        Body=SAMPLE_TEST_DATA,
    )

    return json.loads(response["Body"].read().decode())['predictions']


@task(trigger_rule='all_done')
def cleanup():
    # Delete Endpoint and Endpoint Config
    client = boto3.client('sagemaker')
    endpoint_config_name = client.list_endpoint_configs()['EndpointConfigs'][0]['EndpointConfigName']
    client.delete_endpoint_config(EndpointConfigName=endpoint_config_name)
    client.delete_endpoint(EndpointName=ENDPOINT_NAME)

    # Delete S3 Artifacts
    client = boto3.client('s3')
    object_keys = [
        key['Key'] for key in client.list_objects_v2(Bucket=S3_BUCKET, Prefix=PROJECT_NAME)['Contents']
    ]
    for key in object_keys:
        client.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': [{'Key': key}]})


with DAG(
    dag_id='example_sagemaker_endpoint',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    upload_data = S3CreateObjectOperator(
        task_id='upload_data',
        s3_bucket=S3_BUCKET,
        s3_key=f'{INPUT_DATA_S3_KEY}/train.csv',
        data=TRAIN_DATA,
        replace=True,
    )

    train_model = SageMakerTrainingOperator(
        task_id='train_model',
        config=TRAINING_CONFIG,
        do_xcom_push=False,
    )

    create_model = SageMakerModelOperator(
        task_id='create_model',
        config=MODEL_CONFIG,
        do_xcom_push=False,
    )

    # [START howto_operator_sagemaker_endpoint_config]
    configure_endpoint = SageMakerEndpointConfigOperator(
        task_id='configure_endpoint',
        config=ENDPOINT_CONFIG_CONFIG,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_endpoint_config]

    # [START howto_operator_sagemaker_endpoint]
    deploy_endpoint = SageMakerEndpointOperator(
        task_id='deploy_endpoint',
        config=DEPLOY_ENDPOINT_CONFIG,
        # Waits by default, <setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_endpoint]

    # [START howto_sensor_sagemaker_endpoint]
    await_endpoint = SageMakerEndpointSensor(
        task_id='await_endpoint',
        endpoint_name=ENDPOINT_NAME,
        do_xcom_push=False,
    )
    # [END howto_sensor_sagemaker_endpoint]

    # Trigger rule set to "all_done" so clean up will run regardless of success on other tasks.
    delete_model = SageMakerDeleteModelOperator(
        task_id='delete_model',
        config={'ModelName': MODEL_NAME},
        trigger_rule='all_done',
    )

    (
        upload_data
        >> train_model
        >> create_model
        >> configure_endpoint
        >> deploy_endpoint
        >> await_endpoint
        >> call_endpoint()
        >> cleanup()
        >> delete_model
    )
