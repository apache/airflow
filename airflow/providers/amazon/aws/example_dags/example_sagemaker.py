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

import io
import os
from datetime import datetime

import numpy as np
import pandas as pd
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
    SageMakerModelOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerTrainingSensor, SageMakerTransformSensor

# This Sample DAG demonstrates using SageMaker to identify various species of Iris flower.
# The Project Name variable below will be used to name various tasks and the required S3 keys.
PROJECT_NAME = 'iris'
TIMESTAMP = '{{ ts_nodash }}'

S3_BUCKET = os.getenv('S3_BUCKET', 'S3_bucket')
INPUT_S3_KEY = f'{PROJECT_NAME}/processed-input-data'
OUTPUT_S3_KEY = f'{PROJECT_NAME}/results'
MODEL_NAME = f'{PROJECT_NAME}-KNN-model'
TRAINING_JOB_NAME = f'{PROJECT_NAME}-train-{TIMESTAMP}'

ROLE_ARN = os.getenv(
    'SAGEMAKER_ROLE_ARN',
    'arn:aws:iam::1234567890:role/service-role/AmazonSageMaker-ExecutionRole',
)

# A Sample dataset hosted by UC Irvine's machine learning repository
DATA_URL = 'https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data'

# The URI of an Amazon-provided docker image for handling KNN model training.  This is a public ECR
# repo cited in public SageMaker documentation, so the account number does not need to be redacted.
# For more info see: https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-west-2.html#knn-us-west-2.title
KNN_IMAGE_URI = '174872318107.dkr.ecr.us-west-2.amazonaws.com/knn'

# Define configs for training, model creation, and batch transform jobs
TRAINING_CONFIG = {
    "AlgorithmSpecification": {
        "TrainingImage": KNN_IMAGE_URI,
        "TrainingInputMode": "File",
    },
    "HyperParameters": {
        "predictor_type": "classifier",
        "feature_dim": "4",
        "k": "3",
        "sample_size": "150",
    },
    "InputDataConfig": [
        {
            "ChannelName": "train",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"s3://{S3_BUCKET}/{INPUT_S3_KEY}/train.csv",
                }
            },
            "ContentType": "text/csv",
            "InputMode": "File",
        }
    ],
    "OutputDataConfig": {"S3OutputPath": f"s3://{S3_BUCKET}/{OUTPUT_S3_KEY}/"},
    "ResourceConfig": {
        "InstanceCount": 1,
        "InstanceType": "ml.m5.large",
        "VolumeSizeInGB": 1,
    },
    "RoleArn": ROLE_ARN,
    "StoppingCondition": {"MaxRuntimeInSeconds": 6000},
    "TrainingJobName": TRAINING_JOB_NAME,
}

MODEL_CONFIG = {
    "ExecutionRoleArn": ROLE_ARN,
    "ModelName": MODEL_NAME,
    "PrimaryContainer": {
        "Mode": "SingleModel",
        "Image": KNN_IMAGE_URI,
        "ModelDataUrl": f"s3://{S3_BUCKET}/{OUTPUT_S3_KEY}/{TRAINING_JOB_NAME}/output/model.tar.gz",
    },
}

TRANSFORM_CONFIG = {
    # Transform job names can't be reused, so appending a full timestamp tp ensure it is unique.
    "TransformJobName": f"test-{PROJECT_NAME}-knn-{TIMESTAMP}",
    "TransformInput": {
        "DataSource": {
            "S3DataSource": {
                "S3DataType": "S3Prefix",
                "S3Uri": f"s3://{S3_BUCKET}/{INPUT_S3_KEY}/test.csv",
            }
        },
        "SplitType": "Line",
        "ContentType": "text/csv",
    },
    "TransformOutput": {"S3OutputPath": f"s3://{S3_BUCKET}/{OUTPUT_S3_KEY}"},
    "TransformResources": {
        "InstanceCount": 1,
        "InstanceType": "ml.m5.large",
    },
    "ModelName": MODEL_NAME,
}


@task
def data_prep(data_url, s3_bucket, input_s3_key):
    """
    Grabs the Iris dataset from API, splits into train/test splits, and saves CSV's to S3 using S3 Hook
    """
    # Get data from API
    iris_response = requests.get(data_url).content
    columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
    iris = pd.read_csv(io.StringIO(iris_response.decode('utf-8')), names=columns)

    # Process data
    iris['species'] = iris['species'].replace({'Iris-virginica': 0, 'Iris-versicolor': 1, 'Iris-setosa': 2})
    iris = iris[['species', 'sepal_length', 'sepal_width', 'petal_length', 'petal_width']]

    # Split into test and train data
    iris_train, iris_test = np.split(
        iris.sample(frac=1, random_state=np.random.RandomState()), [int(0.7 * len(iris))]
    )
    iris_test.drop(['species'], axis=1, inplace=True)

    # Save files to S3
    iris_train.to_csv('iris_train.csv', index=False, header=False)
    iris_test.to_csv('iris_test.csv', index=False, header=False)
    s3_hook = S3Hook(aws_conn_id='aws-sagemaker')
    s3_hook.load_file(
        'iris_train.csv',
        f'{input_s3_key}/train.csv',
        bucket_name=s3_bucket,
        replace=True,
    )
    s3_hook.load_file(
        'iris_test.csv',
        f'{input_s3_key}/test.csv',
        bucket_name=s3_bucket,
        replace=True,
    )


with DAG(
    dag_id='example_sagemaker',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_sagemaker_training]
    train_model = SageMakerTrainingOperator(
        task_id='train_model',
        config=TRAINING_CONFIG,
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_training]

    # [START howto_operator_sagemaker_training_sensor]
    await_training = SageMakerTrainingSensor(
        task_id="await_training",
        job_name=TRAINING_JOB_NAME,
    )
    # [END howto_operator_sagemaker_training_sensor]

    # [START howto_operator_sagemaker_model]
    create_model = SageMakerModelOperator(
        task_id='create_model',
        config=MODEL_CONFIG,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_model]

    # [START howto_operator_sagemaker_transform]
    test_model = SageMakerTransformOperator(
        task_id='test_model',
        config=TRANSFORM_CONFIG,
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_transform]

    # [START howto_operator_sagemaker_transform_sensor]
    await_transform = SageMakerTransformSensor(
        task_id="await_transform",
        job_name=f"test-{PROJECT_NAME}-knn-{TIMESTAMP}",
    )
    # [END howto_operator_sagemaker_transform_sensor]

    # [START howto_operator_sagemaker_delete_model]
    delete_model = SageMakerDeleteModelOperator(
        task_id="delete_model",
        config={'ModelName': MODEL_NAME},
        trigger_rule='all_done',
    )
    # [END howto_operator_sagemaker_delete_model]

    (
        data_prep(DATA_URL, S3_BUCKET, INPUT_S3_KEY)
        >> train_model
        >> await_training
        >> create_model
        >> test_model
        >> await_transform
        >> delete_model
    )
