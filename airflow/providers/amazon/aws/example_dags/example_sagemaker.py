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
import base64
import os
import subprocess
from datetime import datetime
from tempfile import NamedTemporaryFile

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
    SageMakerModelOperator,
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
    SageMakerTuningOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker import (
    SageMakerTrainingSensor,
    SageMakerTransformSensor,
    SageMakerTuningSensor,
)

# Project name will be used in naming the S3 buckets and various tasks.
# The dataset used in this example is identifying varieties of the Iris flower.
PROJECT_NAME = 'iris'
TIMESTAMP = '{{ ts_nodash }}'

S3_BUCKET = os.getenv('S3_BUCKET', 'S3_bucket')
RAW_DATA_S3_KEY = f'{PROJECT_NAME}/preprocessing/input.csv'
INPUT_DATA_S3_KEY = f'{PROJECT_NAME}/processed-input-data'
TRAINING_OUTPUT_S3_KEY = f'{PROJECT_NAME}/results'
PREDICTION_OUTPUT_S3_KEY = f'{PROJECT_NAME}/transform'

PROCESSING_LOCAL_INPUT_PATH = '/opt/ml/processing/input'
PROCESSING_LOCAL_OUTPUT_PATH = '/opt/ml/processing/output'

MODEL_NAME = f'{PROJECT_NAME}-KNN-model'
# Job names can't be reused, so appending a timestamp ensures it is unique.
PROCESSING_JOB_NAME = f'{PROJECT_NAME}-processing-{TIMESTAMP}'
TRAINING_JOB_NAME = f'{PROJECT_NAME}-train-{TIMESTAMP}'
TRANSFORM_JOB_NAME = f'{PROJECT_NAME}-transform-{TIMESTAMP}'
TUNING_JOB_NAME = f'{PROJECT_NAME}-tune-{TIMESTAMP}'

ROLE_ARN = os.getenv(
    'SAGEMAKER_ROLE_ARN',
    'arn:aws:iam::1234567890:role/service-role/AmazonSageMaker-ExecutionRole',
)
ECR_REPOSITORY = os.getenv('ECR_REPOSITORY', '1234567890.dkr.ecr.us-west-2.amazonaws.com/process_data')
REGION = ECR_REPOSITORY.split('.')[3]

# For this example we are using a subset of Fischer's Iris Data Set.
# The full dataset can be found at UC Irvine's machine learning repository:
# https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
DATASET = """
        5.1,3.5,1.4,0.2,Iris-setosa
        4.9,3.0,1.4,0.2,Iris-setosa
        7.0,3.2,4.7,1.4,Iris-versicolor
        6.4,3.2,4.5,1.5,Iris-versicolor
        4.9,2.5,4.5,1.7,Iris-virginica
        7.3,2.9,6.3,1.8,Iris-virginica
        """
SAMPLE_SIZE = DATASET.count('\n') - 1

# The URI of an Amazon-provided docker image for handling KNN model training.  This is a public ECR
# repo cited in public SageMaker documentation, so the account number does not need to be redacted.
# For more info see: https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-west-2.html#knn-us-west-2.title
KNN_IMAGE_URI = '174872318107.dkr.ecr.us-west-2.amazonaws.com/knn'

TASK_TIMEOUT = {'MaxRuntimeInSeconds': 6 * 60}

RESOURCE_CONFIG = {
    'InstanceCount': 1,
    'InstanceType': 'ml.m5.large',
    'VolumeSizeInGB': 1,
}

TRAINING_DATA_SOURCE = {
    'CompressionType': 'None',
    'ContentType': 'text/csv',
    'DataSource': {  # type: ignore
        'S3DataSource': {
            'S3DataDistributionType': 'FullyReplicated',
            'S3DataType': 'S3Prefix',
            'S3Uri': f's3://{S3_BUCKET}/{INPUT_DATA_S3_KEY}/train.csv',
        }
    },
}

# Define configs for processing, training, model creation, and batch transform jobs
SAGEMAKER_PROCESSING_JOB_CONFIG = {
    'ProcessingJobName': PROCESSING_JOB_NAME,
    'RoleArn': f'{ROLE_ARN}',
    'ProcessingInputs': [
        {
            'InputName': 'input',
            'AppManaged': False,
            'S3Input': {
                'S3Uri': f's3://{S3_BUCKET}/{RAW_DATA_S3_KEY}',
                'LocalPath': PROCESSING_LOCAL_INPUT_PATH,
                'S3DataType': 'S3Prefix',
                'S3InputMode': 'File',
                'S3DataDistributionType': 'FullyReplicated',
                'S3CompressionType': 'None',
            },
        },
    ],
    'ProcessingOutputConfig': {
        'Outputs': [
            {
                'OutputName': 'output',
                'S3Output': {
                    'S3Uri': f's3://{S3_BUCKET}/{INPUT_DATA_S3_KEY}',
                    'LocalPath': PROCESSING_LOCAL_OUTPUT_PATH,
                    'S3UploadMode': 'EndOfJob',
                },
                'AppManaged': False,
            }
        ]
    },
    'ProcessingResources': {
        'ClusterConfig': RESOURCE_CONFIG,
    },
    'StoppingCondition': TASK_TIMEOUT,
    'AppSpecification': {
        'ImageUri': ECR_REPOSITORY,
    },
}

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
        'sample_size': str(SAMPLE_SIZE),
    },
    'InputDataConfig': [
        {
            'ChannelName': 'train',
            **TRAINING_DATA_SOURCE,  # type: ignore [arg-type]
        }
    ],
    'OutputDataConfig': {'S3OutputPath': f's3://{S3_BUCKET}/{TRAINING_OUTPUT_S3_KEY}/'},
    'ResourceConfig': RESOURCE_CONFIG,
    'StoppingCondition': TASK_TIMEOUT,
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

TRANSFORM_CONFIG = {
    'TransformJobName': TRANSFORM_JOB_NAME,
    'ModelName': MODEL_NAME,
    'TransformInput': {
        'DataSource': {
            'S3DataSource': {
                'S3DataType': 'S3Prefix',
                'S3Uri': f's3://{S3_BUCKET}/{INPUT_DATA_S3_KEY}/test.csv',
            }
        },
        'SplitType': 'Line',
        'ContentType': 'text/csv',
    },
    'TransformOutput': {'S3OutputPath': f's3://{S3_BUCKET}/{PREDICTION_OUTPUT_S3_KEY}'},
    'TransformResources': {
        'InstanceCount': 1,
        'InstanceType': 'ml.m5.large',
    },
}

TUNING_CONFIG = {
    'HyperParameterTuningJobName': TUNING_JOB_NAME,
    'HyperParameterTuningJobConfig': {
        'Strategy': 'Bayesian',
        'HyperParameterTuningJobObjective': {
            'MetricName': 'test:accuracy',
            'Type': 'Maximize',
        },
        'ResourceLimits': {
            # You would bump these up in production as appropriate.
            'MaxNumberOfTrainingJobs': 1,
            'MaxParallelTrainingJobs': 1,
        },
        'ParameterRanges': {
            'CategoricalParameterRanges': [],
            'IntegerParameterRanges': [
                # Set the min and max values of the hyperparameters you want to tune.
                {
                    'Name': 'k',
                    'MinValue': '1',
                    "MaxValue": str(SAMPLE_SIZE),
                },
                {
                    'Name': 'sample_size',
                    'MinValue': '1',
                    'MaxValue': str(SAMPLE_SIZE),
                },
            ],
        },
    },
    'TrainingJobDefinition': {
        'StaticHyperParameters': {
            'predictor_type': 'classifier',
            'feature_dim': '4',
        },
        'AlgorithmSpecification': {'TrainingImage': KNN_IMAGE_URI, 'TrainingInputMode': 'File'},
        'InputDataConfig': [
            {
                'ChannelName': 'train',
                **TRAINING_DATA_SOURCE,  # type: ignore [arg-type]
            },
            {
                'ChannelName': 'test',
                **TRAINING_DATA_SOURCE,  # type: ignore [arg-type]
            },
        ],
        'OutputDataConfig': {'S3OutputPath': f's3://{S3_BUCKET}/{TRAINING_OUTPUT_S3_KEY}'},
        'ResourceConfig': RESOURCE_CONFIG,
        'StoppingCondition': TASK_TIMEOUT,
        'RoleArn': ROLE_ARN,
    },
}


# This script will be the entrypoint for the docker image which will handle preprocessing the raw data
# NOTE:  The following string must remain dedented as it is being written to a file.
PREPROCESS_SCRIPT = (
    """
import boto3
import numpy as np
import pandas as pd

def main():
    # Load the Iris dataset from {input_path}/input.csv, split it into train/test
    # subsets, and write them to {output_path}/ for the Processing Operator.

    columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
    iris = pd.read_csv('{input_path}/input.csv', names=columns)

    # Process data
    iris['species'] = iris['species'].replace({{'Iris-virginica': 0, 'Iris-versicolor': 1, 'Iris-setosa': 2}})
    iris = iris[['species', 'sepal_length', 'sepal_width', 'petal_length', 'petal_width']]

    # Split into test and train data
    iris_train, iris_test = np.split(
        iris.sample(frac=1, random_state=np.random.RandomState()), [int(0.7 * len(iris))]
    )

    # Remove the "answers" from the test set
    iris_test.drop(['species'], axis=1, inplace=True)

    # Write the splits to disk
    iris_train.to_csv('{output_path}/train.csv', index=False, header=False)
    iris_test.to_csv('{output_path}/test.csv', index=False, header=False)

    print('Preprocessing Done.')

if __name__ == "__main__":
    main()

    """
).format(input_path=PROCESSING_LOCAL_INPUT_PATH, output_path=PROCESSING_LOCAL_OUTPUT_PATH)


@task
def upload_dataset_to_s3():
    """Uploads the provided dataset to a designated Amazon S3 bucket."""
    S3Hook().load_string(
        string_data=DATASET,
        bucket_name=S3_BUCKET,
        key=RAW_DATA_S3_KEY,
        replace=True,
    )


@task
def build_and_upload_docker_image():
    """
    We need a Docker image with the following requirements:
      - Has numpy, pandas, requests, and boto3 installed
      - Has our data preprocessing script mounted and set as the entry point
    """

    # Fetch and parse ECR Token to be used for the docker push
    ecr_client = boto3.client('ecr', region_name=REGION)
    token = ecr_client.get_authorization_token()
    credentials = (base64.b64decode(token['authorizationData'][0]['authorizationToken'])).decode('utf-8')
    username, password = credentials.split(':')

    with NamedTemporaryFile(mode='w+t') as preprocessing_script, NamedTemporaryFile(mode='w+t') as dockerfile:

        preprocessing_script.write(PREPROCESS_SCRIPT)
        preprocessing_script.flush()

        dockerfile.write(
            f"""
            FROM amazonlinux
            COPY {preprocessing_script.name.split('/')[2]} /preprocessing.py
            ADD credentials /credentials
            ENV AWS_SHARED_CREDENTIALS_FILE=/credentials
            RUN yum install python3 pip -y
            RUN pip3 install boto3 pandas requests
            CMD [ "python3", "/preprocessing.py"]
            """
        )
        dockerfile.flush()

        docker_build_and_push_commands = f"""
            cp /root/.aws/credentials /tmp/credentials &&
            docker build -f {dockerfile.name} -t {ECR_REPOSITORY} /tmp &&
            rm /tmp/credentials &&
            aws ecr get-login-password --region {REGION} |
            docker login --username {username} --password {password} {ECR_REPOSITORY} &&
            docker push {ECR_REPOSITORY}
            """
        docker_build = subprocess.Popen(
            docker_build_and_push_commands,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, err = docker_build.communicate()

        if docker_build.returncode != 0:
            raise RuntimeError(err)


@task(trigger_rule='all_done')
def cleanup():
    # Delete S3 Artifacts
    client = boto3.client('s3')
    object_keys = [
        key['Key'] for key in client.list_objects_v2(Bucket=S3_BUCKET, Prefix=PROJECT_NAME)['Contents']
    ]
    for key in object_keys:
        client.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': [{'Key': key}]})


with DAG(
    dag_id='example_sagemaker',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_sagemaker_processing]
    preprocess_raw_data = SageMakerProcessingOperator(
        task_id='preprocess_raw_data',
        config=SAGEMAKER_PROCESSING_JOB_CONFIG,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_processing]

    # [START howto_operator_sagemaker_training]
    train_model = SageMakerTrainingOperator(
        task_id='train_model',
        config=TRAINING_CONFIG,
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_training]

    # [START howto_sensor_sagemaker_training]
    await_training = SageMakerTrainingSensor(
        task_id='await_training',
        job_name=TRAINING_JOB_NAME,
    )
    # [END howto_sensor_sagemaker_training]

    # [START howto_operator_sagemaker_model]
    create_model = SageMakerModelOperator(
        task_id='create_model',
        config=MODEL_CONFIG,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_model]

    # [START howto_operator_sagemaker_tuning]
    tune_model = SageMakerTuningOperator(
        task_id='tune_model',
        config=TUNING_CONFIG,
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_tuning]

    # [START howto_sensor_sagemaker_tuning]
    await_tune = SageMakerTuningSensor(
        task_id='await_tuning',
        job_name=TUNING_JOB_NAME,
    )
    # [END howto_sensor_sagemaker_tuning]

    # [START howto_operator_sagemaker_transform]
    test_model = SageMakerTransformOperator(
        task_id='test_model',
        config=TRANSFORM_CONFIG,
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_transform]

    # [START howto_sensor_sagemaker_transform]
    await_transform = SageMakerTransformSensor(
        task_id='await_transform',
        job_name=TRANSFORM_JOB_NAME,
    )
    # [END howto_sensor_sagemaker_transform]

    # Trigger rule set to "all_done" so clean up will run regardless of success on other tasks.
    # [START howto_operator_sagemaker_delete_model]
    delete_model = SageMakerDeleteModelOperator(
        task_id='delete_model',
        config={'ModelName': MODEL_NAME},
        trigger_rule='all_done',
    )
    # [END howto_operator_sagemaker_delete_model]

    (
        upload_dataset_to_s3()
        >> build_and_upload_docker_image()
        >> preprocess_raw_data
        >> train_model
        >> await_training
        >> create_model
        >> tune_model
        >> await_tune
        >> test_model
        >> await_transform
        >> cleanup()
        >> delete_model
    )
