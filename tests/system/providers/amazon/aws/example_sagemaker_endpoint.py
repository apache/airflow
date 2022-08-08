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
from datetime import datetime

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
    SageMakerEndpointConfigOperator,
    SageMakerEndpointOperator,
    SageMakerModelOperator,
    SageMakerTrainingOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerEndpointSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, purge_logs

DAG_ID = 'example_sagemaker_endpoint'

# Externally fetched variables:
ROLE_ARN_KEY = 'ROLE_ARN'
# The URI of a Docker image for handling KNN model training.
# To find the URI of a free Amazon-provided image that can be used, substitute your
# desired region in the following link and find the URI under "Registry Path".
# https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-east-1.html#knn-us-east-1.title
# This URI should be in the format of {12-digits}.dkr.ecr.{region}.amazonaws.com/knn
KNN_IMAGE_URI_KEY = 'KNN_IMAGE_URI'

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(KNN_IMAGE_URI_KEY).add_variable(ROLE_ARN_KEY).build()
)

# For an example of how to obtain the following train and test data, please see
# https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/example_dags/example_sagemaker.py
TRAIN_DATA = '0,4.9,2.5,4.5,1.7\n1,7.0,3.2,4.7,1.4\n0,7.3,2.9,6.3,1.8\n2,5.1,3.5,1.4,0.2\n'
SAMPLE_TEST_DATA = '6.4,3.2,4.5,1.5'


@task
def call_endpoint(endpoint_name):
    response = (
        boto3.Session()
        .client('sagemaker-runtime')
        .invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType='text/csv',
            Body=SAMPLE_TEST_DATA,
        )
    )

    return json.loads(response["Body"].read().decode())['predictions']


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_endpoint_config(endpoint_config_job_name):
    boto3.client('sagemaker').delete_endpoint_config(EndpointConfigName=endpoint_config_job_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_endpoint(endpoint_name):
    boto3.client('sagemaker').delete_endpoint(EndpointName=endpoint_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_logs(env_id, endpoint_name):
    generated_logs = [
        # Format: ('log group name', 'log stream prefix')
        ('/aws/sagemaker/TrainingJobs', env_id),
        (f'/aws/sagemaker/Endpoints/{endpoint_name}', env_id),
    ]

    purge_logs(generated_logs)


@task
def set_up(env_id, knn_image_uri, role_arn, ti=None):
    bucket_name = f'{env_id}-sagemaker'
    input_data_s3_key = f'{env_id}/input-data'
    training_output_s3_key = f'{env_id}/results'

    endpoint_config_job_name = f'{env_id}-endpoint-config'
    endpoint_name = f'{env_id}-endpoint'
    model_name = f'{env_id}-KNN-model'
    training_job_name = f'{env_id}-train'

    training_config = {
        'TrainingJobName': training_job_name,
        'RoleArn': role_arn,
        'AlgorithmSpecification': {
            "TrainingImage": knn_image_uri,
            "TrainingInputMode": "File",
        },
        'HyperParameters': {
            'predictor_type': 'classifier',
            'feature_dim': '4',
            'k': '3',
            'sample_size': str(TRAIN_DATA.count('\n') - 1),
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
                        'S3Uri': f's3://{bucket_name}/{input_data_s3_key}/train.csv',
                    }
                },
            }
        ],
        'OutputDataConfig': {'S3OutputPath': f's3://{bucket_name}/{training_output_s3_key}/'},
        'ResourceConfig': {
            'InstanceCount': 1,
            'InstanceType': 'ml.m5.large',
            'VolumeSizeInGB': 1,
        },
        'StoppingCondition': {'MaxRuntimeInSeconds': 6 * 60},
    }

    model_config = {
        'ModelName': model_name,
        'ExecutionRoleArn': role_arn,
        'PrimaryContainer': {
            'Mode': 'SingleModel',
            'Image': knn_image_uri,
            'ModelDataUrl': f's3://{bucket_name}/{training_output_s3_key}/{training_job_name}/output/model.tar.gz',  # noqa: E501
        },
    }

    endpoint_config_config = {
        'EndpointConfigName': endpoint_config_job_name,
        'ProductionVariants': [
            {
                'VariantName': f'{env_id}-demo',
                'ModelName': model_name,
                'InstanceType': 'ml.t2.medium',
                'InitialInstanceCount': 1,
            },
        ],
    }

    deploy_endpoint_config = {
        'EndpointName': endpoint_name,
        'EndpointConfigName': endpoint_config_job_name,
    }

    ti.xcom_push(key='bucket_name', value=bucket_name)
    ti.xcom_push(key='input_data_s3_key', value=input_data_s3_key)
    ti.xcom_push(key='model_name', value=model_name)
    ti.xcom_push(key='endpoint_name', value=endpoint_name)
    ti.xcom_push(key='endpoint_config_job_name', value=endpoint_config_job_name)
    ti.xcom_push(key='training_config', value=training_config)
    ti.xcom_push(key='model_config', value=model_config)
    ti.xcom_push(key='endpoint_config_config', value=endpoint_config_config)
    ti.xcom_push(key='deploy_endpoint_config', value=deploy_endpoint_config)


with DAG(
    dag_id=DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    test_setup = set_up(
        env_id=test_context[ENV_ID_KEY],
        knn_image_uri=test_context[KNN_IMAGE_URI_KEY],
        role_arn=test_context[ROLE_ARN_KEY],
    )

    create_bucket = S3CreateBucketOperator(
        task_id='create_bucket',
        bucket_name=test_setup['bucket_name'],
    )

    upload_data = S3CreateObjectOperator(
        task_id='upload_data',
        s3_bucket=test_setup['bucket_name'],
        s3_key=f'{test_setup["input_data_s3_key"]}/train.csv',
        data=TRAIN_DATA,
    )

    train_model = SageMakerTrainingOperator(
        task_id='train_model',
        config=test_setup['training_config'],
        do_xcom_push=False,
    )

    create_model = SageMakerModelOperator(
        task_id='create_model',
        config=test_setup['model_config'],
        do_xcom_push=False,
    )

    # [START howto_operator_sagemaker_endpoint_config]
    configure_endpoint = SageMakerEndpointConfigOperator(
        task_id='configure_endpoint',
        config=test_setup['endpoint_config_config'],
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_endpoint_config]

    # [START howto_operator_sagemaker_endpoint]
    deploy_endpoint = SageMakerEndpointOperator(
        task_id='deploy_endpoint',
        config=test_setup['deploy_endpoint_config'],
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
        do_xcom_push=False,
    )
    # [END howto_operator_sagemaker_endpoint]

    # [START howto_sensor_sagemaker_endpoint]
    await_endpoint = SageMakerEndpointSensor(
        task_id='await_endpoint',
        endpoint_name=test_setup['endpoint_name'],
    )
    # [END howto_sensor_sagemaker_endpoint]

    delete_model = SageMakerDeleteModelOperator(
        task_id='delete_model',
        trigger_rule=TriggerRule.ALL_DONE,
        config={'ModelName': test_setup['model_name']},
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id='delete_bucket',
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=test_setup['bucket_name'],
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        test_setup,
        create_bucket,
        upload_data,
        # TEST BODY
        train_model,
        create_model,
        configure_endpoint,
        deploy_endpoint,
        await_endpoint,
        call_endpoint(test_setup['endpoint_name']),
        # TEST TEARDOWN
        delete_endpoint_config(test_setup['endpoint_config_job_name']),
        delete_endpoint(test_setup['endpoint_name']),
        delete_model,
        delete_bucket,
        delete_logs(test_context[ENV_ID_KEY], test_setup['endpoint_name']),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
