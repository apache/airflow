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
from __future__ import annotations

import json
from datetime import datetime

import boto3

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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, prune_logs

DAG_ID = "example_sagemaker_endpoint"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

# The URI of a Docker image for handling KNN model training.
# To find the URI of a free Amazon-provided image that can be used, substitute your
# desired region in the following link and find the URI under "Registry Path".
# https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-east-1.html#knn-us-east-1.title
# This URI should be in the format of {12-digits}.dkr.ecr.{region}.amazonaws.com/knn
KNN_IMAGES_BY_REGION = {
    "us-east-1": "382416733822.dkr.ecr.us-east-1.amazonaws.com/knn:1",
    "us-west-2": "174872318107.dkr.ecr.us-west-2.amazonaws.com/knn:1",
}

# For an example of how to obtain the following train and test data, please see
# https://github.com/apache/airflow/blob/main/providers/amazon/tests/system/amazon/aws/example_sagemaker.py
TRAIN_DATA = "0,4.9,2.5,4.5,1.7\n1,7.0,3.2,4.7,1.4\n0,7.3,2.9,6.3,1.8\n2,5.1,3.5,1.4,0.2\n"
SAMPLE_TEST_DATA = "6.4,3.2,4.5,1.5"


@task
def call_endpoint(endpoint_name):
    response = (
        boto3.Session()
        .client("sagemaker-runtime")
        .invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType="text/csv",
            Body=SAMPLE_TEST_DATA,
        )
    )

    return json.loads(response["Body"].read().decode())["predictions"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_endpoint_config(endpoint_config_job_name):
    boto3.client("sagemaker").delete_endpoint_config(EndpointConfigName=endpoint_config_job_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_endpoint(endpoint_name):
    boto3.client("sagemaker").delete_endpoint(EndpointName=endpoint_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def archive_logs(log_group_name):
    boto3.client("logs").put_retention_policy(logGroupName=log_group_name, retentionInDays=1)


@task
def set_up(env_id, role_arn, ti=None):
    bucket_name = f"{env_id}-sagemaker"
    input_data_s3_key = f"{env_id}/input-data"
    training_output_s3_key = f"{env_id}/results"

    endpoint_config_job_name = f"{env_id}-endpoint-config"
    endpoint_name = f"{env_id}-endpoint"
    model_name = f"{env_id}-KNN-model"
    training_job_name = f"{env_id}-train"

    region = boto3.session.Session().region_name
    try:
        knn_image_uri = KNN_IMAGES_BY_REGION[region]
    except KeyError:
        raise KeyError(
            f"Region name {region} does not have a known KNN "
            f"Image URI.  Please add the region and URI following "
            f"the directions at the top of the system testfile "
        )

    training_config = {
        "TrainingJobName": training_job_name,
        "RoleArn": role_arn,
        "AlgorithmSpecification": {
            "TrainingImage": knn_image_uri,
            "TrainingInputMode": "File",
        },
        "HyperParameters": {
            "predictor_type": "classifier",
            "feature_dim": "4",
            "k": "3",
            "sample_size": str(TRAIN_DATA.count("\n") - 1),
        },
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "CompressionType": "None",
                "ContentType": "text/csv",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataDistributionType": "FullyReplicated",
                        "S3DataType": "S3Prefix",
                        "S3Uri": f"s3://{bucket_name}/{input_data_s3_key}/train.csv",
                    }
                },
            }
        ],
        "OutputDataConfig": {"S3OutputPath": f"s3://{bucket_name}/{training_output_s3_key}/"},
        "ResourceConfig": {
            "InstanceCount": 1,
            "InstanceType": "ml.m5.large",
            "VolumeSizeInGB": 1,
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 6 * 60},
    }

    model_config = {
        "ModelName": model_name,
        "ExecutionRoleArn": role_arn,
        "PrimaryContainer": {
            "Mode": "SingleModel",
            "Image": knn_image_uri,
            "ModelDataUrl": f"s3://{bucket_name}/{training_output_s3_key}/{training_job_name}/output/model.tar.gz",
        },
    }

    endpoint_config_config = {
        "EndpointConfigName": endpoint_config_job_name,
        "ProductionVariants": [
            {
                "VariantName": f"{env_id}-demo",
                "ModelName": model_name,
                "InstanceType": "ml.m5.large",
                "InitialInstanceCount": 1,
            },
        ],
    }

    deploy_endpoint_config = {
        "EndpointName": endpoint_name,
        "EndpointConfigName": endpoint_config_job_name,
    }

    ti.xcom_push(key="bucket_name", value=bucket_name)
    ti.xcom_push(key="input_data_s3_key", value=input_data_s3_key)
    ti.xcom_push(key="model_name", value=model_name)
    ti.xcom_push(key="endpoint_name", value=endpoint_name)
    ti.xcom_push(key="endpoint_config_job_name", value=endpoint_config_job_name)
    ti.xcom_push(key="training_config", value=training_config)
    ti.xcom_push(key="model_config", value=model_config)
    ti.xcom_push(key="endpoint_config_config", value=endpoint_config_config)
    ti.xcom_push(key="deploy_endpoint_config", value=deploy_endpoint_config)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    test_setup = set_up(
        env_id=test_context[ENV_ID_KEY],
        role_arn=test_context[ROLE_ARN_KEY],
    )

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=test_setup["bucket_name"],
    )

    upload_data = S3CreateObjectOperator(
        task_id="upload_data",
        s3_bucket=test_setup["bucket_name"],
        s3_key=f"{test_setup['input_data_s3_key']}/train.csv",
        data=TRAIN_DATA,
    )

    train_model = SageMakerTrainingOperator(
        task_id="train_model",
        config=test_setup["training_config"],
    )

    create_model = SageMakerModelOperator(
        task_id="create_model",
        config=test_setup["model_config"],
    )

    # [START howto_operator_sagemaker_endpoint_config]
    configure_endpoint = SageMakerEndpointConfigOperator(
        task_id="configure_endpoint",
        config=test_setup["endpoint_config_config"],
    )
    # [END howto_operator_sagemaker_endpoint_config]

    # [START howto_operator_sagemaker_endpoint]
    deploy_endpoint = SageMakerEndpointOperator(
        task_id="deploy_endpoint",
        config=test_setup["deploy_endpoint_config"],
    )
    # [END howto_operator_sagemaker_endpoint]

    # SageMakerEndpointOperator waits by default, setting as False to test the Sensor below.
    deploy_endpoint.wait_for_completion = False

    # [START howto_sensor_sagemaker_endpoint]
    await_endpoint = SageMakerEndpointSensor(
        task_id="await_endpoint",
        endpoint_name=test_setup["endpoint_name"],
    )
    # [END howto_sensor_sagemaker_endpoint]

    delete_model = SageMakerDeleteModelOperator(
        task_id="delete_model",
        trigger_rule=TriggerRule.ALL_DONE,
        config={"ModelName": test_setup["model_name"]},
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=test_setup["bucket_name"],
        force_delete=True,
    )

    log_cleanup = prune_logs(
        [
            # Format: ('log group name', 'log stream prefix')
            ("/aws/sagemaker/TrainingJobs", test_context[ENV_ID_KEY]),
        ]
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
        call_endpoint(test_setup["endpoint_name"]),
        # TEST TEARDOWN
        delete_endpoint_config(test_setup["endpoint_config_job_name"]),
        delete_endpoint(test_setup["endpoint_name"]),
        delete_model,
        delete_bucket,
        log_cleanup,
        archive_logs(f"/aws/sagemaker/Endpoints/{test_setup['endpoint_name']}"),
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
