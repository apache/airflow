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

import base64
import json
import logging
import subprocess
from datetime import datetime
from tempfile import NamedTemporaryFile

import boto3

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
    SageMakerModelOperator,
    SageMakerProcessingOperator,
    SageMakerRegisterModelVersionOperator,
    SageMakerStartPipelineOperator,
    SageMakerStopPipelineOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
    SageMakerTuningOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker import (
    SageMakerPipelineSensor,
    SageMakerTrainingSensor,
    SageMakerTransformSensor,
    SageMakerTuningSensor,
)
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder, purge_logs

DAG_ID = "example_sagemaker"

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
SAMPLE_SIZE = DATASET.count("\n") - 1

# This script will be the entrypoint for the docker image which will handle preprocessing the raw data
# NOTE:  The following string must remain dedented as it is being written to a file.
PREPROCESS_SCRIPT_TEMPLATE = """
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


def _create_ecr_repository(repo_name):
    execution_role_arn = boto3.client("sts").get_caller_identity()["Arn"]
    access_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Allow access to the system test execution role",
                "Effect": "Allow",
                "Principal": {"AWS": execution_role_arn},
                "Action": "ecr:*",
            }
        ],
    }

    client = boto3.client("ecr")
    repo = client.create_repository(repositoryName=repo_name)["repository"]
    client.set_repository_policy(repositoryName=repo["repositoryName"], policyText=json.dumps(access_policy))

    return repo["repositoryUri"]


def _build_and_upload_docker_image(preprocess_script, repository_uri):
    """
    We need a Docker image with the following requirements:
      - Has numpy, pandas, requests, and boto3 installed
      - Has our data preprocessing script mounted and set as the entry point
    """
    ecr_region = repository_uri.split(".")[3]

    # Fetch and parse ECR Token to be used for the docker push
    token = boto3.client("ecr", region_name=ecr_region).get_authorization_token()
    credentials = (base64.b64decode(token["authorizationData"][0]["authorizationToken"])).decode("utf-8")
    username, password = credentials.split(":")

    with NamedTemporaryFile(mode="w+t") as preprocessing_script, NamedTemporaryFile(mode="w+t") as dockerfile:
        preprocessing_script.write(preprocess_script)
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
            docker build --platform=linux/amd64 -f {dockerfile.name} -t {repository_uri} /tmp &&
            rm /tmp/credentials &&
            aws ecr get-login-password --region {ecr_region} |
            docker login --username {username} --password {password} {repository_uri} &&
            docker push {repository_uri}
            """
        docker_build = subprocess.Popen(
            docker_build_and_push_commands,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, stderr = docker_build.communicate()
        if docker_build.returncode != 0:
            raise RuntimeError(
                "Failed to push docker image to the repository.  The following error "
                f"message may be useful, but can occasionally be misleading: {stderr}"
            )


@task
def set_up(env_id, role_arn):
    bucket_name = f"{env_id}-sagemaker-example"
    ecr_repository_name = f"{env_id}-repo"
    model_name = f"{env_id}-KNN-model"
    processing_job_name = f"{env_id}-processing"
    training_job_name = f"{env_id}-train"
    transform_job_name = f"{env_id}-transform"
    tuning_job_name = f"{env_id}-tune"
    model_package_group_name = f"{env_id}-group"
    pipeline_name = f"{env_id}-pipe"

    input_data_S3_key = f"{env_id}/processed-input-data"
    prediction_output_s3_key = f"{env_id}/transform"
    processing_local_input_path = "/opt/ml/processing/input"
    processing_local_output_path = "/opt/ml/processing/output"
    raw_data_s3_key = f"{env_id}/preprocessing/input.csv"
    training_output_s3_key = f"{env_id}/results"

    ecr_repository_uri = _create_ecr_repository(ecr_repository_name)
    region = boto3.session.Session().region_name
    try:
        knn_image_uri = KNN_IMAGES_BY_REGION[region]
    except KeyError:
        raise KeyError(
            f"Region name {region} does not have a known KNN "
            f"Image URI.  Please add the region and URI following "
            f"the directions at the top of the system testfile "
        )

    # Json definition for a dummy pipeline of 30 chained "conditional step" checking that 3 < 6
    # Each step takes roughly 1 second to execute, so the pipeline runtimes is ~30 seconds, which should be
    # enough to test stopping and awaiting without race conditions.
    # Built using sagemaker sdk, and using json.loads(pipeline.definition())
    pipeline_json_definition = """{"Version": "2020-12-01", "Metadata": {}, "Parameters": [], "PipelineExperimentConfig": {"ExperimentName": {"Get": "Execution.PipelineName"}, "TrialName": {"Get": "Execution.PipelineExecutionId"}}, "Steps": [{"Name": "DummyCond29", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond28", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond27", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond26", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond25", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond24", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond23", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond22", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond21", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond20", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond19", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond18", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond17", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond16", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond15", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond14", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond13", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond12", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond11", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond10", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond9", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond8", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond7", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond6", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond5", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond4", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond3", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond2", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond1", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond0", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [{"Name": "DummyCond", "Type": "Condition", "Arguments": {"Conditions": [{"Type": "LessThanOrEqualTo", "LeftValue": 3.0, "RightValue": 6.0}], "IfSteps": [], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}], "ElseSteps": []}}]}"""  # noqa: E501
    sgmk_client = boto3.client("sagemaker")
    sgmk_client.create_pipeline(
        PipelineName=pipeline_name, PipelineDefinition=pipeline_json_definition, RoleArn=role_arn
    )

    resource_config = {
        "InstanceCount": 1,
        "InstanceType": "ml.m5.large",
        "VolumeSizeInGB": 1,
    }
    processing_config = {
        "ProcessingJobName": processing_job_name,
        "ProcessingInputs": [
            {
                "InputName": "input",
                "AppManaged": False,
                "S3Input": {
                    "S3Uri": f"s3://{bucket_name}/{raw_data_s3_key}",
                    "LocalPath": processing_local_input_path,
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
                        "S3Uri": f"s3://{bucket_name}/{input_data_S3_key}",
                        "LocalPath": processing_local_output_path,
                        "S3UploadMode": "EndOfJob",
                    },
                    "AppManaged": False,
                }
            ]
        },
        "ProcessingResources": {
            "ClusterConfig": resource_config,
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 60},
        "AppSpecification": {
            "ImageUri": ecr_repository_uri,
        },
        "RoleArn": role_arn,
    }

    training_data_source = {
        "CompressionType": "None",
        "ContentType": "text/csv",
        "DataSource": {
            "S3DataSource": {
                "S3DataDistributionType": "FullyReplicated",
                "S3DataType": "S3Prefix",
                "S3Uri": f"s3://{bucket_name}/{input_data_S3_key}/train.csv",
            }
        },
    }
    training_config = {
        "AlgorithmSpecification": {
            "TrainingImage": knn_image_uri,
            "TrainingInputMode": "File",
        },
        "HyperParameters": {
            "predictor_type": "classifier",
            "feature_dim": "4",
            "k": "3",
            "sample_size": str(SAMPLE_SIZE),
        },
        "InputDataConfig": [
            {
                "ChannelName": "train",
                **training_data_source,
            }
        ],
        "OutputDataConfig": {"S3OutputPath": f"s3://{bucket_name}/{training_output_s3_key}/"},
        "ResourceConfig": resource_config,
        "RoleArn": role_arn,
        "StoppingCondition": {"MaxRuntimeInSeconds": 60},
        "TrainingJobName": training_job_name,
    }
    model_trained_weights = (
        f"s3://{bucket_name}/{training_output_s3_key}/{training_job_name}/output/model.tar.gz"
    )
    model_config = {
        "ExecutionRoleArn": role_arn,
        "ModelName": model_name,
        "PrimaryContainer": {
            "Mode": "SingleModel",
            "Image": knn_image_uri,
            "ModelDataUrl": model_trained_weights,
        },
    }
    tuning_config = {
        "HyperParameterTuningJobName": tuning_job_name,
        "HyperParameterTuningJobConfig": {
            "Strategy": "Bayesian",
            "HyperParameterTuningJobObjective": {
                "MetricName": "test:accuracy",
                "Type": "Maximize",
            },
            "ResourceLimits": {
                "MaxNumberOfTrainingJobs": 10,
                "MaxParallelTrainingJobs": 10,
            },
            "ParameterRanges": {
                "CategoricalParameterRanges": [],
                "IntegerParameterRanges": [
                    # Set the min and max values of the hyperparameters you want to tune.
                    {
                        "Name": "k",
                        "MinValue": "1",
                        "MaxValue": str(SAMPLE_SIZE),
                    },
                    {
                        "Name": "sample_size",
                        "MinValue": "1",
                        "MaxValue": str(SAMPLE_SIZE),
                    },
                ],
            },
        },
        "TrainingJobDefinition": {
            "StaticHyperParameters": {
                "predictor_type": "classifier",
                "feature_dim": "4",
            },
            "AlgorithmSpecification": {"TrainingImage": knn_image_uri, "TrainingInputMode": "File"},
            "InputDataConfig": [
                {
                    "ChannelName": "train",
                    **training_data_source,
                },
                {
                    "ChannelName": "test",
                    **training_data_source,
                },
            ],
            "OutputDataConfig": {"S3OutputPath": f"s3://{bucket_name}/{training_output_s3_key}"},
            "ResourceConfig": resource_config,
            "RoleArn": role_arn,
            "StoppingCondition": {"MaxRuntimeInSeconds": 60},
        },
    }
    transform_config = {
        "TransformJobName": transform_job_name,
        "TransformInput": {
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"s3://{bucket_name}/{input_data_S3_key}/test.csv",
                }
            },
            "SplitType": "Line",
            "ContentType": "text/csv",
        },
        "TransformOutput": {"S3OutputPath": f"s3://{bucket_name}/{prediction_output_s3_key}"},
        "TransformResources": {
            "InstanceCount": 1,
            "InstanceType": "ml.m5.large",
        },
        "ModelName": model_name,
    }

    preprocess_script = PREPROCESS_SCRIPT_TEMPLATE.format(
        input_path=processing_local_input_path, output_path=processing_local_output_path
    )
    _build_and_upload_docker_image(preprocess_script, ecr_repository_uri)

    ti = get_current_context()["ti"]
    ti.xcom_push(key="docker_image", value=ecr_repository_uri)
    ti.xcom_push(key="bucket_name", value=bucket_name)
    ti.xcom_push(key="raw_data_s3_key", value=raw_data_s3_key)
    ti.xcom_push(key="ecr_repository_name", value=ecr_repository_name)
    ti.xcom_push(key="processing_config", value=processing_config)
    ti.xcom_push(key="training_config", value=training_config)
    ti.xcom_push(key="training_job_name", value=training_job_name)
    ti.xcom_push(key="model_package_group_name", value=model_package_group_name)
    ti.xcom_push(key="pipeline_name", value=pipeline_name)
    ti.xcom_push(key="model_config", value=model_config)
    ti.xcom_push(key="model_name", value=model_name)
    ti.xcom_push(key="inference_code_image", value=knn_image_uri)
    ti.xcom_push(key="model_trained_weights", value=model_trained_weights)
    ti.xcom_push(key="tuning_config", value=tuning_config)
    ti.xcom_push(key="tuning_job_name", value=tuning_job_name)
    ti.xcom_push(key="transform_config", value=transform_config)
    ti.xcom_push(key="transform_job_name", value=transform_job_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_ecr_repository(repository_name):
    client = boto3.client("ecr")

    # All images must be removed from the repo before it can be deleted.
    image_ids = client.list_images(repositoryName=repository_name)["imageIds"]
    client.batch_delete_image(
        repositoryName=repository_name,
        imageIds=[{"imageDigest": image["imageDigest"] for image in image_ids}],
    )
    client.delete_repository(repositoryName=repository_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_logs(env_id):
    generated_logs = [
        # Format: ('log group name', 'log stream prefix')
        ("/aws/sagemaker/ProcessingJobs", env_id),
        ("/aws/sagemaker/TrainingJobs", env_id),
        ("/aws/sagemaker/TransformJobs", env_id),
    ]
    purge_logs(generated_logs)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_model_group(group_name, model_version_arn):
    sgmk_client = boto3.client("sagemaker")
    # need to destroy model registered in group first
    sgmk_client.delete_model_package(ModelPackageName=model_version_arn)
    sgmk_client.delete_model_package_group(ModelPackageGroupName=group_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_pipeline(pipeline_name):
    sgmk_client = boto3.client("sagemaker")
    sgmk_client.delete_pipeline(PipelineName=pipeline_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_docker_image(image_name):
    docker_build = subprocess.Popen(
        f"docker rmi {image_name}",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _, stderr = docker_build.communicate()
    if docker_build.returncode != 0:
        logging.error(
            "Failed to delete local docker image. "
            "Run 'docker images' to see if you need to clean it yourself.\n"
            f"error message: {stderr}"
        )


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

    upload_dataset = S3CreateObjectOperator(
        task_id="upload_dataset",
        s3_bucket=test_setup["bucket_name"],
        s3_key=test_setup["raw_data_s3_key"],
        data=DATASET,
        replace=True,
    )

    # [START howto_operator_sagemaker_start_pipeline]
    start_pipeline1 = SageMakerStartPipelineOperator(
        task_id="start_pipeline1",
        pipeline_name=test_setup["pipeline_name"],
    )
    # [END howto_operator_sagemaker_start_pipeline]

    # [START howto_operator_sagemaker_stop_pipeline]
    stop_pipeline1 = SageMakerStopPipelineOperator(
        task_id="stop_pipeline1",
        pipeline_exec_arn=start_pipeline1.output,
    )
    # [END howto_operator_sagemaker_stop_pipeline]

    start_pipeline2 = SageMakerStartPipelineOperator(
        task_id="start_pipeline2",
        pipeline_name=test_setup["pipeline_name"],
    )

    # [START howto_sensor_sagemaker_pipeline]
    await_pipeline2 = SageMakerPipelineSensor(
        task_id="await_pipeline2",
        pipeline_exec_arn=start_pipeline2.output,
    )
    # [END howto_sensor_sagemaker_pipeline]

    # [START howto_operator_sagemaker_processing]
    preprocess_raw_data = SageMakerProcessingOperator(
        task_id="preprocess_raw_data",
        config=test_setup["processing_config"],
    )
    # [END howto_operator_sagemaker_processing]

    # [START howto_operator_sagemaker_training]
    train_model = SageMakerTrainingOperator(
        task_id="train_model",
        config=test_setup["training_config"],
    )
    # [END howto_operator_sagemaker_training]

    # SageMakerTrainingOperator waits by default, setting as False to test the Sensor below.
    train_model.wait_for_completion = False

    # [START howto_sensor_sagemaker_training]
    await_training = SageMakerTrainingSensor(
        task_id="await_training",
        job_name=test_setup["training_job_name"],
    )
    # [END howto_sensor_sagemaker_training]

    # [START howto_operator_sagemaker_model]
    create_model = SageMakerModelOperator(
        task_id="create_model",
        config=test_setup["model_config"],
    )
    # [END howto_operator_sagemaker_model]

    # [START howto_operator_sagemaker_register]
    register_model = SageMakerRegisterModelVersionOperator(
        task_id="register_model",
        image_uri=test_setup["inference_code_image"],
        model_url=test_setup["model_trained_weights"],
        package_group_name=test_setup["model_package_group_name"],
    )
    # [END howto_operator_sagemaker_register]

    # [START howto_operator_sagemaker_tuning]
    tune_model = SageMakerTuningOperator(
        task_id="tune_model",
        config=test_setup["tuning_config"],
    )
    # [END howto_operator_sagemaker_tuning]

    # SageMakerTuningOperator waits by default, setting as False to test the Sensor below.
    tune_model.wait_for_completion = False

    # [START howto_sensor_sagemaker_tuning]
    await_tuning = SageMakerTuningSensor(
        task_id="await_tuning",
        job_name=test_setup["tuning_job_name"],
    )
    # [END howto_sensor_sagemaker_tuning]

    # [START howto_operator_sagemaker_transform]
    test_model = SageMakerTransformOperator(
        task_id="test_model",
        config=test_setup["transform_config"],
    )
    # [END howto_operator_sagemaker_transform]

    # SageMakerTransformOperator waits by default, setting as False to test the Sensor below.
    test_model.wait_for_completion = False

    # [START howto_sensor_sagemaker_transform]
    await_transform = SageMakerTransformSensor(
        task_id="await_transform",
        job_name=test_setup["transform_job_name"],
    )
    # [END howto_sensor_sagemaker_transform]

    # [START howto_operator_sagemaker_delete_model]
    delete_model = SageMakerDeleteModelOperator(
        task_id="delete_model",
        config={"ModelName": test_setup["model_name"]},
    )
    # [END howto_operator_sagemaker_delete_model]
    delete_model.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=test_setup["bucket_name"],
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        test_setup,
        create_bucket,
        upload_dataset,
        # TEST BODY
        start_pipeline1,
        start_pipeline2,
        stop_pipeline1,
        await_pipeline2,
        preprocess_raw_data,
        train_model,
        await_training,
        create_model,
        register_model,
        tune_model,
        await_tuning,
        test_model,
        await_transform,
        # TEST TEARDOWN
        delete_ecr_repository(test_setup["ecr_repository_name"]),
        delete_model_group(test_setup["model_package_group_name"], register_model.output),
        delete_model,
        delete_bucket,
        delete_logs(test_context[ENV_ID_KEY]),
        delete_pipeline(test_setup["pipeline_name"]),
        delete_docker_image(test_setup["docker_image"]),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
