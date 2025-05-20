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

# Use the official AWS Lambda Python base image.
# If you wish to use a different Python version, please update the line below
FROM public.ecr.aws/lambda/python:3.12

# hadolint ignore=DL3041
RUN dnf -y install unzip \
    && dnf clean all \
    && rm -rf /var/cache/dnf

# Install the AWS CLI.
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf ./aws awscliv2.zip

## Install Airflow and dependencies.
# The most current version of Airflow is installed by default, along with the amazon and postgres
# provider packages.
# If you would like to install a specific version, you can use the requirements.txt to change the
# version along with installing your dependencies or update this Dockerfile to install a specific
# version of Airflow.

# NOTE: If you change the below line, specifically removing the amazon extra, please ensure boto3 is
# installed via another method. Boto3 is required for the Lambda executor to function properly.
# hadolint ignore=SC2102
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir apache-airflow[amazon,postgres]
# /tmp is the only writable directory in Lambda, so we need to set the AIRFLOW_HOME there.
ENV AIRFLOW_HOME=/tmp/airflow
# Dags are read-only, so they can be stored in opt (or another path provided).
ARG container_dag_path=/opt/airflow/dags
ENV AIRFLOW__CORE__DAGS_FOLDER=$container_dag_path
RUN mkdir -p /opt/airflow/dags

# Python dependencies can be installed by providing a requirements.txt.
# If the file is in a different location, use the requirements_path build argument to specify
# the file path.
ARG requirements_path=./requirements.txt
ENV REQUIREMENTS_PATH=$requirements_path
# Uncomment the two lines below to copy the requirements.txt file to the container, and
# install the dependencies.
# COPY --chown=airflow:root $REQUIREMENTS_PATH /opt/airflow/requirements.txt
# RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

## AWS Authentication
# The image requires access to AWS services. This Dockerfile supports 2 ways to authenticate with AWS.
# The first is using build arguments where you can provide the AWS credentials as arguments
# passed when building the image. The other option is to leverage the Lambda execution role. Airflow
# will default to using Boto credential strategy which will look for roles from Lambda, this is the
# preferred approach. See the Lambda Executor Airflow documentation for more details.

# If you would like to use an alternative method of authentication, feel free to make the
# necessary changes to this file.

# Uncomment to use these arguments to provide AWS authentication information if not using the Lambda
# execution role.
#ARG aws_access_key_id
#ARG aws_secret_access_key
#ARG aws_default_region
#ARG aws_session_token

#ENV AWS_ACCESS_KEY_ID=$aws_access_key_id
#ENV AWS_SECRET_ACCESS_KEY=$aws_secret_access_key
#ENV AWS_DEFAULT_REGION=$aws_default_region
#ENV AWS_SESSION_TOKEN=$aws_session_token

## Loading DAGs
# This Dockerfile supports 2 ways to load DAGs onto the container.
# One is to download them from S3 at runtime during the Lambda app invocation. The other
# is to copy the dags into the image at build time, this will make task execution
# much faster, since the images will already be present but the image will need to be rebuilt
# every time the DAGs are updated.
# If you would like to use an alternative method of loading DAGs, feel free to make the
# necessary changes to this file.

ARG host_dag_path=./dags
ENV HOST_DAG_PATH=$host_dag_path
# Uncomment the line below to copy the DAGs from the host to the container.
# COPY $HOST_DAG_PATH $AIRFLOW__CORE__DAGS_FOLDER

# Use these arguments to load DAGs at runtime. If you are using the provided Lambda function (app.py),
# it will check for this environment variable and download the DAGs from S3. See the example app in Lambda
# Executor documentation for details.
ARG s3_uri
ENV S3_URI=$s3_uri

# Copy your Lambda function code into the Docker build directory/context.
COPY app.py ${LAMBDA_TASK_ROOT}/

# Specify the Lambda function handler (update if you provide a different handler with a different name).
CMD ["app.lambda_handler"]
