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

# hadolint ignore=DL3007
FROM apache/airflow:latest
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends unzip \
  # The below helps to keep the image size down
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf ./aws awscliv2.zip

RUN curl "https://raw.githubusercontent.com/apache/airflow/main/scripts/docker/entrypoint_prod.sh" -o "/home/airflow/entrypoint.sh" \
    && chmod +x /home/airflow/entrypoint.sh

# Add a script to run the aws s3 sync command when the container is run
COPY <<"EOF" /install_dags_entrypoint.sh
#!/bin/bash

echo "Downloading DAGs from S3 bucket"
aws s3 sync "$S3_URI" "$CONTAINER_DAG_PATH"

/home/airflow/entrypoint.sh "$@"
EOF

RUN chmod +x /install_dags_entrypoint.sh

USER airflow

## Installing Python Dependencies
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
# passed when building the image. The other option is to leverage the ECS execution role which is automatically
# supported by the container running on ECS. Airflow will default to using Boto credential strategy which will
# look for roles from ECS. See the ECS Executor Airflow documentation for more details.

# If you would like to use an alternative method of authentication, feel free to make the
# necessary changes to this file.

# Use these arguments to provide AWS authentication information
ARG aws_access_key_id
ARG aws_secret_access_key
ARG aws_default_region
ARG aws_session_token

ENV AWS_ACCESS_KEY_ID=$aws_access_key_id
ENV AWS_SECRET_ACCESS_KEY=$aws_secret_access_key
ENV AWS_DEFAULT_REGION=$aws_default_region
ENV AWS_SESSION_TOKEN=$aws_session_token


## Loading DAGs
# This Dockerfile supports 2 ways to load DAGs onto the container.
# One is to upload all the DAGs onto an S3 bucket, and then
# download them onto the container. The other is to copy a local folder with
# the DAGs onto the container.
# If you would like to use an alternative method of loading DAGs, feel free to make the
# necessary changes to this file.

ARG host_dag_path=./dags
ENV HOST_DAG_PATH=$host_dag_path
ARG container_dag_path=/opt/airflow/dags
ENV CONTAINER_DAG_PATH=$container_dag_path
# Set host_dag_path to the path of the DAGs on the host
# COPY --chown=airflow:root $HOST_DAG_PATH $CONTAINER_DAG_PATH


# Use these arguments to load DAGs onto the container from S3
ARG s3_uri
ENV S3_URI=$s3_uri
# If using S3 bucket as source of DAGs, uncommenting the next ENTRYPOINT command will overwrite this one.
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/home/airflow/entrypoint.sh"]

# Uncomment the line if using S3 bucket as the source of DAGs
# ENTRYPOINT ["/usr/bin/dumb-init", "--", "/install_dags_entrypoint.sh"]
