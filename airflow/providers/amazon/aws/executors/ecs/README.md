<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# AWS ECS Executor

This is an Airflow executor powered by Amazon Elastic Container Service (ECS). Each task that Airflow schedules for execution is run within its own ECS container. Some benefits of an executor like this include:

1. Task isolation: No task can be a noisy neighbor for another. Resources like CPU, memory and disk are isolated to each individual task. Any actions or failures which affect networking or fail the entire container only affect the single task running in it. No single user can overload the environment by triggering too many tasks, because there are no shared workers.
2. Customized environments: You can build different container images which incorporate specific dependencies (such as system level dependencies), binaries, or data required for a task to run.
3. Cost effective: Compute resources only exist for the lifetime of the Airflow task itself. This saves costs by not requiring persistent/long lived workers ready at all times, which also need maintenance and patching.

For a quick start guide please see [here](Setup_guide.md), it will get you up and running with a basic configuration.

The below sections provide more generic details about configuration, the provided example Dockerfile and logging.

## Config Options

There are a number of configuration options available, which can either be set directly in the airflow.cfg
file under an "aws_ecs_executor" section or via environment variables using the `AIRFLOW__AWS_ECS_EXECUTOR__<OPTION_NAME>`
format, for example `AIRFLOW__AWS_ECS_EXECUTOR__CONTAINER_NAME = "myEcsContainer"`.  For more information
on how to set these options, see [Setting Configuration Options](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html)

In the case of conflicts, the order of precedence is:

1. Load default values for options which have defaults.
2. Load any values provided in the RUN_TASK_KWARGS option if one is provided.
3. Load any values explicitly provided through airflow.cfg or environment variables. These are checked with Airflow's config precedence.

### Required config options:

- CLUSTER - Name of the Amazon ECS Cluster. Required.
- CONTAINER_NAME - Name of the container that will be used to execute Airflow tasks via the ECS executor.
The container should be specified in the ECS Task Definition. Required.
- REGION - The name of the AWS Region where Amazon ECS is configured. Required.

### Optional config options:

- ASSIGN_PUBLIC_IP - "Whether to assign a public IP address to the containers launched by the ECS executor. Defaults to "False".
- CONN_ID - The Airflow connection (i.e. credentials) used by the ECS executor to make API calls to AWS ECS. Defaults to "aws_default".
- LAUNCH_TYPE - Launch type can either be 'FARGATE' OR 'EC2'.  Defaults to "FARGATE".
- PLATFORM_VERSION - The platform version the ECS task uses if the FARGATE launch type is used. Defaults to "LATEST".
- RUN_TASK_KWARGS - A JSON string containing arguments to provide the ECS `run_task` API.
- SECURITY_GROUPS - Up to 5 comma-seperated security group IDs associated with the ECS task. Defaults to the VPC default.
- SUBNETS - Up to 16 comma-separated subnet IDs associated with the ECS task or service. Defaults to the VPC default.
- TASK_DEFINITION - The family and revision (family:revision) or full ARN of the ECS task definition to run. Defaults to the latest ACTIVE revision.

For a more detailed description of available options, including type hints and examples, see the `config_templates` folder in the Amazon provider package.

## Dockerfile for ECS Executor

An example Dockerfile can be found [here](Dockerfile#), it creates an image that can be used on an ECS container to run Airflow tasks using the AWS ECS Executor in Apache Airflow. The image
supports AWS CLI/API integration, allowing you to interact with AWS services within your Airflow environment. It also includes options to load DAGs (Directed Acyclic Graphs) from either an S3 bucket or a local folder.

### Base Image

The Docker image is built upon the `apache/airflow:latest` image. See [here](https://hub.docker.com/r/apache/airflow) for more information about the image.

Important note: The python version in this image must match the python version on the host/container which is running the Airflow scheduler process (which in turn runs the executor). The python version of the image can be verified by running the container, and printing the python version as follows:

```
docker run <image_name> python --version
```

Ensure that this version matches the python version of the host/container which is running the Airflow scheduler process (and thus, the ECS executor.) Apache Airflow images with specific python versions can be downloaded from the Dockerhub registry, and filtering tags by the [python version](https://hub.docker.com/r/apache/airflow/tags?page=1&name=3.8). For example, the tag `latest-python3.8` specifies that the image will have python 3.8 installed.

### Prerequisites

Docker must be installed on your system. Instructions for installing Docker can be found [here](https://docs.docker.com/get-docker/).

### AWS Credentials

The [AWS CLI](https://aws.amazon.com/cli/) is installed within the container, and there are multiple ways to pass AWS authentication information to the container. This guide will cover 2 methods.

The first method is to use the build-time arguments (`aws_access_key_id`, `aws_secret_access_key`, `aws_default_region`, and `aws_session_token`).
To pass AWS authentication information using these arguments, use the `--build-arg` option during the Docker build process. For example:

```
docker build -t my-airflow-image \
 --build-arg aws_access_key_id=YOUR_ACCESS_KEY \
 --build-arg aws_secret_access_key=YOUR_SECRET_KEY \
 --build-arg aws_default_region=YOUR_DEFAULT_REGION \
 --build-arg aws_session_token=YOUR_SESSION_TOKEN .
```

Replace `YOUR_ACCESS_KEY`, `YOUR_SECRET_KEY`, `YOUR_SESSION_TOKEN`, and `YOUR_DEFAULT_REGION` with valid AWS credentials.

Alternatively, you can authenticate to AWS using the `~/.aws` folder. See instructions on how to generate this folder [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html). Uncomment the line in the Dockerfile to copy the `./.aws` folder from your host machine to the container's `/home/airflow/.aws` directory. Keep in mind the Docker build context when copying the `.aws` folder to the container.

### Loading DAGs

There are many ways to load DAGs on the ECS container. This Dockerfile is preconfigured with two possible ways: copying from a local folder, or downloading from an S3 bucket. Other methods of loading DAGs are possible as well.

#### From S3 Bucket

To load DAGs from an S3 bucket, uncomment the entrypoint line in the Dockerfile to synchronize the DAGs from the specified S3 bucket to the `/opt/airflow/dags` directory inside the container. You can optionally provide `container_dag_path` as a build argument if you want to store the DAGs in a directory other than `/opt/airflow/dags`.

Add `--build-arg s3_url=YOUR_S3_URL` in the docker build command.
Replace `YOUR_S3_URL` with the URL of your S3 bucket. Make sure you have the appropriate permissions to read from the bucket.

Note that the following command is also passing in AWS credentials as build arguments.

```
docker build -t my-airflow-image \
 --build-arg aws_access_key_id=YOUR_ACCESS_KEY \
 --build-arg aws_secret_access_key=YOUR_SECRET_KEY \
 --build-arg aws_default_region=YOUR_DEFAULT_REGION \
 --build-arg aws_session_token=YOUR_SESSION_TOKEN \
 --build-arg s3_url=YOUR_S3_URL -t my-airflow-image .
```


#### From Local Folder

To load DAGs from a local folder, place your DAG files in a folder within the docker build context on your host machine, and provide the location of the folder using the `host_dag_path` build argument. By default, the DAGs will be copied to `/opt/airflow/dags`, but this can be changed by passing the `container_dag_path` build-time argument during the Docker build process:

```
docker build -t my-airflow-image --build-arg host_dag_path=./dags_on_host --build-arg container_dag_path=/path/on/container .
```

If choosing to load DAGs onto a different path than `/opt/airflow/dags`, then the new path will need to be updated in the Airflow config.

#### Mounting a Volume

You can optionally mount a local directory as a volume on the container during run-time. This will allow you to make change to files on the mounted directory, and have those changes be reflected in the container. To do this, run the following command:

```
docker run --volume /abs/path/to/local/dir:/abs/path/to/remote/dir <image_name>
```

Note: Doing this will overwrite the contents of the directory on the container with the contents of the local directory.

### Building Image for ECS Executor

Detailed instructions on how to use the Docker image, that you have created via this readme, with the ECS Executor can be found [here](link_to_how_to_guide).


## Logging

Airflow tasks executed via this executor run in ECS containers within the configured VPC. This means that logs are not directly accessible to the Airflow Webserver and when containers are stopped, after task completion, the logs would be permanently lost.

Remote logging can be employed when using the ECS executor to persist your Airflow Task logs and make them viewable from the Airflow Webserver.

### Configuring Remote Logging

There are many ways to configure remote logging and several supported destinations. A general overview of Airflow Task logging can be found [here](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html). Instructions for configuring S3 remote logging can be found [here](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/s3-task-handler.html) and Cloudwatch remote logging [here](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/cloud-watch-task-handlers.html).
Some important things to point out for remote logging in the context of the ECS executor:

 - The configuration options for Airflow remote logging must be configured on the host running the Airflow Webserver (so that it can fetch logs from the remote location) as well as within the ECS container running the Airflow Tasks (so that it can upload the logs to the remote location). See [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html) to read more about how to set Airflow configuration via config file or environment variable exports.
 - Adding the Airflow remote logging config to the container can be done in many ways. Some examples include, but are not limited to:
    - Exported as environment variables directly in the Dockerfile (see the [Dockerfile Section above](#dockerfile-for-ecs-executor))
    - Updating the `airflow.cfg` file or copy/mounting/downloading a custom `ariflow.cfg` in the Dockerfile.
    - Added in the ECS Task Definition in plain text or via [Secrets/System Manager](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/secrets-envvar.html)
    - Or, using [ECS Task Environment Files](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/taskdef-envfiles.html)
 - You must have credentials configured within the container to be able to interact with the remote service for your logs (e.g. S3, CloudWatch Logs, etc). This can be done in many ways. Some examples include, but are not limited to:
    - Export credentials into the Dockerfile directly (see the [Dockerfile Section above](#dockerfile-for-ecs-executor))
    - Configure an Airflow Connection and provide this as the [remote logging conn id](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#remote-log-conn-id) (exported into the container by any of the means listed above or your preferred method). Airflow will then use these credentials _specifically_ for interacting with your chosen remote logging destination.

### A Note on ECS Task Logging

ECS can be configured to use the awslogs log driver to send log information to CloudWatch Logs for the ECS Tasks themselves. These logs will include the Airflow Task Operator logging and any other logging that occurs throughout the life of the process running in the container (in this case the Airflow CLI command `airflow tasks run ...`). This can be helpful for debugging issues with remote logging or while testing remote logging configuration.  Information on enabling this logging can be found [here](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html).

***Note: These logs will _not_ be viewable from the Airflow Webserver UI.***
