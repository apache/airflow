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

Some overview TBD

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

## Dockerfile and Image Building

Contents TBD from Syed

## Logging

Airflow tasks executed via this executor run in ECS containers within the configured VPC. This means that logs are not directly accessible to the Airflow Webserver and when containers are stopped, after task completion, the logs would be permanently lost.

Remote logging can be employed when using the ECS executor to persist your Airflow Task logs and make them viewable from the Airflow Webserver.

### Configuring Remote Logging

There are many ways to configure remote logging and several supported destinations. A general overview of Airflow Task logging can be found [here](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html). Instructions for configuring S3 remote logging can be found [here](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/s3-task-handler.html) and Cloudwatch remote logging [here](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/cloud-watch-task-handlers.html).
Some important things to point out for remote logging in the context of the ECS executor:

 - The configuration options for Airflow remote logging must be configured on the host running the Airflow Webserver (so that it can fetch logs from the remote location) as well as within the ECS container running the Airflow Tasks (so that it can upload the logs to the remote location). See [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html) to read more about how to set Airflow configuration via config file or environment variable exports.
 - Adding the Airflow remote logging config to the container can be done in many ways. Some examples include, but are not limited to:
    - Exported as environment variables directly in the Dockerfile (see the [Dockerfile Section above](#dockerfile-and-image-building))
    - Updating the `airflow.cfg` file or copy/mounting/downloading a custom `ariflow.cfg` in the Dockerfile.
    - Added in the ECS Task Definition in plain text or via [Secrets/System Manager](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/secrets-envvar.html)
    - Or, using [ECS Task Environment Files](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/taskdef-envfiles.html)
 - You must have credentials configured within the container to be able to interact with the remote service for your logs (e.g. S3, CloudWatch Logs, etc). This can be done in many ways. Some examples include, but are not limited to:
    - Export credentials into the Dockerfile directly (see the [Dockerfile Section above](#dockerfile-and-image-building))
    - Configure an Airflow Connection and provide this as the [remote logging conn id](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#remote-log-conn-id) (exported into the container by any of the means listed above or your preferred method). Airflow will then use these credentials _specifically_ for interacting with your chosen remote logging destination.

## A Note on ECS Task Logging

ECS can be configured to use the awslogs log driver to send log information to CloudWatch Logs for the ECS Tasks themselves. These logs will include the Airflow Task Operator logging and any other logging that occurs throughout the life of the process running in the container (in this case the Airflow CLI command `airflow tasks run ...`). This can be helpful for debugging issues with remote logging or while testing remote logging configuration.  Information on enabling this logging can be found [here](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html).
Note: These logs will _not_ be viewable from the Airflow Webserver UI.
