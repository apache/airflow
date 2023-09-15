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

# Setting up an ECS Executor for Apache Airflow

There are 3 steps involved in getting an ECS Executor to work in Apache Airflow:

1. Creating a database that Airflow and the tasks running in ECS can connect to.
2. Creating and configuring an ECS Cluster that can run tasks from Airflow.
3. Configuring Airflow to use the ECS Executor and the database.

There are different options for selecting a database backend. See [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html) for more information about the different options supported by Airflow. The following guide will explain how to set up a PostgreSQL RDS Instance on AWS. The guide will also cover setting up an ECS cluster. The ECS Executor supports various launch types, but this guide will explain how to set up an ECS Fargate cluster.

## Setting up an RDS DB Instance for ECS Executors

### Create the RDS DB Instance

1. Log in to your AWS Management Console and navigate to the RDS service.
2. Click "Create database" to start creating a new RDS instance.
3. Choose the "Standard create" option, and select PostreSQL.
4. Select the appropriate template, availability and durability.
   - NOTE: At the time of this writing, the "Multi-AZ DB **Cluster**" option does not support setting the database name, which is a required step below.
5. Set the DB Instance name, the username and password.
7. Choose the instance configuration, and storage parameters.
8. In the Connectivity section, select Don't connect to an EC2 compute resource
9. Select or create a VPC and subnet, and allow public access to the DB. Select or create security group and select the Availability Zone.
10. Open the Additional Configuration tab and set the database name to `airflow_db`.
11. Select other settings as required, and create the database by clicking Create database.


### Test Connectivity

In order to be able to connect to the new RDS instance, you need to allow inbound traffic to the database from your IP address.


1. Under the "Security" heading in the "Connectivity & security" tab of the RDS instance, find the link to the VPC security group for your new RDS DB instance.
2. Create an inbound rule that allows traffic from your IP address(es) on TCP port 5432 (PostgreSQL).

3. Confirm that you can connect to the DB after modifying the security group. This will require having `psql` installed. Instructions for installing `psql` can be found [here](https://www.postgresql.org/download/).

**NOTE**: Be sure that the status of your DB is Available before testing connectivity

```
psql -h <endpoint> -p 5432 -U <username> <db_name>
```

The endpoint can be found on the "Connectivity and Security" tab, the username (and password) are the credentials used when creating the database.
The db_name should be `airflow_db` (unless a different one was used when creating the database.)

You will be prompted to enter the password if the connection is successful.


## Creating an ECS Cluster with Fargate, and Task Definitions

In order to create a Task Definition for the ECS Cluster that will work with Apache Airflow, you will need a Docker image that is properly configured. See the [Dockerfile](README.md#dockerfile-for-ecs-executor) section for instructions on how to do that.

Once the image is built, it needs to be put in a repository where it can be pulled by ECS. There are multiple ways to accomplish this. This guide will go over doing this using Amazon Elastic Container Registry (ECR).

### Create an ECR Repository

1. Log in to your AWS Management Console and navigate to the ECR service.
2. Click Create repository.
3. Name the repository and fill out other information as required.
4. Click Create Repository.
5. Once the repository has been created, click on the repository. Click on the "View push commands" button on the top right.
6. Follow the instructions to push the Docker image, replacing image names as appropriate. Ensure the image is uploaded by refreshing the page once the image is pushed.

### Create ECS Cluster

1. Log in to your AWS Management Console and navigate to the Amazon Elastic Container Service.
2. Click "Clusters" then click "Create Cluster".
3. Make sure that AWS Fargate (Serverless) is selected under Infrastructure.
4. Select other options as required and click Create to create the cluster.

### Create Task Definition

1. Click on Task Definitions on the left hand bar, and click Create new task definition.
2. Choose the Task Definition Family name. Select AWS Fargate for the Launch Type.
3. Select or create the Task Role and Task Execution Role, and ensure the roles have the required permissions to accomplish their respective tasks. You can choose to create a new Task Execution role that will have the basic minimum permissions in order for the task to run.
4. Select a name for the container, and use the image URI of the image that was pushed in the previous section. Make sure the role being used has the required permissions to pull the image.
5. Add the following environment variables to the container:

 - `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, with the value being the PostgreSQL connection string in the following format using the values set during the [Database section](#create-the-rds-db-instance) above:

```
postgresql+psycopg2://<username>:<password>@<endpoint>/<database_name>
```

 - `AIRFLOW__ECS_EXECUTOR__SECURITY_GROUPS`, with the value being a comma separated list of security group IDs associated with the VPC used for the RDS instance.
 - `AIRFLOW__ECS_EXECUTOR__SUBNETS`, with the value being a comma separated list of subnet IDs of the subnets associated with the RDS instance.

6. Add other configuration as necessary for Airflow generally ([see here](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)), the ECS executor ([see here](README.md#config-options)) or for remote logging ([see here](README.md#logging)).
7. Click Create.

### Allow ECS Containers to Access RDS Database

As a final step, access to the database must be configured for the ECS containers. Many different networking configurations are possible, but one possible approach is:

1. Log in to your AWS Management Console and navigate to the VPC Dashboard.
2. On the left hand, under the Security heading, click Security groups.
3. Select the security group associated with your RDS instance, and click Edit inbound rules.
4. Add a new rule that allows PostgreSQL type traffic to the CIDR of the subnet(s) associated with the DB.

## Configure Airflow

To configure Airflow to utilize the ECS Executor and leverage the resources we've set up, create a script (e.g., `ecs_executor_config.sh`) with the following contents:

```
export AIRFLOW__CORE__EXECUTOR='airflow.providers.amazon.aws.executors.ecs.AwsEcsExecutor'
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=<postgres-connection-string>
export AIRFLOW__AWS_ECS_EXECUTOR__REGION=<executor-region>
export AIRFLOW__AWS_ECS_EXECUTOR__CLUSTER=<ecs-cluster-name>
export AIRFLOW__AWS_ECS_EXECUTOR__CONTAINER_NAME=<ecs-container-name>
export AIRFLOW__AWS_ECS_EXECUTOR__TASK_DEFINITION=<task-definition-name>
export AIRFLOW__AWS_ECS_EXECUTOR__LAUNCH_TYPE='FARGATE'
export AIRFLOW__AWS_ECS_EXECUTOR__PLATFORM_VERSION='LATEST'
export AIRFLOW__AWS_ECS_EXECUTOR__ASSIGN_PUBLIC_IP='True'
export AIRFLOW__AWS_ECS_EXECUTOR__SECURITY_GROUPS=<security-group-id-for-rds>
export AIRFLOW__AWS_ECS_EXECUTOR__SUBNETS=<subnet-id-for-rds>
```

This script should be run on the host(s) running the Airflow Scheduler and Webserver, before those processes are started.

The script sets environment variables that configure Airflow to use the ECS Executor and provide necessary information for task execution.

### Initialize the Airflow DB

The Airflow DB needs to be initialized before it can be used and a user needs to be added for you to log in. The below command adds an admin user (the command will also initialize the DB if it hasn't been already):

```
airflow users create --username admin --password admin --firstname <your first name> --lastname <your last name> --email <your email> --role Admin
```
