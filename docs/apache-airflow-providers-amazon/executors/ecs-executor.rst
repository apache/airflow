.. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


.. |executorName| replace:: ECS
.. |dockerfileLink| replace:: `here <https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/executors/Dockerfile>`__
.. |configKwargs| replace:: SUBMIT_JOB_KWARGS

================
AWS ECS Executor
================

This is an Airflow executor powered by Amazon Elastic Container Service
(ECS). Each task that Airflow schedules for execution is run within its
own ECS container. Some benefits of an executor like this include:

1. Task isolation: No task can be a noisy neighbor for another.
   Resources like CPU, memory and disk are isolated to each individual
   task. Any actions or failures which affect networking or fail the
   entire container only affect the single task running in it. No single
   user can overload the environment by triggering too many tasks,
   because there are no shared workers.
2. Customized environments: You can build different container images
   which incorporate specific dependencies (such as system level
   dependencies), binaries, or data required for a task to run.
3. Cost effective: Compute resources only exist for the lifetime of the
   Airflow task itself. This saves costs by not requiring
   persistent/long lived workers ready at all times, which also need
   maintenance and patching.

For a quick start guide please see :ref:`here <setup_guide>`, it will
get you up and running with a basic configuration.

The below sections provide more generic details about configuration, the
provided example Dockerfile and logging.

.. _config-options:

Config Options
--------------

There are a number of configuration options available, which can either
be set directly in the airflow.cfg file under an "aws_ecs_executor"
section or via environment variables using the
``AIRFLOW__AWS_ECS_EXECUTOR__<OPTION_NAME>`` format, for example
``AIRFLOW__AWS_ECS_EXECUTOR__CONTAINER_NAME = "myEcsContainer"``. For
more information on how to set these options, see `Setting Configuration
Options <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html>`__

.. note::
   Configuration options must be consistent across all the hosts/environments running the Airflow components (Scheduler, Webserver, ECS Task containers, etc). See `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__ for more details on setting configurations.

In the case of conflicts, the order of precedence from lowest to highest is:

1. Load default values for options which have defaults.
2. Load any values explicitly provided through airflow.cfg or
   environment variables. These are checked with Airflow's config
   precedence.
3. Load any values provided in the RUN_TASK_KWARGS option if one is
   provided.

.. note::
   ``exec_config`` is an optional parameter that can be provided to operators. It is a dictionary type and in the context of the ECS Executor it represents a ``run_task_kwargs`` configuration which is then updated over-top of the ``run_task_kwargs`` specified in Airflow config above (if present). It is a recursive update which essentially applies Python update to each nested dictionary in the configuration. Loosely approximated as: ``run_task_kwargs.update(exec_config)``

Required config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  CLUSTER - Name of the Amazon ECS Cluster. Required.
-  CONTAINER_NAME - Name of the container that will be used to execute
   Airflow tasks via the ECS executor. The container should be specified
   in the ECS Task Definition. Required.
-  REGION_NAME - The name of the AWS Region where Amazon ECS is configured.
   Required.

Optional config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  ASSIGN_PUBLIC_IP - Whether to assign a public IP address to the
   containers launched by the ECS executor. Defaults to "False".
-  AWS_CONN_ID - The Airflow connection (i.e. credentials) used by the ECS
   executor to make API calls to AWS ECS. Defaults to "aws_default".
-  LAUNCH_TYPE - Launch type can either be 'FARGATE' OR 'EC2'. Defaults
   to "FARGATE".
-  PLATFORM_VERSION - The platform version the ECS task uses if the
   FARGATE launch type is used. Defaults to "LATEST".
-  RUN_TASK_KWARGS - A JSON string containing arguments to provide the
   ECS ``run_task`` API.
-  SECURITY_GROUPS - Up to 5 comma-separated security group IDs
   associated with the ECS task. Defaults to the VPC default.
-  SUBNETS - Up to 16 comma-separated subnet IDs associated with the ECS
   task or service. Defaults to the VPC default.
-  TASK_DEFINITION - The family and revision (family:revision) or full
   ARN of the ECS task definition to run. Defaults to the latest ACTIVE
   revision.
-  MAX_RUN_TASK_ATTEMPTS - The maximum number of times the Ecs Executor
   should attempt to run a task. This refers to instances where the task
   fails to start (i.e. ECS API failures, container failures etc.)
-  CHECK_HEALTH_ON_STARTUP - Whether or not to check the ECS Executor
   health on startup

For a more detailed description of available options, including type
hints and examples, see the ``config_templates`` folder in the Amazon
provider package.

.. note::
   ``exec_config`` is an optional parameter that can be provided to operators. It is a dictionary type and in the context of the ECS Executor it represents a ``run_task_kwargs`` configuration which is then updated over-top of the ``run_task_kwargs`` specified in Airflow config above (if present). It is a recursive update which essentially applies Python update to each nested dictionary in the configuration. Loosely approximated as: ``run_task_kwargs.update(exec_config)``

.. _dockerfile_for_ecs_executor:

.. include:: general.rst
  :start-after: .. BEGIN DOCKERFILE
  :end-before: .. END DOCKERFILE


The most secure method is to use IAM roles. When creating an ECS Task
Definition, you are able to select a Task Role and a Task Execution
Role. The Task Execution Role is the role that is used by the container
agent to make AWS API requests on your behalf. For the purposes of the
ECS Executor, this role needs to have at least the
``AmazonECSTaskExecutionRolePolicy`` as well as the
``CloudWatchLogsFullAccess`` (or ``CloudWatchLogsFullAccessV2``) policies. The Task Role is the role that is
used by the containers to make AWS API requests. This role needs to have
permissions based on the tasks that are described in the DAG being run.
If you are loading DAGs via an S3 bucket, this role needs to have
permission to read the S3 bucket.

To create a new Task Role or Task Execution Role, follow the steps
below:

1. Navigate to the IAM page on the AWS console, and from the left hand
   tab, under Access Management, select Roles.
2. On the Roles page, click Create role on the top right hand corner.
3. Under Trusted entity type, select AWS Service.
4. Select Elastic Container Service from the drop down under Use case,
   and Elastic Container Service Task as the specific use case. Click
   Next.
5. In the Permissions page, select the permissions the role will need,
   depending on whether the role is a Task Role or a Task Execution
   Role. Click Next after selecting all the required permissions.
6. Enter a name for the new role, and an optional description. Review
   the Trusted Entities, and the permissions for the role. Add any tags
   as necessary, and click Create role.

When creating the Task Definition for the ECS cluster (see the :ref:`setup guide <setup_guide>` for more details), select the appropriate
newly created Task Role and Task Execution role for the Task Definition.

.. include:: general.rst
  :start-after: .. BEGIN DOCKERFILE_AUTH_SECOND_METHOD
  :end-before: .. END DOCKERFILE_AUTH_SECOND_METHOD

.. _logging:

.. include:: general.rst
  :start-after: .. BEGIN LOGGING
  :end-before: .. END LOGGING

-  The configuration options for Airflow remote logging should be
   configured on all hosts and containers running Airflow. For example
   the Webserver requires this config so that it can fetch logs from
   the remote location and the ECS container requires the config so that
   it can upload the logs to the remote location. See
   `here <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html>`__
   to read more about how to set Airflow configuration via config file
   or environment variable exports.
-  Adding the Airflow remote logging config to the container can be done
   in many ways. Some examples include, but are not limited to:

   -  Exported as environment variables directly in the Dockerfile (see
      the Dockerfile section :ref:`above <dockerfile_for_ecs_executor>`)
   -  Updating the ``airflow.cfg`` file or copy/mounting/downloading a
      custom ``airflow.cfg`` in the Dockerfile.
   -  Added in the ECS Task Definition in plain text or via
      `Secrets/System
      Manager <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/secrets-envvar.html>`__
   -  Or, using `ECS Task Environment
      Files <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/taskdef-envfiles.html>`__

-  You must have credentials configured within the container to be able
   to interact with the remote service for your logs (e.g. S3,
   CloudWatch Logs, etc). This can be done in many ways. Some examples
   include, but are not limited to:

   -  Export credentials into the Dockerfile directly (see the
      Dockerfile section :ref:`above <dockerfile_for_ecs_executor>`)
   -  Configure an Airflow Connection and provide this as the `remote
      logging conn
      id <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#remote-log-conn-id>`__
      (exported into the container by any of the means listed above or
      your preferred method). Airflow will then use these credentials
      *specifically* for interacting with your chosen remote logging
      destination.

.. note::
   Configuration options must be consistent across all the hosts/environments running the Airflow components (Scheduler, Webserver, ECS Task containers, etc). See `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__ for more details on setting configurations.

ECS Task Logging
~~~~~~~~~~~~~~~~

ECS can be configured to use the awslogs log driver to send log
information to CloudWatch Logs for the ECS Tasks themselves. These logs
will include the Airflow Task Operator logging and any other logging
that occurs throughout the life of the process running in the container
(in this case the Airflow CLI command ``airflow tasks run ...``). This
can be helpful for debugging issues with remote logging or while testing
remote logging configuration. Information on enabling this logging can
be found
`here <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html>`__.

**Note: These logs will NOT be viewable from the Airflow Webserver UI.**

Performance and Tuning
~~~~~~~~~~~~~~~~~~~~~~

While the ECS executor adds about 50-60 seconds of latency to each
Airflow task execution, due to container startup time, it allows for a
higher degree of parallelism and isolation. We have tested this executor
with over 1,000 tasks scheduled in parallel and observed that up to 500
tasks could be run in parallel simultaneously. The limit of 500 tasks is
in accordance with `ECS Service
Quotas <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-quotas.html>`__.

When running this executor, and Airflow generally, at a large scale
there are some configuration options to take into consideration. Many of
the below configurations will either limit how many tasks can run
concurrently or the performance of the scheduler.

-  `core.max_active_tasks_per_dag <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#max-active-tasks-per-dag>`__
-  `core.max_active_runs_per_dag <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#max-active-runs-per-dag>`__
-  `core.parallelism <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#parallelism>`__
-  `scheduler.max_tis_per_query <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#max-tis-per-query>`__
-  `default_pool_task_slot_count <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#default-pool-task-slot-count>`__
-  `scheduler_health_check_threshold <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#scheduler-health-check-threshold>`__

.. _setup_guide:


Setting up an ECS Executor for Apache Airflow
---------------------------------------------

There are 3 steps involved in getting an ECS Executor to work in Apache Airflow:

1. Creating a database that Airflow and the tasks running in ECS can connect to.

2. Creating and configuring an ECS Cluster that can run tasks from Airflow.

3. Configuring Airflow to use the ECS Executor and the database.

There are different options for selecting a database backend. See `here <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html>`_ for more information about the different options supported by Airflow. The following guide will explain how to set up a PostgreSQL RDS Instance on AWS. The guide will also cover setting up an ECS cluster. The ECS Executor supports various launch types, but this guide will explain how to set up an ECS Fargate cluster.

.. include:: general.rst
  :start-after: .. BEGIN DATABASE_CONNECTION
  :end-before: .. END DATABASE_CONNECTION


Creating an ECS Cluster with Fargate, and Task Definitions
----------------------------------------------------------

In order to create a Task Definition for the ECS Cluster that will work with Apache Airflow, you will need a Docker image that is properly configured. See the :ref:`Dockerfile <dockerfile_for_ecs_executor>` section for instructions on how to do that.

Once the image is built, it needs to be put in a repository where it can be pulled by ECS. There are multiple ways to accomplish this. This guide will go over doing this using Amazon Elastic Container Registry (ECR).

.. include:: general.rst
  :start-after: .. BEGIN ECR_STEPS
  :end-before: .. END ECR_STEPS

Create ECS Cluster
~~~~~~~~~~~~~~~~~~

1. Log in to your AWS Management Console and navigate to the Amazon Elastic Container Service.

2. Click "Clusters" then click "Create Cluster".

3. Make sure that AWS Fargate (Serverless) is selected under Infrastructure.

4. Select other options as required and click Create to create the cluster.

Create Task Definition
~~~~~~~~~~~~~~~~~~~~~~

1. Click on Task Definitions on the left hand bar, and click Create new task definition.

2. Choose the Task Definition Family name. Select AWS Fargate for the Launch Type.

3. Select or create the Task Role and Task Execution Role, and ensure the roles have the required permissions to accomplish their respective tasks. You can choose to create a new Task Execution role that will have the basic minimum permissions in order for the task to run.

4. Select a name for the container, and use the image URI of the image that was pushed in the previous section. Make sure the role being used has the required permissions to pull the image.

5. Add the following environment variables to the container:

- ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``, with the value being the PostgreSQL connection string in the following format using the values set during the `Database section <#create-the-rds-db-instance>`_ above:

.. code-block:: bash

   postgresql+psycopg2://<username>:<password>@<endpoint>/<database_name>


- ``AIRFLOW__ECS_EXECUTOR__SECURITY_GROUPS``, with the value being a comma separated list of security group IDs associated with the VPC used for the RDS instance.

- ``AIRFLOW__ECS_EXECUTOR__SUBNETS``, with the value being a comma separated list of subnet IDs of the subnets associated with the RDS instance.

1. Add other configuration as necessary for Airflow generally (see `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__), the ECS executor (see :ref:`here <config-options>`) or for remote logging (see :ref:`here <logging>`). Note that any configuration changes should be made across the entire Airflow environment to keep configuration consistent.

2. Click Create.

Allow ECS Containers to Access RDS Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As a final step, access to the database must be configured for the ECS containers. Many different networking configurations are possible, but one possible approach is:

1. Log in to your AWS Management Console and navigate to the VPC Dashboard.

2. On the left hand, under the Security heading, click Security groups.

3. Select the security group associated with your RDS instance, and click Edit inbound rules.

4. Add a new rule that allows PostgreSQL type traffic to the CIDR of the subnet(s) associated with the Ecs cluster.

Configure Airflow
~~~~~~~~~~~~~~~~~

To configure Airflow to utilize the ECS Executor and leverage the resources we've set up, create a script (e.g., ``ecs_executor_config.sh``) with the following contents:

.. code-block:: bash

   export AIRFLOW__CORE__EXECUTOR='airflow.providers.amazon.aws.executors.ecs.ecs_executor.AwsEcsExecutor'

   export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=<postgres-connection-string>

   export AIRFLOW__AWS_ECS_EXECUTOR__REGION_NAME=<executor-region>

   export AIRFLOW__AWS_ECS_EXECUTOR__CLUSTER=<ecs-cluster-name>

   export AIRFLOW__AWS_ECS_EXECUTOR__CONTAINER_NAME=<ecs-container-name>

   export AIRFLOW__AWS_ECS_EXECUTOR__TASK_DEFINITION=<task-definition-name>

   export AIRFLOW__AWS_ECS_EXECUTOR__LAUNCH_TYPE='FARGATE'

   export AIRFLOW__AWS_ECS_EXECUTOR__PLATFORM_VERSION='LATEST'

   export AIRFLOW__AWS_ECS_EXECUTOR__ASSIGN_PUBLIC_IP='True'

   export AIRFLOW__AWS_ECS_EXECUTOR__SECURITY_GROUPS=<security-group-id-for-rds>

   export AIRFLOW__AWS_ECS_EXECUTOR__SUBNETS=<subnet-id-for-rds>


.. include:: general.rst
  :start-after: .. BEGIN INIT_DB
  :end-before: .. END INIT_DB
